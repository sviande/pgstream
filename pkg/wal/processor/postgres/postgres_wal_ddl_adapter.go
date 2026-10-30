// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/postgres/ddlrewrite"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// ddlRewriter rewrites the statement of a replicated DDL event before it is
// applied on the target (ENUM->TEXT conversion and table renaming) and returns
// the statements to replay. An empty result means nothing must be applied
// (e.g. CREATE TYPE ... AS ENUM while converting enums to text). It is
// implemented by the schema observer, which owns the live enum tracker.
type ddlRewriter interface {
	RewriteDDLStatements(ddl, commandTag string) []string
}

// ddlAdapter turns DDL events into queries. It keeps track of the captured
// query being replayed, so it is not safe for concurrent use: the batch writer
// calls it from its single sending goroutine, in WAL order.
type ddlAdapter struct {
	rewriter  ddlRewriter
	lsnParser replication.LSNParser
	query     capturedQuery
}

// capturedQuery is a query text captured by the DDL events of one transaction,
// with the position of the next statement to replay and the WAL position of
// the last event replayed.
type capturedQuery struct {
	xid        int64
	ddl        string
	statements []string
	next       int
	lsn        replication.LSN
}

var errNoFiringStatement = errors.New("no statement of the captured query matches the DDL event")

func newDDLAdapter(rewriter ddlRewriter) *ddlAdapter {
	return &ddlAdapter{rewriter: rewriter, lsnParser: pgreplication.NewLSNParser()}
}

func (a *ddlAdapter) walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error) {
	ddlEvent, err := wal.WalDataToDDLEvent(d)
	if err != nil {
		return nil, err
	}

	var lsn replication.LSN
	if d.LSN != "" {
		if lsn, err = a.lsnParser.FromString(d.LSN); err != nil {
			return nil, fmt.Errorf("parsing DDL event LSN %q: %w", d.LSN, err)
		}
	}

	stmt, err := a.firingStatement(d.XID, lsn, ddlEvent)
	if err != nil {
		return nil, err
	}

	statements := []string{stmt}
	if a.rewriter != nil {
		statements = a.rewriter.RewriteDDLStatements(stmt, ddlEvent.CommandTag)
		if len(statements) == 0 {
			// nothing to apply on the target (e.g. enum type DDL)
			return []*query{{}}, nil
		}
	}

	tableName := ""
	tableObjects := ddlEvent.GetTableObjects()
	if len(tableObjects) > 0 {
		tableName = tableObjects[0].GetTable()
	}

	queries := make([]*query, 0, len(statements))
	for _, sql := range statements {
		queries = append(queries, a.newDDLQuery(ddlEvent.SchemaName, tableName, sql))
	}
	return queries, nil
}

// firingStatement returns the statement of the captured query that fired the
// DDL event.
//
// The event trigger captures current_query(), the whole text the client sent,
// and fires once per DDL command: a text holding several DDL statements
// arrives once per statement, each time carrying all of them. Replaying the
// whole text on each event ran every DDL statement as many times as the text
// held DDL, ran its data changes on the target on top of the rows the WAL
// brings for them (a backfill inserted twice), and applied the later DDL ahead
// of the rows written in between. Each event therefore replays its own
// statement only, found from where the previous event of the same query
// stopped. When it cannot be told, the event fails rather than replaying a
// guess: DDL run by procedural code (DO, CALL, a function) is not in the
// captured text at all.
//
// The events of a query share their transaction ID and their text. After a
// reconnection the source sends again, from its start, a transaction whose
// events were already replayed: an event at or before the WAL position of the
// last one replayed starts the query over. Without transaction IDs
// (include-xids off), the same text sent again by a later transaction is
// recognised by starting over once its statements run out.
func (a *ddlAdapter) firingStatement(xid int64, lsn replication.LSN, ddlEvent *wal.DDLEvent) (string, error) {
	q := &a.query
	rewound := lsn != 0 && lsn <= q.lsn
	if q.xid != xid || q.ddl != ddlEvent.DDL || rewound {
		*q = capturedQuery{xid: xid, ddl: ddlEvent.DDL, statements: ddlrewrite.SplitStatements(ddlEvent.DDL)}
	}

	identities := make([]string, 0, len(ddlEvent.Objects))
	for _, obj := range ddlEvent.Objects {
		identities = append(identities, obj.Identity)
	}

	i, ok := ddlrewrite.MatchStatement(q.statements, q.next, ddlEvent.CommandTag, identities)
	if !ok && xid == 0 && q.next > 0 {
		i, ok = ddlrewrite.MatchStatement(q.statements, 0, ddlEvent.CommandTag, identities)
	}
	if !ok {
		return "", fmt.Errorf("%w: command tag %q, objects %q, xid %d, captured query: %s",
			errNoFiringStatement, ddlEvent.CommandTag, identities, xid, truncateDDL(ddlEvent.DDL, maxLoggedDDLBytes))
	}
	q.next = i + 1
	q.lsn = lsn
	return q.statements[i], nil
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}
