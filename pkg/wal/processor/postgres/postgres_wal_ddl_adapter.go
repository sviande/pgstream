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
	statements []ddlrewrite.FiringStatement
	next       int
	lsn        replication.LSN
	// extensions holds the statements running an extension script that were
	// replayed already, by index: each is replayed once, however many events
	// its script fired.
	extensions map[int]struct{}
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

	stmt, commandTag, err := a.firingStatement(d.XID, lsn, ddlEvent)
	if err != nil {
		return nil, err
	}
	if stmt == "" {
		// an event of an extension script whose statement was replayed already
		return []*query{{}}, nil
	}

	statements := []string{stmt}
	if a.rewriter != nil {
		statements = a.rewriter.RewriteDDLStatements(stmt, commandTag)
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
// DDL event, with the command tag to rewrite it under. The statement is empty
// when nothing must be replayed for the event.
//
// The event trigger captures current_query(), the whole text the client sent,
// and fires once per DDL command: a text holding several DDL statements
// arrives once per statement, each time carrying all of them. Replaying the
// whole text on each event ran every DDL statement as many times as the text
// held DDL, ran its data changes on the target on top of the rows the WAL
// brings for them (a backfill inserted twice), and applied the later DDL ahead
// of the rows written in between. Each event therefore replays its own
// statement only, found from where the previous event of the same query
// stopped. DDL written in a DO block is found in the body of the block. When
// it cannot be told, the event fails rather than replaying a guess: DDL run by
// a function, a procedure (CALL) or dynamic SQL (EXECUTE) is not in the
// captured text at all, and a statement of a DO block whose code may have
// skipped it is told only when nothing else can have fired the event.
//
// A statement running the script of an extension (CREATE EXTENSION) fires one
// event per command of the script, none of them written in the captured text
// and none naming an object, and no event of its own. An event naming no
// object therefore belongs to such a statement when it is the first one, in
// text order, that can have fired it. The statement is replayed on the first of
// these events, so the target installs the extension itself, at the position
// in the WAL where the source did; the other events replay nothing.
//
// The events of a query share their transaction ID and their text. After a
// reconnection the source sends again, from its start, a transaction whose
// events were already replayed: an event at or before the WAL position of the
// last one replayed starts the query over. Without transaction IDs
// (include-xids off), the same text sent again by a later transaction is
// recognised by starting over once its statements run out.
func (a *ddlAdapter) firingStatement(xid int64, lsn replication.LSN, ddlEvent *wal.DDLEvent) (string, string, error) {
	q := &a.query
	rewound := lsn != 0 && lsn <= q.lsn
	if q.xid != xid || q.ddl != ddlEvent.DDL || rewound {
		*q = capturedQuery{xid: xid, ddl: ddlEvent.DDL, statements: ddlrewrite.SplitFiringStatements(ddlEvent.DDL)}
	}

	identities := make([]string, 0, len(ddlEvent.Objects))
	for _, obj := range ddlEvent.Objects {
		identities = append(identities, obj.Identity)
	}

	i, ok := ddlrewrite.MatchFiringStatement(q.statements, q.next, ddlEvent.CommandTag, identities)
	if !ok && xid == 0 && q.next > 0 {
		i, ok = ddlrewrite.MatchFiringStatement(q.statements, 0, ddlEvent.CommandTag, identities)
	}
	if len(identities) == 0 {
		if e, found := ddlrewrite.ExtensionStatement(q.statements, q.next); found && (!ok || e < i) {
			// stay on the statement: the rest of its script is still to come
			q.next = e
			q.lsn = lsn
			return q.extensionStatement(e)
		}
	}
	if !ok {
		return "", "", fmt.Errorf("%w: command tag %q, objects %q, xid %d, captured query: %s",
			errNoFiringStatement, ddlEvent.CommandTag, identities, xid, truncateDDL(ddlEvent.DDL, maxLoggedDDLBytes))
	}
	q.next = i + 1
	q.lsn = lsn
	if ddlrewrite.IsExtensionStatement(q.statements[i].SQL) {
		return q.extensionStatement(i)
	}
	return q.statements[i].SQL, ddlEvent.CommandTag, nil
}

// extensionStatement returns the statement at index i, which runs the script
// of an extension, with its own command tag (the event carries the tag of a
// command of the script), the first time it is asked for. It returns an empty
// statement afterwards.
func (q *capturedQuery) extensionStatement(i int) (string, string, error) {
	if _, replayed := q.extensions[i]; replayed {
		return "", "", nil
	}
	if q.extensions == nil {
		q.extensions = make(map[int]struct{})
	}
	q.extensions[i] = struct{}{}
	stmt := q.statements[i].SQL
	return stmt, ddlrewrite.StatementTag(stmt), nil
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}
