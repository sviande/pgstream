// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

// ddlRewriter rewrites a raw replicated DDL payload before it is applied on the
// target (ENUM->TEXT conversion and table renaming) and returns the statements
// to replay, one entry per statement — the captured DDL is current_query(), so
// it may hold a whole migration file. An empty result means nothing must be
// applied (e.g. CREATE TYPE ... AS ENUM while converting enums to text). It is
// implemented by the schema observer, which owns the live enum tracker.
type ddlRewriter interface {
	RewriteDDLStatements(ddl, commandTag string) []string
}

type ddlAdapter struct {
	rewriter ddlRewriter
}

func newDDLAdapter(rewriter ddlRewriter) *ddlAdapter {
	return &ddlAdapter{rewriter: rewriter}
}

func (a *ddlAdapter) walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error) {
	ddlEvent, err := wal.WalDataToDDLEvent(d)
	if err != nil {
		return nil, err
	}

	statements := []string{ddlEvent.DDL}
	if a.rewriter != nil {
		statements = a.rewriter.RewriteDDLStatements(ddlEvent.DDL, ddlEvent.CommandTag)
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

	// One query per statement: the writer executes them in order and carries on
	// after a failure, so a statement that is already applied on the target does
	// not take the rest of the migration down with it.
	queries := make([]*query, 0, len(statements))
	for _, sql := range statements {
		queries = append(queries, a.newDDLQuery(ddlEvent.SchemaName, tableName, sql))
	}
	return queries, nil
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}
