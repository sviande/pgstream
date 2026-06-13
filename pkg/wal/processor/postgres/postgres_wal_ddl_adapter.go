// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

// ddlRewriter rewrites a raw replicated DDL statement before it is applied on
// the target (ENUM->TEXT conversion and table renaming). It returns skip=true
// when the statement must not be applied (e.g. CREATE TYPE ... AS ENUM while
// converting enums to text). It is implemented by the schema observer, which
// owns the live enum tracker.
type ddlRewriter interface {
	RewriteDDL(ddl, commandTag string) (sql string, skip bool)
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

	sql := ddlEvent.DDL
	if a.rewriter != nil {
		var skip bool
		sql, skip = a.rewriter.RewriteDDL(ddlEvent.DDL, ddlEvent.CommandTag)
		if skip {
			// the statement must not be applied on the target (e.g. enum type DDL)
			return []*query{{}}, nil
		}
	}

	tableName := ""
	tableObjects := ddlEvent.GetTableObjects()
	if len(tableObjects) > 0 {
		tableName = tableObjects[0].GetTable()
	}

	return []*query{
		a.newDDLQuery(ddlEvent.SchemaName, tableName, sql),
	}, nil
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}
