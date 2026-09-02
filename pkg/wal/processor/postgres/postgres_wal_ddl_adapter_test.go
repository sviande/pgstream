// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestDDLAdapter_walDataToQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		walData *wal.Data

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - CREATE TABLE with table object",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "CREATE TABLE public.test_table (id integer PRIMARY KEY, name text);",
					"schema_name": "public",
					"command_tag": "CREATE TABLE",
					"objects": [
						{
							"type": "table",
							"identity": "public.test_table",
							"schema": "public"
						}
					]
				}`,
			},
			wantQueries: []*query{
				{
					schema: "public",
					table:  "test_table",
					sql:    "CREATE TABLE public.test_table (id integer PRIMARY KEY, name text);",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - ALTER TABLE with table object",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "ALTER TABLE public.test_table ADD COLUMN email text;",
					"schema_name": "public",
					"command_tag": "ALTER TABLE",
					"objects": [
						{
							"type": "table",
							"identity": "public.test_table",
							"schema": "public"
						}
					]
				}`,
			},
			wantQueries: []*query{
				{
					schema: "public",
					table:  "test_table",
					sql:    "ALTER TABLE public.test_table ADD COLUMN email text;",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - DROP TABLE with table object",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "DROP TABLE public.test_table;",
					"schema_name": "public",
					"command_tag": "DROP TABLE",
					"objects": [
						{
							"type": "table",
							"identity": "public.test_table",
							"schema": "public"
						}
					]
				}`,
			},
			wantQueries: []*query{
				{
					schema: "public",
					table:  "test_table",
					sql:    "DROP TABLE public.test_table;",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - CREATE INDEX without table object",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "CREATE INDEX idx_test ON public.test_table(name);",
					"schema_name": "public",
					"command_tag": "CREATE INDEX",
					"objects": []
				}`,
			},
			wantQueries: []*query{
				{
					schema: "public",
					table:  "",
					sql:    "CREATE INDEX idx_test ON public.test_table(name);",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - CREATE SCHEMA without table object",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "CREATE SCHEMA test_schema;",
					"schema_name": "test_schema",
					"command_tag": "CREATE SCHEMA",
					"objects": []
				}`,
			},
			wantQueries: []*query{
				{
					schema: "test_schema",
					table:  "",
					sql:    "CREATE SCHEMA test_schema;",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - GRANT statement",
			walData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
				Content: `{
					"ddl": "GRANT SELECT ON public.test_table TO test_role;",
					"schema_name": "public",
					"command_tag": "GRANT",
					"objects": []
				}`,
			},
			wantQueries: []*query{
				{
					schema: "public",
					table:  "",
					sql:    "GRANT SELECT ON public.test_table TO test_role;",
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - not a DDL event (wrong action)",
			walData: &wal.Data{
				Action:  "I",
				Prefix:  wal.DDLPrefix,
				Content: `{"ddl": "CREATE TABLE test;"}`,
			},
			wantQueries: nil,
			wantErr:     wal.ErrNotDDLEvent,
		},
		{
			name: "error - not a DDL event (wrong prefix)",
			walData: &wal.Data{
				Action:  wal.LogicalMessageAction,
				Prefix:  "some.other.prefix",
				Content: `{"ddl": "CREATE TABLE test;"}`,
			},
			wantQueries: nil,
			wantErr:     wal.ErrNotDDLEvent,
		},
		{
			name: "error - invalid JSON content",
			walData: &wal.Data{
				Action:  wal.LogicalMessageAction,
				Prefix:  wal.DDLPrefix,
				Content: `{invalid json}`,
			},
			wantQueries: nil,
			wantErr:     wal.ErrInvalidDDLEventContent,
		},
		{
			name: "error - empty content",
			walData: &wal.Data{
				Action:  wal.LogicalMessageAction,
				Prefix:  wal.DDLPrefix,
				Content: ``,
			},
			wantQueries: nil,
			wantErr:     wal.ErrInvalidDDLEventContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adapter := newDDLAdapter(nil)
			queries, err := adapter.walDataToQueries(context.Background(), tc.walData)
			require.ErrorIs(t, err, tc.wantErr)
			require.ElementsMatch(t, queries, tc.wantQueries)
		})
	}
}

// stubRewriter returns a fixed set of statements, standing in for the schema
// observer.
type stubRewriter struct{ statements []string }

func (s *stubRewriter) RewriteDDLStatements(_, _ string) []string { return s.statements }

// The captured DDL is current_query(), so a migration file arrives as one event
// carrying several statements. Each must become its own query: the writer runs
// them in order and carries on after a failure, so an "already exists" on a
// replayed statement no longer drops the rest of the migration.
func TestDDLAdapter_walDataToQueries_multiStatement(t *testing.T) {
	t.Parallel()

	walData := &wal.Data{
		Action: wal.LogicalMessageAction,
		Prefix: wal.DDLPrefix,
		Content: `{
			"ddl": "irrelevant, the rewriter is stubbed",
			"schema_name": "public",
			"command_tag": "CREATE TYPE",
			"objects": [{"type": "table", "identity": "public.test_table", "schema": "public"}]
		}`,
	}

	t.Run("one query per statement", func(t *testing.T) {
		t.Parallel()

		adapter := newDDLAdapter(&stubRewriter{statements: []string{
			`CREATE TABLE "piana_test_table" ("id" TEXT);`,
			`ALTER TABLE "piana_test_table" ADD COLUMN "c" text;`,
		}})

		queries, err := adapter.walDataToQueries(context.Background(), walData)
		require.NoError(t, err)
		require.Len(t, queries, 2)
		for _, q := range queries {
			require.True(t, q.isDDL)
			require.Equal(t, "public", q.schema)
			require.Equal(t, "test_table", q.table)
		}
		require.Equal(t, `CREATE TABLE "piana_test_table" ("id" TEXT);`, queries[0].sql)
		require.Equal(t, `ALTER TABLE "piana_test_table" ADD COLUMN "c" text;`, queries[1].sql)
	})

	t.Run("nothing to apply yields an empty query", func(t *testing.T) {
		t.Parallel()

		adapter := newDDLAdapter(&stubRewriter{statements: nil})

		queries, err := adapter.walDataToQueries(context.Background(), walData)
		require.NoError(t, err)
		require.Len(t, queries, 1)
		require.True(t, queries[0].IsEmpty())
	})
}
