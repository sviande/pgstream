// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
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
// observer, and records the statements it is given.
type stubRewriter struct {
	statements []string
	got        []string
}

func (s *stubRewriter) RewriteDDLStatements(ddl, _ string) []string {
	s.got = append(s.got, ddl)
	return s.statements
}

func TestDDLAdapter_walDataToQueries_rewriter(t *testing.T) {
	t.Parallel()

	walData := ddlWalData(1, "0/100", "CREATE TABLE public.test_table (id text);", "CREATE TABLE", "public.test_table")

	t.Run("one query per rewritten statement", func(t *testing.T) {
		t.Parallel()

		rewriter := &stubRewriter{statements: []string{
			`CREATE TABLE "piana_test_table" ("id" TEXT);`,
			`ALTER TABLE "piana_test_table" ADD COLUMN "c" text;`,
		}}
		adapter := newDDLAdapter(rewriter)

		queries, err := adapter.walDataToQueries(context.Background(), walData)
		require.NoError(t, err)
		require.Equal(t, []string{"CREATE TABLE public.test_table (id text);"}, rewriter.got)
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

// ddlWalData builds the logical message emit_ddl writes for one DDL command.
func ddlWalData(xid int64, lsn, ddl, commandTag string, identities ...string) *wal.Data {
	objects := make([]wal.DDLObject, 0, len(identities))
	for _, identity := range identities {
		objects = append(objects, wal.DDLObject{Type: "table", Identity: identity, Schema: "public"})
	}
	content, err := json.Marshal(wal.DDLEvent{DDL: ddl, SchemaName: "public", CommandTag: commandTag, Objects: objects})
	if err != nil {
		panic(err)
	}
	return &wal.Data{Action: wal.LogicalMessageAction, Prefix: wal.DDLPrefix, XID: xid, LSN: lsn, Content: string(content)}
}

// A query holding several DDL statements fires one event per statement, each
// carrying the whole text. Each event must replay its own statement, at its own
// position in the WAL: replaying all of them on the first event would drop and
// rename the table before the rows copied into the new one arrive.
func TestDDLAdapter_walDataToQueries_capturedQuery(t *testing.T) {
	t.Parallel()

	const rebuild = `CREATE TABLE "Item_new" ("id" text PRIMARY KEY);
INSERT INTO "Item_new" SELECT "id" FROM "Item";
DROP TABLE "Item";
ALTER TABLE "Item_new" RENAME TO "Item";`

	var (
		createNew = `CREATE TABLE "Item_new" ("id" text PRIMARY KEY);`
		dropOld   = `DROP TABLE "Item";`
		renameNew = `ALTER TABLE "Item_new" RENAME TO "Item";`
		addColumn = `ALTER TABLE "Item" ADD COLUMN "sku" text;`
		created   = func(xid int64, lsn string) *wal.Data {
			return ddlWalData(xid, lsn, rebuild, "CREATE TABLE", `public."Item_new"`)
		}
		dropped = func(xid int64, lsn string) *wal.Data {
			return ddlWalData(xid, lsn, rebuild, "DROP TABLE", `public."Item"`)
		}
		renamed = func(xid int64, lsn string) *wal.Data {
			return ddlWalData(xid, lsn, rebuild, "ALTER TABLE", `public."Item"`)
		}
		added = func(xid int64, lsn string) *wal.Data {
			return ddlWalData(xid, lsn, addColumn, "ALTER TABLE", `public."Item"`)
		}
		procedural = `DO $$ BEGIN EXECUTE 'CREATE TABLE "Item" ("id" text)'; END $$;`

		// a foreign key added in a DO block, under a lock timeout
		lockSafeKey = `ALTER TABLE "Item" ADD COLUMN IF NOT EXISTS "ownerId" text;
BEGIN;
SET LOCAL lock_timeout = '500ms';
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'Item_ownerId_fkey') THEN
    ALTER TABLE "Item" ADD CONSTRAINT "Item_ownerId_fkey" FOREIGN KEY ("ownerId") REFERENCES "Owner"("id");
  END IF;
END $$;
COMMIT;`
		addOwner    = `ALTER TABLE "Item" ADD COLUMN IF NOT EXISTS "ownerId" text;`
		addOwnerKey = `ALTER TABLE "Item" ADD CONSTRAINT "Item_ownerId_fkey" FOREIGN KEY ("ownerId") REFERENCES "Owner"("id");`
		keyed       = func(xid int64, lsn string) *wal.Data {
			return ddlWalData(xid, lsn, lockSafeKey, "ALTER TABLE", `public."Item"`)
		}
	)

	tests := []struct {
		name   string
		events []*wal.Data

		wantSQL []string
		wantErr error
	}{
		{
			name:    "each event replays its own statement",
			events:  []*wal.Data{created(7, "0/100"), dropped(7, "0/200"), renamed(7, "0/300")},
			wantSQL: []string{createNew, dropOld, renameNew},
		},
		{
			name:    "the same text in a new transaction starts over",
			events:  []*wal.Data{created(7, "0/100"), dropped(7, "0/200"), renamed(7, "0/300"), created(8, "0/400")},
			wantSQL: []string{createNew, dropOld, renameNew, createNew},
		},
		{
			name:    "without transaction IDs, the same text sent again starts over",
			events:  []*wal.Data{created(0, "0/100"), dropped(0, "0/200"), renamed(0, "0/300"), created(0, "0/400")},
			wantSQL: []string{createNew, dropOld, renameNew, createNew},
		},
		{
			name: "a transaction sent again from its start after a reconnection",
			events: []*wal.Data{
				created(7, "0/100"), dropped(7, "0/200"),
				created(7, "0/100"), dropped(7, "0/200"), renamed(7, "0/300"),
			},
			wantSQL: []string{createNew, dropOld, createNew, dropOld, renameNew},
		},
		{
			name:    "a lone statement sent again after a reconnection",
			events:  []*wal.Data{added(7, "0/100"), added(7, "0/100")},
			wantSQL: []string{addColumn, addColumn},
		},
		{
			name:    "the same text sent again in the same transaction fails",
			events:  []*wal.Data{created(7, "0/100"), created(7, "0/400")},
			wantSQL: []string{createNew},
			wantErr: errNoFiringStatement,
		},
		{
			name:    "DDL run by dynamic SQL fails",
			events:  []*wal.Data{ddlWalData(7, "0/100", procedural, "CREATE TABLE", `public."Item"`)},
			wantErr: errNoFiringStatement,
		},
		{
			name:    "DDL written in a DO block replays its statement",
			events:  []*wal.Data{keyed(7, "0/100"), keyed(7, "0/200")},
			wantSQL: []string{addOwner, addOwnerKey},
		},
		{
			name:    "DDL written in a DO block, sent again after a reconnection",
			events:  []*wal.Data{keyed(7, "0/100"), keyed(7, "0/200"), keyed(7, "0/100"), keyed(7, "0/200")},
			wantSQL: []string{addOwner, addOwnerKey, addOwner, addOwnerKey},
		},
		{
			name:    "a third event on the table of a DO block fails",
			events:  []*wal.Data{keyed(7, "0/100"), keyed(7, "0/200"), keyed(7, "0/300")},
			wantSQL: []string{addOwner, addOwnerKey},
			wantErr: errNoFiringStatement,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adapter := newDDLAdapter(nil)
			var gotSQL []string
			var err error
			for _, ev := range tc.events {
				var queries []*query
				if queries, err = adapter.walDataToQueries(context.Background(), ev); err != nil {
					break
				}
				require.Len(t, queries, 1)
				gotSQL = append(gotSQL, queries[0].sql)
			}
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantSQL, gotSQL)
		})
	}
}
