// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// rebuildTableMigration rebuilds a table in a single query: create, copy, drop,
// rename. Each of its DDL statements fires an event carrying the whole text.
const rebuildTableMigration = `CREATE TABLE "Item_new" ("id" text PRIMARY KEY, "name" text);
INSERT INTO "Item_new" SELECT "id", "name" FROM "Item";
DROP TABLE "Item";
ALTER TABLE "Item_new" RENAME TO "Item";`

func TestMatchStatement(t *testing.T) {
	t.Parallel()

	block := SplitStatements(migrationTransactionBlock)
	rebuild := SplitStatements(rebuildTableMigration)

	tests := []struct {
		name       string
		statements []string
		from       int
		commandTag string
		identities []string

		wantIndex int
		wantOK    bool
	}{
		{
			name:       "function of the migration block, past BEGIN and LOCK",
			statements: block,
			commandTag: "CREATE FUNCTION",
			identities: []string{`public.insert_setting_history()`},
			wantIndex:  2,
			wantOK:     true,
		},
		{
			name:       "first trigger of the migration block",
			statements: block,
			from:       3,
			commandTag: "CREATE TRIGGER",
			identities: []string{`setting_history_on_insert on public."Setting"`},
			wantIndex:  3,
			wantOK:     true,
		},
		{
			name:       "second trigger of the migration block",
			statements: block,
			from:       4,
			commandTag: "CREATE TRIGGER",
			identities: []string{`setting_history_on_update on public."Setting"`},
			wantIndex:  4,
			wantOK:     true,
		},
		{
			name:       "rebuilt table created",
			statements: rebuild,
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item_new"`},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "old table dropped, past the copy",
			statements: rebuild,
			from:       1,
			commandTag: "DROP TABLE",
			identities: []string{`public."Item"`},
			wantIndex:  2,
			wantOK:     true,
		},
		{
			name:       "rebuilt table renamed",
			statements: rebuild,
			from:       3,
			commandTag: "ALTER TABLE",
			identities: []string{`public."Item"`},
			wantIndex:  3,
			wantOK:     true,
		},
		{
			name:       "statement already replayed",
			statements: rebuild,
			from:       1,
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item_new"`},
			wantOK:     false,
		},
		{
			name:       "temporary table, which fires no event, skipped",
			statements: SplitStatements(`CREATE TEMP TABLE "staging" ("id" text); CREATE TABLE "Item" ("id" text);`),
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item"`},
			wantIndex:  1,
			wantOK:     true,
		},
		{
			name:       "identifier inside a longer one is no mention",
			statements: SplitStatements(`CREATE TEMP TABLE "Item_staging" ("id" text); CREATE TABLE "Item" ("id" text);`),
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item"`},
			wantIndex:  1,
			wantOK:     true,
		},
		{
			name:       "unquoted identifier written in another case",
			statements: SplitStatements(`ALTER TABLE ITEMS ADD COLUMN note text;`),
			commandTag: "ALTER TABLE",
			identities: []string{`public.items`},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "index named by Postgres, single candidate",
			statements: SplitStatements(`ALTER TABLE "Item" ADD COLUMN "sku" text; CREATE INDEX ON "Item" ("sku");`),
			commandTag: "CREATE INDEX",
			identities: []string{`public."Item_sku_idx"`},
			wantIndex:  1,
			wantOK:     true,
		},
		{
			name:       "indexes named by Postgres, ambiguous",
			statements: SplitStatements(`CREATE INDEX ON "Item" ("sku"); CREATE INDEX ON "Item" ("name");`),
			commandTag: "CREATE INDEX",
			identities: []string{`public."Item_sku_idx"`},
			wantOK:     false,
		},
		{
			name:       "no object to name",
			statements: SplitStatements(`GRANT SELECT ON "Item" TO reader;`),
			commandTag: "GRANT",
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "modifiers left out of the tag",
			statements: SplitStatements(`CREATE OR REPLACE RECURSIVE VIEW "tree" ("id") AS SELECT 1;`),
			commandTag: "CREATE VIEW",
			identities: []string{`public.tree`},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "CREATE TABLE AS",
			statements: SplitStatements(`CREATE TABLE "Report" AS SELECT 1 AS n;`),
			commandTag: "CREATE TABLE AS",
			identities: []string{`public."Report"`},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "SELECT INTO",
			statements: SplitStatements(`SELECT 1 AS n INTO "Report";`),
			commandTag: "SELECT INTO",
			identities: []string{`public."Report"`},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "DDL run by a DO block",
			statements: SplitStatements(`DO $$ BEGIN EXECUTE 'CREATE TABLE "Item" ("id" text)'; END $$;`),
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item"`},
			wantOK:     false,
		},
		{
			name:       "DDL run by a function",
			statements: SplitStatements(`SELECT create_item_table();`),
			commandTag: "CREATE TABLE",
			identities: []string{`public."Item"`},
			wantOK:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			index, ok := MatchStatement(tc.statements, tc.from, tc.commandTag, tc.identities)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantIndex, index)
			}
		})
	}
}

func TestIdentityNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		identity string
		want     []string
	}{
		{identity: `public.users`, want: []string{"users"}},
		{identity: `public."CompanySetting"`, want: []string{"CompanySetting"}},
		{identity: `public.test_table.username`, want: []string{"username"}},
		{identity: `public.insert_setting_history()`, want: []string{"insert_setting_history"}},
		{identity: `public.f(public.amount, integer)`, want: []string{"f"}},
		{identity: `setting_history_on_insert on public."Setting"`, want: []string{"setting_history_on_insert", "Setting"}},
		{identity: `public."a.b"`, want: []string{"a.b"}},
		{identity: `public."say ""hi"""`, want: []string{`say "hi"`}},
		{identity: `reporting`, want: []string{"reporting"}},
	}

	for _, tc := range tests {
		t.Run(tc.identity, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, identityNames(tc.identity))
		})
	}
}
