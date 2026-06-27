// SPDX-License-Identifier: Apache-2.0

package renamer

import (
	"testing"
)

// TestRenameInSQL_UnqualifiedTableReferences covers the CDC/DDL regression from
// logs.txt:622 (relation "User" does not exist): live DDL often references a
// table without a schema qualifier, which the original schema-qualified-only
// patterns missed. The renamer must rewrite unqualified table references that
// follow a structural DDL keyword, while leaving column names, constraint names
// and values untouched.
func TestRenameInSQL_UnqualifiedTableReferences(t *testing.T) {
	t.Parallel()

	// Same rule as production (config.yaml.template).
	r, err := NewTableRenamer(&Config{Rules: []Rule{
		{Schema: "public", Match: "^(.*)$", Replace: "piana_$1"},
	}})
	if err != nil {
		t.Fatalf("build renamer: %v", err)
	}

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "ALTER TABLE renames table, not column",
			in:   `ALTER TABLE "User" ADD COLUMN "status" text;`,
			want: `ALTER TABLE "piana_User" ADD COLUMN "status" text;`,
		},
		{
			name: "CREATE INDEX ON renames target, not index/column",
			in:   `CREATE INDEX "idx_user_email" ON "User" ("email");`,
			want: `CREATE INDEX "idx_user_email" ON "piana_User" ("email");`,
		},
		{
			name: "FOREIGN KEY REFERENCES renames both tables, not constraint/columns",
			in:   `ALTER TABLE "Account" ADD CONSTRAINT "fk" FOREIGN KEY ("uid") REFERENCES "User" ("id");`,
			want: `ALTER TABLE "piana_Account" ADD CONSTRAINT "fk" FOREIGN KEY ("uid") REFERENCES "piana_User" ("id");`,
		},
		{
			name: "qualified name still renamed once, schema untouched",
			in:   `ALTER TABLE "public"."User" ADD COLUMN x int;`,
			want: `ALTER TABLE "public"."piana_User" ADD COLUMN x int;`,
		},
		{
			name: "schema-qualified after ON keeps schema, no double rename",
			in:   `CREATE INDEX i ON public."User" (id);`,
			want: `CREATE INDEX i ON public."piana_User" (id);`,
		},
		{
			name: "bare quoted identifier not after a keyword is left alone",
			in:   `SELECT * WHERE "internalId" = 1 AND "status" = 'x';`,
			want: `SELECT * WHERE "internalId" = 1 AND "status" = 'x';`,
		},
		{
			name: "TRUNCATE renames table",
			in:   `TRUNCATE "User";`,
			want: `TRUNCATE "piana_User";`,
		},
		{
			// Real CDC regression: an "ALTER + backfill" block captured whole by
			// current_query(). Only the tables (UPDATE target, FROM source) are
			// renamed; columns, aliases and alias-qualified refs are untouched.
			name: "backfill UPDATE ... FROM renames tables, not columns/aliases",
			in: `ALTER TABLE "EvPassOrder" ADD COLUMN "companyName" TEXT;
UPDATE "EvPassOrder" AS "order"
SET "companyName" = "company"."name"
FROM "Company" AS "company"
WHERE "order"."companyId" = "company"."id";
ALTER TABLE "EvPassOrder" ALTER COLUMN "companyName" SET NOT NULL;`,
			want: `ALTER TABLE "piana_EvPassOrder" ADD COLUMN "companyName" TEXT;
UPDATE "piana_EvPassOrder" AS "order"
SET "companyName" = "company"."name"
FROM "piana_Company" AS "company"
WHERE "order"."companyId" = "company"."id";
ALTER TABLE "piana_EvPassOrder" ALTER COLUMN "companyName" SET NOT NULL;`,
		},
		{
			name: "backfill simple UPDATE renames table, not columns",
			in:   `UPDATE "BlockingPolicy" SET "rulesUpdatedAt" = "updatedAt";`,
			want: `UPDATE "piana_BlockingPolicy" SET "rulesUpdatedAt" = "updatedAt";`,
		},
		{
			name: "JOIN renames joined table",
			in:   `UPDATE "A" SET x = 1 FROM "B" INNER JOIN "C" ON "B"."id" = "C"."bId";`,
			want: `UPDATE "piana_A" SET x = 1 FROM "piana_B" INNER JOIN "piana_C" ON "B"."id" = "C"."bId";`,
		},
		{
			name: "DELETE FROM renames table",
			in:   `DELETE FROM "Session" WHERE "expiresAt" < now();`,
			want: `DELETE FROM "piana_Session" WHERE "expiresAt" < now();`,
		},
		{
			name: "INSERT INTO ... SELECT FROM renames both tables",
			in:   `INSERT INTO "Archive" SELECT * FROM "Order";`,
			want: `INSERT INTO "piana_Archive" SELECT * FROM "piana_Order";`,
		},
		{
			// Second check: FROM inside a value-expression function separates
			// arguments; the quoted identifier is a column and must NOT be renamed.
			name: "EXTRACT FROM column is not renamed",
			in:   `UPDATE "Event" SET "y" = EXTRACT(YEAR FROM "createdAt");`,
			want: `UPDATE "piana_Event" SET "y" = EXTRACT(YEAR FROM "createdAt");`,
		},
		{
			name: "TRIM FROM column is not renamed",
			in:   `UPDATE "Event" SET "label" = TRIM(BOTH ' ' FROM "label");`,
			want: `UPDATE "piana_Event" SET "label" = TRIM(BOTH ' ' FROM "label");`,
		},
		{
			// The function argument contains a ')' inside a string literal: the
			// paren scan must skip it to find the real closing paren.
			name: "TRIM FROM column with paren in string literal is not renamed",
			in:   `UPDATE "Event" SET "label" = TRIM(')' FROM "label");`,
			want: `UPDATE "piana_Event" SET "label" = TRIM(')' FROM "label");`,
		},
		{
			// Non-regression: a real table inside a subquery FROM must still be
			// renamed (the paren here opens a subquery, not a value function).
			name: "subquery FROM table is still renamed",
			in:   `UPDATE "Order" SET "c" = sub."n" FROM (SELECT "id", "n" FROM "Company") AS sub;`,
			want: `UPDATE "piana_Order" SET "c" = sub."n" FROM (SELECT "id", "n" FROM "piana_Company") AS sub;`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := string(r.RenameInSQL([]byte(tc.in)))
			if got != tc.want {
				t.Errorf("RenameInSQL mismatch:\n in:   %s\n got:  %s\n want: %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestRegressionCases(t *testing.T) {
	t.Parallel()

	r, err := NewTableRenamer(&Config{Rules: []Rule{
		{Schema: "public", Match: "^(.*)$", Replace: "piana_$1"},
	}})
	if err != nil {
		t.Fatalf("build renamer: %v", err)
	}

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "COMMENT ON TABLE (was not in Pattern 4 before)",
			in:   `COMMENT ON TABLE "User" IS 'A user table';`,
			want: `COMMENT ON TABLE "piana_User" IS 'A user table';`,
		},
		{
			name: "GRANT ON (was not in Pattern 4 before)",
			in:   `GRANT SELECT ON "User" TO role;`,
			want: `GRANT SELECT ON "piana_User" TO role;`,
		},
		{
			name: "CREATE FUNCTION $$ ... $$ with FROM inside",
			in:   `CREATE FUNCTION process() RETURNS void AS $$ SELECT * FROM "OldTable" WHERE x = 1 $$ LANGUAGE SQL;`,
			want: `CREATE FUNCTION process() RETURNS void AS $$ SELECT * FROM "piana_OldTable" WHERE x = 1 $$ LANGUAGE SQL;`,
		},
		{
			name: "Column literally named FROM should not be renamed as table",
			in:   `SELECT "FROM", "INTO" FROM "User";`,
			want: `SELECT "FROM", "INTO" FROM "piana_User";`,
		},
		{
			name: "DELETE FROM ... USING",
			in:   `DELETE FROM "User" USING "Session" WHERE "User".id = "Session"."userId";`,
			want: `DELETE FROM "piana_User" USING "piana_Session" WHERE "User".id = "Session"."userId";`,
		},
		{
			name: "CTE WITH ... AS (SELECT FROM) - table inside subquery must be renamed",
			in:   `WITH company AS (SELECT * FROM "Company") SELECT * FROM company;`,
			want: `WITH company AS (SELECT * FROM "piana_Company") SELECT * FROM company;`,
		},
		{
			// String literal content is data: a keyword + quoted identifier inside
			// '...' must NOT be renamed. The real table ("User") still is.
			name: "keyword+table inside string literal is not renamed",
			in:   `COMMENT ON TABLE "User" IS 'UPDATE "Profile" SET x = 1';`,
			want: `COMMENT ON TABLE "piana_User" IS 'UPDATE "Profile" SET x = 1';`,
		},
		{
			name: "embedded SQL in VALUES string literal is not renamed",
			in:   `INSERT INTO "Log" (msg) VALUES ('INSERT INTO "Archive" VALUES (1)');`,
			want: `INSERT INTO "piana_Log" (msg) VALUES ('INSERT INTO "Archive" VALUES (1)');`,
		},
		{
			name: "table in line comment is not renamed",
			in:   "CREATE INDEX i ON \"User\" (x); -- UPDATE \"OldUser\" SET y=1\n",
			want: "CREATE INDEX i ON \"piana_User\" (x); -- UPDATE \"OldUser\" SET y=1\n",
		},
		{
			name: "table in block comment is not renamed",
			in:   `UPDATE "User" SET x = 1 /* was: UPDATE "Legacy" */;`,
			want: `UPDATE "piana_User" SET x = 1 /* was: UPDATE "Legacy" */;`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := string(r.RenameInSQL([]byte(tc.in)))
			if got != tc.want {
				t.Errorf("RenameInSQL mismatch:\n in:   %s\n got:  %s\n want: %s", tc.in, got, tc.want)
			}
		})
	}
}
