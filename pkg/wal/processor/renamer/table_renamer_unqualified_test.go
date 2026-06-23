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
