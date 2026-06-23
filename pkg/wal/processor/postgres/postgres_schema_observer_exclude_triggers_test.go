// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/xataio/pgstream/internal/postgres/ddlrewrite"
)

func TestPGSchemaObserver_RewriteDDL_ExcludeTriggers(t *testing.T) {
	t.Parallel()

	// Verbatim trigger from logs.txt:622 that failed on the target with
	// `relation "User" does not exist`.
	const triggerDDL = `CREATE TRIGGER user_disabled_delete_swan_user_reference
AFTER UPDATE OF "status" ON "User"
FOR EACH ROW EXECUTE FUNCTION delete_swan_user_reference_on_user_disable();`

	tests := []struct {
		name            string
		excludeTriggers bool
		ddl             string
		commandTag      string
		wantSkip        bool
	}{
		{
			name:            "exclude on - CREATE TRIGGER skipped",
			excludeTriggers: true,
			ddl:             triggerDDL,
			commandTag:      "CREATE TRIGGER",
			wantSkip:        true,
		},
		{
			name:            "exclude on - DROP TRIGGER skipped",
			excludeTriggers: true,
			ddl:             `DROP TRIGGER user_disabled_delete_swan_user_reference ON "User";`,
			commandTag:      "DROP TRIGGER",
			wantSkip:        true,
		},
		{
			name:            "exclude on - CONSTRAINT TRIGGER tag skipped",
			excludeTriggers: true,
			ddl:             triggerDDL,
			commandTag:      "CREATE CONSTRAINT TRIGGER",
			wantSkip:        true,
		},
		{
			name:            "exclude on - non-trigger DDL not skipped",
			excludeTriggers: true,
			ddl:             `ALTER TABLE "public"."Account" ADD COLUMN "x" int;`,
			commandTag:      "ALTER TABLE",
			wantSkip:        false,
		},
		{
			name:            "exclude off - CREATE TRIGGER replicated",
			excludeTriggers: false,
			ddl:             triggerDDL,
			commandTag:      "CREATE TRIGGER",
			wantSkip:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &pgSchemaObserver{
				excludeTriggers: tc.excludeTriggers,
				enumTracker:     ddlrewrite.NewEnumTypeTracker(),
			}
			sql, skip := o.RewriteDDL(tc.ddl, tc.commandTag)
			if skip != tc.wantSkip {
				t.Fatalf("RewriteDDL skip = %v, want %v (sql=%q)", skip, tc.wantSkip, sql)
			}
		})
	}
}
