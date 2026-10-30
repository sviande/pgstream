// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/postgres/ddlrewrite"
)

func TestPGSchemaObserver_RewriteDDLStatements_ExcludeTriggers(t *testing.T) {
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
			statements := o.RewriteDDLStatements(tc.ddl, tc.commandTag)
			if skip := len(statements) == 0; skip != tc.wantSkip {
				t.Fatalf("RewriteDDLStatements skip = %v, want %v (statements=%q)", skip, tc.wantSkip, statements)
			}
		})
	}
}

// A trigger bundled with other DDL in one payload is dropped on its own: the
// function it calls is still replayed, whichever event carries the payload.
func TestPGSchemaObserver_RewriteDDLStatements_ExcludeTriggersInPayload(t *testing.T) {
	t.Parallel()

	const function = `CREATE OR REPLACE FUNCTION "insert_setting_history"() RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO "SettingHistory" ("id", "settingId") VALUES (gen_random_uuid(), NEW."id");
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;`
	const trigger = `CREATE TRIGGER "setting_history_on_insert" AFTER INSERT ON "Setting" FOR EACH ROW EXECUTE FUNCTION "insert_setting_history"();`
	const constraintTrigger = `CREATE CONSTRAINT TRIGGER "setting_check" AFTER INSERT ON "Setting" FOR EACH ROW EXECUTE FUNCTION "insert_setting_history"();`
	payload := "BEGIN;\nLOCK TABLE \"Setting\" IN SHARE ROW EXCLUSIVE MODE;\n" + function + "\n" + trigger + "\n" + constraintTrigger + "\nCOMMIT;"

	tests := []struct {
		name            string
		excludeTriggers bool
		want            []string
	}{
		{name: "exclude on", excludeTriggers: true, want: []string{function}},
		{name: "exclude off", excludeTriggers: false, want: []string{function, trigger, constraintTrigger}},
	}

	for _, tc := range tests {
		for _, commandTag := range []string{"CREATE FUNCTION", "CREATE TRIGGER"} {
			t.Run(tc.name+"/"+commandTag, func(t *testing.T) {
				t.Parallel()
				o := &pgSchemaObserver{
					excludeTriggers: tc.excludeTriggers,
					enumTracker:     ddlrewrite.NewEnumTypeTracker(),
				}
				require.Equal(t, tc.want, o.RewriteDDLStatements(payload, commandTag))
			})
		}
	}
}

// Foreign keys and CHECK constraints, added by ALTER TABLE or declared in
// CREATE TABLE, are dropped like the schema snapshot drops them; the rest of
// the migration is still replayed.
func TestPGSchemaObserver_RewriteDDLStatements_ExcludeConstraints(t *testing.T) {
	t.Parallel()

	const createTable = `-- CreateTable
CREATE TABLE "HomeChargingVehicleHistory" ("id" TEXT NOT NULL, "homeChargingVehicleId" TEXT NOT NULL, CONSTRAINT "HomeChargingVehicleHistory_pkey" PRIMARY KEY ("id"), CONSTRAINT "HomeChargingVehicleHistory_vehicle_check" CHECK ("homeChargingVehicleId" <> ''));`
	const createTableWithoutCheck = `-- CreateTable
CREATE TABLE "HomeChargingVehicleHistory" ("id" TEXT NOT NULL, "homeChargingVehicleId" TEXT NOT NULL, CONSTRAINT "HomeChargingVehicleHistory_pkey" PRIMARY KEY ("id"));`
	const createIndex = `-- CreateIndex
CREATE INDEX "HomeChargingVehicleHistory_homeChargingVehicleId_idx" ON "HomeChargingVehicleHistory"("homeChargingVehicleId");`
	const foreignKey = `-- AddForeignKey
ALTER TABLE "HomeChargingVehicleHistory" ADD CONSTRAINT "HomeChargingVehicleHistory_homeChargingVehicleId_fkey" FOREIGN KEY ("homeChargingVehicleId") REFERENCES "HomeChargingVehicle"("id") ON DELETE CASCADE ON UPDATE CASCADE;`
	const check = `ALTER TABLE "HomeChargingVehicleHistory" ADD CONSTRAINT "HomeChargingVehicleHistory_id_check" CHECK ("id" <> '');`
	const unique = `ALTER TABLE "HomeChargingVehicleHistory" ADD CONSTRAINT "HomeChargingVehicleHistory_id_key" UNIQUE ("id");`
	payload := strings.Join([]string{createTable, createIndex, foreignKey, check, unique}, "\n\n")

	tests := []struct {
		name        string
		foreignKeys bool
		checks      bool
		want        []string
	}{
		{name: "both excluded", foreignKeys: true, checks: true, want: []string{createTableWithoutCheck, createIndex, unique}},
		{name: "foreign keys excluded", foreignKeys: true, want: []string{createTable, createIndex, check, unique}},
		{name: "checks excluded", checks: true, want: []string{createTableWithoutCheck, createIndex, foreignKey, unique}},
		{name: "nothing excluded", want: []string{createTable, createIndex, foreignKey, check, unique}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &pgSchemaObserver{
				excludeForeignKeys:      tc.foreignKeys,
				excludeCheckConstraints: tc.checks,
				enumTracker:             ddlrewrite.NewEnumTypeTracker(),
			}
			require.Equal(t, tc.want, o.RewriteDDLStatements(payload, "ALTER TABLE"))
		})
	}
}
