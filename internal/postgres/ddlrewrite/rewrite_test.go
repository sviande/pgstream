// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
)

// fakeRenamer renames public.<t> -> public.piana_<t> in raw SQL, mimicking the
// production table renamer closely enough for these tests.
type fakeRenamer struct{ re *regexp.Regexp }

func newFakeRenamer() *fakeRenamer {
	// match identifiers after common table-introducing keywords
	return &fakeRenamer{re: regexp.MustCompile(`(?i)(\bpublic\.)("?)([A-Za-z0-9_]+)("?)`)}
}

func (f *fakeRenamer) HasRules() bool { return true }
func (f *fakeRenamer) RenameInSQL(sql []byte) []byte {
	return f.re.ReplaceAll(sql, []byte(`${1}${2}piana_${3}${4}`))
}

func trackerWith(names ...string) *EnumTypeTracker {
	t := NewEnumTypeTracker()
	for _, n := range names {
		t.Add(n)
	}
	t.ComputeSortedPatterns()
	return t
}

func TestRewriteDDL_SkipEnumTypeDDL(t *testing.T) {
	tr := trackerWith("public.user_status")
	cases := []struct {
		name string
		ddl  string
		tag  string
	}{
		{"create enum", "CREATE TYPE public.user_status AS ENUM ('a', 'b');", "CREATE TYPE"},
		{"alter add value", "ALTER TYPE public.user_status ADD VALUE 'c';", "ALTER TYPE"},
		{"alter rename value", "ALTER TYPE public.user_status RENAME VALUE 'a' TO 'z';", "ALTER TYPE"},
		{"drop enum", "DROP TYPE public.user_status;", "DROP TYPE"},
		{"drop enum if exists", "DROP TYPE IF EXISTS public.user_status CASCADE;", "DROP TYPE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, skip := RewriteDDL(c.ddl, c.tag, true, tr, nil)
			if !skip {
				t.Errorf("expected skip=true for %q", c.ddl)
			}
		})
	}
}

func TestRewriteDDL_MultiStatementDropTypeStripped(t *testing.T) {
	tr := trackerWith("public.MonEnum1", "public.MonEnum2")
	ddl := "-- Remove the scraper cache.\n-- Second comment line.\n\nDROP TABLE \"VehicleScraperCache\";\n\nDROP TYPE \"MonEnum1\";\n\nDROP TYPE \"MonEnum2\";\n"

	for _, tag := range []string{"DROP TABLE", "DROP TYPE"} {
		t.Run(tag, func(t *testing.T) {
			got, skip := RewriteDDL(ddl, tag, true, tr, nil)
			if skip {
				t.Fatalf("unexpected skip")
			}
			if strings.Contains(got, "DROP TYPE") {
				t.Errorf("DROP TYPE statements not stripped:\n%s", got)
			}
			if strings.Contains(got, "text") {
				t.Errorf("enum drop must not be rewritten to DROP TYPE text:\n%s", got)
			}
			if !strings.Contains(got, `DROP TABLE "VehicleScraperCache"`) {
				t.Errorf("DROP TABLE must be preserved:\n%s", got)
			}
		})
	}
}

func TestRewriteDDL_NonEnumDropTypeNotStripped(t *testing.T) {
	tr := trackerWith("public.MonEnum1")
	ddl := "DROP TABLE public.users;\nDROP TYPE public.my_composite;"
	got, skip := RewriteDDL(ddl, "DROP TABLE", true, tr, nil)
	if skip {
		t.Fatalf("unexpected skip")
	}
	if !strings.Contains(got, "DROP TYPE public.my_composite") {
		t.Errorf("composite DROP TYPE must be preserved:\n%s", got)
	}
}

func TestRewriteDDL_MultiStatementEnumTypeStripped(t *testing.T) {
	tr := trackerWith("public.StatusEnum", "public.ProviderEnum")

	tests := []struct {
		name         string
		ddl          string
		tag          string
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "single-line CREATE TYPE AS ENUM + table (Prisma-style comments)",
			ddl:          "-- CreateEnum\nCREATE TYPE \"StatusEnum\" AS ENUM ('InProgress', 'Completed');\n\n-- CreateTable\nCREATE TABLE \"Order\" (\n    \"id\" TEXT NOT NULL,\n    \"status\" \"StatusEnum\" NOT NULL DEFAULT 'InProgress'\n);",
			tag:          "CREATE TABLE",
			wantContains: []string{`CREATE TABLE "Order"`, `"status" text NOT NULL DEFAULT 'InProgress'`},
			wantAbsent:   []string{"CREATE TYPE", "AS ENUM", "StatusEnum"},
		},
		{
			name:         "multi-line CREATE TYPE AS ENUM is fully removed",
			ddl:          "CREATE TYPE \"StatusEnum\" AS ENUM (\n    'InProgress',\n    'Completed'\n);\nCREATE TABLE \"Order\" (\"id\" TEXT, \"status\" \"StatusEnum\");",
			tag:          "CREATE TABLE",
			wantContains: []string{`CREATE TABLE "Order"`, `"status" text`},
			wantAbsent:   []string{"CREATE TYPE", "AS ENUM", "InProgress", "StatusEnum"},
		},
		{
			name:         "ALTER TYPE ADD VALUE is removed",
			ddl:          "-- AlterEnum\nALTER TYPE \"StatusEnum\" ADD VALUE 'Mobility';\nCREATE TABLE t (id int);",
			tag:          "CREATE TABLE",
			wantContains: []string{"CREATE TABLE t (id int);"},
			wantAbsent:   []string{"ALTER TYPE", "ADD VALUE"},
		},
		{
			name:         "ALTER TYPE RENAME TO a tracked enum is removed",
			ddl:          "ALTER TYPE \"StatusEnum\" RENAME TO \"StatusEnum2\";\nCREATE TABLE t (id int);",
			tag:          "CREATE TABLE",
			wantContains: []string{"CREATE TABLE t (id int);"},
			wantAbsent:   []string{"ALTER TYPE", "RENAME TO"},
		},
		{
			name:         "reported CREATE TYPE text failure block (enums + alter + tables)",
			ddl:          "-- CreateEnum\nCREATE TYPE \"ProviderEnum\" AS ENUM ('Dkv');\n\n-- CreateEnum\nCREATE TYPE \"StatusEnum\" AS ENUM ('Ordered', 'Active');\n\n-- AlterEnum\nALTER TYPE \"StatusEnum\" ADD VALUE 'Mobility';\n\n-- CreateTable\nCREATE TABLE \"piana_MobilityCard\" (\n    \"id\" TEXT NOT NULL,\n    \"provider\" \"ProviderEnum\" NOT NULL DEFAULT 'Dkv',\n    \"status\" \"StatusEnum\" NOT NULL DEFAULT 'Ordered'\n);",
			tag:          "CREATE TABLE",
			wantContains: []string{`CREATE TABLE "piana_MobilityCard"`, `"provider" text NOT NULL DEFAULT 'Dkv'`, `"status" text NOT NULL DEFAULT 'Ordered'`},
			wantAbsent:   []string{"CREATE TYPE", "ALTER TYPE", "AS ENUM", "ADD VALUE", "ProviderEnum", "StatusEnum"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, skip := RewriteDDL(tt.ddl, tt.tag, true, tr, nil)
			if skip {
				t.Fatalf("unexpected skip")
			}
			for _, w := range tt.wantContains {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in:\n%s", w, got)
				}
			}
			for _, w := range tt.wantAbsent {
				if strings.Contains(got, w) {
					t.Errorf("unexpected %q in:\n%s", w, got)
				}
			}
		})
	}
}

func TestRewriteDDL_LeadingCommentSkipsEnumTypeDDL(t *testing.T) {
	tr := trackerWith("public.StatusEnum")
	_, skip := RewriteDDL("-- CreateEnum\nCREATE TYPE \"StatusEnum\" AS ENUM ('a');", "CREATE TYPE", true, tr, nil)
	if !skip {
		t.Errorf("CREATE TYPE AS ENUM behind a leading comment must be skipped")
	}
}

func TestStripEnumTypeStatements(t *testing.T) {
	tr := trackerWith("public.color", "public.mood")
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "multi-line CREATE TYPE AS ENUM removed",
			in:   "CREATE TYPE \"color\" AS ENUM (\n    'red',\n    'green'\n);\nCREATE TABLE t (id int);",
			want: "CREATE TABLE t (id int);",
		},
		{
			name: "composite CREATE TYPE preserved",
			in:   "CREATE TYPE \"pair\" AS (a int, b int);\nCREATE TABLE t (id int);",
			want: "CREATE TYPE \"pair\" AS (a int, b int);\nCREATE TABLE t (id int);",
		},
		{
			name: "DROP TYPE of composite (non-tracked) preserved, tracked enum removed",
			in:   "DROP TYPE \"pair\";\nDROP TYPE \"color\";",
			want: "DROP TYPE \"pair\";",
		},
		{
			name: "ALTER TYPE ADD VALUE removed, unrelated ALTER TABLE kept",
			in:   "ALTER TYPE \"mood\" ADD VALUE 'ok';\nALTER TABLE t ADD COLUMN x int;",
			want: "ALTER TABLE t ADD COLUMN x int;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripEnumTypeStatements(tt.in, tr); got != tt.want {
				t.Errorf("stripEnumTypeStatements()\n got:  %q\n want: %q", got, tt.want)
			}
		})
	}
}

func TestRewriteDDL_NonEnumTypeNotSkipped(t *testing.T) {
	tr := trackerWith("public.user_status")
	// composite type, not an enum -> must NOT be skipped
	ddl := "CREATE TYPE public.my_composite AS (a int, b text);"
	_, skip := RewriteDDL(ddl, "CREATE TYPE", true, tr, nil)
	if skip {
		t.Errorf("composite CREATE TYPE should not be skipped")
	}
}

func TestRewriteDDL_CreateTableEnumToTextAndRename(t *testing.T) {
	tr := trackerWith("public.user_status", "public.country_code")
	ddl := "CREATE TABLE public.users (\n    id integer NOT NULL,\n    status public.user_status NOT NULL,\n    countries public.country_code[]\n);"
	got, skip := RewriteDDL(ddl, "CREATE TABLE", true, tr, newFakeRenamer())
	if skip {
		t.Fatalf("unexpected skip")
	}
	if strings.Contains(got, "user_status") || strings.Contains(got, "country_code") {
		t.Errorf("enum types not converted:\n%s", got)
	}
	if !strings.Contains(got, "status text") || !strings.Contains(got, "countries text[]") {
		t.Errorf("expected text/text[] columns:\n%s", got)
	}
	if !strings.Contains(got, "public.piana_users") {
		t.Errorf("expected table rename to piana_users:\n%s", got)
	}
}

func TestRewriteDDL_CreateTableSingleLine(t *testing.T) {
	// CDC DDL is typically a single line; ConvertEnumColumnsToText would skip the
	// "CREATE TABLE" line entirely, so the enum must still be converted here.
	tr := trackerWith("public.color")
	ddl := "CREATE TABLE public.items (id serial PRIMARY KEY, label text NOT NULL, c public.color NOT NULL DEFAULT 'red', palette public.color[]);"
	got, skip := RewriteDDL(ddl, "CREATE TABLE", true, tr, newFakeRenamer())
	if skip {
		t.Fatalf("unexpected skip")
	}
	if strings.Contains(got, "color") {
		t.Errorf("enum type not converted in single-line CREATE TABLE:\n%s", got)
	}
	if !strings.Contains(got, "c text") || !strings.Contains(got, "palette text[]") {
		t.Errorf("expected text/text[] columns:\n%s", got)
	}
	if !strings.Contains(got, "public.piana_items") {
		t.Errorf("expected table rename:\n%s", got)
	}
}

func TestRewriteDDL_AddColumnEnum(t *testing.T) {
	tr := trackerWith("public.user_status")
	ddl := "ALTER TABLE public.users ADD COLUMN status public.user_status DEFAULT 'a'::public.user_status;"
	got, skip := RewriteDDL(ddl, "ALTER TABLE", true, tr, newFakeRenamer())
	if skip {
		t.Fatalf("unexpected skip")
	}
	if strings.Contains(got, "user_status") {
		t.Errorf("enum not converted in ADD COLUMN:\n%s", got)
	}
	if !strings.Contains(got, "status text") || !strings.Contains(got, "'a'::text") {
		t.Errorf("expected text column and ::text cast:\n%s", got)
	}
	if !strings.Contains(got, "public.piana_users") {
		t.Errorf("expected rename:\n%s", got)
	}
}

func TestRewriteDDL_AlterColumnType(t *testing.T) {
	tr := trackerWith("public.user_status")
	ddl := "ALTER TABLE public.users ALTER COLUMN status TYPE public.user_status;"
	got, _ := RewriteDDL(ddl, "ALTER TABLE", true, tr, nil)
	if strings.Contains(got, "user_status") {
		t.Errorf("ALTER COLUMN TYPE enum not converted:\n%s", got)
	}
	if !strings.Contains(got, "TYPE text") {
		t.Errorf("expected TYPE text:\n%s", got)
	}
}

func TestRewriteDDL_NonEnumDDLRenameOnly(t *testing.T) {
	tr := trackerWith("public.user_status")
	ddl := "CREATE INDEX idx_users_name ON public.users (name);"
	got, skip := RewriteDDL(ddl, "CREATE INDEX", true, tr, newFakeRenamer())
	if skip {
		t.Fatalf("unexpected skip")
	}
	if !strings.Contains(got, "public.piana_users") {
		t.Errorf("expected table rename in CREATE INDEX:\n%s", got)
	}
}

func TestUpdateTrackerFromEnumDDL(t *testing.T) {
	tr := NewEnumTypeTracker()

	// CREATE TYPE ... AS ENUM registers it
	UpdateTrackerFromEnumDDL(tr, "CREATE TYPE", "CREATE TYPE public.color AS ENUM ('r', 'g');")
	if !tr.IsEnum("public.color") {
		t.Fatalf("CREATE TYPE enum not tracked")
	}

	// non-enum CREATE TYPE is ignored
	UpdateTrackerFromEnumDDL(tr, "CREATE TYPE", "CREATE TYPE public.pair AS (a int, b int);")
	if tr.IsEnum("public.pair") {
		t.Errorf("composite type should not be tracked")
	}

	// ALTER TYPE ADD VALUE registers a not-yet-known enum
	UpdateTrackerFromEnumDDL(tr, "ALTER TYPE", "ALTER TYPE public.size ADD VALUE 'xl';")
	if !tr.IsEnum("public.size") {
		t.Errorf("ALTER TYPE ADD VALUE enum not tracked")
	}

	// DROP TYPE removes it
	UpdateTrackerFromEnumDDL(tr, "DROP TYPE", "DROP TYPE public.color;")
	if tr.IsEnum("public.color") {
		t.Errorf("dropped enum should be untracked")
	}
}

// TestRewriteDDL_MultiStatementBackfillRename exercises the full RewriteDDL ->
// renamer.RenameInSQL path with the production rule on an "ALTER + backfill"
// block captured whole by current_query(). The ALTER statements are renamed
// while the backfill UPDATE is dropped: the rows it writes reach the target
// through the WAL, and a SET NOT NULL replayed before them fails harmlessly
// then succeeds when the event of that statement is replayed, after them.
func TestRewriteDDL_MultiStatementBackfillRename(t *testing.T) {
	r, err := renamer.NewTableRenamer(&renamer.Config{Rules: []renamer.Rule{
		{Schema: "public", Match: "^(.*)$", Replace: "piana_$1"},
	}})
	if err != nil {
		t.Fatalf("build renamer: %v", err)
	}

	ddl := `ALTER TABLE "EvPassOrder" ADD COLUMN "companyName" TEXT;
UPDATE "EvPassOrder" AS "order"
SET "companyName" = "company"."name"
FROM "Company" AS "company"
WHERE "order"."companyId" = "company"."id";
ALTER TABLE "EvPassOrder" ALTER COLUMN "companyName" SET NOT NULL;`

	want := `ALTER TABLE "piana_EvPassOrder" ADD COLUMN "companyName" TEXT;
ALTER TABLE "piana_EvPassOrder" ALTER COLUMN "companyName" SET NOT NULL;`

	got, skip := RewriteDDL(ddl, "ALTER TABLE", false, nil, r)
	if skip {
		t.Fatalf("unexpected skip")
	}
	if got != want {
		t.Errorf("RewriteDDL mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// migrationTransactionBlock has the shape of the Prisma migration that stopped
// the replication on 2026-10-27: a block sent as one query, opened by BEGIN and
// a LOCK, holding a trigger function, its triggers and an INSERT ... SELECT
// backfill. The function body holds BEGIN, INSERT and END IF of its own, which
// must stay inside the CREATE FUNCTION statement.
const migrationTransactionBlock = `BEGIN;

-- No row may change between the backfill below and the triggers taking over.
LOCK TABLE "Setting" IN SHARE ROW EXCLUSIVE MODE;

-- Every write path leaves a trace.
CREATE OR REPLACE FUNCTION "insert_setting_history"()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO "SettingHistory" ("id", "settingId", "enabled", "createdAt")
    VALUES (gen_random_uuid(), NEW."id", NEW."enabled", NEW."createdAt");
  ELSIF NEW."enabled" IS DISTINCT FROM OLD."enabled" THEN
    -- clock_timestamp(); so a change in the creating transaction sorts after it.
    INSERT INTO "SettingHistory" ("id", "settingId", "enabled", "createdAt")
    VALUES (gen_random_uuid(), NEW."id", NEW."enabled", clock_timestamp() AT TIME ZONE 'UTC');
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "setting_history_on_insert"
AFTER INSERT ON "Setting"
FOR EACH ROW
EXECUTE FUNCTION "insert_setting_history"();

CREATE TRIGGER "setting_history_on_update"
AFTER UPDATE OF "enabled" ON "Setting"
FOR EACH ROW
EXECUTE FUNCTION "insert_setting_history"();

-- Existing settings start with their current value.
INSERT INTO "SettingHistory" ("id", "settingId", "enabled", "createdAt")
SELECT gen_random_uuid(), "Setting"."id", "Setting"."enabled", LEAST("Setting"."createdAt", "Company"."createdAt")
FROM "Setting"
JOIN "Company" ON "Company"."id" = "Setting"."companyId";

COMMIT;`

// Replaying the block statement by statement ran LOCK outside a transaction
// (25P01, fatal for the writer) and would have replayed the backfill, which
// already reaches the target through the WAL. Only the DDL survives, with the
// function body intact.
func TestRewriteDDLStatements_MigrationTransactionBlock(t *testing.T) {
	t.Parallel()

	// every event fired by the block carries it whole, whatever its own tag
	for _, commandTag := range []string{"CREATE FUNCTION", "CREATE TRIGGER"} {
		t.Run(commandTag, func(t *testing.T) {
			t.Parallel()

			statements := RewriteDDLStatements(migrationTransactionBlock, commandTag, false, nil, nil)

			tags := make([]string, 0, len(statements))
			for _, stmt := range statements {
				tags = append(tags, stmt.Tag)
			}
			require.Equal(t, []string{"CREATE FUNCTION", "CREATE TRIGGER", "CREATE TRIGGER"}, tags)

			function := statements[0].SQL
			require.True(t, strings.HasPrefix(function, "-- Every write path leaves a trace.\nCREATE OR REPLACE FUNCTION"), function)
			require.True(t, strings.HasSuffix(function, "$$ LANGUAGE plpgsql;"), function)
			require.Equal(t, 2, strings.Count(function, `INSERT INTO "SettingHistory"`), "the function body must stay whole")
		})
	}
}

// Transaction control, session state and data statements bundled with DDL are
// dropped; DDL and procedural code are kept.
func TestRewriteDDLStatements_DropsNonDDL(t *testing.T) {
	t.Parallel()

	const createTable = `CREATE TABLE "t" ("id" text PRIMARY KEY);`

	tests := []struct {
		name    string
		stmt    string
		wantTag string // empty when the statement must be dropped
	}{
		{name: "BEGIN", stmt: "BEGIN;"},
		{name: "START TRANSACTION", stmt: "START TRANSACTION ISOLATION LEVEL SERIALIZABLE;"},
		{name: "COMMIT", stmt: "COMMIT;"},
		{name: "END", stmt: "END;"},
		{name: "ROLLBACK", stmt: "ROLLBACK;"},
		{name: "SAVEPOINT", stmt: "SAVEPOINT before_backfill;"},
		{name: "RELEASE", stmt: "RELEASE SAVEPOINT before_backfill;"},
		{name: "LOCK", stmt: `LOCK TABLE "t" IN ACCESS EXCLUSIVE MODE;`},
		{name: "SET", stmt: "SET LOCAL lock_timeout = '5s';"},
		{name: "RESET", stmt: "RESET lock_timeout;"},
		{name: "INSERT", stmt: `INSERT INTO "t" ("id") VALUES ('a');`},
		{name: "UPDATE", stmt: `UPDATE "t" SET "id" = 'b';`},
		{name: "DELETE", stmt: `DELETE FROM "t";`},
		{name: "MERGE", stmt: `MERGE INTO "t" USING "u" ON "t"."id" = "u"."id" WHEN NOT MATCHED THEN INSERT VALUES ("u"."id");`},
		{name: "TRUNCATE", stmt: `TRUNCATE "t";`},
		{name: "SELECT", stmt: `SELECT setval('"t_id_seq"', 1);`},
		{name: "WITH", stmt: `WITH "moved" AS (DELETE FROM "t" RETURNING *) INSERT INTO "u" SELECT * FROM "moved";`},
		{name: "ALTER TABLE", stmt: `ALTER TABLE "t" ADD COLUMN "c" text;`, wantTag: "ALTER TABLE"},
		{name: "CREATE INDEX", stmt: `CREATE UNIQUE INDEX "t_c_key" ON "t"("c");`, wantTag: "CREATE INDEX"},
		{name: "COMMENT", stmt: `COMMENT ON TABLE "t" IS 'x';`, wantTag: "COMMENT"},
		{name: "DO", stmt: `DO $$ BEGIN ALTER TABLE "t" ADD COLUMN "d" text; END $$;`, wantTag: "DO"},
		{name: "CALL", stmt: `CALL "migrate_t"();`, wantTag: "CALL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statements := RewriteDDLStatements(createTable+"\n"+tc.stmt, "CREATE TABLE", false, nil, nil)

			require.Equal(t, createTable, statements[0].SQL)
			if tc.wantTag == "" {
				require.Len(t, statements, 1, "%q must be dropped", tc.stmt)
				return
			}
			require.Len(t, statements, 2, "%q must be kept", tc.stmt)
			require.Equal(t, tc.wantTag, statements[1].Tag)
			require.Equal(t, tc.stmt, statements[1].SQL)
		})
	}
}

// A lone statement is DDL by definition, since the event trigger fired for it:
// it is kept with the event's tag whatever its leading keyword.
func TestRewriteDDLStatements_LoneStatementKept(t *testing.T) {
	t.Parallel()

	const ddl = `SELECT create_partition("events", '2026-11');`

	statements := RewriteDDLStatements(ddl, "CREATE TABLE", false, nil, nil)

	require.Equal(t, []Statement{{SQL: ddl, Tag: "CREATE TABLE"}}, statements)
}

func TestRewriteDDL_ConvertDisabledIsPassThrough(t *testing.T) {
	tr := trackerWith("public.user_status")
	ddl := "CREATE TYPE public.user_status AS ENUM ('a');"
	got, skip := RewriteDDL(ddl, "CREATE TYPE", false, tr, nil)
	if skip {
		t.Errorf("must not skip when convertEnums is false")
	}
	if got != ddl {
		t.Errorf("expected pass-through when convert disabled and no renamer")
	}
}

// A migration file arrives as one current_query() payload with several
// statements. Each must be rewritten on its own: the CREATE TYPE ... AS ENUM is
// dropped (the target has no enums) while the CREATE TABLE that follows it
// survives, with its enum columns turned into text.
func TestRewriteDDL_MultiStatementMigration(t *testing.T) {
	t.Parallel()

	newTracker := func() *EnumTypeTracker {
		tracker := NewEnumTypeTracker()
		tracker.Add(`"MobilityProvider"`)
		tracker.Add(`"CountryCode"`)
		return tracker
	}

	createTable := `CREATE TABLE "MobilityLocation" (
    "id" TEXT NOT NULL,
    "provider" "MobilityProvider" NOT NULL,
    "country" "CountryCode",
    CONSTRAINT "MobilityLocation_pkey" PRIMARY KEY ("id")
);`

	tests := []struct {
		name       string
		ddl        string
		commandTag string
	}{
		{
			name:       "batch opening with CREATE TYPE",
			commandTag: "CREATE TYPE",
			ddl:        "CREATE TYPE \"MobilityProvider\" AS ENUM ('Dkv');\n" + createTable,
		},
		{
			name:       "batch opening with a comment",
			commandTag: "CREATE TYPE",
			ddl:        "-- CreateEnum\nCREATE TYPE \"MobilityProvider\" AS ENUM ('Dkv');\n\n-- CreateTable\n" + createTable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, skip := RewriteDDL(tc.ddl, tc.commandTag, true, newTracker(), nil)

			require.False(t, skip, "the batch must not be dropped: it carries a CREATE TABLE")
			require.Contains(t, sql, `CREATE TABLE "MobilityLocation"`)
			require.NotContains(t, sql, "AS ENUM", "enum types must not reach the target")
			require.NotContains(t, sql, "CREATE TYPE text", "the enum name must never be rewritten in place")
			require.Contains(t, sql, `"provider" text NOT NULL`)
			require.Contains(t, sql, `"country" text`)
		})
	}
}

// A batch made only of enum DDL still collapses to a skip.
func TestRewriteDDL_MultiStatementAllSkipped(t *testing.T) {
	t.Parallel()

	tracker := NewEnumTypeTracker()
	tracker.Add(`"A"`)
	tracker.Add(`"B"`)

	sql, skip := RewriteDDL(
		"CREATE TYPE \"A\" AS ENUM ('x');\nCREATE TYPE \"B\" AS ENUM ('y');",
		"CREATE TYPE", true, tracker, nil)

	require.True(t, skip)
	require.Empty(t, sql)
}

// The tracker must learn about every enum in a batch, not just the first.
func TestUpdateTrackerFromEnumDDL_MultiStatement(t *testing.T) {
	t.Parallel()

	tracker := NewEnumTypeTracker()
	UpdateTrackerFromEnumDDL(tracker,
		"CREATE TYPE",
		"-- CreateEnum\nCREATE TYPE \"A\" AS ENUM ('x');\n\n-- CreateEnum\nCREATE TYPE \"B\" AS ENUM ('y');")

	require.True(t, tracker.IsEnum(`"A"`))
	require.True(t, tracker.IsEnum(`"B"`))
}

// A statement whose first line is a comment must still be classified by its SQL.
func TestRewriteDDL_LeadingCommentSingleStatement(t *testing.T) {
	t.Parallel()

	tracker := NewEnumTypeTracker()
	tracker.Add(`"P"`)

	sql, skip := RewriteDDL("-- CreateEnum\nCREATE TYPE \"P\" AS ENUM ('a');", "CREATE TYPE", true, tracker, nil)

	require.True(t, skip, "a commented CREATE TYPE is still a CREATE TYPE")
	require.Empty(t, sql)
}
