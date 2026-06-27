// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"regexp"
	"strings"
	"testing"

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
// block captured whole by current_query(). The UPDATE/FROM tables must be
// renamed (else the backfill fails with "relation does not exist" and rolls back
// the ADD COLUMN), while columns, aliases and alias-qualified refs stay intact.
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
UPDATE "piana_EvPassOrder" AS "order"
SET "companyName" = "company"."name"
FROM "piana_Company" AS "company"
WHERE "order"."companyId" = "company"."id";
ALTER TABLE "piana_EvPassOrder" ALTER COLUMN "companyName" SET NOT NULL;`

	got, skip := RewriteDDL(ddl, "ALTER TABLE", false, nil, r)
	if skip {
		t.Fatalf("unexpected skip")
	}
	if got != want {
		t.Errorf("RewriteDDL mismatch:\n got:  %s\n want: %s", got, want)
	}
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
