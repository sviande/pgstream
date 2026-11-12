// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// lockSafeKeyMigration adds a foreign key the way a migration does when it
// must not block writes for long: the key is added inside a DO block, under IF
// NOT EXISTS so the file can run again after a lock timeout. The ALTER TABLE
// event of the key carries the whole text, where no top-level statement adds
// it.
const lockSafeKeyMigration = `-- Each statement can be replayed.
ALTER TABLE "Draft" ADD COLUMN IF NOT EXISTS "memberId" TEXT;

CREATE INDEX IF NOT EXISTS "Draft_memberId_idx" ON "Draft"("memberId");

-- Adding the key locks Member against writes.
BEGIN;
SET LOCAL lock_timeout = '500ms';
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'Draft_memberId_fkey') THEN
    ALTER TABLE "Draft" ADD CONSTRAINT "Draft_memberId_fkey" FOREIGN KEY ("memberId") REFERENCES "Member"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;
END $$;
COMMIT;`

// lockRetryMigration adds a column and its foreign key to a table written on
// every request: both in one DO block that retries until it gets the lock, so
// the block fires two ALTER TABLE events on the same table.
const lockRetryMigration = `-- The column and its key, under a lock taken for 500 ms at most per attempt.
BEGIN;
SET LOCAL lock_timeout = '500ms';

DO $$
BEGIN
  FOR attempt IN 1..20 LOOP
    BEGIN
      ALTER TABLE "Event" ADD COLUMN "envelopeId" TEXT;

      ALTER TABLE "Event"
        ADD CONSTRAINT "Event_envelopeId_fkey" FOREIGN KEY ("envelopeId") REFERENCES "Envelope"("id") NOT VALID;

      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      PERFORM pg_sleep(1);
    END;
  END LOOP;
  RAISE EXCEPTION 'Event stayed locked' USING ERRCODE = 'lock_not_available';
END $$;

COMMIT;`

func TestSplitFiringStatements(t *testing.T) {
	t.Parallel()

	inRun := func(run int, sql string) FiringStatement { return FiringStatement{SQL: sql, InBlock: true, Run: run} }
	inBlock := func(sql string) FiringStatement { return inRun(1, sql) }

	tests := []struct {
		name string
		sql  string
		want []FiringStatement
	}{
		{
			name: "foreign key added in a DO block",
			sql:  lockSafeKeyMigration,
			want: []FiringStatement{
				{SQL: "-- Each statement can be replayed.\nALTER TABLE \"Draft\" ADD COLUMN IF NOT EXISTS \"memberId\" TEXT;"},
				{SQL: `CREATE INDEX IF NOT EXISTS "Draft_memberId_idx" ON "Draft"("memberId");`},
				{SQL: "-- Adding the key locks Member against writes.\nBEGIN;"},
				{SQL: `SET LOCAL lock_timeout = '500ms';`},
				inBlock(`ALTER TABLE "Draft" ADD CONSTRAINT "Draft_memberId_fkey" FOREIGN KEY ("memberId") REFERENCES "Member"("id") ON DELETE CASCADE ON UPDATE CASCADE;`),
				{SQL: `COMMIT;`},
			},
		},
		{
			name: "language before the body",
			sql:  `DO LANGUAGE plpgsql $body$ BEGIN DROP TABLE "Old"; END $body$;`,
			want: []FiringStatement{inBlock(`DROP TABLE "Old";`)},
		},
		{
			name: "language after the body, quoted",
			sql:  `do $$ begin drop table "Old"; end $$ language 'plpgsql';`,
			want: []FiringStatement{inBlock(`drop table "Old";`)},
		},
		{
			name: "body as a quoted string",
			sql:  `DO 'BEGIN DROP TABLE "it''s"; END';`,
			want: []FiringStatement{inBlock(`DROP TABLE "it's";`)},
		},
		{
			name: "control flow, labels, nested blocks and handlers",
			sql: `DO $$
DECLARE
  n integer := 0;
BEGIN
  <<retry>>
  BEGIN
    SELECT count(*) INTO n FROM "Item";
    IF n = 0 THEN
      DROP TABLE "Item";
    ELSIF n < 10 THEN
      ALTER TABLE "Item" ADD COLUMN "archived" boolean;
    ELSE
      RAISE NOTICE 'kept %; nothing to drop', n;
    END IF;
    CREATE TRIGGER "item_audit" AFTER UPDATE ON "Item" FOR EACH ROW EXECUTE FUNCTION audit();
  EXCEPTION WHEN undefined_table THEN
    CREATE TABLE "Item" ("id" text);
  END;
  WHILE n > 0 LOOP
    n := n - 1; -- a comment; with a semicolon
  END LOOP retry;
END $$;`,
			want: []FiringStatement{
				inRun(1, `n integer := 0;`),
				inRun(2, `SELECT count(*) INTO n FROM "Item";`),
				inRun(3, `DROP TABLE "Item";`),
				inRun(4, `ALTER TABLE "Item" ADD COLUMN "archived" boolean;`),
				inRun(5, `CREATE TRIGGER "item_audit" AFTER UPDATE ON "Item" FOR EACH ROW EXECUTE FUNCTION audit();`),
				inRun(6, `CREATE TABLE "Item" ("id" text);`),
				inRun(7, `n := n - 1;`),
			},
		},
		{
			name: "statements written one after the other share a run",
			sql:  lockRetryMigration,
			want: []FiringStatement{
				{SQL: "-- The column and its key, under a lock taken for 500 ms at most per attempt.\nBEGIN;"},
				{SQL: `SET LOCAL lock_timeout = '500ms';`},
				inRun(1, `ALTER TABLE "Event" ADD COLUMN "envelopeId" TEXT;`),
				inRun(1, "ALTER TABLE \"Event\"\n        ADD CONSTRAINT \"Event_envelopeId_fkey\" FOREIGN KEY (\"envelopeId\") REFERENCES \"Envelope\"(\"id\") NOT VALID;"),
				{SQL: `COMMIT;`},
			},
		},
		{
			name: "runs are numbered across the blocks of a query",
			sql: `DO $$ BEGIN DROP TABLE "A"; DROP TABLE "B"; RAISE NOTICE 'dropped'; DROP TABLE "C"; END $$;
DO $$ BEGIN DROP TABLE "D"; END $$;`,
			want: []FiringStatement{
				inRun(1, `DROP TABLE "A";`), inRun(1, `DROP TABLE "B";`), inRun(2, `DROP TABLE "C";`),
				inRun(3, `DROP TABLE "D";`),
			},
		},
		{
			name: "keywords in parentheses do not start a statement",
			sql:  `DO $$ BEGIN IF EXISTS (SELECT 1 FROM "Item" WHERE CASE WHEN "id" = '' THEN true ELSE false END) THEN DROP TABLE "Item"; END IF; END $$;`,
			want: []FiringStatement{inBlock(`DROP TABLE "Item";`)},
		},
		{
			name: "function defined in a block",
			sql:  `DO $outer$ BEGIN CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; END $outer$;`,
			want: []FiringStatement{inBlock(`CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;`)},
		},
		{
			name: "block without SQL",
			sql:  `DO $$ BEGIN RAISE NOTICE 'hello; world'; END $$;`,
			want: nil,
		},
		{
			name: "dynamic SQL cannot be read",
			sql:  `DO $$ BEGIN EXECUTE 'DROP TABLE "Old"'; END $$;`,
			want: []FiringStatement{{SQL: `DO $$ BEGIN EXECUTE 'DROP TABLE "Old"'; END $$;`, Opaque: true}},
		},
		{
			name: "dynamic SQL in a loop cannot be read",
			sql:  `DO $$ DECLARE r record; BEGIN FOR r IN EXECUTE 'SELECT 1' LOOP DROP TABLE "Old"; END LOOP; END $$;`,
			want: []FiringStatement{{SQL: `DO $$ DECLARE r record; BEGIN FOR r IN EXECUTE 'SELECT 1' LOOP DROP TABLE "Old"; END LOOP; END $$;`, Opaque: true}},
		},
		{
			name: "another language cannot be read",
			sql:  `DO LANGUAGE plperl $$ spi_exec_query('DROP TABLE "Old"'); $$;`,
			want: []FiringStatement{{SQL: `DO LANGUAGE plperl $$ spi_exec_query('DROP TABLE "Old"'); $$;`, Opaque: true}},
		},
		{
			name: "a function body is not run",
			sql:  `CREATE FUNCTION f() RETURNS void AS $$ BEGIN DROP TABLE "Old"; END $$ LANGUAGE plpgsql;`,
			want: []FiringStatement{{SQL: `CREATE FUNCTION f() RETURNS void AS $$ BEGIN DROP TABLE "Old"; END $$ LANGUAGE plpgsql;`}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, SplitFiringStatements(tc.sql))
		})
	}
}

func TestMatchFiringStatement(t *testing.T) {
	t.Parallel()

	const (
		draft     = `public."Draft"`
		addColumn = `ALTER TABLE "Draft" ADD COLUMN "x" int;`
		dynamic   = `DO $$ BEGIN EXECUTE 'ALTER TABLE "Draft" ADD COLUMN "y" int'; END $$;`
		guarded   = `DO $$ BEGIN IF NOT EXISTS (SELECT 1) THEN ALTER TABLE "Draft" ADD CONSTRAINT "c" CHECK (true); END IF; END $$;`
	)

	tests := []struct {
		name       string
		sql        string
		from       int
		identities []string

		wantIndex int
		wantOK    bool
	}{
		{
			name:       "statement of a DO block, past BEGIN and SET",
			sql:        lockSafeKeyMigration,
			from:       2,
			identities: []string{draft},
			wantIndex:  4,
			wantOK:     true,
		},
		{
			name:       "top-level statement ahead of the block",
			sql:        lockSafeKeyMigration,
			identities: []string{draft},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "the block may have skipped its statement: a later one on the same table",
			sql:        guarded + "\n" + addColumn,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "two branches on the same table",
			sql:        `DO $$ BEGIN IF true THEN ALTER TABLE "Draft" ADD COLUMN "a" int; ELSE ALTER TABLE "Draft" ADD COLUMN "b" int; END IF; END $$;`,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "two statements of one run on the same table: the first fired the first event",
			sql:        lockRetryMigration,
			identities: []string{`public."Event"`},
			wantIndex:  2,
			wantOK:     true,
		},
		{
			name:       "two statements of one run on the same table: the second fired the next event",
			sql:        lockRetryMigration,
			from:       3,
			identities: []string{`public."Event"`},
			wantIndex:  3,
			wantOK:     true,
		},
		{
			name:       "the first statement of a run may do nothing: IF NOT EXISTS",
			sql:        `DO $$ BEGIN ALTER TABLE "Draft" ADD COLUMN IF NOT EXISTS "a" int; ALTER TABLE "Draft" ADD COLUMN "b" int; END $$;`,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "a control statement between two statements on the same table",
			sql:        `DO $$ BEGIN ALTER TABLE "Draft" ADD COLUMN "a" int; IF false THEN RETURN; END IF; ALTER TABLE "Draft" ADD COLUMN "b" int; END $$;`,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "a run on the same table, then another statement on it past the run",
			sql:        `DO $$ BEGIN ALTER TABLE "Draft" ADD COLUMN "a" int; ALTER TABLE "Draft" ADD COLUMN "b" int; END $$;` + "\n" + addColumn,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "a later statement on another table does not stand in the way",
			sql:        guarded + "\n" + `ALTER TABLE "Other" ADD COLUMN "x" int;`,
			identities: []string{draft},
			wantIndex:  0,
			wantOK:     true,
		},
		{
			name:       "a block that cannot be read, ahead",
			sql:        dynamic + "\n" + guarded,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "a block that cannot be read, further on",
			sql:        guarded + "\n" + dynamic,
			identities: []string{draft},
			wantOK:     false,
		},
		{
			name:       "a top-level statement past a block that cannot be read",
			sql:        dynamic + "\n" + addColumn,
			identities: []string{draft},
			wantIndex:  1,
			wantOK:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			index, ok := MatchFiringStatement(SplitFiringStatements(tc.sql), tc.from, "ALTER TABLE", tc.identities)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantIndex, index)
			}
		})
	}
}
