// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		count int
		first string
	}{
		{
			name:  "single statement",
			sql:   `CREATE TABLE "a" ("id" TEXT);`,
			count: 1,
			first: `CREATE TABLE "a" ("id" TEXT);`,
		},
		{
			name:  "migration file",
			sql:   "-- CreateEnum\nCREATE TYPE \"P\" AS ENUM ('a');\n\n-- CreateTable\nCREATE TABLE \"t\" (\"p\" \"P\");",
			count: 2,
			first: "-- CreateEnum\nCREATE TYPE \"P\" AS ENUM ('a');",
		},
		{
			name:  "semicolon inside a string literal",
			sql:   `INSERT INTO "t" VALUES ('a;b'); SELECT 1;`,
			count: 2,
			first: `INSERT INTO "t" VALUES ('a;b');`,
		},
		{
			name:  "semicolon inside a quoted identifier",
			sql:   `CREATE TABLE "we;ird" ("id" TEXT); SELECT 1;`,
			count: 2,
			first: `CREATE TABLE "we;ird" ("id" TEXT);`,
		},
		{
			name:  "semicolon inside a line comment",
			sql:   "-- drop this; really\nSELECT 1;",
			count: 1,
		},
		{
			name:  "semicolon inside a block comment",
			sql:   "/* a; b */ SELECT 1;",
			count: 1,
		},
		{
			name:  "dollar quoted function body",
			sql:   "CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;\nSELECT 1;",
			count: 2,
		},
		{
			name:  "tagged dollar quote",
			sql:   "CREATE FUNCTION f() RETURNS trigger AS $body$ BEGIN RETURN NEW; END; $body$ LANGUAGE plpgsql;",
			count: 1,
		},
		{
			name:  "escaped quote inside literal",
			sql:   `SELECT 'it''s; fine'; SELECT 2;`,
			count: 2,
		},
		{
			name:  "trailing statement without semicolon",
			sql:   `SELECT 1; SELECT 2`,
			count: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SplitStatements(tc.sql)
			require.Len(t, got, tc.count)
			if tc.first != "" {
				require.Equal(t, tc.first, got[0])
			}
		})
	}
}

func TestStatementTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stmt string
		want string
	}{
		{stmt: `CREATE TYPE "P" AS ENUM ('a');`, want: "CREATE TYPE"},
		{stmt: "-- CreateEnum\nCREATE TYPE \"P\" AS ENUM ('a');", want: "CREATE TYPE"},
		{stmt: "/* header */ CREATE TABLE \"t\" ();", want: "CREATE TABLE"},
		{stmt: `ALTER TABLE "t" ADD COLUMN "c" text;`, want: "ALTER TABLE"},
		{stmt: `DROP TYPE "P";`, want: "DROP TYPE"},
		{stmt: `UPDATE "t" SET "c" = 1;`, want: "UPDATE"},
		{stmt: `CREATE TABLE("t");`, want: "CREATE TABLE"},
	}

	for _, tc := range tests {
		t.Run(tc.want+"/"+strings.SplitN(tc.stmt, "\n", 2)[0], func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, StatementTag(tc.stmt))
		})
	}
}
