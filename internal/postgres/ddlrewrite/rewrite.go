// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"regexp"
	"strings"
)

// TableRenamer renames table identifiers in raw SQL. It is satisfied by
// renamer.TableRenamer.
type TableRenamer interface {
	RenameInSQL(sql []byte) []byte
	HasRules() bool
}

var dropTypeRegex = regexp.MustCompile(`(?i)^\s*DROP\s+TYPE\s+(?:IF\s+EXISTS\s+)?(.+?)(?:\s+CASCADE|\s+RESTRICT)?\s*;?\s*$`)

// RewriteDDL rewrites raw DDL captured from the replication stream for replay
// onto the target, mirroring the schema snapshot generator so a table created
// via snapshot and via live DDL replication end up identical.
//
// The captured DDL is current_query(): the whole query text the client sent,
// which for a migration tool is the entire migration file. It is therefore
// split into individual statements and each one is rewritten on its own, using
// its own command tag; commandTag only serves as the fallback for a lone
// statement. Rewriting a multi-statement batch as a unit applied one
// statement's decision to all the others: a migration opening with
// CREATE TYPE ... AS ENUM was dropped whole, taking with it the CREATE TABLE
// that followed.
//
// When convertEnums is true:
//   - CREATE TYPE ... AS ENUM, ALTER TYPE ... ADD/RENAME VALUE (and any ALTER/DROP
//     TYPE targeting a tracked enum) are skipped, since the target has no ENUM
//     types when conversion is enabled.
//   - ENUM column types are rewritten to TEXT/TEXT[] (CREATE TABLE, ALTER TABLE
//     ADD/ALTER COLUMN, and casts/defaults), using tracker to know which type
//     names are enums.
//
// When a renamer with rules is provided, table identifiers are rewritten last
// (after enum conversion, so the renamer never touches type names).
//
// Returns the rewritten SQL and skip=true when the statement must not be applied
// on the target.
func RewriteDDL(ddl, commandTag string, convertEnums bool, tracker *EnumTypeTracker, renamer TableRenamer) (sql string, skip bool) {
	statements := RewriteDDLStatements(ddl, commandTag, convertEnums, tracker, renamer)
	if len(statements) == 0 {
		return "", true
	}
	return strings.Join(statements, "\n"), false
}

// RewriteDDLStatements rewrites the captured DDL and returns the statements to
// replay, one entry per statement, in order. It returns an empty slice when
// nothing must be applied on the target.
//
// Callers should execute the statements one by one rather than as a single
// query: a statement that fails on the target (typically an "already exists"
// when a DDL is replayed after a restart) then no longer takes the rest of the
// migration down with it.
func RewriteDDLStatements(ddl, commandTag string, convertEnums bool, tracker *EnumTypeTracker, renamer TableRenamer) []string {
	statements := SplitStatements(ddl)
	if len(statements) == 0 {
		return nil
	}
	// A lone statement keeps the caller-provided tag: it is the authoritative
	// one, coming from the event trigger.
	if len(statements) == 1 {
		statements[0] = ddl
	}

	kept := make([]string, 0, len(statements))
	for _, stmt := range statements {
		tag := commandTag
		if len(statements) > 1 {
			tag = StatementTag(stmt)
		}
		stmtSQL, stmtSkip := rewriteStatement(stmt, tag, convertEnums, tracker)
		if stmtSkip {
			continue
		}
		if renamer != nil && renamer.HasRules() {
			stmtSQL = string(renamer.RenameInSQL([]byte(stmtSQL)))
		}
		kept = append(kept, stmtSQL)
	}
	return kept
}

// rewriteStatement applies the enum-conversion rules to one statement. The
// table renamer is deliberately left to the caller so it runs once, last, on
// each rewritten statement — after enum conversion, so it never touches type
// names.
func rewriteStatement(ddl, commandTag string, convertEnums bool, tracker *EnumTypeTracker) (sql string, skip bool) {
	tag := strings.ToUpper(strings.TrimSpace(commandTag))
	firstLine := firstNonEmptyLine(ddl)

	if convertEnums {
		switch {
		case strings.HasPrefix(tag, "CREATE TYPE"):
			// CREATE TYPE ... AS ENUM -> no enum type on the target
			if ExtractEnumNameFromCreateType(firstLine) != "" {
				return "", true
			}
		case strings.HasPrefix(tag, "ALTER TYPE"):
			up := strings.ToUpper(ddl)
			// ADD VALUE / RENAME VALUE are enum-only; otherwise check the tracker
			if strings.Contains(up, "ADD VALUE") || strings.Contains(up, "RENAME VALUE") || IsAlterTypeForEnum(firstLine, tracker) {
				return "", true
			}
		case strings.HasPrefix(tag, "DROP TYPE"):
			if dropTargetsTrackedEnum(firstLine, tracker) {
				return "", true
			}
		}
	}

	sql = ddl
	if convertEnums {
		// Strip enum type DDL bundled into a multi-statement query first (this does
		// not require tracked types: CREATE TYPE ... AS ENUM is detected
		// syntactically). Otherwise the enum name would be converted to TEXT,
		// producing invalid SQL such as "CREATE TYPE text AS ENUM ..." or
		// "DROP TYPE text".
		sql = stripEnumTypeStatements(sql, tracker)

		if tracker != nil && tracker.TypeCount() > 0 {
			tracker.ComputeSortedPatterns()
			switch {
			case strings.HasPrefix(tag, "ALTER TABLE") && strings.Contains(sql, "ALTER COLUMN") && strings.Contains(sql, "TYPE"):
				sql = ConvertEnumTypeInAlterColumn(sql, tracker)
			default:
				// Covers CREATE TABLE, ALTER TABLE ADD COLUMN, casts/defaults, etc.
				// We use the line-level converter on the whole statement rather than
				// ConvertEnumColumnsToText: CDC DDL is a single statement (often a
				// single line), and ConvertEnumColumnsToText skips lines starting with
				// "CREATE TABLE", which would miss a single-line table definition.
				sql = ConvertEnumTypeInLine(sql, tracker)
			}
		}
	}

	return sql, false
}

// UpdateTrackerFromEnumDDL keeps the enum tracker in sync with CREATE/ALTER/DROP
// TYPE DDL flowing through the replication stream: it registers newly created
// enums (and enums gaining/renaming values), and unregisters dropped enums.
// Non-type DDL is ignored. Callers are responsible for synchronisation.
func UpdateTrackerFromEnumDDL(tracker *EnumTypeTracker, commandTag, ddl string) {
	if tracker == nil {
		return
	}
	if statements := SplitStatements(ddl); len(statements) > 1 {
		for _, stmt := range statements {
			UpdateTrackerFromEnumDDL(tracker, StatementTag(stmt), stmt)
		}
		return
	}
	tag := strings.ToUpper(strings.TrimSpace(commandTag))
	firstLine := firstNonEmptyLine(ddl)
	switch {
	case strings.HasPrefix(tag, "CREATE TYPE"):
		if name := ExtractEnumNameFromCreateType(firstLine); name != "" {
			tracker.Add(name)
		}
	case strings.HasPrefix(tag, "ALTER TYPE"):
		up := strings.ToUpper(ddl)
		if strings.Contains(up, "ADD VALUE") || strings.Contains(up, "RENAME VALUE") {
			if m := alterTypeRegex.FindStringSubmatch(firstLine); len(m) > 1 {
				tracker.Add(m[1])
			}
		}
	case strings.HasPrefix(tag, "DROP TYPE"):
		if m := dropTypeRegex.FindStringSubmatch(firstLine); len(m) > 1 {
			for _, name := range strings.Split(m[1], ",") {
				name = strings.TrimSpace(name)
				if tracker.IsEnum(name) {
					tracker.Remove(name)
				}
			}
		}
	}
}

// dropTargetsTrackedEnum reports whether a DROP TYPE statement drops at least one
// tracked enum type.
func dropTargetsTrackedEnum(line string, tracker *EnumTypeTracker) bool {
	if tracker == nil {
		return false
	}
	m := dropTypeRegex.FindStringSubmatch(line)
	if len(m) < 2 {
		return false
	}
	for _, name := range strings.Split(m[1], ",") {
		if tracker.IsEnum(strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

// stripEnumTypeStatements removes enum type DDL from a possibly multi-statement
// DDL block: CREATE TYPE ... AS ENUM, ALTER TYPE ... ADD/RENAME VALUE, ALTER TYPE
// targeting a tracked enum, and DROP TYPE of a tracked enum. When converting
// enums to TEXT the target has no enum types, so these statements must be dropped
// rather than converted — a blind enum->text rewrite would otherwise produce
// invalid SQL such as "CREATE TYPE text AS ENUM ..." or "DROP TYPE text".
// current_query() can bundle enum type DDL with unrelated statements (a migration
// block), so the whole event can't be skipped upstream. Statements are matched
// line by line; a multi-line CREATE TYPE is dropped up to its terminating ";".
func stripEnumTypeStatements(sql string, tracker *EnumTypeTracker) string {
	up := strings.ToUpper(sql)
	if !strings.Contains(up, "CREATE TYPE") && !strings.Contains(up, "ALTER TYPE") && !strings.Contains(up, "DROP TYPE") {
		return sql
	}

	lines := strings.Split(sql, "\n")
	kept := make([]string, 0, len(lines))
	skipping := false
	for _, line := range lines {
		if skipping {
			if endsStatement(line) {
				skipping = false
			}
			continue
		}
		if isEnumTypeStatementStart(line, tracker) {
			skipping = !endsStatement(line)
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func endsStatement(line string) bool {
	return strings.HasSuffix(strings.TrimSpace(line), ";")
}

// isEnumTypeStatementStart reports whether line begins an enum type statement
// that must be dropped when converting enums to text.
func isEnumTypeStatementStart(line string, tracker *EnumTypeTracker) bool {
	trimmed := strings.TrimSpace(line)
	up := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(up, "CREATE TYPE"):
		return strings.Contains(up, "AS ENUM")
	case strings.HasPrefix(up, "ALTER TYPE"):
		return strings.Contains(up, "ADD VALUE") || strings.Contains(up, "RENAME VALUE") || IsAlterTypeForEnum(trimmed, tracker)
	case strings.HasPrefix(up, "DROP TYPE"):
		return dropTargetsTrackedEnum(trimmed, tracker)
	}
	return false
}

// firstNonEmptyLine returns the first line that is neither blank nor a SQL line
// comment, so a leading "-- CreateEnum" comment does not hide the CREATE/ALTER/
// DROP TYPE statement it precedes.
func firstNonEmptyLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		return l
	}
	return s
}
