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

// RewriteDDL rewrites a single raw DDL statement for replication onto the
// target, mirroring the schema snapshot generator so a table created via
// snapshot and via live DDL replication end up identical.
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
	if convertEnums && tracker != nil && tracker.TypeCount() > 0 {
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

	if renamer != nil && renamer.HasRules() {
		sql = string(renamer.RenameInSQL([]byte(sql)))
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

func firstNonEmptyLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			return l
		}
	}
	return s
}
