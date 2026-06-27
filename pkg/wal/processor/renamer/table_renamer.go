// SPDX-License-Identifier: Apache-2.0

package renamer

import (
	"fmt"
	"regexp"
	"strings"
)

// Rule defines a table renaming rule with regex pattern matching.
type Rule struct {
	// Schema filter - only apply to tables in this schema.
	// Use "*" or empty string to match all schemas.
	Schema string
	// Match is the regex pattern to match against the table name.
	Match string
	// Replace is the replacement string (supports capture groups $1, $2, etc.).
	Replace string
}

// Config holds the configuration for table renaming.
type Config struct {
	Rules []Rule
}

// compiledRule holds a pre-compiled regex rule for efficient matching.
type compiledRule struct {
	schema     string
	matchRegex *regexp.Regexp
	replaceStr string
}

// TableRenamer provides table name transformation using regex rules.
// It can be used by both the WAL processor and the schema snapshot generator.
type TableRenamer struct {
	rules []compiledRule
}

// NewTableRenamer creates a new TableRenamer with the given configuration.
// Returns an error if any regex pattern is invalid.
func NewTableRenamer(cfg *Config) (*TableRenamer, error) {
	if cfg == nil || len(cfg.Rules) == 0 {
		return &TableRenamer{rules: nil}, nil
	}

	rules := make([]compiledRule, 0, len(cfg.Rules))
	for i, rule := range cfg.Rules {
		if rule.Match == "" {
			return nil, fmt.Errorf("rule %d: match pattern cannot be empty", i)
		}

		compiled, err := regexp.Compile(rule.Match)
		if err != nil {
			return nil, fmt.Errorf("rule %d: invalid regex pattern %q: %w", i, rule.Match, err)
		}

		schema := rule.Schema
		if schema == "" {
			schema = "*" // default to all schemas
		}

		rules = append(rules, compiledRule{
			schema:     schema,
			matchRegex: compiled,
			replaceStr: rule.Replace,
		})
	}

	return &TableRenamer{rules: rules}, nil
}

// RenameTable applies the renaming rules to a table name.
// Rules are applied in order, and each rule can modify the result of the previous one.
func (r *TableRenamer) RenameTable(schema, table string) string {
	if r == nil || len(r.rules) == 0 {
		return table
	}

	result := table
	for _, rule := range r.rules {
		// Check schema filter
		if rule.schema != "*" && rule.schema != schema {
			continue
		}

		// Apply regex replacement if it matches
		if rule.matchRegex.MatchString(result) {
			result = rule.matchRegex.ReplaceAllString(result, rule.replaceStr)
		}
	}

	return result
}

// HasRules returns true if there are any renaming rules configured.
func (r *TableRenamer) HasRules() bool {
	return r != nil && len(r.rules) > 0
}

// RenameInSQL applies the renaming rules to table names in a SQL dump.
// It handles PostgreSQL quoted identifiers ("schema"."table").
func (r *TableRenamer) RenameInSQL(sql []byte) []byte {
	if r == nil || len(r.rules) == 0 {
		return sql
	}

	result := string(sql)

	for _, rule := range r.rules {
		result = r.renameTablesInSQLForRule(result, rule)
	}

	return []byte(result)
}

// renameTablesInSQLForRule applies a single rule to all table occurrences in SQL.
func (r *TableRenamer) renameTablesInSQLForRule(sql string, rule compiledRule) string {
	// Pattern to match schema-qualified table names in SQL.
	// pg_dump generates different formats:
	// 1. "schema"."table" - both quoted (rare)
	// 2. schema."TableName" - schema unquoted, table quoted (common for mixed case)
	// 3. schema.tablename - both unquoted (common for lowercase)

	// Pattern 1: "schema"."table" - both quoted
	quotedPattern := regexp.MustCompile(`"([^"]+)"\."([^"]+)"`)

	result := quotedPattern.ReplaceAllStringFunc(sql, func(match string) string {
		parts := quotedPattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		schema := parts[1]
		table := parts[2]

		// Check schema filter
		if rule.schema != "*" && rule.schema != schema {
			return match
		}

		// Apply regex replacement to table name
		if rule.matchRegex.MatchString(table) {
			newTable := rule.matchRegex.ReplaceAllString(table, rule.replaceStr)
			return fmt.Sprintf(`"%s"."%s"`, schema, newTable)
		}

		return match
	})

	// Pattern 2: schema."table" - schema unquoted, table quoted (common pg_dump format)
	mixedPattern := regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)\."([^"]+)"`)

	result = mixedPattern.ReplaceAllStringFunc(result, func(match string) string {
		parts := mixedPattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		schema := parts[1]
		table := parts[2]

		// Skip system schemas
		if isReservedWord(schema) {
			return match
		}

		// Check schema filter
		if rule.schema != "*" && rule.schema != schema {
			return match
		}

		// Apply regex replacement to table name
		if rule.matchRegex.MatchString(table) {
			newTable := rule.matchRegex.ReplaceAllString(table, rule.replaceStr)
			return fmt.Sprintf(`%s."%s"`, schema, newTable)
		}

		return match
	})

	// Pattern 3: schema.table - both unquoted (lowercase names)
	unquotedPattern := regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)

	result = unquotedPattern.ReplaceAllStringFunc(result, func(match string) string {
		parts := unquotedPattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		schema := parts[1]
		table := parts[2]

		// Skip system schemas and already processed patterns (with quotes)
		if isReservedWord(schema) || isReservedWord(table) {
			return match
		}

		// Check schema filter
		if rule.schema != "*" && rule.schema != schema {
			return match
		}

		// Apply regex replacement to table name
		if rule.matchRegex.MatchString(table) {
			newTable := rule.matchRegex.ReplaceAllString(table, rule.replaceStr)
			return fmt.Sprintf("%s.%s", schema, newTable)
		}

		return match
	})

	// Pattern 4: unqualified quoted table name preceded by a keyword that
	// introduces a table reference (e.g. ALTER TABLE "User", CREATE INDEX ... ON
	// "User", REFERENCES "Account", and the DML keywords of backfill statements
	// embedded in a captured DDL block such as UPDATE "User" / FROM "User"). Live
	// DDL frequently omits the schema qualifier (current_query() captures it as
	// typed), so patterns 1-3 miss it and the statement is replayed verbatim on a
	// renamed target -> "relation \"User\" does not exist".
	//
	// Restricted to *quoted* identifiers right after a keyword so it never
	// touches column names, values or keywords (IF, NOT, EXISTS...). The schema
	// cannot be known for an unqualified name, so the rule is applied regardless
	// of its schema filter (assumes the default/search-path schema). FROM inside
	// a value-expression function is filtered out (see renameUnqualifiedTables).
	result = renameUnqualifiedTables(result, rule)

	return result
}

// unqualifiedTablePattern matches a quoted identifier that directly follows a
// keyword introducing a table reference. It covers both structural DDL keywords
// (TABLE, ONLY, ON, REFERENCES, INHERITS, TRUNCATE) and the DML keywords used by
// backfill statements embedded in a captured DDL block (UPDATE, JOIN, FROM,
// INTO, USING) — e.g. "ALTER TABLE ...; UPDATE ... FROM ...;" captured as a
// single current_query() string. The third group captures an optional trailing
// dot so qualified names (schema part) can be skipped.
//
// FROM is ambiguous: in the value-expression functions EXTRACT/TRIM/SUBSTRING/
// OVERLAY it separates arguments rather than introducing a table
// (EXTRACT(YEAR FROM "createdAt")). Those occurrences are filtered out via
// valueExprFuncRanges so a quoted column there is never mistaken for a table.
var unqualifiedTablePattern = regexp.MustCompile(`(?i)(\b(?:TABLE|ONLY|ON|REFERENCES|INHERITS|TRUNCATE|UPDATE|JOIN|FROM|INTO|USING)\s+)"([^"]+)"(\.?)`)

// valueExprFuncStart matches the opening "<func>(" of a value-expression
// function whose FROM is an argument separator rather than a clause keyword.
var valueExprFuncStart = regexp.MustCompile(`(?i)\b(?:EXTRACT|TRIM|SUBSTRING|OVERLAY)\s*\(`)

// renameUnqualifiedTables applies a single rule to unqualified, quoted table
// references introduced by a keyword (see unqualifiedTablePattern). It walks the
// matches by index so it can skip matches that fall inside string literals or
// comments (data/comment text, not tables) and a FROM that is an argument
// separator of a value-expression function rather than a clause.
func renameUnqualifiedTables(sql string, rule compiledRule) string {
	matches := unqualifiedTablePattern.FindAllStringSubmatchIndex(sql, -1)
	if len(matches) == 0 {
		return sql
	}

	// Single-quoted strings and comments hold data/comment text: a keyword +
	// quoted identifier there (e.g. 'see UPDATE "Audit"') is not a table. Note
	// dollar-quoted function bodies are deliberately NOT skipped — their table
	// references must be renamed too on the target.
	skipRanges := literalAndCommentRanges(sql)

	var valueExprRanges [][2]int
	valueExprComputed := false

	var b strings.Builder
	last := 0
	for _, m := range matches {
		matchStart, matchEnd := m[0], m[1]
		keyword := sql[m[2]:m[3]]
		table := sql[m[4]:m[5]]
		trailingDot := sql[m[6]:m[7]]

		// A trailing dot means this is the schema part of a qualified name
		// (e.g. ON "schema"."table"); leave it to patterns 1-3.
		if trailingDot == "." {
			continue
		}

		// Inside a string literal or comment: this is data/comment text.
		if positionInRanges(matchStart, skipRanges) {
			continue
		}

		// FROM is ambiguous: inside EXTRACT/TRIM/SUBSTRING/OVERLAY it separates
		// arguments and the following quoted identifier is a column, not a table.
		if strings.EqualFold(strings.TrimSpace(keyword), "FROM") {
			if !valueExprComputed {
				valueExprRanges = valueExprFuncRanges(sql)
				valueExprComputed = true
			}
			if positionInRanges(matchStart, valueExprRanges) {
				continue
			}
		}

		if !rule.matchRegex.MatchString(table) {
			continue
		}

		newTable := rule.matchRegex.ReplaceAllString(table, rule.replaceStr)
		b.WriteString(sql[last:matchStart])
		b.WriteString(keyword)
		b.WriteByte('"')
		b.WriteString(newTable)
		b.WriteByte('"')
		last = matchEnd
	}
	b.WriteString(sql[last:])
	return b.String()
}

// valueExprFuncRanges returns the [openParen, closeParen) ranges of the argument
// lists of value-expression functions that use FROM as an argument separator
// (EXTRACT, TRIM, SUBSTRING, OVERLAY). A FROM whose position falls inside one of
// these ranges introduces a column, not a table, and must not be renamed.
func valueExprFuncRanges(sql string) [][2]int {
	locs := valueExprFuncStart.FindAllStringIndex(sql, -1)
	if len(locs) == 0 {
		return nil
	}
	ranges := make([][2]int, 0, len(locs))
	for _, loc := range locs {
		open := loc[1] - 1 // index of the '(' captured by the pattern
		if closeIdx := matchingParen(sql, open); closeIdx >= 0 {
			ranges = append(ranges, [2]int{open, closeIdx})
		}
	}
	return ranges
}

func positionInRanges(pos int, ranges [][2]int) bool {
	for _, rg := range ranges {
		if pos >= rg[0] && pos < rg[1] {
			return true
		}
	}
	return false
}

// matchingParen returns the index of the ')' that closes the '(' at open,
// skipping single-quoted strings, double-quoted identifiers and dollar-quoted
// strings so parentheses inside them are not miscounted. Returns -1 if no
// matching paren is found.
func matchingParen(sql string, open int) int {
	depth := 0
	for i := open; i < len(sql); {
		switch sql[i] {
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
			i++
		case '\'':
			i = skipQuoted(sql, i, '\'')
		case '"':
			i = skipQuoted(sql, i, '"')
		case '$':
			if next, ok := skipDollarQuoted(sql, i); ok {
				i = next
			} else {
				i++
			}
		default:
			i++
		}
	}
	return -1
}

// skipQuoted returns the index just past a quote-delimited token starting at i
// (sql[i] == q), treating a doubled quote (” or "") as an escaped quote.
func skipQuoted(sql string, i int, q byte) int {
	for i++; i < len(sql); i++ {
		if sql[i] != q {
			continue
		}
		if i+1 < len(sql) && sql[i+1] == q {
			i++ // skip the escaped quote; loop's i++ moves past the pair
			continue
		}
		return i + 1
	}
	return i
}

// skipDollarQuoted handles a $tag$...$tag$ (or $$...$$) string starting at i
// (sql[i] == '$'). It returns the index just past the closing tag and true, or
// (i, false) if i does not start a valid dollar-quote opener.
func skipDollarQuoted(sql string, i int) (int, bool) {
	j := i + 1
	for j < len(sql) && isIdentByte(sql[j]) {
		j++
	}
	if j >= len(sql) || sql[j] != '$' {
		return i, false
	}
	tag := sql[i : j+1]
	if idx := strings.Index(sql[j+1:], tag); idx >= 0 {
		return j + 1 + idx + len(tag), true
	}
	return len(sql), true // unterminated; consume the rest
}

func isIdentByte(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// literalAndCommentRanges returns the [start, end) ranges of single-quoted
// string literals, line comments (-- ... \n) and block comments (/* ... */,
// nestable as in PostgreSQL). A keyword/table match inside one of these is data
// or comment text and must not be rewritten. Double-quoted identifiers and
// dollar-quoted strings are scanned over but NOT returned: identifiers hold the
// table names we rename, and dollar-quoted function bodies must be renamed too.
func literalAndCommentRanges(sql string) [][2]int {
	var ranges [][2]int
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			start := i
			i = skipQuoted(sql, i, '\'')
			ranges = append(ranges, [2]int{start, i})
		case sql[i] == '"':
			i = skipQuoted(sql, i, '"')
		case sql[i] == '$':
			if next, ok := skipDollarQuoted(sql, i); ok {
				i = next
			} else {
				i++
			}
		case sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-':
			start := i
			i = skipLineComment(sql, i)
			ranges = append(ranges, [2]int{start, i})
		case sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*':
			start := i
			i = skipBlockComment(sql, i)
			ranges = append(ranges, [2]int{start, i})
		default:
			i++
		}
	}
	return ranges
}

// skipLineComment returns the index of the newline ending the -- comment at i
// (or len(sql) if the comment runs to EOF).
func skipLineComment(sql string, i int) int {
	for ; i < len(sql); i++ {
		if sql[i] == '\n' {
			return i
		}
	}
	return i
}

// skipBlockComment returns the index just past the */ closing the /* comment at
// i, honouring PostgreSQL's nestable block comments. Returns len(sql) if the
// comment is unterminated.
func skipBlockComment(sql string, i int) int {
	depth := 0
	for i < len(sql) {
		switch {
		case i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*':
			depth++
			i += 2
		case i+1 < len(sql) && sql[i] == '*' && sql[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return i
}

// isReservedWord checks if a word is a SQL reserved word that should not be treated as a table name.
func isReservedWord(word string) bool {
	reserved := map[string]bool{
		"pg_catalog":         true,
		"information_schema": true,
		"pg_toast":           true,
		"pg_temp":            true,
	}
	return reserved[strings.ToLower(word)]
}
