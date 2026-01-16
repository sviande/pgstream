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

	return result
}

// isReservedWord checks if a word is a SQL reserved word that should not be treated as a table name.
func isReservedWord(word string) bool {
	reserved := map[string]bool{
		"pg_catalog": true,
		"information_schema": true,
		"pg_toast": true,
		"pg_temp": true,
	}
	return reserved[strings.ToLower(word)]
}
