// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"regexp"
	"sort"
	"strings"
)

// enumTypeTracker tracks ENUM types found in the dump
type enumTypeTracker struct {
	types            map[string]bool // "schema.typename" or "typename" -> true
	sortedPatterns   []patternInfo   // Pre-computed patterns sorted by length (longest first)
	patternsComputed bool            // Track if patterns have been computed
}

// patternInfo holds a pattern string and its length for efficient sorting
type patternInfo struct {
	pattern string
	length  int
}

func newEnumTypeTracker() *enumTypeTracker {
	return &enumTypeTracker{
		types:            make(map[string]bool),
		sortedPatterns:   nil,
		patternsComputed: false,
	}
}

func (et *enumTypeTracker) add(typeName string) {
	// Add the original name
	et.types[typeName] = true

	// Also add cleaned variants
	cleanName := strings.Trim(typeName, `"`)
	et.types[cleanName] = true

	// Add without schema prefix
	if idx := strings.LastIndex(cleanName, "."); idx != -1 {
		typeNameOnly := cleanName[idx+1:]
		et.types[typeNameOnly] = true
		et.types[`"`+typeNameOnly+`"`] = true
	}
}

func (et *enumTypeTracker) isEnum(typeName string) bool {
	// Check direct match
	if et.types[typeName] {
		return true
	}

	// Strip quotes and schema qualification for matching
	cleanName := strings.Trim(typeName, `"`)
	if et.types[cleanName] {
		return true
	}

	// Check without schema prefix
	parts := strings.Split(cleanName, ".")
	if len(parts) > 1 {
		if et.types[parts[len(parts)-1]] {
			return true
		}
	}

	return false
}

// computeSortedPatterns pre-computes all replacement patterns and sorts them by length
// This is called once after all ENUMs have been added to the tracker
func (et *enumTypeTracker) computeSortedPatterns() {
	if et.patternsComputed {
		return // Already computed
	}

	var allPatterns []patternInfo

	// For each tracked ENUM type, generate all possible pattern representations
	for typeName := range et.types {
		// Parse schema and type, handling quoted identifiers correctly
		schema, typeOnly := parseSchemaAndType(typeName)

		if schema != "" {
			// Schema-qualified patterns - try ALL quoting combinations
			// pg_dump can generate any of these variants
			allPatterns = append(allPatterns,
				// Fully quoted
				patternInfo{`"` + schema + `"."` + typeOnly + `"[]`, len(`"` + schema + `"."` + typeOnly + `"[]`)},
				patternInfo{`"` + schema + `"."` + typeOnly + `"`, len(`"` + schema + `"."` + typeOnly + `"`)},
				// Schema unquoted, type quoted
				patternInfo{schema + `."` + typeOnly + `"[]`, len(schema + `."` + typeOnly + `"[]`)},
				patternInfo{schema + `."` + typeOnly + `"`, len(schema + `."` + typeOnly + `"`)},
				// Schema quoted, type unquoted
				patternInfo{`"` + schema + `".` + typeOnly + "[]", len(`"` + schema + `".` + typeOnly + "[]")},
				patternInfo{`"` + schema + `".` + typeOnly, len(`"` + schema + `".` + typeOnly)},
				// Both unquoted
				patternInfo{schema + "." + typeOnly + "[]", len(schema + "." + typeOnly + "[]")},
				patternInfo{schema + "." + typeOnly, len(schema + "." + typeOnly)},
			)
		}

		// Unqualified patterns (no schema) - these are SHORTER, try them LAST
		allPatterns = append(allPatterns,
			patternInfo{`"` + typeOnly + `"[]`, len(`"` + typeOnly + `"[]`)},
			patternInfo{`"` + typeOnly + `"`, len(`"` + typeOnly + `"`)},
			patternInfo{typeOnly + "[]", len(typeOnly + "[]")},
			patternInfo{typeOnly, len(typeOnly)},
		)
	}

	// Sort patterns by length (descending) - longest first
	// This ensures we match schema.type before just type
	sort.Slice(allPatterns, func(i, j int) bool {
		return allPatterns[i].length > allPatterns[j].length
	})

	et.sortedPatterns = allPatterns
	et.patternsComputed = true
}

var (
	// Regular expressions for parsing SQL statements
	createTypeRegex = regexp.MustCompile(`(?i)^CREATE\s+TYPE\s+([^\s]+)\s+AS\s+ENUM`)
	alterTypeRegex  = regexp.MustCompile(`(?i)^ALTER\s+TYPE\s+([^\s]+)`)
)

// parseSchemaAndType parses a PostgreSQL qualified type name into schema and type parts.
// Handles quoted identifiers correctly.
// Examples:
//   - public.status -> ("public", "status")
//   - public."Status" -> ("public", "Status")
//   - "public"."Status" -> ("public", "Status")
//   - "my-schema"."my-type" -> ("my-schema", "my-type")
//   - status -> ("", "status")
func parseSchemaAndType(qualifiedName string) (schema, typeName string) {
	// Remove outer quotes if present
	name := strings.Trim(qualifiedName, `"`)

	// Look for the separator dot, but not inside quotes
	inQuotes := false
	lastDotPos := -1

	for i, ch := range name {
		if ch == '"' {
			inQuotes = !inQuotes
		} else if ch == '.' && !inQuotes {
			lastDotPos = i
		}
	}

	if lastDotPos == -1 {
		// No unquoted dot found - this is just a type name without schema
		return "", strings.Trim(name, `"`)
	}

	// Split at the dot
	schema = strings.Trim(name[:lastDotPos], `"`)
	typeName = strings.Trim(name[lastDotPos+1:], `"`)

	return schema, typeName
}

// extractEnumNameFromCreateType extracts the type name from a CREATE TYPE AS ENUM statement
// Examples:
//   - "CREATE TYPE public.status AS ENUM" -> "public.status"
//   - "CREATE TYPE status AS ENUM" -> "status"
//   - "CREATE TYPE \"my-enum\" AS ENUM" -> "\"my-enum\""
func extractEnumNameFromCreateType(line string) string {
	matches := createTypeRegex.FindStringSubmatch(line)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// collectMultiLineStatement collects lines from scanner until a semicolon is found
// Returns the complete statement as a single string
func collectMultiLineStatement(scanner *bufio.Scanner, firstLine string) string {
	var buffer strings.Builder
	buffer.WriteString(firstLine)
	buffer.WriteString("\n")

	// If first line already ends with semicolon, return it
	if strings.HasSuffix(strings.TrimSpace(firstLine), ";") {
		return buffer.String()
	}

	// Collect remaining lines until semicolon
	for scanner.Scan() {
		line := scanner.Text()
		buffer.WriteString(line)
		buffer.WriteString("\n")

		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			break
		}
	}

	return buffer.String()
}

// convertEnumColumnsToText converts ENUM column types to TEXT in a CREATE TABLE statement
// It handles both simple and array ENUM types
func convertEnumColumnsToText(createTableSQL string, tracker *enumTypeTracker) string {
	if tracker == nil || len(tracker.types) == 0 {
		return createTableSQL
	}

	lines := strings.Split(createTableSQL, "\n")
	var result strings.Builder

	for _, line := range lines {
		// Skip if not a column definition line
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "CREATE TABLE") ||
			strings.HasPrefix(trimmed, ");") ||
			strings.HasPrefix(trimmed, "PRIMARY KEY") ||
			strings.HasPrefix(trimmed, "UNIQUE") ||
			strings.HasPrefix(trimmed, "CHECK") ||
			strings.HasPrefix(trimmed, "CONSTRAINT") ||
			trimmed == "" {
			result.WriteString(line)
			result.WriteString("\n")
			continue
		}

		// Try to parse column definition
		convertedLine := convertEnumTypeInLine(line, tracker)
		result.WriteString(convertedLine)
		result.WriteString("\n")
	}

	return strings.TrimRight(result.String(), "\n")
}

// convertEnumTypeInLine converts ENUM types to TEXT in a single line
// Handles column definitions, DEFAULT values with casts, etc.
func convertEnumTypeInLine(line string, tracker *enumTypeTracker) string {
	if tracker == nil {
		return line
	}

	// Ensure patterns are computed (done once)
	if !tracker.patternsComputed {
		tracker.computeSortedPatterns()
	}

	result := line

	// Use pre-computed sorted patterns (already sorted longest-first)
	for _, p := range tracker.sortedPatterns {
		pattern := p.pattern

		// Skip if pattern not in the line
		if !strings.Contains(result, pattern) {
			continue
		}

		isArray := strings.HasSuffix(pattern, "[]")

		if isArray {
			// For array types, do simple string replacement
			result = strings.ReplaceAll(result, pattern, "text[]")
		} else {
			// For non-array types, use regex to ensure we don't replace partial matches
			// Match the pattern followed by a space, comma, semicolon, or end of string
			regex := regexp.MustCompile(regexp.QuoteMeta(pattern) + `(\s|,|;|\)|$)`)
			result = regex.ReplaceAllString(result, "text${1}")
		}

		// Also handle type casts like ::schema.type or ::type
		result = strings.ReplaceAll(result, "::"+pattern, "::text")

		// Note: We DON'T break here anymore because a line can contain multiple ENUM types
		// (e.g., function signatures with multiple ENUM parameters)
		// We need to check all patterns to convert all ENUMs in the line
	}

	return result
}

// convertEnumTypeInAlterColumn converts ENUM type to TEXT in ALTER COLUMN TYPE statements
// Example: "ALTER TABLE users ALTER COLUMN status TYPE public.status"
//
//	-> "ALTER TABLE users ALTER COLUMN status TYPE text"
func convertEnumTypeInAlterColumn(alterSQL string, tracker *enumTypeTracker) string {
	if tracker == nil || len(tracker.types) == 0 {
		return alterSQL
	}

	// Pattern: ALTER TABLE ... ALTER COLUMN ... TYPE <type>
	result := alterSQL

	for typeName := range tracker.types {
		patterns := []struct {
			pattern     *regexp.Regexp
			replacement string
		}{
			// Array type
			{
				regexp.MustCompile(`(?i)(TYPE\s+)` + regexp.QuoteMeta(typeName) + `\[\]`),
				`${1}text[]`,
			},
			// Simple type
			{
				regexp.MustCompile(`(?i)(TYPE\s+)` + regexp.QuoteMeta(typeName) + `(\s|;|$)`),
				`${1}text$2`,
			},
		}

		for _, p := range patterns {
			result = p.pattern.ReplaceAllString(result, p.replacement)
		}
	}

	return result
}

// isAlterTypeForEnum checks if an ALTER TYPE statement is for an ENUM type
func isAlterTypeForEnum(line string, tracker *enumTypeTracker) bool {
	if tracker == nil {
		return false
	}

	// Extract type name from ALTER TYPE statement
	matches := alterTypeRegex.FindStringSubmatch(line)
	if len(matches) > 1 {
		typeName := matches[1]
		return tracker.isEnum(typeName)
	}

	return false
}
