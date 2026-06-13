// SPDX-License-Identifier: Apache-2.0

package postgres

import "strings"

// IsEnumType checks if a column type matches any known ENUM type.
// It handles schema-qualified names (public.MyEnum), quoted names ("MyEnum"),
// and array types (MyEnum[]).
func IsEnumType(columnType string, knownEnums map[string]bool) bool {
	if len(knownEnums) == 0 {
		return false
	}

	// Strip array suffix if present
	typeName := strings.TrimSuffix(columnType, "[]")

	// Check direct match
	if knownEnums[typeName] {
		return true
	}

	// Strip quotes for matching
	cleanName := strings.Trim(typeName, `"`)
	if knownEnums[cleanName] {
		return true
	}

	// Extract just the type name without schema (handles quoted identifiers)
	// Use the original typeName to properly handle fully quoted names like "public"."status"
	typeOnly := extractTypeName(typeName)
	if knownEnums[typeOnly] {
		return true
	}

	return false
}

// extractTypeName extracts the type name from a possibly schema-qualified name.
// Handles quoted identifiers correctly.
// Examples:
//   - public.status -> status
//   - public."Status" -> Status
//   - "public"."Status" -> Status
//   - "my-schema"."my-type" -> my-type
//   - status -> status
func extractTypeName(qualifiedName string) string {
	// Look for the separator dot, but not inside quotes
	inQuotes := false
	lastDotPos := -1

	for i, ch := range qualifiedName {
		if ch == '"' {
			inQuotes = !inQuotes
		} else if ch == '.' && !inQuotes {
			lastDotPos = i
		}
	}

	if lastDotPos == -1 {
		// No unquoted dot found - this is just a type name without schema
		return strings.Trim(qualifiedName, `"`)
	}

	// Return the part after the dot, stripped of quotes
	return strings.Trim(qualifiedName[lastDotPos+1:], `"`)
}
