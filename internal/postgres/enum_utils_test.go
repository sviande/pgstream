// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

func TestIsEnumType(t *testing.T) {
	t.Parallel()

	knownEnums := map[string]bool{
		"status":   true,
		"category": true,
	}

	tests := []struct {
		name       string
		columnType string
		expected   bool
	}{
		{"direct match", "status", true},
		{"schema qualified", "public.status", true},
		{"quoted", `"status"`, true},
		{"schema and quoted", `public."status"`, true},
		{"fully quoted", `"public"."status"`, true},
		{"array type", "status[]", true},
		{"schema qualified array", "public.status[]", true},
		{"quoted array", `"status"[]`, true},
		{"schema and quoted array", `public."status"[]`, true},
		{"not enum", "text", false},
		{"unknown type", "other", false},
		{"partial match should not match", "status_extended", false},
		{"empty type", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsEnumType(tt.columnType, knownEnums)
			if result != tt.expected {
				t.Errorf("IsEnumType(%q) = %v, expected %v", tt.columnType, result, tt.expected)
			}
		})
	}
}

func TestIsEnumType_EmptyKnownEnums(t *testing.T) {
	t.Parallel()

	result := IsEnumType("status", map[string]bool{})
	if result {
		t.Error("IsEnumType should return false for empty knownEnums")
	}

	result = IsEnumType("status", nil)
	if result {
		t.Error("IsEnumType should return false for nil knownEnums")
	}
}
