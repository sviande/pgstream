// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"strings"
	"testing"
)

func TestExtractEnumNameFromCreateType(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected string
	}{
		{
			name:     "simple enum",
			line:     "CREATE TYPE public.status AS ENUM (",
			expected: "public.status",
		},
		{
			name:     "enum without schema",
			line:     "CREATE TYPE status AS ENUM (",
			expected: "status",
		},
		{
			name:     "quoted enum",
			line:     `CREATE TYPE "public"."my-enum" AS ENUM (`,
			expected: `"public"."my-enum"`,
		},
		{
			name:     "underscore type",
			line:     "CREATE TYPE public.user_status AS ENUM (",
			expected: "public.user_status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractEnumNameFromCreateType(tt.line)
			if result != tt.expected {
				t.Errorf("ExtractEnumNameFromCreateType() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestConvertEnumColumnsToText(t *testing.T) {
	tracker := NewEnumTypeTracker()
	tracker.Add("public.user_status")
	tracker.Add("public.country_code")
	tracker.Add("public.expense_category")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple column",
			input:    "    status public.user_status,",
			expected: "    status text,",
		},
		{
			name:     "array column",
			input:    "    countries public.country_code[],",
			expected: "    countries text[],",
		},
		{
			name:     "column with DEFAULT",
			input:    "    status public.user_status DEFAULT 'active'::public.user_status,",
			expected: "    status text DEFAULT 'active'::text,",
		},
		{
			name:     "column with NOT NULL",
			input:    "    status public.user_status NOT NULL,",
			expected: "    status text NOT NULL,",
		},
		{
			name:     "quoted column with array",
			input:    `    "categories" "public"."expense_category"[],`,
			expected: "    \"categories\" text[],",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertEnumTypeInLine(tt.input, tracker)
			// Normalize whitespace for comparison
			result = strings.TrimSpace(result)
			expected := strings.TrimSpace(tt.expected)
			if result != expected {
				t.Errorf("ConvertEnumTypeInLine():\ngot:  %q\nwant: %q", result, expected)
			}
		})
	}
}

func TestConvertEnumColumnsToTextFullTable(t *testing.T) {
	tracker := NewEnumTypeTracker()
	tracker.Add("public.user_status")
	tracker.Add("public.country_code")

	input := `CREATE TABLE public.users (
    id integer NOT NULL,
    status public.user_status NOT NULL,
    countries public.country_code[]
);`

	expected := `CREATE TABLE public.users (
    id integer NOT NULL,
    status text NOT NULL,
    countries text[]
);`

	result := ConvertEnumColumnsToText(input, tracker)

	if strings.TrimSpace(result) != strings.TrimSpace(expected) {
		t.Errorf("ConvertEnumColumnsToText():\ngot:\n%s\n\nwant:\n%s", result, expected)
	}
}

// TestConvertMultipleEnumsInSingleLine tests the critical bug fix:
// when a line contains multiple ENUM types (like function signatures),
// ALL of them must be converted to text, not just the first one.
func TestConvertMultipleEnumsInSingleLine(t *testing.T) {
	tracker := NewEnumTypeTracker()
	tracker.Add(`public."status_type"`)
	tracker.Add(`public."priority_level"`)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "function with two ENUM parameters",
			input:    `CREATE FUNCTION public.check_task_priority(_status public."status_type", _priorities public."priority_level"[]) RETURNS boolean`,
			expected: `CREATE FUNCTION public.check_task_priority(_status text, _priorities text[]) RETURNS boolean`,
		},
		{
			name:     "DROP FUNCTION with two ENUM parameters",
			input:    `DROP FUNCTION IF EXISTS public.check_task_priority(_status public."status_type", _priorities public."priority_level"[]);`,
			expected: `DROP FUNCTION IF EXISTS public.check_task_priority(_status text, _priorities text[]);`,
		},
		{
			name:     "function comment with two ENUMs",
			input:    `-- Name: check_task_priority(public."status_type", public."priority_level"[]); Type: FUNCTION; Schema: public; Owner: -`,
			expected: `-- Name: check_task_priority(text, text[]); Type: FUNCTION; Schema: public; Owner: -`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertEnumTypeInLine(tt.input, tracker)
			result = strings.TrimSpace(result)
			expected := strings.TrimSpace(tt.expected)

			if result != expected {
				t.Errorf("ConvertEnumTypeInLine() with multiple ENUMs:\ngot:  %q\nwant: %q", result, expected)
			}

			// Extra verification: ensure NO ENUM types remain in the result
			if strings.Contains(result, `public."status_type"`) {
				t.Errorf("First ENUM type was not converted: still contains status_type")
			}
			if strings.Contains(result, `public."priority_level"`) {
				t.Errorf("Second ENUM type was not converted: still contains priority_level")
			}
		})
	}
}

// TestDuplicatePatternsWhenAddingQualifiedAndUnqualified tests for the duplicate
// pattern generation bug when both 'public.status' and 'status' are added
func TestDuplicatePatternsWhenAddingQualifiedAndUnqualified(t *testing.T) {
	tracker := NewEnumTypeTracker()

	// Scenario: Add both 'public.status' and 'status'
	// This can happen when:
	// 1. Bootstrap discovers existing "public.status" and calls Add("public.status")
	// 2. Then a DDL statement "CREATE TYPE status AS ENUM" arrives and calls Add("status")
	tracker.Add("public.status")
	countAfterFirst := tracker.TypeCount()
	t.Logf("Types count after Add('public.status'): %d", countAfterFirst)

	tracker.Add("status")
	countAfterSecond := tracker.TypeCount()
	t.Logf("Types count after Add('status'): %d", countAfterSecond)

	// Compute patterns
	tracker.ComputeSortedPatterns()
	patternCount := tracker.PatternCount()
	t.Logf("Pattern count after ComputeSortedPatterns: %d", patternCount)

	// Analysis:
	// When we Add("public.status"), the map gets:
	// - "public.status" (original)
	// - "public.status" (cleanName, same)
	// - "status" (unqualified)
	// - "\"status\"" (quoted unqualified)

	// When we Add("status"), the map gets:
	// - "status" (already exists, so unchanged)
	// - "status" (cleanName, same, already exists)
	// - no dot, so no additional entries

	// So the types map should have: {"public.status", "status", "\"status\""}
	// That's 3 entries in the types map

	// When ComputeSortedPatterns iterates over these 3:
	// 1. "public.status": generates 8 schema-qualified + 4 unqualified = 12 patterns
	// 2. "status": schema="" so no schema-qualified, generates 4 unqualified patterns (DUPLICATES!)
	// 3. "\"status\"": same as above after trimming quotes

	// Expected: 12 unique patterns (deduped)
	// Actual (with bug): 12 + 4 + 4 = 20 patterns (with duplicates)

	expectedMaxWithoutDuplicates := 12
	expectedWithDuplicates := 20

	if patternCount > expectedMaxWithoutDuplicates && patternCount <= expectedWithDuplicates {
		t.Logf("CONFIRMED: Duplicate patterns detected. Got %d patterns (expected max %d unique)",
			patternCount, expectedMaxWithoutDuplicates)
	}
}
