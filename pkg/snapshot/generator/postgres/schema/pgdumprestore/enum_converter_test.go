// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

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
			result := extractEnumNameFromCreateType(tt.line)
			if result != tt.expected {
				t.Errorf("extractEnumNameFromCreateType() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestConvertEnumColumnsToText(t *testing.T) {
	tracker := newEnumTypeTracker()
	tracker.add("public.user_status")
	tracker.add("public.country_code")
	tracker.add("public.expense_category")

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
			result := convertEnumTypeInLine(tt.input, tracker)
			// Normalize whitespace for comparison
			result = strings.TrimSpace(result)
			expected := strings.TrimSpace(tt.expected)
			if result != expected {
				t.Errorf("convertEnumTypeInLine():\ngot:  %q\nwant: %q", result, expected)
			}
		})
	}
}

func TestConvertEnumColumnsToTextFullTable(t *testing.T) {
	tracker := newEnumTypeTracker()
	tracker.add("public.user_status")
	tracker.add("public.country_code")

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

	result := convertEnumColumnsToText(input, tracker)

	if strings.TrimSpace(result) != strings.TrimSpace(expected) {
		t.Errorf("convertEnumColumnsToText():\ngot:\n%s\n\nwant:\n%s", result, expected)
	}
}

// TestConvertMultipleEnumsInSingleLine tests the critical bug fix:
// when a line contains multiple ENUM types (like function signatures),
// ALL of them must be converted to text, not just the first one.
func TestConvertMultipleEnumsInSingleLine(t *testing.T) {
	tracker := newEnumTypeTracker()
	tracker.add(`public."status_type"`)
	tracker.add(`public."priority_level"`)

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
			result := convertEnumTypeInLine(tt.input, tracker)
			result = strings.TrimSpace(result)
			expected := strings.TrimSpace(tt.expected)

			if result != expected {
				t.Errorf("convertEnumTypeInLine() with multiple ENUMs:\ngot:  %q\nwant: %q", result, expected)
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
