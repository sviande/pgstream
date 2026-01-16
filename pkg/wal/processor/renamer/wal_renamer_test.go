// SPDX-License-Identifier: Apache-2.0

package renamer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func TestNew(t *testing.T) {
	t.Parallel()

	mockProcessor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			return nil
		},
	}

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: false,
		},
		{
			name:    "empty rules",
			config:  &Config{Rules: []Rule{}},
			wantErr: false,
		},
		{
			name: "valid config",
			config: &Config{
				Rules: []Rule{
					{Schema: "public", Match: "^prod_(.+)$", Replace: "dev_$1"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid regex",
			config: &Config{
				Rules: []Rule{
					{Schema: "public", Match: "[invalid", Replace: "test"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty match pattern",
			config: &Config{
				Rules: []Rule{
					{Schema: "public", Match: "", Replace: "test"},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			renamer, err := New(mockProcessor, tc.config)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, renamer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, renamer)
			}
		})
	}
}

func TestRenamer_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        *Config
		inputEvent    *wal.Event
		expectedTable string
	}{
		{
			name:   "nil event",
			config: &Config{Rules: []Rule{{Schema: "*", Match: ".*", Replace: "renamed"}}},
			inputEvent: nil,
			expectedTable: "",
		},
		{
			name:   "nil data",
			config: &Config{Rules: []Rule{{Schema: "*", Match: ".*", Replace: "renamed"}}},
			inputEvent: &wal.Event{Data: nil},
			expectedTable: "",
		},
		{
			name:   "no rules - passthrough",
			config: nil,
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "users"},
			},
			expectedTable: "users",
		},
		{
			name: "simple replacement",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "^old_", Replace: "new_"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "old_users"},
			},
			expectedTable: "new_users",
		},
		{
			name: "capture group replacement",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "^prod_(.+)$", Replace: "dev_$1"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "prod_orders"},
			},
			expectedTable: "dev_orders",
		},
		{
			name: "schema filter - matches",
			config: &Config{
				Rules: []Rule{
					{Schema: "public", Match: "^test_", Replace: "prod_"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "test_users"},
			},
			expectedTable: "prod_users",
		},
		{
			name: "schema filter - no match",
			config: &Config{
				Rules: []Rule{
					{Schema: "private", Match: "^test_", Replace: "prod_"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "test_users"},
			},
			expectedTable: "test_users",
		},
		{
			name: "multiple rules - chaining",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "^old_", Replace: "new_"},
					{Schema: "*", Match: "_v1$", Replace: "_v2"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "old_users_v1"},
			},
			expectedTable: "new_users_v2",
		},
		{
			name: "no match - unchanged",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "^nonexistent_", Replace: "replaced_"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "users"},
			},
			expectedTable: "users",
		},
		{
			name: "remove suffix",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "(.+)_legacy$", Replace: "$1"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "orders_legacy"},
			},
			expectedTable: "orders",
		},
		{
			name: "add prefix",
			config: &Config{
				Rules: []Rule{
					{Schema: "*", Match: "^(.+)$", Replace: "imported_$1"},
				},
			},
			inputEvent: &wal.Event{
				Data: &wal.Data{Schema: "public", Table: "users"},
			},
			expectedTable: "imported_users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var processedEvent *wal.Event
			mockProcessor := &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					processedEvent = walEvent
					return nil
				},
			}

			renamer, err := New(mockProcessor, tc.config)
			require.NoError(t, err)

			err = renamer.ProcessWALEvent(context.Background(), tc.inputEvent)
			require.NoError(t, err)

			if tc.inputEvent == nil || tc.inputEvent.Data == nil {
				require.Equal(t, tc.inputEvent, processedEvent)
			} else {
				require.NotNil(t, processedEvent)
				require.NotNil(t, processedEvent.Data)
				require.Equal(t, tc.expectedTable, processedEvent.Data.Table)
			}
		})
	}
}

func TestTableRenamer_RenameTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rules    []Rule
		schema   string
		table    string
		expected string
	}{
		{
			name:     "no rules",
			rules:    nil,
			schema:   "public",
			table:    "users",
			expected: "users",
		},
		{
			name: "wildcard schema",
			rules: []Rule{
				{Schema: "*", Match: "^old_", Replace: "new_"},
			},
			schema:   "any_schema",
			table:    "old_users",
			expected: "new_users",
		},
		{
			name: "empty schema defaults to wildcard",
			rules: []Rule{
				{Schema: "", Match: "^old_", Replace: "new_"},
			},
			schema:   "any_schema",
			table:    "old_users",
			expected: "new_users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Rules: tc.rules}
			renamer, err := NewTableRenamer(cfg)
			require.NoError(t, err)

			result := renamer.RenameTable(tc.schema, tc.table)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestTableRenamer_RenameInSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rules    []Rule
		input    string
		expected string
	}{
		{
			name:     "no rules",
			rules:    nil,
			input:    `CREATE TABLE "public"."users" (id int);`,
			expected: `CREATE TABLE "public"."users" (id int);`,
		},
		{
			name: "rename quoted table",
			rules: []Rule{
				{Schema: "public", Match: "^old_", Replace: "new_"},
			},
			input:    `CREATE TABLE "public"."old_users" (id int);`,
			expected: `CREATE TABLE "public"."new_users" (id int);`,
		},
		{
			name: "rename multiple occurrences",
			rules: []Rule{
				{Schema: "*", Match: "^prod_", Replace: "dev_"},
			},
			input: `CREATE TABLE "public"."prod_users" (id int);
ALTER TABLE "public"."prod_users" ADD COLUMN name text;`,
			expected: `CREATE TABLE "public"."dev_users" (id int);
ALTER TABLE "public"."dev_users" ADD COLUMN name text;`,
		},
		{
			name: "schema filter in SQL",
			rules: []Rule{
				{Schema: "private", Match: "^test_", Replace: "prod_"},
			},
			input:    `CREATE TABLE "public"."test_users" (id int);`,
			expected: `CREATE TABLE "public"."test_users" (id int);`,
		},
		{
			name: "rename mixed format - schema unquoted, table quoted (pg_dump format)",
			rules: []Rule{
				{Schema: "*", Match: "^(.+)$", Replace: "newprefix_$1"},
			},
			input:    `CREATE TABLE public."Users" (id int);`,
			expected: `CREATE TABLE public."newprefix_Users" (id int);`,
		},
		{
			name: "rename unquoted lowercase table",
			rules: []Rule{
				{Schema: "*", Match: "^(.+)$", Replace: "newprefix_$1"},
			},
			input:    `CREATE TABLE public.users (id int);`,
			expected: `CREATE TABLE public.newprefix_users (id int);`,
		},
		{
			name: "rename real pg_dump output",
			rules: []Rule{
				{Schema: "public", Match: "^(.+)$", Replace: "new_$1"},
			},
			input: `CREATE TABLE public."MyTable" (
    id integer NOT NULL
);

ALTER TABLE public."MyTable" OWNER TO postgres;

COMMENT ON TABLE public."MyTable" IS 'Test';`,
			expected: `CREATE TABLE public."new_MyTable" (
    id integer NOT NULL
);

ALTER TABLE public."new_MyTable" OWNER TO postgres;

COMMENT ON TABLE public."new_MyTable" IS 'Test';`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &Config{Rules: tc.rules}
			renamer, err := NewTableRenamer(cfg)
			require.NoError(t, err)

			result := renamer.RenameInSQL([]byte(tc.input))
			require.Equal(t, tc.expected, string(result))
		})
	}
}

func TestRenamer_NameAndClose(t *testing.T) {
	t.Parallel()

	closeCalled := false
	mockProcessor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			return nil
		},
		CloseFn: func() error {
			closeCalled = true
			return nil
		},
	}

	renamer, err := New(mockProcessor, nil)
	require.NoError(t, err)

	require.Equal(t, "mock", renamer.Name())

	err = renamer.Close()
	require.NoError(t, err)
	require.True(t, closeCalled)
}
