// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
)

func TestDDLAdapter_walDataToQueries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)
	id := xid.New()

	table1 := "table1"
	table2 := "table2"

	testLogEntry := func(version int64) *schemalog.LogEntry {
		return &schemalog.LogEntry{
			ID:         id,
			Version:    version,
			SchemaName: testSchema,
			CreatedAt:  schemalog.NewSchemaCreatedAtTimestamp(now),
		}
	}

	tests := []struct {
		name         string
		querier      schemalogQuerier
		schemaDiffer schemaDiffer
		logEntry     *schemalog.LogEntry

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - initial schema",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					return nil, errors.New("unexpected call to FetchFn")
				},
			},
			schemaDiffer: func(old, new *schemalog.LogEntry) *schemalog.Diff {
				return &schemalog.Diff{
					TablesChanged: []schemalog.TableDiff{
						{
							TableName: table2,
							TableNameChange: &schemalog.ValueChange[string]{
								Old: table1,
								New: table2,
							},
						},
					},
				}
			},
			logEntry: testLogEntry(0),

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - fetching existing schema",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchema, schemaName)
					require.Equal(t, int(0), version)
					return testLogEntry(0), nil
				},
			},
			schemaDiffer: func(old, new *schemalog.LogEntry) *schemalog.Diff {
				return &schemalog.Diff{
					TablesChanged: []schemalog.TableDiff{
						{
							TableName: table2,
							TableNameChange: &schemalog.ValueChange[string]{
								Old: table1,
								New: table2,
							},
						},
					},
				}
			},
			logEntry: testLogEntry(1),

			wantQueries: []*query{
				{
					schema: testSchema,
					sql:    fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(testSchema)),
					isDDL:  true,
				},
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "error - fetching schema log",
			querier: &schemalogmocks.Store{
				FetchFn: func(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			logEntry: testLogEntry(1),

			wantQueries: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := ddlAdapter{
				schemalogQuerier: tc.querier,
				schemaDiffer:     tc.schemaDiffer,
			}

			queries, err := a.schemaLogToQueries(context.Background(), tc.logEntry)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestDDLAdapter_schemaDiffToQueries(t *testing.T) {
	t.Parallel()

	table1 := "test-table-1"
	table2 := "test-table-2"
	defaultAge := "0"

	tests := []struct {
		name string
		diff *schemalog.Diff

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - empty schema diff",
			diff: &schemalog.Diff{},

			wantQueries: []*query{},
			wantErr:     nil,
		},
		{
			name: "ok - table removed",
			diff: &schemalog.Diff{
				TablesRemoved: []schemalog.Table{
					{Name: table1},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name:              table1,
						PrimaryKeyColumns: []string{"id"},
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: true},
							{Name: "name", DataType: "text", Nullable: true, Unique: true},
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"name\" text,\n\"age\" int NOT NULL DEFAULT 0,\nUNIQUE (\"name\"),\nPRIMARY KEY (\"id\")\n)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table added no primary key or unique constraint",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name: table1,
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: false},
							{Name: "name", DataType: "text", Nullable: true, Unique: false},
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"name\" text,\n\"age\" int NOT NULL DEFAULT 0)", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table renamed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table2,
						TableNameChange: &schemalog.ValueChange[string]{
							Old: table1,
							New: table2,
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table2,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedTableName(testSchema, table1), table2),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column dropped",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsRemoved: []schemalog.Column{
							{Name: "age"},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s DROP COLUMN \"age\"", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column added",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsAdded: []schemalog.Column{
							{Name: "age", DataType: "int", Nullable: false, DefaultValue: &defaultAge},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"age\" int NOT NULL DEFAULT 0", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column renamed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NameChange: &schemalog.ValueChange[string]{Old: "name", New: "new_name"},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s RENAME COLUMN \"name\" TO \"new_name\"", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column type changed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								TypeChange: &schemalog.ValueChange[string]{Old: "uuid", New: "int"},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" TYPE int", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column changed to nullable",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NullChange: &schemalog.ValueChange[bool]{Old: false, New: true},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" DROP NOT NULL", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column changed to not nullable",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName: "name",
								NullChange: &schemalog.ValueChange[bool]{Old: true, New: false},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"name\" SET NOT NULL", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column default removed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "age",
								DefaultChange: &schemalog.ValueChange[*string]{Old: &defaultAge, New: nil},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"age\" DROP DEFAULT", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - table changed, column default changed",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "age",
								DefaultChange: &schemalog.ValueChange[*string]{Old: nil, New: &defaultAge},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"age\" SET DEFAULT 0", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ddlAdapter := newDDLAdapter(nil)

			queries, err := ddlAdapter.schemaDiffToQueries(testSchema, tc.diff)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestDDLAdapter_convertEnumCastsInDefault(t *testing.T) {
	t.Parallel()

	knownEnums := map[string]bool{
		"ChorusProSubmissionStatus": true,
		"MyEnum":                   true,
	}

	tests := []struct {
		name     string
		enums    map[string]bool
		input    string
		expected string
	}{
		{
			name:     "schema-qualified quoted enum",
			enums:    knownEnums,
			input:    `'Pending'::public."ChorusProSubmissionStatus"`,
			expected: `'Pending'::text`,
		},
		{
			name:     "schema-qualified unquoted enum",
			enums:    knownEnums,
			input:    `'value'::public.MyEnum`,
			expected: `'value'::text`,
		},
		{
			name:     "quoted enum without schema",
			enums:    knownEnums,
			input:    `'value'::"MyEnum"`,
			expected: `'value'::text`,
		},
		{
			name:     "unquoted enum without schema",
			enums:    knownEnums,
			input:    `'value'::MyEnum`,
			expected: `'value'::text`,
		},
		{
			name:     "fully quoted schema and enum",
			enums:    knownEnums,
			input:    `'Pending'::"public"."ChorusProSubmissionStatus"`,
			expected: `'Pending'::text`,
		},
		{
			name:     "text cast unchanged",
			enums:    knownEnums,
			input:    `'IN_DP_E2_CII_FACTURX'::text`,
			expected: `'IN_DP_E2_CII_FACTURX'::text`,
		},
		{
			name:     "integer cast unchanged",
			enums:    knownEnums,
			input:    `'5'::integer`,
			expected: `'5'::integer`,
		},
		{
			name:     "no cast",
			enums:    knownEnums,
			input:    `0`,
			expected: `0`,
		},
		{
			name:     "empty enum cache",
			enums:    map[string]bool{},
			input:    `'Pending'::public."ChorusProSubmissionStatus"`,
			expected: `'Pending'::public."ChorusProSubmissionStatus"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &ddlAdapter{
				convertEnumsToText: true,
				enumTypeNames:      tc.enums,
			}
			result := a.convertEnumCastsInDefault(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestDDLAdapter_schemaDiffToQueries_enumConversion(t *testing.T) {
	t.Parallel()

	table1 := "test-table-1"
	defaultEnumVal := `'Pending'::public."ChorusProSubmissionStatus"`
	defaultIntVal := "0"
	newDefaultEnumVal := `'Active'::public."ChorusProSubmissionStatus"`

	tests := []struct {
		name string
		diff *schemalog.Diff

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "ok - CREATE TABLE with enum column and default converts cast to text",
			diff: &schemalog.Diff{
				TablesAdded: []schemalog.Table{
					{
						Name:              table1,
						PrimaryKeyColumns: []string{"id"},
						Columns: []schemalog.Column{
							{Name: "id", DataType: "uuid", Nullable: false, Unique: true},
							{Name: "status", DataType: `public."ChorusProSubmissionStatus"`, Nullable: false, DefaultValue: &defaultEnumVal},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql: fmt.Sprintf(
						"CREATE TABLE IF NOT EXISTS %s (\n\"id\" uuid NOT NULL,\n\"status\" text NOT NULL DEFAULT 'Pending'::text,\nPRIMARY KEY (\"id\")\n)",
						quotedTableName(testSchema, table1),
					),
					isDDL: true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - ALTER COLUMN SET DEFAULT with enum cast converts to text",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "status",
								DefaultChange: &schemalog.ValueChange[*string]{Old: &defaultEnumVal, New: &newDefaultEnumVal},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"status\" SET DEFAULT 'Active'::text", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - DEFAULT without enum cast unchanged with convertEnumsToText",
			diff: &schemalog.Diff{
				TablesChanged: []schemalog.TableDiff{
					{
						TableName: table1,
						ColumnsChanged: []schemalog.ColumnDiff{
							{
								ColumnName:    "age",
								DefaultChange: &schemalog.ValueChange[*string]{Old: nil, New: &defaultIntVal},
							},
						},
					},
				},
			},

			wantQueries: []*query{
				{
					schema: testSchema,
					table:  table1,
					sql:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"age\" SET DEFAULT 0", quotedTableName(testSchema, table1)),
					isDDL:  true,
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := newDDLAdapter(nil, withConvertEnumsToText(true))
			a.enumTypeNames = map[string]bool{
				"ChorusProSubmissionStatus": true,
			}

			queries, err := a.schemaDiffToQueries(testSchema, tc.diff)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}
