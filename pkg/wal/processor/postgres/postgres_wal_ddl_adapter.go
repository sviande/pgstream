// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

// schemaTableFilter implements TableFilter using SchemaTableMap for include/exclude logic
type schemaTableFilter struct {
	includeTableMap pglib.SchemaTableMap
	excludeTableMap pglib.SchemaTableMap
}

// NewSchemaTableFilter creates a new TableFilter from include/exclude table lists
func NewSchemaTableFilter(includeTables, excludeTables []string) (TableFilter, error) {
	if len(includeTables) == 0 && len(excludeTables) == 0 {
		return nil, nil
	}

	filter := &schemaTableFilter{}
	var err error

	if len(includeTables) > 0 {
		filter.includeTableMap, err = pglib.NewSchemaTableMap(includeTables)
		if err != nil {
			return nil, err
		}
	}

	if len(excludeTables) > 0 {
		filter.excludeTableMap, err = pglib.NewSchemaTableMap(excludeTables)
		if err != nil {
			return nil, err
		}
	}

	return filter, nil
}

// FilterTables filters tables based on include/exclude configuration
func (f *schemaTableFilter) FilterTables(schemaName string, tables []schemalog.Table) []schemalog.Table {
	if len(f.includeTableMap) == 0 && len(f.excludeTableMap) == 0 {
		return tables
	}

	filteredTables := make([]schemalog.Table, 0, len(tables))
	for _, table := range tables {
		switch {
		case len(f.includeTableMap) != 0:
			if f.includeTableMap.ContainsSchemaTable(schemaName, table.Name) {
				filteredTables = append(filteredTables, table)
			}
		case len(f.excludeTableMap) != 0:
			if !f.excludeTableMap.ContainsSchemaTable(schemaName, table.Name) {
				filteredTables = append(filteredTables, table)
			}
		}
	}

	return filteredTables
}

// TableFilter filters tables from a schema based on include/exclude configuration
type TableFilter interface {
	FilterTables(schemaName string, tables []schemalog.Table) []schemalog.Table
}

// TableRenamer defines the interface for renaming tables
type TableRenamer interface {
	RenameTable(schema, table string) string
}

type ddlAdapter struct {
	schemalogQuerier   schemalogQuerier
	schemaDiffer       schemaDiffer
	tableFilter        TableFilter
	tableRenamer       TableRenamer
	convertEnumsToText bool
	enumTypeNames      map[string]bool // cache: "type_name" -> true
}

type schemalogQuerier interface {
	Fetch(ctx context.Context, schemaName string, version int) (*schemalog.LogEntry, error)
}

type schemaDiffer func(old, new *schemalog.LogEntry) *schemalog.Diff

type logEntryAdapter func(*wal.Data) (*schemalog.LogEntry, error)

type ddlAdapterOption func(*ddlAdapter)

func withTableFilter(filter TableFilter) ddlAdapterOption {
	return func(a *ddlAdapter) {
		a.tableFilter = filter
	}
}

func withConvertEnumsToText(convert bool) ddlAdapterOption {
	return func(a *ddlAdapter) {
		a.convertEnumsToText = convert
		if convert {
			a.enumTypeNames = make(map[string]bool)
		}
	}
}

func withTableRenamer(renamer TableRenamer) ddlAdapterOption {
	return func(a *ddlAdapter) {
		a.tableRenamer = renamer
	}
}

// renameTable applies table renaming if a renamer is configured
func (a *ddlAdapter) renameTable(schema, table string) string {
	if a.tableRenamer == nil {
		return table
	}
	return a.tableRenamer.RenameTable(schema, table)
}

// renamedTableName returns the quoted, renamed table name for DDL queries
func (a *ddlAdapter) renamedTableName(schema, table string) string {
	return quotedTableName(schema, a.renameTable(schema, table))
}

func newDDLAdapter(querier schemalogQuerier, opts ...ddlAdapterOption) *ddlAdapter {
	adapter := &ddlAdapter{
		schemalogQuerier: querier,
		schemaDiffer:     schemalog.ComputeSchemaDiff,
	}
	for _, opt := range opts {
		opt(adapter)
	}
	return adapter
}

func (a *ddlAdapter) schemaLogToQueries(ctx context.Context, schemaLog *schemalog.LogEntry) ([]*query, error) {
	// Update ENUM types cache before processing
	a.updateEnumTypesCache(schemaLog)

	var previousSchemaLog *schemalog.LogEntry
	if schemaLog.Version > 0 {
		var err error
		previousSchemaLog, err = a.schemalogQuerier.Fetch(ctx, schemaLog.SchemaName, int(schemaLog.Version)-1)
		if err != nil && !errors.Is(err, schemalog.ErrNoRows) {
			return nil, fmt.Errorf("fetching existing schema log entry: %w", err)
		}
		// Filter the previous schema log to match the filtering applied to the
		// current schema log. This ensures that tables excluded via
		// include/exclude filters are not incorrectly seen as "removed" in the
		// diff, which would generate DROP TABLE statements for excluded tables.
		if previousSchemaLog != nil && a.tableFilter != nil {
			previousSchemaLog.Schema.Tables = a.tableFilter.FilterTables(
				previousSchemaLog.SchemaName,
				previousSchemaLog.Schema.Tables,
			)
		}
	}

	diff := a.schemaDiffer(previousSchemaLog, schemaLog)

	queries := []*query{
		a.createSchemaIfNotExists(schemaLog.SchemaName),
	}

	schemaQueries, err := a.schemaDiffToQueries(schemaLog.SchemaName, diff)
	if err != nil {
		return nil, err
	}

	return append(queries, schemaQueries...), nil
}

const createSchemaIfNotExistsQuery = "CREATE SCHEMA IF NOT EXISTS %s"

func (a *ddlAdapter) createSchemaIfNotExists(schemaName string) *query {
	createSchemaQuery := fmt.Sprintf(createSchemaIfNotExistsQuery, pglib.QuoteIdentifier(schemaName))
	return a.newDDLQuery(schemaName, "", createSchemaQuery)
}

func (a *ddlAdapter) schemaDiffToQueries(schemaName string, diff *schemalog.Diff) ([]*query, error) {
	if diff.IsEmpty() {
		return []*query{}, nil
	}

	queries := []*query{}

	// Handle types BEFORE tables, since tables may depend on types
	// Create new types first
	for _, t := range diff.TypesAdded {
		if q := a.buildCreateTypeQuery(schemaName, t); q != nil {
			queries = append(queries, q)
		}
	}

	// Alter existing types (add new enum values)
	for _, typeDiff := range diff.TypesChanged {
		queries = append(queries, a.buildAlterTypeQueries(schemaName, typeDiff)...)
	}

	for _, table := range diff.TablesRemoved {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", a.renamedTableName(schemaName, table.Name))
		queries = append(queries, a.newDDLQuery(schemaName, table.Name, dropQuery))
	}

	for _, table := range diff.TablesAdded {
		queries = append(queries, a.buildCreateTableQuery(schemaName, table))
	}

	for _, tableDiff := range diff.TablesChanged {
		queries = append(queries, a.buildAlterTableQueries(schemaName, tableDiff)...)
	}

	// Drop types AFTER tables, since tables may depend on types
	for _, t := range diff.TypesRemoved {
		dropQuery := fmt.Sprintf("DROP TYPE IF EXISTS %s", pglib.QuoteQualifiedIdentifier(schemaName, t.Name))
		queries = append(queries, a.newDDLQuery(schemaName, "", dropQuery))
	}

	return queries, nil
}

func (a *ddlAdapter) buildCreateTableQuery(schemaName string, table schemalog.Table) *query {
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", a.renamedTableName(schemaName, table.Name))
	uniqueConstraints := make([]string, 0, len(table.Columns))
	columnDefinitions := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		columnDefinitions = append(columnDefinitions, a.buildColumnDefinition(&col))
		// if there's a unique constraint associated to the column, and it's not
		// the primary key, explicitly add it
		if uniqueConstraint := a.buildUniqueConstraint(col); uniqueConstraint != "" && !slices.Contains(table.PrimaryKeyColumns, col.Name) {
			uniqueConstraints = append(uniqueConstraints, uniqueConstraint)
		}
	}

	createQuery = fmt.Sprintf("%s\n%s", createQuery, strings.Join(columnDefinitions, ",\n"))
	if len(uniqueConstraints) > 0 {
		createQuery = fmt.Sprintf("%s,\n%s", createQuery, strings.Join(uniqueConstraints, ",\n"))
	}

	primaryKeys := make([]string, 0, len(table.PrimaryKeyColumns))
	for _, col := range table.PrimaryKeyColumns {
		primaryKeys = append(primaryKeys, pglib.QuoteIdentifier(col))
	}

	if len(primaryKeys) > 0 {
		createQuery = fmt.Sprintf("%s,\nPRIMARY KEY (%s)\n", createQuery, strings.Join(primaryKeys, ","))
	}

	createQuery += ")"

	return a.newDDLQuery(schemaName, table.Name, createQuery)
}

func (a *ddlAdapter) buildColumnDefinition(column *schemalog.Column) string {
	dataType := column.DataType

	// Convert ENUM types to TEXT if enabled
	if a.isEnumType(dataType) {
		isArray := strings.HasSuffix(dataType, "[]")
		if isArray {
			dataType = "text[]"
		} else {
			dataType = "text"
		}
	}

	colDefinition := fmt.Sprintf("%s %s", pglib.QuoteIdentifier(column.Name), dataType)
	if !column.Nullable {
		colDefinition = fmt.Sprintf("%s NOT NULL", colDefinition)
	}
	// do not set default values with sequences and generated columns since they
	// must be aligned between source/target. Keep source database as source of
	// truth.
	if column.DefaultValue != nil && !strings.Contains(*column.DefaultValue, "seq") && !column.Generated {
		defaultValue := *column.DefaultValue
		// When converting enums to text, also convert enum type casts in
		// default values (e.g. 'Pending'::public."MyEnum" -> 'Pending'::text)
		if a.convertEnumsToText {
			defaultValue = a.convertEnumCastsInDefault(defaultValue)
		}
		colDefinition = fmt.Sprintf("%s DEFAULT %s", colDefinition, defaultValue)
	}

	return colDefinition
}

func (a *ddlAdapter) buildUniqueConstraint(column schemalog.Column) string {
	if column.Unique {
		return fmt.Sprintf("UNIQUE (%s)", pglib.QuoteIdentifier(column.Name))
	}
	return ""
}

func (a *ddlAdapter) buildAlterTableQueries(schemaName string, tableDiff schemalog.TableDiff) []*query {
	if tableDiff.IsEmpty() {
		return []*query{}
	}

	queries := []*query{}
	if tableDiff.TableNameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			a.renamedTableName(schemaName, tableDiff.TableNameChange.Old),
			tableDiff.TableNameChange.New,
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, col := range tableDiff.ColumnsRemoved {
		alterQuery := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", a.renamedTableName(schemaName, tableDiff.TableName), pglib.QuoteIdentifier(col.Name))
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, col := range tableDiff.ColumnsAdded {
		alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", a.renamedTableName(schemaName, tableDiff.TableName), a.buildColumnDefinition(&col))
		queries = append(queries, a.newDDLQuery(schemaName, tableDiff.TableName, alterQuery))
	}

	for _, colDiff := range tableDiff.ColumnsChanged {
		alterQueries := a.buildAlterColumnQueries(schemaName, tableDiff.TableName, &colDiff)
		queries = append(queries, alterQueries...)
	}

	return queries
}

func (a *ddlAdapter) buildAlterColumnQueries(schemaName, tableName string, columnDiff *schemalog.ColumnDiff) []*query {
	if columnDiff.IsEmpty() {
		return []*query{}
	}

	queries := []*query{}
	if columnDiff.NameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			a.renamedTableName(schemaName, tableName),
			pglib.QuoteIdentifier(columnDiff.NameChange.Old),
			pglib.QuoteIdentifier(columnDiff.NameChange.New),
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.TypeChange != nil {
		newType := columnDiff.TypeChange.New

		// Convert ENUM types to TEXT if enabled
		if a.isEnumType(newType) {
			isArray := strings.HasSuffix(newType, "[]")
			if isArray {
				newType = "text[]"
			} else {
				newType = "text"
			}
		}

		alterQuery := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			a.renamedTableName(schemaName, tableName),
			pglib.QuoteIdentifier(columnDiff.ColumnName),
			newType,
		)
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.NullChange != nil {
		alterQuery := ""
		switch {
		// from not nullable to nullable
		case columnDiff.NullChange.New:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
				a.renamedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		default:
			// from nullable to not nullable
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
				a.renamedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	if columnDiff.DefaultChange != nil {
		alterQuery := ""
		switch columnDiff.DefaultChange.New {
		// removing the default
		case nil:
			alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
				a.renamedTableName(schemaName, tableName),
				pglib.QuoteIdentifier(columnDiff.ColumnName),
			)
		default:
			// do not set default values with sequences since they will differ between
			// source/target. Keep source database as source of truth.
			if !strings.Contains(*columnDiff.DefaultChange.New, "seq") {
				defaultValue := *columnDiff.DefaultChange.New
				if a.convertEnumsToText {
					defaultValue = a.convertEnumCastsInDefault(defaultValue)
				}
				alterQuery = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
					a.renamedTableName(schemaName, tableName),
					pglib.QuoteIdentifier(columnDiff.ColumnName),
					defaultValue,
				)
			}
		}
		queries = append(queries, a.newDDLQuery(schemaName, tableName, alterQuery))
	}

	// TODO: add support for unique constraint changes

	return queries
}

func (a *ddlAdapter) newDDLQuery(schema, table, sql string) *query {
	return &query{
		schema: schema,
		table:  table,
		sql:    sql,
		isDDL:  true,
	}
}

func (a *ddlAdapter) buildCreateTypeQuery(schemaName string, t schemalog.Type) *query {
	if t.Kind != "enum" {
		return nil
	}

	// Skip ENUM creation if conversion to text is enabled
	if a.convertEnumsToText {
		return nil
	}

	quotedValues := make([]string, len(t.Values))
	for i, v := range t.Values {
		quotedValues[i] = pglib.QuoteLiteral(v)
	}

	createQuery := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)",
		pglib.QuoteQualifiedIdentifier(schemaName, t.Name),
		strings.Join(quotedValues, ", "),
	)
	return a.newDDLQuery(schemaName, "", createQuery)
}

func (a *ddlAdapter) buildAlterTypeQueries(schemaName string, typeDiff schemalog.TypeDiff) []*query {
	if typeDiff.IsEmpty() {
		return []*query{}
	}

	queries := []*query{}

	// Handle name change
	if typeDiff.NameChange != nil {
		alterQuery := fmt.Sprintf("ALTER TYPE %s RENAME TO %s",
			pglib.QuoteQualifiedIdentifier(schemaName, typeDiff.NameChange.Old),
			pglib.QuoteIdentifier(typeDiff.NameChange.New),
		)
		queries = append(queries, a.newDDLQuery(schemaName, "", alterQuery))
	}

	// Add new enum values (skip if converting enums to text)
	if !a.convertEnumsToText {
		for _, val := range typeDiff.ValuesAdded {
			alterQuery := fmt.Sprintf("ALTER TYPE %s ADD VALUE IF NOT EXISTS %s",
				pglib.QuoteQualifiedIdentifier(schemaName, typeDiff.TypeName),
				pglib.QuoteLiteral(val),
			)
			queries = append(queries, a.newDDLQuery(schemaName, "", alterQuery))
		}
	}

	return queries
}

// updateEnumTypesCache updates the cache of ENUM type names from schema log
func (a *ddlAdapter) updateEnumTypesCache(schemaLog *schemalog.LogEntry) {
	if !a.convertEnumsToText || schemaLog == nil {
		return
	}

	// Clear and rebuild cache
	a.enumTypeNames = make(map[string]bool)
	for _, t := range schemaLog.Schema.Types {
		if t.Kind == "enum" {
			a.enumTypeNames[t.Name] = true
		}
	}
}

// isEnumType checks if a column type is an ENUM type
func (a *ddlAdapter) isEnumType(columnType string) bool {
	if !a.convertEnumsToText {
		return false
	}
	return pglib.IsEnumType(columnType, a.enumTypeNames)
}

// convertEnumCastsInDefault replaces enum type casts in a DEFAULT value expression
// with ::text. For example, 'Pending'::public."MyEnum" becomes 'Pending'::text.
func (a *ddlAdapter) convertEnumCastsInDefault(defaultValue string) string {
	if len(a.enumTypeNames) == 0 {
		return defaultValue
	}

	var result strings.Builder
	i := 0
	for i < len(defaultValue) {
		// Look for :: cast operator
		if i+1 < len(defaultValue) && defaultValue[i] == ':' && defaultValue[i+1] == ':' {
			castStart := i
			i += 2 // skip ::

			// Parse the type identifier after ::
			typeName, end := parseTypeIdentifier(defaultValue, i)
			if typeName != "" && pglib.IsEnumType(typeName, a.enumTypeNames) {
				result.WriteString("::text")
			} else {
				result.WriteString(defaultValue[castStart:end])
			}
			i = end
		} else {
			result.WriteByte(defaultValue[i])
			i++
		}
	}

	return result.String()
}

// parseTypeIdentifier parses a possibly schema-qualified, possibly quoted
// type identifier starting at position pos in s. It returns the extracted
// identifier string and the position after the last consumed character.
func parseTypeIdentifier(s string, pos int) (string, int) {
	i := pos
	var ident strings.Builder

	for i < len(s) {
		if s[i] == '"' {
			// Quoted identifier: consume until closing quote
			ident.WriteByte('"')
			i++
			for i < len(s) && s[i] != '"' {
				ident.WriteByte(s[i])
				i++
			}
			if i < len(s) {
				ident.WriteByte('"')
				i++ // skip closing quote
			}
		} else if isIdentChar(s[i]) {
			ident.WriteByte(s[i])
			i++
		} else if s[i] == '.' {
			// Schema separator — only valid between identifiers
			ident.WriteByte('.')
			i++
		} else {
			break
		}
	}

	return ident.String(), i
}

// isIdentChar returns true if c is a valid unquoted SQL identifier character.
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}
