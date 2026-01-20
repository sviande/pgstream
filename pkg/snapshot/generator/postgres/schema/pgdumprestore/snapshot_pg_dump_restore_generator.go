// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemaloginstrumentation "github.com/xataio/pgstream/pkg/schemalog/instrumentation"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
)

// SnapshotGenerator generates postgres schema snapshots using pg_dump and
// pg_restore
type SnapshotGenerator struct {
	sourceURL               string
	targetURL               string
	pgDumpFn                pglib.PGDumpFn
	pgDumpAllFn             pglib.PGDumpAllFn
	pgRestoreFn             pglib.PGRestoreFn
	schemalogStore          schemalog.Store
	connBuilder             pglib.QuerierBuilder
	logger                  loglib.Logger
	generator               generator.SnapshotGenerator
	dumpDebugFile           string
	excludedSecurityLabels  []string
	excludedViews           map[string][]string
	excludeCheckConstraints bool
	excludeTriggers         bool
	excludeForeignKeys      bool
	convertEnumsToText      bool
	excludeDropSchema       bool
	roleSQLParser           *roleSQLParser
	optionGenerator         *optionGenerator
	// table renamer for transforming table names in the dump
	tableRenamer *renamer.TableRenamer
}

type Config struct {
	SourcePGURL    string
	TargetPGURL    string
	CleanTargetDB  bool
	CreateTargetDB bool
	// if set to true the snapshot will include all database objects, not tied
	// to any particular schema, such as extensions or triggers.
	IncludeGlobalDBObjects bool
	// Role name to be used to create the dump
	Role string
	// "enabled", "disabled", or "no_passwords"
	RolesSnapshotMode string
	// Do not output commands to set ownership of objects to match the original
	// database.
	NoOwner bool
	// Prevent dumping of access privileges (grant/revoke commands)
	NoPrivileges bool
	// if set, the dump will be written to this file for debugging purposes
	DumpDebugFile string
	// if set, security label providers that will be excluded from the dump
	ExcludedSecurityLabels []string
	// Views to exclude from the snapshot (filtered during dump parsing for wildcards)
	ExcludedViews map[string][]string
	// ExcludeCheckConstraints excludes CHECK constraints from schema snapshot
	ExcludeCheckConstraints bool
	// ExcludeTriggers excludes triggers from schema snapshot
	ExcludeTriggers bool
	// ExcludeForeignKeys excludes foreign key constraints from schema snapshot
	ExcludeForeignKeys bool
	// ConvertEnumsToText converts all ENUM types to TEXT in the target database.
	// When enabled, ENUM types will not be created and ENUM columns will be created as TEXT/TEXT[]
	ConvertEnumsToText bool
	// ExcludeDropSchema excludes DROP SCHEMA and CREATE SCHEMA statements from cleanup
	// when clean_target_db is enabled. Useful when schemas should already exist in target.
	ExcludeDropSchema bool
}

type Option func(s *SnapshotGenerator)

type dump struct {
	full                  []byte
	filtered              []byte
	cleanupPart           []byte
	indicesAndConstraints []byte
	sequences             []string
	enumTracker           *enumTypeTracker // Tracks ENUM types for conversion after table renaming
	roles                 map[string]role
}

const (
	publicSchema = "public"
	wildcard     = "*"
)

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config, opts ...Option) (*SnapshotGenerator, error) {
	sg := &SnapshotGenerator{
		sourceURL:               c.SourcePGURL,
		targetURL:               c.TargetPGURL,
		pgDumpFn:                pglib.RunPGDump,
		pgDumpAllFn:             pglib.RunPGDumpAll,
		pgRestoreFn:             pglib.RunPGRestore,
		connBuilder:             pglib.ConnBuilder,
		logger:                  loglib.NewNoopLogger(),
		dumpDebugFile:           c.DumpDebugFile,
		excludedSecurityLabels:  c.ExcludedSecurityLabels,
		excludedViews:           c.ExcludedViews,
		excludeCheckConstraints: c.ExcludeCheckConstraints,
		excludeTriggers:         c.ExcludeTriggers,
		excludeForeignKeys:      c.ExcludeForeignKeys,
		convertEnumsToText:      c.ConvertEnumsToText,
		excludeDropSchema:       c.ExcludeDropSchema,
		roleSQLParser:           &roleSQLParser{},
		optionGenerator:         newOptionGenerator(pglib.ConnBuilder, c),
	}

	if err := sg.initialiseSchemaLogStore(ctx); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(sg)
	}

	return sg, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(sg *SnapshotGenerator) {
		sg.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_schema_snapshot_generator",
		})
	}
}

func WithSnapshotGenerator(g generator.SnapshotGenerator) Option {
	return func(sg *SnapshotGenerator) {
		sg.generator = g
	}
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(sg *SnapshotGenerator) {
		var err error
		sg.connBuilder, err = pglibinstrumentation.NewQuerierBuilder(sg.connBuilder, i)
		if err != nil {
			// this should never happen
			panic(err)
		}

		sg.pgDumpFn = pglibinstrumentation.NewPGDumpFn(sg.pgDumpFn, i)
		sg.pgDumpAllFn = pglibinstrumentation.NewPGDumpAllFn(sg.pgDumpAllFn, i)
		sg.pgRestoreFn = pglibinstrumentation.NewPGRestoreFn(sg.pgRestoreFn, i)
		if sg.schemalogStore != nil {
			sg.schemalogStore = schemaloginstrumentation.NewStore(sg.schemalogStore, i)
		}
	}
}

// WithTableRenamer sets the table renamer for transforming table names in the SQL dump.
func WithTableRenamer(tr *renamer.TableRenamer) Option {
	return func(sg *SnapshotGenerator) {
		sg.tableRenamer = tr
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) (err error) {
	s.logger.Info("creating schema snapshot", loglib.Fields{"schemaTables": ss.SchemaTables})
	defer func() {
		if err == nil {
			// if we perform a schema snapshot using pg_dump/pg_restore, we need to make
			// sure the schema_log table is updated accordingly with the schema view so that
			// replication can work as expected if configured.
			err = s.syncSchemaLog(ctx, ss.SchemaTables, ss.SchemaExcludedTables)
		}
	}()

	// make sure any empty schemas are filtered out
	dumpSchemas := make(map[string][]string, len(ss.SchemaTables))
	for schema, tables := range ss.SchemaTables {
		if len(tables) > 0 {
			dumpSchemas[schema] = tables
		}
	}
	// nothing to dump
	if len(dumpSchemas) == 0 {
		return nil
	}

	// DUMP

	dump, err := s.dumpSchema(ctx, dumpSchemas, ss.SchemaExcludedTables, ss.SchemaExcludedViews)
	if err != nil {
		return err
	}

	// the schema will include the sequences but will not produce the `SETVAL`
	// queries since that's considered data and it's a schema only dump. Produce
	// the data only dump for the sequences only and restore it along with the
	// schema.
	sequenceDump, err := s.dumpSequenceValues(ctx, dump.sequences)
	if err != nil {
		return err
	}

	// the schema dump will not include the roles, so we need to dump them
	// separately and restore them as well.
	rolesDump, err := s.dumpRoles(ctx, dump.roles)
	if err != nil {
		return err
	}

	// RESTORE

	if err := s.restoreSchemas(ctx, dumpSchemas); err != nil {
		return err
	}

	if s.optionGenerator.cleanTargetDB {
		s.logger.Info("restoring cleanup")
		if err := s.restoreDump(ctx, dump.cleanupPart); err != nil {
			return err
		}
	}

	if rolesDump != nil {
		s.logger.Info("restoring roles")
		if err := s.restoreDump(ctx, rolesDump); err != nil {
			return err
		}
	}

	s.logger.Info("restoring schema")
	if err := s.restoreDump(ctx, dump.filtered, dump.enumTracker); err != nil {
		return err
	}

	// call the wrapped snapshot generator if any before restoring sequences,
	// indices and constraints to improve performance.
	if s.generator != nil {
		if err := s.generator.CreateSnapshot(ctx, ss); err != nil {
			return err
		}
	}

	// apply the sequences, indices and constraints when the wrapped generator has finished
	s.logger.Info("restoring sequence data", loglib.Fields{"schemaTables": ss.SchemaTables})
	if err := s.restoreDump(ctx, sequenceDump); err != nil {
		return err
	}

	s.logger.Info("restoring schema indices and constraints", loglib.Fields{"schemaTables": ss.SchemaTables})
	return s.restoreDump(ctx, dump.indicesAndConstraints, dump.enumTracker)
}

func (s *SnapshotGenerator) Close() error {
	if s.schemalogStore != nil {
		return s.schemalogStore.Close()
	}

	if s.generator != nil {
		return s.generator.Close()
	}

	return nil
}

func (s *SnapshotGenerator) dumpSchema(ctx context.Context, schemaTables map[string][]string, excludedTables map[string][]string, excludedViews map[string][]string) (*dump, error) {
	pgdumpOpts, err := s.optionGenerator.pgdumpOptions(ctx, schemaTables, excludedTables, excludedViews)
	if err != nil {
		return nil, fmt.Errorf("preparing pg_dump options: %w", err)
	}

	s.logger.Debug("dumping schema", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
	d, err := s.pgDumpFn(ctx, *pgdumpOpts)
	defer s.dumpToFile(s.dumpDebugFile, pgdumpOpts, d)
	if err != nil {
		s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
		return nil, fmt.Errorf("dumping schema: %w", err)
	}

	parsedDump := s.parseDump(d)

	if pgdumpOpts.Clean {
		// In case clean is enabled, we need the cleanup part of the dump separately, which will be restored before the roles dump.
		// This will allow us to drop the roles safely, without getting dependency erros.
		pgdumpOpts.Clean = false
		s.logger.Debug("dumping schema again without clean", loglib.Fields{"pg_dump_options": pgdumpOpts.ToArgs(), "schema_tables": schemaTables})
		dumpWithoutClean, err := s.pgDumpFn(ctx, *pgdumpOpts)
		if err != nil {
			s.logger.Error(err, "pg_dump for schema failed", loglib.Fields{"pgdumpOptions": pgdumpOpts.ToArgs()})
			return nil, fmt.Errorf("dumping schema: %w", err)
		}
		parsedDump.cleanupPart = getDumpsDiff(d, dumpWithoutClean)

		// Filter out DROP SCHEMA and CREATE SCHEMA if configured
		if s.excludeDropSchema {
			s.logger.Debug("excluding DROP SCHEMA and CREATE SCHEMA from cleanup part")
			parsedDump.cleanupPart = filterDropAndCreateSchema(parsedDump.cleanupPart)
		}
	}

	s.dumpToFile(s.getDumpFileName("-filtered"), pgdumpOpts, parsedDump.filtered)
	s.dumpToFile(s.getDumpFileName("-indices-constraints"), pgdumpOpts, parsedDump.indicesAndConstraints)

	return parsedDump, nil
}

func (s *SnapshotGenerator) dumpSequenceValues(ctx context.Context, sequences []string) ([]byte, error) {
	opts := s.optionGenerator.pgdumpSequenceDataOptions(sequences)
	if opts == nil {
		return nil, nil
	}

	s.logger.Debug("dumping sequence data", loglib.Fields{"pg_dump_options": opts.ToArgs(), "sequences": sequences})
	d, err := s.pgDumpFn(ctx, *opts)
	defer s.dumpToFile(s.sequenceDumpFile(), opts, d)
	if err != nil {
		s.logger.Error(err, "pg_dump for sequences failed", loglib.Fields{"pgdumpOptions": opts.ToArgs()})
		return nil, fmt.Errorf("dumping sequence values: %w", err)
	}
	return d, nil
}

func (s *SnapshotGenerator) dumpRoles(ctx context.Context, rolesInSchemaDump map[string]role) ([]byte, error) {
	opts := s.optionGenerator.pgdumpRolesOptions()
	if opts == nil {
		return nil, nil
	}

	// 1. dump all roles in the database
	s.logger.Debug("dumping roles", loglib.Fields{"pg_dumpall_options": opts.ToArgs()})
	d, err := s.pgDumpAllFn(ctx, *opts)
	if err != nil {
		s.logger.Error(err, "pg_dumpall for roles failed", loglib.Fields{"pgdumpallOptions": opts.ToArgs()})
		return nil, fmt.Errorf("dumping roles: %w", err)
	}

	// 2. extract the role names from the dump
	rolesInRoleDump := s.roleSQLParser.extractRoleNamesFromDump(d)

	s.logger.Debug("dumped roles", loglib.Fields{"roles in schema dump": rolesInSchemaDump, "roles in role dump": rolesInRoleDump})

	// 3. add any dependencies found in the role dump for the schema dump roles
	for _, role := range rolesInRoleDump {
		if _, found := rolesInSchemaDump[role.name]; !found {
			// if the role is not in the schema dump, we don't need to include its dependencies
			continue
		}
		for _, dep := range role.roleDependencies {
			rolesInSchemaDump[dep.name] = dep
		}
	}

	// 4. filter the dump statements to include only the roles found in the
	// schema dump and their dependencies
	filteredRolesDump := s.filterRolesDump(d, rolesInSchemaDump)
	s.dumpToFile(s.rolesDumpFile(), opts, filteredRolesDump)

	return filteredRolesDump, nil
}

// if we use table filtering in the pg_dump command, the schema creation will
// not be dumped, so it needs to be created explicitly (except for public
// schema)
func (s *SnapshotGenerator) restoreSchemas(ctx context.Context, schemaTables map[string][]string) error {
	for schema, tables := range schemaTables {
		if len(tables) > 0 && schema != publicSchema && schema != wildcard {
			if err := s.createSchemaIfNotExists(ctx, schema); err != nil {
				return err
			}
		}
	}
	return nil
}

// filterDropTypeStatements removes all DROP TYPE statements from the dump.
// This is necessary when converting ENUMs to TEXT because:
// 1. The ENUMs are converted to text
// 2. The DROP TYPE statements would then try to drop "text" (after conversion)
// 3. This fails because "text" is a system type
func (s *SnapshotGenerator) filterDropTypeStatements(dump []byte) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Split(bufio.ScanLines)
	var result strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip DROP TYPE statements
		if strings.HasPrefix(strings.TrimSpace(line), "DROP TYPE") {
			continue
		}

		result.WriteString(line)
		result.WriteString("\n")
	}

	return []byte(result.String())
}

// fixRenamedTextType fixes any incorrectly renamed "text" type back to "text".
// After table renaming, patterns like "schema.text" might become "schema.piana_text",
// which is incorrect since "text" is a PostgreSQL built-in type.
func (s *SnapshotGenerator) fixRenamedTextType(dump []byte) []byte {
	if s.tableRenamer == nil || !s.tableRenamer.HasRules() {
		return dump
	}

	result := string(dump)

	// The table renamer might have transformed "text" into various renamed forms
	// We need to detect and fix these patterns
	// Common patterns after renaming:
	// - piana_text -> text
	// - test_text -> text
	// - prefix_text -> text

	// Get the renaming pattern by applying it to a dummy "text" identifier
	// This tells us what prefix is being added
	dummyRenamed := s.tableRenamer.RenameTable("public", "text")

	if dummyRenamed != "text" {
		// The table renamer did rename "text", so we need to fix it
		// Fix all possible quoting combinations
		// Pattern 1: renamed_text (unquoted)
		result = strings.ReplaceAll(result, " "+dummyRenamed+" ", " text ")
		result = strings.ReplaceAll(result, " "+dummyRenamed+",", " text,")
		result = strings.ReplaceAll(result, " "+dummyRenamed+";", " text;")
		result = strings.ReplaceAll(result, " "+dummyRenamed+")", " text)")
		result = strings.ReplaceAll(result, "("+dummyRenamed+" ", "(text ")
		result = strings.ReplaceAll(result, "("+dummyRenamed+")", "(text)")

		// Pattern 2: "renamed_text" (quoted)
		result = strings.ReplaceAll(result, `"`+dummyRenamed+`"`, "text")

		// Pattern 3: schema.renamed_text
		result = strings.ReplaceAll(result, "."+dummyRenamed+" ", ".text ")
		result = strings.ReplaceAll(result, "."+dummyRenamed+",", ".text,")
		result = strings.ReplaceAll(result, "."+dummyRenamed+";", ".text;")
		result = strings.ReplaceAll(result, "."+dummyRenamed+")", ".text)")
		result = strings.ReplaceAll(result, "."+dummyRenamed+"[]", ".text[]")

		// Pattern 4: schema."renamed_text"
		result = strings.ReplaceAll(result, `."`+dummyRenamed+`"`, ".text")

		// Pattern 5: "schema"."renamed_text"
		result = strings.ReplaceAll(result, `"."`+dummyRenamed+`"`, `".text`)

		// Pattern 6: renamed_text[] (array)
		result = strings.ReplaceAll(result, dummyRenamed+"[]", "text[]")
		result = strings.ReplaceAll(result, `"`+dummyRenamed+`"[]`, "text[]")
	}

	return []byte(result)
}

// convertEnumTypesInDump converts all tracked ENUM types to TEXT in the dump.
// This is called AFTER table renaming to avoid the renamer accidentally renaming type names.
func (s *SnapshotGenerator) convertEnumTypesInDump(dump []byte, tracker *enumTypeTracker) []byte {
	if tracker == nil || len(tracker.types) == 0 {
		return dump
	}

	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Split(bufio.ScanLines)
	var result strings.Builder
	inCreateTable := false
	createTableBuffer := strings.Builder{}

	for scanner.Scan() {
		line := scanner.Text()

		// Handle CREATE TABLE multi-line statements
		if inCreateTable {
			createTableBuffer.WriteString(line)
			createTableBuffer.WriteString("\n")

			if strings.HasSuffix(strings.TrimSpace(line), ");") {
				// End of CREATE TABLE - convert ENUMs in the whole statement
				fullCreateTable := createTableBuffer.String()
				convertedCreateTable := convertEnumColumnsToText(fullCreateTable, tracker)
				result.WriteString(convertedCreateTable)
				inCreateTable = false
				createTableBuffer.Reset()
			}
			continue
		}

		// Detect start of CREATE TABLE
		if strings.HasPrefix(line, "CREATE TABLE") {
			if strings.HasSuffix(strings.TrimSpace(line), ");") {
				// Single-line CREATE TABLE (rare) - convert directly
				converted := convertEnumColumnsToText(line+"\n", tracker)
				result.WriteString(converted)
			} else {
				// Multi-line CREATE TABLE (common) - start buffering
				inCreateTable = true
				createTableBuffer.WriteString(line)
				createTableBuffer.WriteString("\n")
			}
			continue
		}

		// Handle ALTER COLUMN TYPE statements (for ENUM conversion)
		if strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "ALTER COLUMN") && strings.Contains(line, "TYPE") {
			convertedLine := convertEnumTypeInAlterColumn(line, tracker)
			result.WriteString(convertedLine)
			result.WriteString("\n")
			continue
		}

		// Convert ENUMs in all other lines (catch-all for type casts, defaults, etc.)
		// This handles cases like:
		// - DEFAULT 'value'::enum_type
		// - Type casts in constraints
		// - Any other ENUM references
		convertedLine := convertEnumTypeInLine(line, tracker)
		result.WriteString(convertedLine)
		result.WriteString("\n")
	}

	return []byte(result.String())
}

func (s *SnapshotGenerator) restoreDump(ctx context.Context, dump []byte, enumTracker ...*enumTypeTracker) error {
	if len(dump) == 0 {
		return nil
	}

	// If ENUM conversion is enabled, remove DROP TYPE statements from the dump
	// because after conversion they would try to drop "text" which is a system type
	if s.convertEnumsToText {
		dump = s.filterDropTypeStatements(dump)
	}

	// Apply ENUM to TEXT conversion BEFORE table renaming (if configured)
	// This must happen BEFORE renaming to avoid complex pattern matching issues
	if s.convertEnumsToText && len(enumTracker) > 0 && enumTracker[0] != nil {
		// Pre-compute all replacement patterns once for performance
		// This avoids regenerating patterns for every line in the dump
		enumTracker[0].computeSortedPatterns()
		s.logger.Info("converting ENUM types to TEXT", loglib.Fields{"enum_count": len(enumTracker[0].types), "pattern_count": len(enumTracker[0].sortedPatterns)})
		dump = s.convertEnumTypesInDump(dump, enumTracker[0])
	}

	// Apply table renaming if configured
	if s.tableRenamer != nil && s.tableRenamer.HasRules() {
		s.logger.Info("applying table renaming to schema dump", loglib.Fields{"dump_size": len(dump)})
		dump = s.tableRenamer.RenameInSQL(dump)

		// IMPORTANT: After table renaming, the word "text" might have been renamed to "piana_text" etc.
		// We need to fix this since "text" is a PostgreSQL built-in type and should not be renamed
		dump = s.fixRenamedTextType(dump)
	}

	_, err := s.pgRestoreFn(ctx, s.optionGenerator.pgrestoreOptions(), dump)
	pgrestoreErr := &pglib.PGRestoreErrors{}
	if err != nil {
		switch {
		case errors.As(err, &pgrestoreErr):
			if pgrestoreErr.HasCriticalErrors() {
				return err
			}
			ignoredErrors := pgrestoreErr.GetIgnoredErrors()
			s.logger.Warn(err, fmt.Sprintf("restore: %d errors ignored", len(ignoredErrors)), loglib.Fields{"errors_ignored": ignoredErrors})
		default:
			return err
		}
	}

	return nil
}

func (s *SnapshotGenerator) syncSchemaLog(ctx context.Context, schemaTables, excludeSchemaTables map[string][]string) error {
	if s.schemalogStore == nil {
		return nil
	}

	s.logger.Info("syncing schema log", loglib.Fields{"schemaTables": schemaTables, "excludeSchemaTables": excludeSchemaTables})

	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	schemas := make([]string, 0, len(schemaTables))
	switch {
	case hasWildcardSchema(schemaTables):
		schemas, err = pglib.DiscoverAllSchemas(ctx, conn)
		if err != nil {
			return fmt.Errorf("discovering schemas: %w", err)
		}
	default:
		for schema := range schemaTables {
			schemas = append(schemas, schema)
		}
	}

	for _, schema := range schemas {
		if _, found := excludeSchemaTables[schema]; found {
			continue
		}
		if _, err := s.schemalogStore.Insert(ctx, schema); err != nil {
			return fmt.Errorf("inserting schemalog entry for schema %q after schema snapshot: %w", schema, err)
		}
	}
	return nil
}

func (s *SnapshotGenerator) createSchemaIfNotExists(ctx context.Context, schemaName string) error {
	targetConn, err := s.connBuilder(ctx, s.targetURL)
	if err != nil {
		return err
	}
	defer targetConn.Close(context.Background())

	_, err = targetConn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	return err
}

func (s *SnapshotGenerator) initialiseSchemaLogStore(ctx context.Context) error {
	exists, err := s.schemalogExists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	s.schemalogStore, err = schemalogpg.NewStore(ctx, schemalogpg.Config{URL: s.sourceURL})
	return err
}

const existsTableQuery = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"

func (s *SnapshotGenerator) schemalogExists(ctx context.Context) (bool, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	// check if the pgstream.schema_log table exists, if not, we can skip the initialisation
	// of the schemalog store
	var exists bool
	err = conn.QueryRow(ctx, []any{&exists}, existsTableQuery, schemalog.SchemaName, schemalog.TableName)
	if err != nil {
		return false, fmt.Errorf("checking schemalog table existence: %w", err)
	}

	return exists, nil
}

func (s *SnapshotGenerator) parseDump(d []byte) *dump {
	scanner := bufio.NewScanner(bytes.NewReader(d))
	scanner.Split(bufio.ScanLines)
	indicesAndConstraints := strings.Builder{}
	filteredDump := strings.Builder{}
	sequenceNames := []string{}
	dumpRoles := make(map[string]role)
	alterTable := ""
	// Track CREATE TABLE for multi-line processing and constraint removal
	inCreateTable := false
	createTableBuffer := strings.Builder{}
	// Track ENUM types for conversion to TEXT
	var enumTracker *enumTypeTracker
	if s.convertEnumsToText {
		enumTracker = newEnumTypeTracker()
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Handle CREATE TABLE multi-line statements
		if inCreateTable {
			createTableBuffer.WriteString(line)
			createTableBuffer.WriteString("\n")

			if strings.HasSuffix(strings.TrimSpace(line), ");") {
				// End of CREATE TABLE
				fullCreateTable := createTableBuffer.String()
				cleanedCreateTable := s.cleanCreateTableConstraints(fullCreateTable)
				filteredDump.WriteString(cleanedCreateTable)
				inCreateTable = false
				createTableBuffer.Reset()
			}
			continue
		}

		// Detect start of CREATE TABLE
		if strings.HasPrefix(line, "CREATE TABLE") {
			if strings.HasSuffix(strings.TrimSpace(line), ");") {
				// Single-line CREATE TABLE (rare)
				cleanedLine := s.cleanCreateTableConstraints(line + "\n")
				filteredDump.WriteString(cleanedLine)
			} else {
				// Multi-line CREATE TABLE (common)
				inCreateTable = true
				createTableBuffer.WriteString(line)
				createTableBuffer.WriteString("\n")
			}
			continue
		}

		switch {
		case strings.HasPrefix(line, "SECURITY LABEL") &&
			isSecurityLabelForExcludedProvider(line, s.excludedSecurityLabels):
			// skip security labels if configured to do so for the specified providers
			continue
		case alterTable != "":
			// check if the previous alter table line is split in two lines and matches a constraint
			if strings.Contains(line, "ADD CONSTRAINT") {
				// Reconstruct full constraint statement for checking
				fullConstraintLine := alterTable + " " + line
				if !s.shouldExcludeConstraint(fullConstraintLine) {
					indicesAndConstraints.WriteString(alterTable)
					indicesAndConstraints.WriteString("\n")
					indicesAndConstraints.WriteString(line)
					indicesAndConstraints.WriteString("\n\n")
				} else {
					s.logger.Debug("excluding constraint (multi-line)", loglib.Fields{"constraint": fullConstraintLine})
				}
				alterTable = ""
			} else {
				filteredDump.WriteString(alterTable)
				filteredDump.WriteString("\n")
				filteredDump.WriteString(line)
				filteredDump.WriteString("\n")
				alterTable = ""
			}
		case strings.Contains(line, `\connect`):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "CREATE TYPE") && strings.Contains(line, "AS ENUM"):
			// Handle CREATE TYPE AS ENUM
			if s.convertEnumsToText && enumTracker != nil {
				// Extract enum name and track it
				enumName := extractEnumNameFromCreateType(line)
				if enumName != "" {
					enumTracker.add(enumName)
				}
				// Skip the entire CREATE TYPE statement (may be multi-line)
				if !strings.HasSuffix(strings.TrimSpace(line), ";") {
					// Multi-line ENUM, skip until semicolon
					for scanner.Scan() {
						nextLine := scanner.Text()
						if strings.HasSuffix(strings.TrimSpace(nextLine), ";") {
							break
						}
					}
				}
				// Don't write to filtered dump (skip ENUM creation)
				continue
			}
			// If not converting ENUMs, keep the statement
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "ALTER TYPE"):
			// Handle ALTER TYPE statements
			if s.convertEnumsToText && enumTracker != nil {
				// Check if it's an ENUM type being altered
				if strings.Contains(line, "ADD VALUE") || strings.Contains(line, "RENAME VALUE") {
					// This is ENUM-specific, skip it
					continue
				}
				// ALTER TYPE RENAME TO is not ENUM-specific, but check if it's an ENUM
				if strings.Contains(line, "RENAME TO") && isAlterTypeForEnum(line, enumTracker) {
					// Skip renaming ENUM types as they won't exist
					continue
				}
			}
			// Keep non-ENUM ALTER TYPE or if not converting
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "CREATE TRIGGER") || strings.HasPrefix(line, "CREATE CONSTRAINT TRIGGER"):
			if !s.excludeTriggers {
				s.logger.Debug("including trigger", loglib.Fields{"line": line})
				indicesAndConstraints.WriteString(line)
				indicesAndConstraints.WriteString("\n\n")
			} else {
				s.logger.Debug("excluding trigger", loglib.Fields{"line": line})
			}
		case strings.HasPrefix(line, "CREATE INDEX"),
			strings.HasPrefix(line, "CREATE UNIQUE INDEX"),
			strings.HasPrefix(line, "CREATE CONSTRAINT"),
			strings.HasPrefix(line, "COMMENT ON CONSTRAINT"),
			strings.HasPrefix(line, "COMMENT ON INDEX"):
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
		case strings.HasPrefix(line, "COMMENT ON TRIGGER"):
			if !s.excludeTriggers {
				indicesAndConstraints.WriteString(line)
				indicesAndConstraints.WriteString("\n\n")
			}
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "ADD CONSTRAINT"):
			if !s.shouldExcludeConstraint(line) {
				indicesAndConstraints.WriteString(line)
			}
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "ALTER COLUMN") && strings.Contains(line, "TYPE"):
			// Handle ALTER TABLE ... ALTER COLUMN ... TYPE
			convertedLine := line
			if s.convertEnumsToText && enumTracker != nil {
				convertedLine = convertEnumTypeInAlterColumn(line, enumTracker)
			}
			filteredDump.WriteString(convertedLine)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "ALTER TABLE") && strings.Contains(line, "REPLICA IDENTITY"):
			// REPLICA IDENTITY lines should be in the indicesAndConstraints section
			// since they reference constraints/indices that are also there
			indicesAndConstraints.WriteString(line)
			indicesAndConstraints.WriteString("\n\n")
		case strings.HasPrefix(line, "ALTER TABLE") && !strings.HasSuffix(line, ";"):
			// keep it in case the alter table is provided in two lines (pg_dump format)
			alterTable = line
		case strings.HasPrefix(line, "CREATE VIEW") || strings.HasPrefix(line, "CREATE OR REPLACE VIEW"):
			// Filter out views that match excluded_views patterns (wildcards only)
			if s.shouldExcludeView(line) {
				// Skip this view and all following lines until we hit a semicolon
				for scanner.Scan() {
					viewLine := scanner.Text()
					if strings.HasSuffix(strings.TrimSpace(viewLine), ";") {
						break
					}
				}
				continue
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "CREATE SEQUENCE"):
			qualifiedName, err := pglib.NewQualifiedName(strings.TrimPrefix(line, "CREATE SEQUENCE "))
			if err == nil {
				sequenceNames = append(sequenceNames, qualifiedName.String())
			}
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		case strings.HasPrefix(line, "DROP SCHEMA"):
			// Skip DROP SCHEMA if exclude_drop_schema is enabled, otherwise include it
			if !s.excludeDropSchema {
				filteredDump.WriteString(line)
				filteredDump.WriteString("\n")
			}
		case strings.HasPrefix(line, "CREATE SCHEMA"):
			// Skip CREATE SCHEMA if exclude_drop_schema is enabled, otherwise include it
			if !s.excludeDropSchema {
				filteredDump.WriteString(line)
				filteredDump.WriteString("\n")
			}
		case strings.HasPrefix(line, "ALTER SCHEMA") && strings.Contains(line, "OWNER TO"):
			// Skip ALTER SCHEMA OWNER TO if exclude_drop_schema is enabled, otherwise include it
			if !s.excludeDropSchema {
				filteredDump.WriteString(line)
				filteredDump.WriteString("\n")
			}
		case isRoleStatement(line):
			roles := s.roleSQLParser.extractRoleNamesFromLine(line)
			if hasExcludedRole(roles) {
				// if any of the roles is excluded or predefined, skip the whole line
				continue
			}

			for _, role := range roles {
				dumpRoles[role.name] = role
			}

			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		default:
			filteredDump.WriteString(line)
			filteredDump.WriteString("\n")
		}

	}

	return &dump{
		full:                  d,
		filtered:              []byte(filteredDump.String()),
		indicesAndConstraints: []byte(indicesAndConstraints.String()),
		sequences:             sequenceNames,
		roles:                 dumpRoles,
		enumTracker:           enumTracker,
	}
}

func (s *SnapshotGenerator) filterRolesDump(rolesDump []byte, keepRoles map[string]role) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(rolesDump))
	scanner.Split(bufio.ScanLines)
	var filteredDump strings.Builder

	if s.optionGenerator.cleanTargetDB {
		// add cleanup for the manual grants
		for _, role := range keepRoles {
			if isPredefinedRole(role.name) || isExcludedRole(role.name) || !role.isOwner() {
				continue
			}
			for schema := range role.schemasWithOwnership {
				filteredDump.WriteString(fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s;\n", pglib.QuoteIdentifier(schema), pglib.QuoteIdentifier(role.name)))
			}
		}
	}

	skipLine := func(lineRoles []role) bool {
		for _, role := range lineRoles {
			_, roleFound := keepRoles[role.name]
			if !roleFound || isPredefinedRole(role.name) || isExcludedRole(role.name) {
				return true
			}
		}
		return false
	}

	for scanner.Scan() {
		line := scanner.Text()
		lineRoles := s.roleSQLParser.extractRoleNamesFromLine(line)
		if skipLine(lineRoles) {
			continue
		}

		// remove role attributes that require superuser privileges to be set
		// when the value is the same as the default.
		line = removeDefaultRoleAttributes(line)

		filteredDump.WriteString(line)
		filteredDump.WriteString("\n")
	}

	for _, role := range keepRoles {
		if isPredefinedRole(role.name) || isExcludedRole(role.name) || !role.isOwner() {
			continue
		}
		// add a line to grant the role to the current user to avoid permission
		// issues when granting ownership (OWNER TO) when using non superuser
		// roles to restore the dump
		filteredDump.WriteString(fmt.Sprintf("GRANT %s TO CURRENT_USER;\n", pglib.QuoteIdentifier(role.name)))
		// add lines to grant access to roles that have object ownership in
		// the schema, otherwise restoring will fail with permission denied for
		// schema.
		for schema := range role.schemasWithOwnership {
			filteredDump.WriteString(fmt.Sprintf("GRANT ALL ON SCHEMA %s TO %s;\n", pglib.QuoteIdentifier(schema), pglib.QuoteIdentifier(role.name)))
		}
	}

	return []byte(filteredDump.String())
}

type options interface {
	ToArgs() []string
}

func (s *SnapshotGenerator) dumpToFile(file string, opts options, d []byte) {
	if s.dumpDebugFile != "" {
		b := bytes.NewBufferString(fmt.Sprintf("pg_dump options: %v\n\n%s", opts.ToArgs(), string(d)))
		if err := os.WriteFile(file, b.Bytes(), 0o644); err != nil { //nolint:gosec
			s.logger.Error(err, fmt.Sprintf("writing dump to debug file %s", file))
		}
	}
}

func (s *SnapshotGenerator) sequenceDumpFile() string {
	return s.getDumpFileName("-sequences")
}

func (s *SnapshotGenerator) rolesDumpFile() string {
	return s.getDumpFileName("-roles")
}

func (s *SnapshotGenerator) getDumpFileName(suffix string) string {
	if s.dumpDebugFile == "" {
		return ""
	}

	fileExtension := filepath.Ext(s.dumpDebugFile)
	if fileExtension == "" {
		// if there's no extension, we assume it's a plain text file
		return s.dumpDebugFile + suffix
	}

	// if there's an extension, we append the suffix before the extension
	baseName := strings.TrimSuffix(s.dumpDebugFile, fileExtension)
	return baseName + suffix + fileExtension
}

func hasWildcardTable(tables []string) bool {
	return slices.Contains(tables, wildcard)
}

func hasWildcardSchema(schemaTables map[string][]string) bool {
	return schemaTables[wildcard] != nil
}

// shouldExcludeView checks if a CREATE VIEW statement should be excluded
// based on the excluded_views configuration (only wildcards are checked here,
// specific views are excluded by pg_dump --exclude-table)
func (s *SnapshotGenerator) shouldExcludeView(line string) bool {
	if s.excludedViews == nil || len(s.excludedViews) == 0 {
		return false
	}

	// Extract view name from "CREATE VIEW schema.viewname" or "CREATE OR REPLACE VIEW schema.viewname"
	viewPrefix := "CREATE VIEW "
	if strings.HasPrefix(line, "CREATE OR REPLACE VIEW") {
		viewPrefix = "CREATE OR REPLACE VIEW "
	}

	viewNamePart := strings.TrimPrefix(line, viewPrefix)
	viewNamePart = strings.TrimSpace(viewNamePart)
	// Extract just the qualified name (before AS or any other clause)
	if idx := strings.Index(viewNamePart, " "); idx > 0 {
		viewNamePart = viewNamePart[:idx]
	}

	qualifiedName, err := pglib.NewQualifiedName(viewNamePart)
	if err != nil {
		s.logger.Debug("failed to parse view name from line", loglib.Fields{"line": line, "error": err})
		return false
	}

	s.logger.Debug("checking view against excludedViews", loglib.Fields{
		"view":          qualifiedName.String(),
		"schema":        qualifiedName.Schema(),
		"excludedViews": s.excludedViews,
	})

	// Check if this view matches any wildcard exclusion pattern
	if views, ok := s.excludedViews[qualifiedName.Schema()]; ok {
		s.logger.Debug("found schema in excludedViews", loglib.Fields{"schema": qualifiedName.Schema(), "views": views})
		if hasWildcardTable(views) {
			// Wildcard exclusion for this schema
			s.logger.Debug("excluding view due to wildcard match", loglib.Fields{"view": qualifiedName.String()})
			return true
		}
	}

	s.logger.Debug("not excluding view", loglib.Fields{"view": qualifiedName.String()})
	return false
}

// shouldExcludeConstraint checks if an ALTER TABLE ADD CONSTRAINT should be excluded
// based on exclude_check_constraints and exclude_foreign_keys configuration
func (s *SnapshotGenerator) shouldExcludeConstraint(line string) bool {
	// Never exclude PRIMARY KEY (critical for replication)
	if strings.Contains(line, "PRIMARY KEY") {
		s.logger.Debug("not excluding PRIMARY KEY constraint", loglib.Fields{"line": line})
		return false
	}

	// Never exclude UNIQUE constraints (critical for replication)
	// Note: Check that it's not part of a FOREIGN KEY UNIQUE combo
	if strings.Contains(line, "UNIQUE") && !strings.Contains(line, "FOREIGN KEY") {
		s.logger.Debug("not excluding UNIQUE constraint", loglib.Fields{"line": line})
		return false
	}

	// Check for CHECK constraints
	if s.excludeCheckConstraints {
		if strings.Contains(line, "CHECK (") || strings.Contains(line, "CHECK(") {
			s.logger.Debug("excluding CHECK constraint", loglib.Fields{"line": line})
			return true
		}
	}

	// Check for FOREIGN KEY constraints
	if s.excludeForeignKeys {
		if strings.Contains(line, "FOREIGN KEY") {
			s.logger.Debug("excluding FOREIGN KEY constraint", loglib.Fields{"line": line})
			return true
		}
	}

	return false
}

// cleanCreateTableConstraints removes inline constraints from CREATE TABLE statements
// using a robust parser that handles nested parentheses
func (s *SnapshotGenerator) cleanCreateTableConstraints(createTableSQL string) string {
	if !s.excludeCheckConstraints && !s.excludeForeignKeys {
		return createTableSQL
	}

	result := createTableSQL

	// Remove inline CHECK constraints (with robust parentheses handling)
	if s.excludeCheckConstraints {
		result = s.removeInlineCheckConstraints(result)
	}

	// Remove inline FOREIGN KEY references (simple pattern, no ON DELETE/UPDATE)
	if s.excludeForeignKeys {
		result = s.removeInlineForeignKeys(result)
	}

	return result
}

// removeInlineCheckConstraints removes CHECK constraints from CREATE TABLE
// using a character-by-character parser to handle nested parentheses correctly
func (s *SnapshotGenerator) removeInlineCheckConstraints(sql string) string {
	result := strings.Builder{}
	i := 0

	for i < len(sql) {
		// Look for "CONSTRAINT name CHECK (" or just "CHECK ("
		checkIdx := -1
		isNamed := false
		constraintStartRelative := -1 // Store where CONSTRAINT starts relative to i

		// Look for anonymous CHECK first
		anonCheckIdx := strings.Index(strings.ToUpper(sql[i:]), " CHECK ")

		// Look for named CONSTRAINT ... CHECK
		constraintIdx := strings.Index(strings.ToUpper(sql[i:]), "CONSTRAINT ")
		var namedCheckIdx int = -1
		if constraintIdx != -1 {
			// Check if there's a CHECK after this CONSTRAINT
			afterConstraint := constraintIdx + 11 // len("CONSTRAINT ")
			remainingUpper := strings.ToUpper(sql[i+afterConstraint:])

			// Find the constraint name end (next space or CHECK keyword)
			nameEnd := strings.Index(remainingUpper, " CHECK ")
			if nameEnd != -1 {
				namedCheckIdx = i + afterConstraint + nameEnd + 1 // +1 for the space
				constraintStartRelative = constraintIdx           // Store where CONSTRAINT keyword starts
			}
		}

		// Choose whichever comes first: anonymous CHECK or named CONSTRAINT CHECK
		// Note: anonCheckIdx points to the space before CHECK, so we need to add 1 for comparison
		if anonCheckIdx != -1 && (namedCheckIdx == -1 || (i+anonCheckIdx+1) < namedCheckIdx) {
			// Anonymous CHECK comes first (or is the only one)
			checkIdx = i + anonCheckIdx + 1 // +1 to include the leading space
			isNamed = false
		} else if namedCheckIdx != -1 {
			// Named CONSTRAINT CHECK comes first (or is the only one)
			checkIdx = namedCheckIdx
			isNamed = true
		}

		// No CHECK found, copy the rest and done
		if checkIdx == -1 {
			result.WriteString(sql[i:])
			break
		}

		// Copy everything before the CHECK (or CONSTRAINT if named)
		if isNamed && constraintStartRelative != -1 {
			// Copy up to CONSTRAINT keyword
			copyUntil := i + constraintStartRelative
			// Trim trailing space before CONSTRAINT
			beforeConstraint := sql[i:copyUntil]
			beforeConstraint = strings.TrimRight(beforeConstraint, " \t")
			result.WriteString(beforeConstraint)
			s.logger.Debug("removing inline CHECK constraint (named)", loglib.Fields{"position": checkIdx})
		} else {
			result.WriteString(sql[i:checkIdx])
			s.logger.Debug("removing inline CHECK constraint (anonymous)", loglib.Fields{"position": checkIdx})
		}

		// Find the opening parenthesis after CHECK
		checkKeywordEnd := checkIdx + 6 // len(" CHECK")
		openParenPos := strings.Index(sql[checkKeywordEnd:], "(")
		if openParenPos == -1 {
			// Malformed, just skip CHECK keyword and continue
			result.WriteString(sql[checkIdx:checkKeywordEnd])
			i = checkKeywordEnd
			continue
		}

		// Start parsing from the opening parenthesis
		startPos := checkKeywordEnd + openParenPos
		parenCount := 0
		endPos := startPos

		// Count parentheses to find the matching closing parenthesis
		for endPos < len(sql) {
			if sql[endPos] == '(' {
				parenCount++
			} else if sql[endPos] == ')' {
				parenCount--
				if parenCount == 0 {
					// Found the matching closing parenthesis
					endPos++
					break
				}
			}
			endPos++
		}

		// Skip past the CHECK constraint (don't write it to result)
		i = endPos

		// Clean up any trailing comma/space after removed constraint
		hasCommaAfter := false
		whitespaceAfter := 0
		for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n') {
			i++
			whitespaceAfter++
		}
		if i < len(sql) && sql[i] == ',' {
			hasCommaAfter = true
			i++ // skip trailing comma
			// Also skip whitespace after comma
			for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n') {
				i++
			}
		}

		// If no comma after, check if we need to clean a trailing comma before
		// This handles the case: "col1 INT, CHECK(...)" where we need to remove the comma before CHECK
		if !hasCommaAfter {
			// Look back in result to remove trailing comma if present
			resultStr := result.String()
			trimmed := strings.TrimRight(resultStr, " \t\n")
			if strings.HasSuffix(trimmed, ",") {
				// Remove the trailing comma
				trimmed = trimmed[:len(trimmed)-1]
				// Reset result builder with trimmed content
				result.Reset()
				result.WriteString(trimmed)
			}
		}
	}

	return result.String()
}

// removeInlineForeignKeys removes REFERENCES clauses from CREATE TABLE
// Simple pattern matching - does not handle ON DELETE/ON UPDATE (as per requirement)
func (s *SnapshotGenerator) removeInlineForeignKeys(sql string) string {
	// Pattern: REFERENCES table_name(column_name)
	// Simplified regex for basic REFERENCES without ON DELETE/UPDATE
	referencesPattern := regexp.MustCompile(`(?i)\s+REFERENCES\s+[a-zA-Z_][a-zA-Z0-9_.]*\s*\([^)]*\)`)

	matches := referencesPattern.FindAllStringIndex(sql, -1)
	if len(matches) > 0 {
		s.logger.Debug("removing inline FOREIGN KEY references", loglib.Fields{"count": len(matches)})
	}

	return referencesPattern.ReplaceAllString(sql, "")
}

// returns all the lines of d1 that are not in d2
func getDumpsDiff(d1, d2 []byte) []byte {
	var diff strings.Builder
	lines1 := bytes.Split(d1, []byte("\n"))
	lines2 := bytes.Split(d2, []byte("\n"))
	lines2map := make(map[string]bool)
	for _, line := range lines2 {
		lines2map[string(line)] = true
	}

	for _, line := range lines1 {
		if !lines2map[string(line)] {
			diff.Write(line)
			diff.WriteString("\n")
		}
	}

	return []byte(diff.String())
}

// filterDropAndCreateSchema removes DROP SCHEMA, CREATE SCHEMA, and ALTER SCHEMA OWNER TO
// statements from the cleanup dump. This is useful when schemas should already exist in the
// target database and should not be dropped or recreated.
func filterDropAndCreateSchema(cleanup []byte) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(cleanup))
	scanner.Split(bufio.ScanLines)
	var filtered strings.Builder
	skipUntilSemicolon := false

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		// If we're in the middle of a multi-line statement, continue skipping
		if skipUntilSemicolon {
			if strings.HasSuffix(trimmedLine, ";") {
				skipUntilSemicolon = false
			}
			continue
		}

		// Detect DROP SCHEMA statements (with IF EXISTS and/or CASCADE)
		if strings.HasPrefix(line, "DROP SCHEMA") {
			if !strings.HasSuffix(trimmedLine, ";") {
				skipUntilSemicolon = true
			}
			continue
		}

		// Detect CREATE SCHEMA statements
		if strings.HasPrefix(line, "CREATE SCHEMA") {
			if !strings.HasSuffix(trimmedLine, ";") {
				skipUntilSemicolon = true
			}
			continue
		}

		// Skip ALTER SCHEMA OWNER TO statements (often follows CREATE SCHEMA)
		if strings.HasPrefix(line, "ALTER SCHEMA") && strings.Contains(line, "OWNER TO") {
			if !strings.HasSuffix(trimmedLine, ";") {
				skipUntilSemicolon = true
			}
			continue
		}

		// Keep all other lines
		filtered.WriteString(line)
		filtered.WriteString("\n")
	}

	return []byte(filtered.String())
}

func isSecurityLabelForExcludedProvider(line string, excludedProviders []string) bool {
	if slices.Contains(excludedProviders, wildcard) {
		return true
	}
	for _, provider := range excludedProviders {
		if strings.Contains(line, fmt.Sprintf("SECURITY LABEL FOR %s ", provider)) {
			return true
		}
	}
	return false
}
