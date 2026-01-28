// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	URL               string
	BatchConfig       batch.Config
	SchemaLogStore    schemalogpg.Config
	DisableTriggers   bool
	OnConflictAction  string
	BulkIngestEnabled bool
	RetryPolicy       backoff.Config
	// IncludeTables is a list of tables to include in DDL replication.
	// If set, only these tables will have their DDL changes replicated.
	IncludeTables []string
	// ExcludeTables is a list of tables to exclude from DDL replication.
	// Tables in this list will not have their DDL changes replicated.
	ExcludeTables []string
	// ExcludeCheckConstraints excludes CHECK constraints from schema sync (snapshot only)
	ExcludeCheckConstraints bool
	// ExcludeTriggers excludes triggers from schema sync (snapshot only)
	ExcludeTriggers bool
	// ExcludeForeignKeys excludes foreign key constraints from schema sync (snapshot only)
	ExcludeForeignKeys bool
	// ConvertEnumsToText converts all ENUM types to TEXT in the target database.
	// This is useful for compatibility with tools like Metabase that may have issues with custom ENUM types.
	// When enabled:
	// - ENUM types will not be created in the target
	// - ENUM columns will be created as TEXT
	// - ENUM[] columns will be created as TEXT[]
	// - ALTER TYPE operations on ENUMs will be ignored
	ConvertEnumsToText bool
	// TableRenamer is used to rename tables in DDL queries.
	// If set, table names will be transformed using this renamer before generating DDL statements.
	TableRenamer TableRenamer
}

const (
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 30 * time.Second
)

func (c *Config) retryPolicy() backoff.Config {
	if c.RetryPolicy.IsSet() {
		return c.RetryPolicy
	}
	return backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultInitialInterval,
			MaxInterval:     defaultMaxInterval,
		},
	}
}
