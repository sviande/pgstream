// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
)

type Config struct {
	URL               string
	BatchConfig       batch.Config
	DisableTriggers   bool
	OnConflictAction  string
	BulkIngestEnabled bool
	RetryPolicy       backoff.Config
	IgnoreDDL         bool
	// ExcludeTriggers, when true, skips replicated trigger DDL (CREATE/ALTER/DROP
	// TRIGGER) so the target never gets triggers, matching the schema snapshot's
	// exclude_triggers behaviour. Without it, trigger DDL is only filtered from
	// the initial snapshot but still replayed live during CDC.
	ExcludeTriggers bool
	// SourceURL is the source database URL. It is only used to bootstrap the
	// set of existing ENUM types when ConvertEnumsToText is enabled, so that DDL
	// referencing a pre-existing enum is converted even though its CREATE TYPE
	// predates the replication stream.
	SourceURL string
	// ConvertEnumsToText rewrites replicated DDL so ENUM types are never created
	// on the target and ENUM columns become TEXT/TEXT[], matching the snapshot.
	ConvertEnumsToText bool
	// TableRenamer, when set, rewrites table identifiers in replicated DDL (e.g.
	// public.foo -> public.piana_foo), matching the snapshot schema renamer.
	TableRenamer *renamer.TableRenamer
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
