// SPDX-License-Identifier: Apache-2.0

package builder

import (
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
)

type SnapshotListenerConfig struct {
	Data     *pgsnapshotgenerator.Config
	Adapter  adapter.SnapshotConfig
	Recorder *SnapshotRecorderConfig
	Schema   *SchemaSnapshotConfig
	// TableRenamer is used to transform table names in schema snapshots
	TableRenamer *renamer.TableRenamer
}

type SchemaSnapshotConfig struct {
	SchemaLogStore *schemalogpg.Config
	DumpRestore    *pgdumprestore.Config
}

type SnapshotRecorderConfig struct {
	RepeatableSnapshots bool
	SnapshotStoreURL    string
}
