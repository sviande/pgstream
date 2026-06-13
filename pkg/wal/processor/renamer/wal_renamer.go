// SPDX-License-Identifier: Apache-2.0

package renamer

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Renamer is a processor wrapper that transforms table names in WAL events
// before passing them to the wrapped processor.
type Renamer struct {
	processor    processor.Processor
	tableRenamer *TableRenamer
	logger       loglib.Logger
}

// Option is a functional option for configuring the Renamer.
type Option func(*Renamer)

// New creates a new Renamer processor wrapper with the given configuration.
// The renamer will apply table name transformations to WAL events before
// passing them to the wrapped processor.
func New(proc processor.Processor, cfg *Config, opts ...Option) (*Renamer, error) {
	tableRenamer, err := NewTableRenamer(cfg)
	if err != nil {
		return nil, err
	}

	r := &Renamer{
		processor:    proc,
		tableRenamer: tableRenamer,
		logger:       loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// WithLogger sets the logger for the Renamer.
func WithLogger(logger loglib.Logger) Option {
	return func(r *Renamer) {
		r.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_renamer",
		})
	}
}

// ProcessWALEvent transforms the table name in the WAL event and delegates
// to the wrapped processor.
func (r *Renamer) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event != nil && event.Data != nil {
		r.transformTableName(event.Data)
	}
	return r.processor.ProcessWALEvent(ctx, event)
}

// transformTableName applies the renaming rules to the table name in the WAL data.
func (r *Renamer) transformTableName(data *wal.Data) {
	if !r.tableRenamer.HasRules() {
		return
	}

	originalTable := data.Table
	newTable := r.tableRenamer.RenameTable(data.Schema, data.Table)

	if newTable != originalTable {
		r.logger.Trace("renamed table", loglib.Fields{
			"schema":         data.Schema,
			"original_table": originalTable,
			"new_table":      newTable,
		})
		data.Table = newTable
	}
}

// Name returns the name of the wrapped processor.
func (r *Renamer) Name() string {
	return r.processor.Name()
}

// Close closes the wrapped processor.
func (r *Renamer) Close() error {
	return r.processor.Close()
}

// GetTableRenamer returns the underlying TableRenamer for use by other components
// (e.g., the schema snapshot generator).
func (r *Renamer) GetTableRenamer() *TableRenamer {
	return r.tableRenamer
}
