// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
)

func TestAddProcessorModifiers_FilterBeforeRenamer(t *testing.T) {
	t.Parallel()

	// Track events that reach the final processor
	var receivedEvents []*wal.Event
	mockProcessor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			receivedEvents = append(receivedEvents, walEvent)
			return nil
		},
		CloseFn: func() error {
			return nil
		},
	}

	// Config with filter excluding "ExcludedTable" and renamer adding "newprefix_" prefix
	config := &Config{
		Processor: ProcessorConfig{
			Filter: &filter.Config{
				ExcludeTables: []string{"public.ExcludedTable"},
			},
			TableRenamer: &renamer.Config{
				Rules: []renamer.Rule{
					{Schema: "public", Match: "^(.*)$", Replace: "newprefix_$1"},
				},
			},
		},
	}

	logger := log.NewNoopLogger()

	// Build processor chain with modifiers
	processor, closer, err := addProcessorModifiers(context.Background(), config, logger, mockProcessor, nil)
	require.NoError(t, err)
	defer closer()

	// Test 1: Event for excluded table should be filtered (not reach mock processor)
	excludedEvent := &wal.Event{
		Data: &wal.Data{
			Schema: "public",
			Table:  "ExcludedTable",
			Action: "I",
		},
	}
	err = processor.ProcessWALEvent(context.Background(), excludedEvent)
	require.NoError(t, err)
	require.Empty(t, receivedEvents, "excluded table event should not reach the processor")

	// Test 2: Event for non-excluded table should pass and be renamed
	receivedEvents = nil // reset
	allowedEvent := &wal.Event{
		Data: &wal.Data{
			Schema: "public",
			Table:  "Users",
			Action: "I",
		},
	}
	err = processor.ProcessWALEvent(context.Background(), allowedEvent)
	require.NoError(t, err)
	require.Len(t, receivedEvents, 1, "allowed table event should reach the processor")
	require.Equal(t, "newprefix_Users", receivedEvents[0].Data.Table, "table should be renamed")
}
