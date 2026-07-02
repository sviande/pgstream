// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
	"github.com/xataio/pgstream/pkg/wal/processor/renamer"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

func TestAddProcessorModifiers_TransformerRunsBeforeRenamer(t *testing.T) {
	var captured *wal.Event
	target := &mocks.Processor{
		ProcessWALEventFn: func(_ context.Context, e *wal.Event) error {
			captured = e
			return nil
		},
	}

	config := &Config{
		Processor: ProcessorConfig{
			Transformer: &transformer.Config{
				ValidationMode: "relaxed",
				TransformerRules: []transformer.TableRules{
					{
						Schema: "public",
						Table:  "users",
						ColumnRules: map[string]transformer.TransformerRules{
							"email": {Name: "string"},
						},
					},
				},
			},
			TableRenamer: &renamer.Config{
				Rules: []renamer.Rule{
					{Schema: "public", Match: "^users$", Replace: "piana_users"},
				},
			},
		},
	}

	proc, closer, err := addProcessorModifiers(context.Background(), config, log.NewNoopLogger(), target, nil)
	require.NoError(t, err)
	require.NotNil(t, closer)

	const email = "alice@example.com"
	event := &wal.Event{Data: &wal.Data{
		Action:  "I",
		Schema:  "public",
		Table:   "users",
		Columns: []wal.Column{{Name: "email", Type: "text", Value: email}},
	}}

	require.NoError(t, proc.ProcessWALEvent(context.Background(), event))

	require.NotNil(t, captured)
	require.Equal(t, "piana_users", captured.Data.Table)
	require.NotEqual(t, email, captured.Data.Columns[0].Value)
}
