// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

func TestHandler_sendHeartbeat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connBroken bool
		stopped    bool
		sendErr    error

		wantSent       []uint64
		wantErr        error
		wantConnBroken bool
	}{
		{
			name: "ok",

			wantSent: []uint64{testLSN},
		},
		{
			name:       "ok - broken connection left to the listener",
			connBroken: true,

			wantConnBroken: true,
		},
		{
			name:    "ok - stopped",
			stopped: true,
		},
		{
			name:    "error - sending status update",
			sendErr: errTest,

			wantSent:       []uint64{testLSN},
			wantErr:        errTest,
			wantConnBroken: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sent []uint64
			h := Handler{
				logger: log.NewNoopLogger(),
				pgReplicationConn: &pgmocks.ReplicationConn{
					SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
						sent = append(sent, lsn)
						return tc.sendErr
					},
				},
				lsnParser:         NewLSNParser(),
				lastSyncedLSN:     replication.LSN(testLSN),
				heartbeatInterval: time.Second,
				connBroken:        tc.connBroken,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.stopped {
				cancel()
			}

			err := h.sendHeartbeat(ctx)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantSent, sent)
			require.Equal(t, tc.wantConnBroken, h.connBroken)
		})
	}
}

// TestHandler_Heartbeat verifies that once replication has started the
// heartbeat keeps sending the last synced LSN, and that Close stops it.
func TestHandler_Heartbeat(t *testing.T) {
	t.Parallel()

	var sent atomic.Int32
	conn := &pgmocks.ReplicationConn{
		StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error { return nil },
		SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
			if lsn != testLSN {
				return fmt.Errorf("unexpected LSN %d", lsn)
			}
			sent.Add(1)
			return nil
		},
		CloseFn: func(ctx context.Context) error { return nil },
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	h := Handler{
		logger:            log.NewNoopLogger(),
		pgReplicationConn: conn,
		lsnParser:         NewLSNParser(),
		logFields:         log.Fields{},
		heartbeatInterval: 10 * time.Millisecond,
		heartbeatCtx:      heartbeatCtx,
		stopHeartbeat:     stopHeartbeat,
	}

	require.NoError(t, h.StartReplicationFromLSN(context.Background(), replication.LSN(testLSN)))
	// one status update from the start, then the heartbeat's
	require.Eventually(t, func() bool { return sent.Load() >= 3 }, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, h.Close())
	afterClose := sent.Load()
	time.Sleep(5 * h.heartbeatInterval)
	require.Equal(t, afterClose, sent.Load(), "the heartbeat must stop on Close")
}
