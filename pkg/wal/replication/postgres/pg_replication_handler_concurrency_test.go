// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// TestHandler_ConcurrentConnAccess exercises ReceiveMessage (listener goroutine),
// SyncLSN (checkpointer goroutine) and ResetConnection concurrently. Run with
// -race: without Handler.connMu the shared mock state and the pgReplicationConn
// swap race, which is the root cause of the production "unexpected message:
// ReadyForQuery" desyncs.
func TestHandler_ConcurrentConnAccess(t *testing.T) {
	t.Parallel()

	// shared is intentionally unguarded by its own lock: it relies on the
	// Handler serializing every connection access through connMu.
	shared := 0
	newConn := func() *pgmocks.ReplicationConn {
		return &pgmocks.ReplicationConn{
			ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
				shared++
				return &pglib.ReplicationMessage{LSN: testLSN, ServerTime: now, WALData: []byte("x")}, nil
			},
			SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
				shared++
				return nil
			},
			StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error { return nil },
			CloseFn:            func(ctx context.Context) error { return nil },
		}
	}

	h := Handler{
		logger:            log.NewNoopLogger(),
		pgReplicationConn: newConn(),
		pgReplicationConnBuilder: func() (pglib.ReplicationQuerier, error) {
			return newConn(), nil
		},
		pgConnBuilder: func() (pglib.Querier, error) {
			return &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					if s, ok := dest[0].(*string); ok {
						*s = testLSNStr
					}
					return nil
				},
				CloseFn: func(ctx context.Context) error { return nil },
			}, nil
		},
		pgReplicationSlotName: testSlot,
		lsnParser:             NewLSNParser(),
		logFields:             log.Fields{},
		pluginArguments:       defaultPluginArguments,
	}

	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { // listener
		defer wg.Done()
		for range iterations {
			_, _ = h.ReceiveMessage(context.Background())
		}
	}()
	go func() { // checkpointer
		defer wg.Done()
		for range iterations {
			_ = h.SyncLSN(context.Background(), replication.LSN(testLSN))
		}
	}()
	go func() { // reconnector
		defer wg.Done()
		for range iterations {
			h.connMu.Lock()
			h.connBroken = true
			h.connMu.Unlock()
			_ = h.ResetConnection(context.Background())
		}
	}()

	wg.Wait()
}

// TestHandler_ResetConnection_SingleFlight verifies that when two goroutines
// observe the same broken connection and both trigger ResetConnection, the
// connection is rebuilt exactly once (so a freshly reconnected stream is not
// immediately torn down again, avoiding extra "slot active for PID" churn).
func TestHandler_ResetConnection_SingleFlight(t *testing.T) {
	t.Parallel()

	var builderCalls atomic.Int32
	h := Handler{
		logger:            log.NewNoopLogger(),
		pgReplicationConn: &pgmocks.ReplicationConn{CloseFn: func(ctx context.Context) error { return nil }},
		pgReplicationConnBuilder: func() (pglib.ReplicationQuerier, error) {
			builderCalls.Add(1)
			return &pgmocks.ReplicationConn{
				StartReplicationFn:        func(ctx context.Context, cfg pglib.ReplicationConfig) error { return nil },
				SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error { return nil },
				CloseFn:                   func(ctx context.Context) error { return nil },
			}, nil
		},
		pgConnBuilder: func() (pglib.Querier, error) {
			return &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					if s, ok := dest[0].(*string); ok {
						*s = testLSNStr
					}
					return nil
				},
				CloseFn: func(ctx context.Context) error { return nil },
			}, nil
		},
		pgReplicationSlotName: testSlot,
		lsnParser:             NewLSNParser(),
		logFields:             log.Fields{},
		pluginArguments:       defaultPluginArguments,
		connBroken:            true, // both goroutines observed the same failure
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_ = h.ResetConnection(context.Background())
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), builderCalls.Load(), "connection should be rebuilt exactly once")
	require.False(t, h.connBroken, "connBroken should be cleared after a successful reset")
}
