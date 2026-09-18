package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

var errBatcherTestSend = errors.New("test ClickHouse unavailable")

type batcherTestConn struct {
	driver.Conn
	mu        sync.Mutex
	failures  int
	failAt    int
	attempts  [][]string
	delivered []string
}

func (c *batcherTestConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &batcherTestBatch{conn: c}, nil
}

type batcherTestBatch struct {
	driver.Batch
	conn *batcherTestConn
	ids  []string
}

func (b *batcherTestBatch) Append(values ...any) error {
	b.ids = append(b.ids, values[0].(string))
	return nil
}

func (b *batcherTestBatch) Send() error {
	c := b.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts = append(c.attempts, append([]string(nil), b.ids...))
	if c.failures > 0 {
		c.failures--
		return errBatcherTestSend
	}
	if c.failAt == len(c.attempts) {
		return errBatcherTestSend
	}
	c.delivered = append(c.delivered, b.ids...)
	return nil
}

func (c *batcherTestConn) snapshot() ([][]string, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.attempts...), append([]string(nil), c.delivered...)
}

func batcherTestNew(c *batcherTestConn, size int) *batcher {
	return newBatcher(c, "archive", size, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func batcherTestEnqueue(t *testing.T, b *batcher, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := b.enqueue(&nostr.Event{ID: id}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
}

func TestBatcherRetryDoesNotConsumeInput(t *testing.T) {
	conn := &batcherTestConn{failures: 1}
	b := batcherTestNew(conn, 2)
	b.start()
	defer b.shutdown()
	batcherTestEnqueue(t, b, "a", "b")
	batcherTestWait(t, func() bool { attempts, _ := conn.snapshot(); return len(attempts) >= 1 })
	for i := 0; i < cap(b.in); i++ {
		batcherTestEnqueue(t, b, fmt.Sprintf("queued-%d", i))
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(b.in); got != cap(b.in) {
		t.Fatalf("retry consumed input: buffered %d, want %d", got, cap(b.in))
	}
	if !errors.Is(b.enqueue(&nostr.Event{}), ErrBatchFull) {
		t.Fatal("full retry buffer did not load-shed")
	}
	time.Sleep(100 * time.Millisecond)
	attempts, _ := conn.snapshot()
	if len(attempts) != 1 {
		t.Fatalf("retried before backoff elapsed: %v", attempts)
	}
	batcherTestWait(t, func() bool { _, delivered := conn.snapshot(); return len(delivered) == cap(b.in)+2 })
	attempts, delivered := conn.snapshot()
	if len(attempts) < 2 || !reflect.DeepEqual(attempts[1], []string{"a", "b"}) {
		t.Fatalf("pending mixed with new input: %v", attempts)
	}
	if len(delivered) != cap(b.in)+2 {
		t.Fatalf("recovery delivered %d events, want %d", len(delivered), cap(b.in)+2)
	}
}

func TestBatcherFlushAllDuringBackoff(t *testing.T) {
	conn := &batcherTestConn{failures: 2}
	b := batcherTestNew(conn, 2)
	b.start()
	defer b.shutdown()
	batcherTestEnqueue(t, b, "a", "b")
	batcherTestWait(t, func() bool { attempts, _ := conn.snapshot(); return len(attempts) >= 1 })
	batcherTestEnqueue(t, b, "c")
	start := time.Now()
	if err := b.FlushAll(); !errors.Is(err, errBatcherTestSend) {
		t.Fatalf("FlushAll error = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond || len(b.in) != 1 {
		t.Fatal("FlushAll slept through backoff or consumed input after failure")
	}
	// The explicit failed attempt resets the timer to the second backoff.
	time.Sleep(100 * time.Millisecond)
	attempts, _ := conn.snapshot()
	if len(attempts) != 2 {
		t.Fatalf("second retry was early: %v", attempts)
	}
	batcherTestWait(t, func() bool { attempts, _ := conn.snapshot(); return len(attempts) >= 3 })
	if err := b.FlushAll(); err != nil {
		t.Fatalf("FlushAll after recovery: %v", err)
	}
	_, delivered := conn.snapshot()
	if !reflect.DeepEqual(delivered, []string{"a", "b", "c"}) {
		t.Fatalf("delivered %v", delivered)
	}
}

func TestBatcherStopDuringBackoff(t *testing.T) {
	for _, failures := range []int{1, 2} {
		t.Run(fmt.Sprintf("failures-%d", failures), func(t *testing.T) {
			conn := &batcherTestConn{failures: failures}
			b := batcherTestNew(conn, 2)
			var logs bytes.Buffer
			b.log = slog.New(slog.NewTextHandler(&logs, nil))
			b.start()
			batcherTestEnqueue(t, b, "a", "b")
			batcherTestWait(t, func() bool { attempts, _ := conn.snapshot(); return len(attempts) >= 1 })
			batcherTestEnqueue(t, b, "c")
			done := make(chan struct{})
			go func() {
				b.shutdown() // waits for the worker's WaitGroup
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("shutdown failed to terminate during backoff")
			}
			attempts, delivered := conn.snapshot()
			if len(attempts) < 2 {
				t.Fatal("final flush not attempted")
			}
			if failures == 2 {
				if !strings.Contains(logs.String(), "events lost") || !strings.Contains(logs.String(), "events_lost=3") {
					t.Fatalf("missing honest loss count: %s", logs.String())
				}
			} else if !reflect.DeepEqual(delivered, []string{"a", "b", "c"}) {
				t.Fatalf("final flush delivered %v", delivered)
			}
			for i := 0; i < 10; i++ {
				if err := b.enqueue(&nostr.Event{}); !errors.Is(err, ErrBatchFull) {
					t.Fatalf("enqueue after stop = %v", err)
				}
			}
			if err := b.FlushAll(); !errors.Is(err, ErrBatcherStopped) {
				t.Fatalf("FlushAll after stop = %v", err)
			}
			b.shutdown() // repeated shutdown must also be safe
		})
	}
}

func TestBatcherPartialChunking(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("fail-at-%d", failAt), func(t *testing.T) {
			conn := &batcherTestConn{failAt: failAt}
			b := batcherTestNew(conn, 2)
			var flushed []string
			var flushedMu sync.Mutex
			onFlushed := func(events []*nostr.Event) {
				flushedMu.Lock()
				defer flushedMu.Unlock()
				for _, event := range events {
					flushed = append(flushed, event.ID)
				}
			}
			b.onFlushed.Store(&onFlushed)
			// A 2.5x backlog is captured before the worker starts.
			want := []string{"a", "b", "c", "d", "e"}
			batcherTestEnqueue(t, b, want...)
			b.start()
			defer b.shutdown()
			batcherTestWait(t, func() bool { attempts, _ := conn.snapshot(); return len(attempts) >= failAt })
			_, delivered := conn.snapshot()
			flushedMu.Lock()
			gotFlushed := append([]string(nil), flushed...)
			flushedMu.Unlock()
			if !reflect.DeepEqual(gotFlushed, delivered) || len(gotFlushed) != (failAt-1)*2 {
				t.Fatalf("callback before accepted chunk: flushed=%v delivered=%v", gotFlushed, delivered)
			}
			batcherTestWait(t, func() bool { _, delivered := conn.snapshot(); return len(delivered) == len(want) })
			if err := b.FlushAll(); err != nil {
				t.Fatal(err)
			}
			attempts, delivered := conn.snapshot()
			for _, chunk := range attempts {
				if len(chunk) > b.maxSize {
					t.Fatalf("oversized send: %v", chunk)
				}
			}
			flushedMu.Lock()
			defer flushedMu.Unlock()
			if !reflect.DeepEqual(delivered, want) || !reflect.DeepEqual(flushed, want) {
				t.Fatalf("want exactly once %v: delivered=%v onFlushed=%v", want, delivered, flushed)
			}
		})
	}
}

func batcherTestWait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for batcher")
		}
		time.Sleep(time.Millisecond)
	}
}
