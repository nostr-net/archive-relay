package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nostr-net/archive-relay/internal/config"
)

type tombTestConn struct {
	driver.Conn
	mu                          sync.Mutex
	failures, prepares, reloads int
	probeMisses                 int
	reloadTimes                 []time.Time
	rows                        []tombstoneReq
	probeErr                    error
	execErr                     error
}

func (c *tombTestConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepares++
	if c.failures > 0 {
		c.failures--
		return nil, errors.New("prepare unavailable")
	}
	return &tombTestBatch{conn: c}, nil
}
func (c *tombTestConn) Exec(_ context.Context, q string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasPrefix(q, "SYSTEM RELOAD") {
		c.reloads++
		c.reloadTimes = append(c.reloadTimes, time.Now())
	}
	if strings.Contains(q, "MATERIALIZE INDEX") {
		return c.execErr
	}
	return nil
}
func (c *tombTestConn) QueryRow(ctx context.Context, q string, args ...any) driver.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	found := uint8(1)
	// the schema index-introspection query is NOT the dict sanity probe — it
	// must succeed even when the probe is set to fail
	if !strings.Contains(q, "data_skipping_indices") {
		if c.probeMisses > 0 {
			c.probeMisses--
			found = 0
		}
		return tombTestRow{found: found, err: c.probeErr}
	}
	return tombTestRow{found: found}
}

type tombTestRow struct {
	driver.Row
	found uint8
	err   error
}

func (r tombTestRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case *uint8:
		*d = uint8(r.found)
	case *uint64:
		*d = uint64(r.found)
	default:
		return fmt.Errorf("tombTestRow.Scan: unsupported dest %T", dest[0])
	}
	return nil
}

type tombTestBatch struct {
	driver.Batch
	conn *tombTestConn
	rows []tombstoneReq
}

func (b *tombTestBatch) Append(v ...any) error {
	b.rows = append(b.rows, tombstoneReq{v[0].(string), v[1].(string), v[2].(string)})
	return nil
}
func (b *tombTestBatch) Send() error {
	b.conn.mu.Lock()
	defer b.conn.mu.Unlock()
	b.conn.rows = append(b.conn.rows, b.rows...)
	return nil
}
func (b *tombTestBatch) Abort() error { return nil }
func tombTestLogger() *slog.Logger    { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func tombTestIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%064x", i+1)
	}
	return ids
}
func tombTestWait(t *testing.T, d time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for tombstone writer")
}
func tombTestStop(t *testing.T, w *tombstoneWriter) {
	t.Helper()
	done := make(chan struct{})
	go func() { w.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not stop")
	}
	select {
	case <-w.done:
	default:
		t.Fatal("writer goroutine still running")
	}
}
func TestTombstoneCoalescing(t *testing.T) {
	c := &tombTestConn{}
	w := newTombstoneWriter(c, tombTestLogger())
	// Queue before starting to make the size-trigger deterministic.
	if err := w.retire(tombTestIDs(1000), "nip09", "author"); err != nil {
		t.Fatal(err)
	}
	w.start()
	tombTestWait(t, time.Second, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.rows) == 1000 })
	tombTestStop(t, w)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prepares != 1 || c.reloads != 1 {
		t.Fatalf("prepares=%d reloads=%d", c.prepares, c.reloads)
	}
	for _, row := range c.rows {
		if row.reason != "nip09" || row.deletedBy != "author" {
			t.Fatalf("lost metadata: %+v", row)
		}
	}
}
func TestTombstoneRetainsAndRetries(t *testing.T) {
	c := &tombTestConn{failures: 2}
	w := newTombstoneWriter(c, tombTestLogger())
	w.start()
	defer tombTestStop(t, w)
	if err := w.retire(tombTestIDs(1000), "retry", ""); err != nil {
		t.Fatal(err)
	}
	tombTestWait(t, 2*time.Second, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.rows) == 1000 })
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prepares != 3 {
		t.Fatalf("prepares=%d want 3", c.prepares)
	}
	for i, row := range c.rows {
		if row.id != fmt.Sprintf("%064x", i+1) {
			t.Fatalf("lost id at %d", i)
		}
	}
}
func TestTombstoneStopDrainsAndRejects(t *testing.T) {
	c := &tombTestConn{}
	w := newTombstoneWriter(c, tombTestLogger())
	if err := w.retire(tombTestIDs(3501), "drain", ""); err != nil {
		t.Fatal(err)
	}
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.start()
	tombTestStop(t, w)
	for i := 0; i < 100; i++ {
		if err := w.retire([]string{"late"}, "", ""); !errors.Is(err, errTombstoneWriterStopped) {
			t.Fatalf("retire after stop: %v", err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rows) != 3501 || c.prepares != 4 {
		t.Fatalf("rows=%d batches=%d", len(c.rows), c.prepares)
	}
}
func TestTombstoneReloadRetriesUnapplied(t *testing.T) {
	c := &tombTestConn{probeMisses: 1}
	w := newTombstoneWriter(c, tombTestLogger())
	w.start()
	defer tombTestStop(t, w)
	if err := w.retire([]string{"probe"}, "", ""); err != nil {
		t.Fatal(err)
	}
	tombTestWait(t, 4*time.Second, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.reloads >= 2 })
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reloadTimes[1].Sub(c.reloadTimes[0]) < tombstoneReloadEvery {
		t.Fatal("reload retry exceeded rate bound")
	}
	if c.prepares != 1 {
		t.Fatalf("unexpected reinsertion: %d", c.prepares)
	}
}
func TestTombstoneRetireTimeout(t *testing.T) {
	w := newTombstoneWriter(&tombTestConn{}, tombTestLogger())
	if err := w.retire(tombTestIDs(cap(w.in)), "", ""); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := w.retire([]string{"blocked"}, "", ""); !errors.Is(err, errTombstoneWriterFull) {
		t.Fatalf("timeout error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Second || elapsed > 6*time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
}
func TestSchemaSanityProbeAndMutationFailure(t *testing.T) {
	c := &tombTestConn{execErr: errors.New("transient mutation error")}
	s := &Store{wch: c, cfg: &config.Config{}, log: tombTestLogger()}
	if err := s.initSchema(context.Background()); err != nil {
		t.Fatalf("mutation failure prevented startup: %v", err)
	}
	c.probeErr = errors.New("dictionary cannot load")
	if err := s.initSchema(context.Background()); err == nil || !strings.Contains(err.Error(), "dictionary startup sanity probe") {
		t.Fatalf("sanity error: %v", err)
	}
}

func TestTombstoneRetainedBacklogBoundAndLossLogging(t *testing.T) {
	c := &tombTestConn{failures: 100}
	var logs bytes.Buffer
	w := newTombstoneWriter(c, slog.New(slog.NewTextHandler(&logs, nil)))
	if err := w.retire(tombTestIDs(4096), "outage", ""); err != nil {
		t.Fatal(err)
	}
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.start()
	tombTestStop(t, w)
	output := logs.String()
	if !strings.Contains(output, "level=ERROR") || !strings.Contains(output, "tombstones dropped") || !strings.Contains(output, "tombstones lost at shutdown") || !strings.Contains(output, "n=3000") {
		t.Fatalf("missing bounded backlog loss accounting: %s", output)
	}
}

func TestTombstoneStopUnblocksProducers(t *testing.T) {
	c := &tombTestConn{}
	w := newTombstoneWriter(c, tombTestLogger())
	if err := w.retire(tombTestIDs(cap(w.in)), "", ""); err != nil {
		t.Fatal(err)
	}
	producerDone := make(chan error, 1)
	go func() { producerDone <- w.retire([]string{"blocked"}, "", "") }()
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.start()
	tombTestStop(t, w)
	select {
	case err := <-producerDone:
		if !errors.Is(err, errTombstoneWriterStopped) {
			t.Fatalf("blocked producer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer leaked during shutdown")
	}
	// stop is safe and idempotent even when called concurrently.
	var joined sync.WaitGroup
	for i := 0; i < 10; i++ {
		joined.Add(1)
		go func() { defer joined.Done(); w.stop() }()
	}
	joined.Wait()
}
