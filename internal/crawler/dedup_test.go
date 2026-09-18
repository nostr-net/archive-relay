package crawler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
)

func testDedup(t *testing.T) (*Dedup, *control.DB) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctrl, err := control.Open(filepath.Join(t.TempDir(), "c.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctrl.Close() })
	return NewDedup(ctrl, log), ctrl
}

func hexID(n int) string {
	return fmt.Sprintf("%064x", n)
}

func startWriter(t *testing.T, d *Dedup) {
	t.Helper()
	d.StartWriter(context.Background())
	t.Cleanup(d.StopWriter)
}

func waitPersisted(t *testing.T, db *control.DB, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ids, err := db.LoadRecentSeen(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range ids {
			if got == id {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("id %s not persisted to seen_events", id)
}

func TestDedupGenerationRotationBoundsMemory(t *testing.T) {
	oldCap := memCap
	memCap = 10
	defer func() { memCap = oldCap }()
	d, _ := testDedup(t)

	// 25 marks with cap 10 → three generations; only the newest two survive.
	for i := 0; i < 25; i++ {
		d.Mark(hexID(i))
	}
	for i := 0; i < 10; i++ { // generation 1 rotated out
		if d.Seen(hexID(i)) {
			t.Errorf("%s should have aged out of the bounded cache", hexID(i))
		}
	}
	for i := 10; i < 25; i++ { // generations 2+3 still cached
		if !d.Seen(hexID(i)) {
			t.Errorf("%s should still be seen", hexID(i))
		}
	}
	d.Unmark(hexID(24))
	if d.Seen(hexID(24)) {
		t.Error("Unmark should remove the id from the cache")
	}
}

func TestCheckAndMarkSingleLock(t *testing.T) {
	d, _ := testDedup(t)
	id := hexID(1)
	var won atomic.Int32
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if d.CheckAndMark(id) {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := won.Load(); got != 1 {
		t.Errorf("CheckAndMark succeeded %d times, want 1", got)
	}
	if !d.Seen(id) {
		t.Error("winner should have marked the id")
	}
}

func TestCheckAndMarkInvalidHexNotPoison(t *testing.T) {
	d, _ := testDedup(t)
	if !d.CheckAndMark("not-hex") {
		t.Error("invalid hex should be treated as not seen")
	}
	if d.Seen("not-hex") {
		t.Error("invalid hex must not be stored in the map")
	}
	if !d.CheckAndMark("not-hex") {
		t.Error("invalid hex remains not-seen on a second CheckAndMark")
	}
	// short / odd-length / non-hex chars
	for _, id := range []string{"", "abc", hexID(1)[:63], hexID(1) + "0", "gg" + hexID(1)[2:]} {
		if d.Seen(id) {
			t.Errorf("Seen(%q) should be false", id)
		}
		d.Mark(id)
		if d.Seen(id) {
			t.Errorf("Mark(%q) must not poison the map", id)
		}
	}
}

func TestMarkMany(t *testing.T) {
	d, _ := testDedup(t)
	d.MarkMany(hexID(1), hexID(2), "not-hex", hexID(3))
	for _, n := range []int{1, 2, 3} {
		if !d.Seen(hexID(n)) {
			t.Errorf("MarkMany should mark %s", hexID(n))
		}
	}
	if d.Seen("not-hex") {
		t.Error("MarkMany must not store invalid hex")
	}
}

func TestDedupOnFlushedMarksAndPersists(t *testing.T) {
	oldCap := memCap
	memCap = 3
	defer func() { memCap = oldCap }()
	d, db := testDedup(t)
	startWriter(t, d)

	// pre-fill so OnFlushed's MarkMany has to rotate a generation
	for i := 0; i < 3; i++ {
		d.Mark(hexID(100 + i))
	}
	id := hexID(1)
	d.OnFlushed([]*nostr.Event{{ID: id, CreatedAt: 100}})
	if !d.Seen(id) {
		t.Error("OnFlushed should mark the id in memory")
	}
	waitPersisted(t, db, id)
	// already persisted: a second MarkSeen reports already-seen
	if ok, _ := db.MarkSeen(context.Background(), id, 100); ok {
		t.Error("OnFlushed should have already persisted the id to seen_events")
	}
}

func TestDedupWriterQueueDropDoesNotBlock(t *testing.T) {
	d, _ := testDedup(t)
	// Do NOT StartWriter: the queue fills and further OnFlushed calls must
	// drop rather than block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < writeQueueCap+10; i++ {
			d.OnFlushed([]*nostr.Event{{ID: hexID(i), CreatedAt: 1}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnFlushed blocked on a full durable-write queue")
	}
	if got := d.DroppedWrites(); got < 10 {
		t.Errorf("dropped batches = %d, want >= 10", got)
	}
	// in-memory MarkMany still happened for every batch
	if !d.Seen(hexID(writeQueueCap + 9)) {
		t.Error("queue drop must not skip the synchronous in-memory MarkMany")
	}
}

func TestDedupWarmReloadsRecentSeen(t *testing.T) {
	d, db := testDedup(t)
	startWriter(t, d)
	ctx := context.Background()

	a, b := hexID(10), hexID(11)
	d.OnFlushed([]*nostr.Event{{ID: a, CreatedAt: 1}, {ID: b, CreatedAt: 2}})
	waitPersisted(t, db, a)
	waitPersisted(t, db, b)

	// simulate a restart: a fresh Dedup over the same DB starts cold
	d2 := NewDedup(db, nil)
	if d2.Seen(a) || d2.Seen(b) {
		t.Fatal("fresh Dedup should start cold")
	}
	if err := d2.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	if !d2.Seen(a) || !d2.Seen(b) {
		t.Error("Warm should reload recently-seen ids into memory")
	}
}

func TestStopWriterPersistsQueuedBatches(t *testing.T) {
	d, db := testDedup(t)
	const n = 8
	for i := 0; i < n; i++ {
		d.OnFlushed([]*nostr.Event{{ID: hexID(i), CreatedAt: nostr.Timestamp(i + 1)}})
	}
	d.StartWriter(context.Background())
	d.StopWriter()

	ids, err := db.LoadRecentSeen(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	for i := 0; i < n; i++ {
		if !got[hexID(i)] {
			t.Errorf("id %s missing from seen_events after StopWriter", hexID(i))
		}
	}
}

func TestWriterCtxCancelDrainsQueue(t *testing.T) {
	d, db := testDedup(t)
	const n = 5
	for i := 0; i < n; i++ {
		d.OnFlushed([]*nostr.Event{{ID: hexID(i), CreatedAt: nostr.Timestamp(i + 1)}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.StartWriter(ctx)
	cancel()
	d.StopWriter()

	ids, err := db.LoadRecentSeen(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	for i := 0; i < n; i++ {
		if !got[hexID(i)] {
			t.Errorf("id %s missing from seen_events after writer ctx cancel", hexID(i))
		}
	}
}

func TestPruneSeenByRowidOffByOne(t *testing.T) {
	_, db := testDedup(t)
	ctx := context.Background()
	const cap = 3
	for i := 0; i < cap+3; i++ {
		if _, err := db.MarkSeen(ctx, hexID(i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := db.PruneSeenByRowid(ctx, cap)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("PruneSeenByRowid deleted %d rows, want 3", n)
	}
	ids, err := db.LoadRecentSeen(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != cap {
		t.Errorf("remaining %d rows, want exactly %d", len(ids), cap)
	}
}
