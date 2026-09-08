package crawler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

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

func TestDedupGenerationRotationBoundsMemory(t *testing.T) {
	oldCap := memCap
	memCap = 10
	defer func() { memCap = oldCap }()
	d, _ := testDedup(t)

	// 25 marks with cap 10 → three generations; only the newest two survive.
	for i := 0; i < 25; i++ {
		d.Mark(fmt.Sprintf("id-%02d", i))
	}
	for i := 0; i < 10; i++ { // generation 1 rotated out
		if d.Seen(fmt.Sprintf("id-%02d", i)) {
			t.Errorf("id-%02d should have aged out of the bounded cache", i)
		}
	}
	for i := 10; i < 25; i++ { // generations 2+3 still cached
		if !d.Seen(fmt.Sprintf("id-%02d", i)) {
			t.Errorf("id-%02d should still be seen", i)
		}
	}
	d.Unmark("id-24")
	if d.Seen("id-24") {
		t.Error("Unmark should remove the id from the cache")
	}
}

func TestDedupOnFlushedMarksAndPersists(t *testing.T) {
	oldCap := memCap
	memCap = 3
	defer func() { memCap = oldCap }()
	d, db := testDedup(t)

	// pre-fill so OnFlushed's Mark has to rotate a generation
	for i := 0; i < 3; i++ {
		d.Mark(fmt.Sprintf("old-%d", i))
	}
	d.OnFlushed([]*nostr.Event{{ID: "flushed-1", CreatedAt: 100}})
	if !d.Seen("flushed-1") {
		t.Error("OnFlushed should mark the id in memory")
	}
	// ...and persist it durably (the batched MarkSeen path)
	if ok, _ := db.MarkSeen(context.Background(), "flushed-1", 100); ok {
		t.Error("OnFlushed should have already persisted flushed-1 to seen_events")
	}
}

// TestDedupWarmReloadsRecentSeen verifies a restart-warm repopulates the
// in-memory cache from the durable seen_events table.
func TestDedupWarmReloadsRecentSeen(t *testing.T) {
	d, db := testDedup(t)
	ctx := context.Background()

	d.OnFlushed([]*nostr.Event{{ID: "a", CreatedAt: 1}, {ID: "b", CreatedAt: 2}})

	// simulate a restart: a fresh Dedup over the same DB starts cold
	d2 := NewDedup(db, nil)
	if d2.Seen("a") || d2.Seen("b") {
		t.Fatal("fresh Dedup should start cold")
	}
	if err := d2.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	if !d2.Seen("a") || !d2.Seen("b") {
		t.Error("Warm should reload recently-seen ids into memory")
	}
}
