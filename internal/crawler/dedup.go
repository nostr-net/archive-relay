package crawler

import (
	"context"
	"sync"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
)

// Dedup is the shared in-memory + durable (SQLite seen_events) dedup layer used
// by every ingestion path. The durable record is written ONLY after a batch is
// flushed to ClickHouse (via store.OnFlushed), so a crash never marks an event
// "seen" before it is durably stored — a later re-sync simply re-ingests it and
// ReplacingMergeTree collapses the duplicate on read.
type Dedup struct {
	ctrl *control.DB
	mem  sync.Map // id -> struct{}
}

// NewDedup wraps the control plane's durable dedup table with an in-memory cache.
func NewDedup(ctrl *control.DB) *Dedup {
	return &Dedup{ctrl: ctrl}
}

// Seen reports whether id was ingested recently (in-memory hot path).
func (d *Dedup) Seen(id string) bool {
	_, ok := d.mem.Load(id)
	return ok
}

// Mark records id as in-flight (optimistic; the durable record happens OnFlushed).
func (d *Dedup) Mark(id string) { d.mem.Store(id, struct{}{}) }

// Unmark removes the optimistic mark (e.g. on batch-full backpressure).
func (d *Dedup) Unmark(id string) { d.mem.Delete(id) }

// OnFlushed records durable dedup state for a batch after it is safely in
// ClickHouse. Wire this to store.SetOnFlushed.
func (d *Dedup) OnFlushed(events []*nostr.Event) {
	ctx := context.Background()
	for _, ev := range events {
		d.mem.Store(ev.ID, struct{}{})
		_, _ = d.ctrl.MarkSeen(ctx, ev.ID, int64(ev.CreatedAt))
	}
}
