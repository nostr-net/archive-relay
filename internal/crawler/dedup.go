package crawler

import (
	"context"
	"log/slog"
	"sync"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
)

// memCap is the per-generation size of the in-memory seen-id cache. An
// unbounded map is a memory leak at firehose rates (GBs/day), so the cache
// rotates: at cap, the current generation becomes the previous one and a fresh
// current starts — at most ~2×memCap ids held, O(1) ops, no LRU bookkeeping.
// Longer lookback comes from the durable seen_events table, which Warm loads
// at startup (bounded by memCap) and OnFlushed appends to per flush.
var memCap = 2_000_000

// Dedup is the shared in-memory + durable (SQLite seen_events) dedup layer used
// by every ingestion path. The in-memory cache is bounded (two generations);
// ids that age out are simply re-ingested later and collapsed by
// ReplacingMergeTree on read. The durable record is written ONLY after a batch
// is flushed to ClickHouse (via store.OnFlushed, batched in one transaction),
// so a crash never marks an event "seen" before it is durably stored — and on
// restart Warm reloads the most recent ids so the firehose isn't re-ingested.
type Dedup struct {
	ctrl *control.DB
	log  *slog.Logger

	mu   sync.RWMutex
	cur  map[string]struct{}
	prev map[string]struct{}
}

// NewDedup wraps the control plane's durable dedup table with a bounded cache.
// The cache is allocated lazily (no up-front capacity hint) so quiet relays
// don't pay ~100MB for a table they may never fill. Call Warm at startup to
// preload recent ids from seen_events.
func NewDedup(ctrl *control.DB, log *slog.Logger) *Dedup {
	return &Dedup{ctrl: ctrl, log: log, cur: make(map[string]struct{})}
}

// Warm preloads the most recent seen_events ids into the in-memory cache so a
// restart doesn't trigger a mass re-ingest of already-stored events. Bounded
// by memCap. Non-fatal: on error the cache just starts cold.
func (d *Dedup) Warm(ctx context.Context) error {
	ids, err := d.ctrl.LoadRecentSeen(ctx, memCap)
	if err != nil {
		return err
	}
	d.mu.Lock()
	for _, id := range ids {
		d.cur[id] = struct{}{}
	}
	d.mu.Unlock()
	if d.log != nil {
		d.log.Info("dedup cache warmed", "ids", len(ids))
	}
	return nil
}

// Seen reports whether id was ingested recently (in-memory hot path).
func (d *Dedup) Seen(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if _, ok := d.cur[id]; ok {
		return true
	}
	_, ok := d.prev[id]
	return ok
}

// Mark records id as in-flight (optimistic; the durable record happens OnFlushed).
func (d *Dedup) Mark(id string) {
	d.mu.Lock()
	d.cur[id] = struct{}{}
	if len(d.cur) >= memCap {
		d.prev = d.cur
		d.cur = make(map[string]struct{})
	}
	d.mu.Unlock()
}

// Unmark removes the optimistic mark (e.g. on batch-full backpressure).
func (d *Dedup) Unmark(id string) {
	d.mu.Lock()
	delete(d.cur, id)
	delete(d.prev, id)
	d.mu.Unlock()
}

// OnFlushed records durable dedup state for a batch after it is safely in
// ClickHouse: one SQLite transaction for the whole batch (not one autocommit
// per event, which would stall the flush loop at firehose rates). Wire this
// to store.SetOnFlushed.
func (d *Dedup) OnFlushed(events []*nostr.Event) {
	refs := make([]control.SeenRef, 0, len(events))
	for _, ev := range events {
		d.Mark(ev.ID)
		refs = append(refs, control.SeenRef{ID: ev.ID, CreatedAt: int64(ev.CreatedAt)})
	}
	if err := d.ctrl.MarkSeenBatch(context.Background(), refs); err != nil && d.log != nil {
		d.log.Warn("seen_events batch write failed", "n", len(refs), "err", err)
	}
}
