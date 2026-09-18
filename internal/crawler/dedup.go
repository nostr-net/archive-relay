package crawler

import (
	"context"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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

// writeQueueCap is the bounded durable-seen writer queue. OnFlushed MarkMany
// is synchronous; the SQLite write is async. A full queue drops the batch
// (durable dedup is an optimization; re-ingest is the safety net).
const writeQueueCap = 64

const (
	// DefaultSeenCap is the max seen_events rows kept by the hourly cap-prune.
	// At ~5k events/s a 1h window is ~18M rows, so 20M leaves a little headroom.
	// Tunable: pass a different cap to StartPrune.
	DefaultSeenCap = 20_000_000
	// DefaultPruneEvery is the cap-prune and age-prune cadence.
	DefaultPruneEvery = time.Hour
	// DefaultPruneMaxAge drops seen_events rows older than this (age-based).
	DefaultPruneMaxAge = 7 * 24 * time.Hour
)

// Dedup is the shared in-memory + durable (SQLite seen_events) dedup layer used
// by every ingestion path. The in-memory cache is bounded (two generations);
// ids that age out are simply re-ingested later and collapsed by
// ReplacingMergeTree on read. The durable record is written ONLY after a batch
// is flushed to ClickHouse (via store.OnFlushed, queued to StartWriter),
// so a crash never marks an event "seen" before it is durably stored — and on
// restart Warm reloads the most recent ids so the firehose isn't re-ingested.
//
// In-memory keys are [32]byte (hex-decoded once). Non-hex ids are never stored
// in the map — they would fail CheckID anyway — and are treated as unseen so
// they cannot poison the cache.
type Dedup struct {
	ctrl *control.DB
	log  *slog.Logger

	mu   sync.RWMutex
	cur  map[[32]byte]struct{}
	prev map[[32]byte]struct{}

	writeQ        chan []control.SeenRef
	droppedWrites atomic.Uint64
	droppedBadID  atomic.Uint64

	writerMu      sync.Mutex
	writerRunning bool
	stopWriter    chan struct{}
	writerDone    chan struct{}
}

// parseEventID hex-decodes a 64-char event id. Invalid hex (wrong length or
// non-hex) returns ok=false; callers must not insert those into the map.
func parseEventID(id string) (key [32]byte, ok bool) {
	if len(id) != 64 {
		return key, false
	}
	n, err := hex.Decode(key[:], []byte(id))
	if err != nil || n != 32 {
		return key, false
	}
	return key, true
}

// NewDedup wraps the control plane's durable dedup table with a bounded cache.
// The cache is allocated lazily (no up-front capacity hint) so quiet relays
// don't pay ~100MB for a table they may never fill. Call Warm at startup to
// preload recent ids from seen_events. Call StartWriter to consume the durable
// queue and StartPrune to run seen_events maintenance (main wiring).
func NewDedup(ctrl *control.DB, log *slog.Logger) *Dedup {
	return &Dedup{
		ctrl:   ctrl,
		log:    log,
		cur:    make(map[[32]byte]struct{}),
		writeQ: make(chan []control.SeenRef, writeQueueCap),
	}
}

// Warm preloads the most recent seen_events ids into the in-memory cache so a
// restart doesn't trigger a mass re-ingest of already-stored events. Bounded
// by memCap. Non-fatal: on error the cache just starts cold. Invalid-hex ids
// in the table are skipped (not cached).
func (d *Dedup) Warm(ctx context.Context) error {
	ids, err := d.ctrl.LoadRecentSeen(ctx, memCap)
	if err != nil {
		return err
	}
	d.mu.Lock()
	n := 0
	for _, id := range ids {
		key, ok := parseEventID(id)
		if !ok {
			continue
		}
		d.cur[key] = struct{}{}
		n++
	}
	d.mu.Unlock()
	if d.log != nil {
		d.log.Info("dedup cache warmed", "ids", n)
	}
	return nil
}

// Seen reports whether id was ingested recently (in-memory hot path).
// Invalid-hex ids are never cached, so Seen reports false for them.
func (d *Dedup) Seen(id string) bool {
	key, ok := parseEventID(id)
	if !ok {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if _, ok := d.cur[key]; ok {
		return true
	}
	_, ok = d.prev[key]
	return ok
}

// Mark records id as in-flight (optimistic; the durable record happens OnFlushed).
// Invalid-hex ids are ignored (not inserted).
func (d *Dedup) Mark(id string) {
	key, ok := parseEventID(id)
	if !ok {
		return
	}
	d.mu.Lock()
	d.insertLocked(key)
	d.mu.Unlock()
}

// CheckAndMark reports whether id is newly marked under a single lock
// (true = not previously seen, now marked). Replaces the Seen-then-Mark race.
// Invalid-hex ids are treated as unseen and are not inserted; CheckAndMark
// returns true so the caller proceeds (CheckID will drop them).
func (d *Dedup) CheckAndMark(id string) bool {
	key, ok := parseEventID(id)
	if !ok {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.cur[key]; ok {
		return false
	}
	if _, ok := d.prev[key]; ok {
		return false
	}
	d.insertLocked(key)
	return true
}

// MarkMany records many ids as seen. Used by the synchronous OnFlushed
// in-memory path. Invalid-hex ids are skipped.
func (d *Dedup) MarkMany(ids ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		key, ok := parseEventID(id)
		if !ok {
			continue
		}
		d.insertLocked(key)
	}
}

func (d *Dedup) insertLocked(key [32]byte) {
	d.cur[key] = struct{}{}
	if len(d.cur) >= memCap {
		d.prev = d.cur
		d.cur = make(map[[32]byte]struct{})
	}
}

// Unmark removes the optimistic mark (e.g. on batch-full backpressure).
func (d *Dedup) Unmark(id string) {
	key, ok := parseEventID(id)
	if !ok {
		return
	}
	d.mu.Lock()
	delete(d.cur, key)
	delete(d.prev, key)
	d.mu.Unlock()
}

// DroppedWrites is the number of durable seen_events batches dropped because
// the writer queue was full.
func (d *Dedup) DroppedWrites() uint64 { return d.droppedWrites.Load() }

// DroppedBadID is the number of unseen events dropped for an id↔body mismatch.
func (d *Dedup) DroppedBadID() uint64 { return d.droppedBadID.Load() }

// OnFlushed records durable dedup state for a batch after it is safely in
// ClickHouse. In-memory MarkMany is synchronous (cheap). The SQLite write is
// queued to StartWriter's goroutine; a full queue drops the batch with a
// metric (durable dedup is an optimization). Wire this to store.SetOnFlushed.
func (d *Dedup) OnFlushed(events []*nostr.Event) {
	if len(events) == 0 {
		return
	}
	ids := make([]string, 0, len(events))
	refs := make([]control.SeenRef, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.ID)
		refs = append(refs, control.SeenRef{ID: ev.ID, CreatedAt: int64(ev.CreatedAt)})
	}
	d.MarkMany(ids...)
	select {
	case d.writeQ <- refs:
	default:
		n := d.droppedWrites.Add(1)
		if d.log != nil {
			d.log.Warn("seen_events write queue full; dropping durable batch",
				"n", len(refs), "droppedBatches", n)
		}
	}
}

// StartWriter starts the goroutine that drains the durable seen_events queue.
// Call once from main after Warm. The loop exits on ctx cancel or StopWriter;
// remaining queued batches are discarded (re-ingest is the safety net).
func (d *Dedup) StartWriter(ctx context.Context) {
	d.writerMu.Lock()
	defer d.writerMu.Unlock()
	if d.writerRunning {
		return
	}
	d.stopWriter = make(chan struct{})
	d.writerDone = make(chan struct{})
	d.writerRunning = true
	go d.writeLoop(ctx)
}

func (d *Dedup) writeLoop(ctx context.Context) {
	defer close(d.writerDone)
	for {
		select {
		case <-ctx.Done():
			d.discardQueued()
			return
		case <-d.stopWriter:
			d.discardQueued()
			return
		case batch := <-d.writeQ:
			if err := d.ctrl.MarkSeenBatch(context.Background(), batch); err != nil && d.log != nil {
				d.log.Warn("seen_events batch write failed", "n", len(batch), "err", err)
			}
		}
	}
}

func (d *Dedup) discardQueued() {
	for {
		select {
		case <-d.writeQ:
		default:
			return
		}
	}
}

// StopWriter signals the writer to drain+discard the queue and waits for it to
// exit. Safe to call if StartWriter was never invoked. Call before cdb.Close.
func (d *Dedup) StopWriter() {
	d.writerMu.Lock()
	defer d.writerMu.Unlock()
	if !d.writerRunning {
		return
	}
	close(d.stopWriter)
	<-d.writerDone
	d.writerRunning = false
}

// StartPrune runs age-based PruneSeen and cap-based PruneSeenByRowid on their
// own ticker, independent of crawler sources (zero sources must still prune).
// every/cap <= 0 fall back to DefaultPruneEvery / DefaultSeenCap.
func (d *Dedup) StartPrune(ctx context.Context, every time.Duration, cap int) {
	if every <= 0 {
		every = DefaultPruneEvery
	}
	if cap <= 0 {
		cap = DefaultSeenCap
	}
	go d.pruneLoop(ctx, every, cap, DefaultPruneMaxAge)
}

func (d *Dedup) pruneLoop(ctx context.Context, every time.Duration, cap int, maxAge time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := d.ctrl.PruneSeen(ctx, maxAge); err != nil {
				if d.log != nil {
					d.log.Warn("prune seen_events (age) failed", "err", err)
				}
			} else if n > 0 && d.log != nil {
				d.log.Info("pruned seen_events by age", "rows", n)
			}
			if n, err := d.ctrl.PruneSeenByRowid(ctx, cap); err != nil {
				if d.log != nil {
					d.log.Warn("prune seen_events (cap) failed", "err", err)
				}
			} else if n > 0 && d.log != nil {
				d.log.Info("pruned seen_events by cap", "rows", n, "cap", cap)
			}
		}
	}
}
