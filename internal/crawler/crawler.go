// Package crawler ingests events from upstream nostr relays into the store.
// Each source relay gets a goroutine that subscribes to the in-scope kinds and
// feeds received events through the store's batched SaveEvent path, with
// SQLite-backed dedup so re-ingest is idempotent.
package crawler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/store"
)

// Crawler fans out one ingestion goroutine per source relay URL.
type Crawler struct {
	sources []string
	store   *store.Store
	ctrl    *control.DB
	log     *slog.Logger
	seen    sync.Map // in-memory dedup cache (id -> struct{}); fast hot-path skip
}

// New constructs a Crawler for the given source relay URLs. It wires the
// store's post-flush hook so durable dedup state (seen_events) is recorded
// ONLY after events are safely in ClickHouse — preventing the crash-hole where
// an event is marked "seen" but never actually stored.
func New(sources []string, s *store.Store, ctrl *control.DB, log *slog.Logger) *Crawler {
	c := &Crawler{sources: sources, store: s, ctrl: ctrl, log: log}
	s.SetOnFlushed(func(events []*nostr.Event) {
		ctx := context.Background()
		for _, ev := range events {
			c.seen.Store(ev.ID, struct{}{})
			_, _ = ctrl.MarkSeen(ctx, ev.ID, int64(ev.CreatedAt))
		}
	})
	return c
}

// Run starts ingestion from all sources and blocks until ctx is canceled.
func (c *Crawler) Run(ctx context.Context) {
	if len(c.sources) == 0 {
		c.log.Warn("crawler has no source relays configured")
		return
	}
	// Prune the dedup table hourly so it doesn't grow unbounded. seen_events is
	// only useful for the backfill window; older rows are dead weight.
	go c.pruneLoop(ctx, time.Hour, 7*24*time.Hour)

	kinds := store.InScopeKinds()
	var wg sync.WaitGroup
	for _, url := range c.sources {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			c.runSource(ctx, url, kinds)
		}(url)
	}
	wg.Wait()
}

func (c *Crawler) pruneLoop(ctx context.Context, every, maxAge time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := c.ctrl.PruneSeen(ctx, maxAge); err != nil {
				c.log.Warn("prune seen_events failed", "err", err)
			} else if n > 0 {
				c.log.Info("pruned seen_events", "rows", n)
			}
		}
	}
}

// runSource connects (with reconnect+backoff) and subscribes to the in-scope
// kinds, persisting every received event.
func (c *Crawler) runSource(ctx context.Context, url string, kinds []int) {
	log := c.log.With("source", url)
	filter := nostr.Filter{Kinds: kinds}
	// no Limit: take the relay's historical backlog, then stay open for live.
	backoff := time.Second

	for ctx.Err() == nil {
		relay := nostr.NewRelay(ctx, url)
		if err := relay.Connect(ctx); err != nil {
			log.Warn("connect failed; retrying", "err", err, "backoff", backoff)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second
		log.Info("connected; subscribing", "kinds", kinds)

		sub, err := relay.Subscribe(ctx, nostr.Filters{filter})
		if err != nil {
			log.Warn("subscribe failed; reconnecting", "err", err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}

		ingested, skipped := 0, 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.EndOfStoredEvents:
				log.Info("EOSE; continuing for live events", "ingested", ingested, "skipped", skipped)
			case reason := <-sub.ClosedReason:
				log.Warn("subscription closed; reconnecting", "reason", reason)
				sleepCtx(ctx, 2*time.Second)
				goto reconnect
			case ev, ok := <-sub.Events:
				if !ok {
					log.Info("events channel closed; reconnecting", "ingested", ingested)
					goto reconnect
				}
				if c.handle(ctx, ev) {
					ingested++
				} else {
					skipped++
				}
			}
		}
	reconnect:
		_ = relay.Close()
	}
}

// handle applies in-memory dedup (fast hot path) and saves new events to
// ClickHouse. Durable dedup (seen_events) is recorded post-flush by the
// OnFlushed hook wired in New — so a crash between enqueue and flush leaves
// the event un-marked, and a later negentropy re-sync recovers it (RMT dedups
// the duplicate). Returns true if the event was newly enqueued.
func (c *Crawler) handle(ctx context.Context, ev *nostr.Event) bool {
	if _, ok := c.seen.Load(ev.ID); ok {
		return false // hot-path dedup (in-memory)
	}
	if store.TierForKind(ev.Kind) == store.TierDrop {
		return false // out of scope
	}
	if ok, _ := ev.CheckSignature(); !ok {
		return false // invalid signature
	}
	// Optimistic: mark in-memory seen now to dedupe within the current buffer
	// window; the durable record happens post-flush. If SaveEvent fails below,
	// we leave it marked (harmless: worst case it's skipped until restart, and
	// RMT/seen reconciliation recovers).
	c.seen.Store(ev.ID, struct{}{})
	if err := c.store.SaveEvent(ctx, ev); err != nil {
		if errors.Is(err, store.ErrBatchFull) {
			c.seen.Delete(ev.ID) // back off; let a later re-pull try again
			c.log.Warn("batch full; event dropped (will re-ingest next sync)", "id", ev.ID)
		} else {
			c.log.Warn("save failed", "id", ev.ID, "err", err)
		}
		return false
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	if d < 0 || math.IsInf(float64(d), 0) {
		d = 60 * time.Second
	}
	return d
}
