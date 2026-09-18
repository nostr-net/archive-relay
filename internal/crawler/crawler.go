// Package crawler ingests events from upstream nostr relays into the store.
// Each source relay gets a goroutine that subscribes to the in-scope kinds and
// feeds received events through the store's batched SaveEvent path, with
// SQLite-backed dedup so re-ingest is idempotent.
package crawler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/store"
)

// reconnectOverlap is the since-window applied on reconnect AFTER a source has
// completed its first EOSE. Mid-backfill disconnects restart with Since=nil so
// unfinished history is not abandoned (plan §1.8 / §3).
const reconnectOverlap = 2 * time.Hour

// Crawler fans out one ingestion goroutine per source relay URL.
type Crawler struct {
	sources []string
	store   *store.Store
	dedup   *Dedup
	log     *slog.Logger
}

// New constructs a firehose Crawler for the given source relay URLs. dedup is
// shared with any per-pubkey PriorityCrawler; wire dedup.OnFlushed to
// store.SetOnFlushed once at startup. Pruning and the durable seen_events
// writer live on Dedup (StartWriter / StartPrune) so they run even with zero
// sources — call those from main, not from Run.
func New(sources []string, s *store.Store, dedup *Dedup, log *slog.Logger) *Crawler {
	return &Crawler{sources: sources, store: s, dedup: dedup, log: log}
}

// Run starts ingestion from all sources and blocks until ctx is canceled.
func (c *Crawler) Run(ctx context.Context) {
	if len(c.sources) == 0 {
		c.log.Warn("crawler has no source relays configured")
		return
	}

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

// newUpstreamRelay builds a go-nostr Relay with AssumeValid set BEFORE Connect.
// v0.52.3 has no ctor arg for this; it is a field assignment. AssumeValid skips
// the library subscription-loop signature check, so ingest still CheckSignatures
// every unseen event (and CheckID).
func newUpstreamRelay(ctx context.Context, url string) *nostr.Relay {
	r := nostr.NewRelay(ctx, url)
	r.AssumeValid = true
	return r
}

// sinceForReconnect returns a since timestamp only after the source has
// completed its first EOSE. A nil result means "full backfill" (Since unset).
func sinceForReconnect(backfillComplete bool, lastDisconnect time.Time) *nostr.Timestamp {
	if !backfillComplete || lastDisconnect.IsZero() {
		return nil
	}
	sec := lastDisconnect.Add(-reconnectOverlap).Unix()
	if sec <= 0 {
		return nil
	}
	ts := nostr.Timestamp(sec)
	return &ts
}

// runSource connects (with reconnect+backoff) and subscribes to the in-scope
// kinds, persisting every received event. Reconnect `since` is backfill-aware:
// unfinished history is never abandoned.
func (c *Crawler) runSource(ctx context.Context, url string, kinds []int) {
	log := c.log.With("source", url)
	backoff := time.Second
	backfillComplete := false
	var lastDisconnect time.Time

	for ctx.Err() == nil {
		filter := nostr.Filter{Kinds: kinds}
		if since := sinceForReconnect(backfillComplete, lastDisconnect); since != nil {
			filter.Since = since
		}

		relay := newUpstreamRelay(ctx, url)
		if err := relay.Connect(ctx); err != nil {
			log.Warn("connect failed; retrying", "err", err, "backoff", backoff)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second
		log.Info("connected; subscribing", "kinds", kinds, "backfillComplete", backfillComplete)

		sub, err := relay.Subscribe(ctx, nostr.Filters{filter})
		if err != nil {
			log.Warn("subscribe failed; reconnecting", "err", err)
			_ = relay.Close()
			sleepCtx(ctx, 2*time.Second)
			continue
		}

		ingested, skipped := 0, 0
		for {
			select {
			case <-ctx.Done():
				lastDisconnect = time.Now()
				_ = relay.Close()
				return
			case <-sub.EndOfStoredEvents:
				backfillComplete = true
				log.Info("EOSE; continuing for live events", "ingested", ingested, "skipped", skipped)
			case reason := <-sub.ClosedReason:
				log.Warn("subscription closed; reconnecting", "reason", reason)
				lastDisconnect = time.Now()
				_ = relay.Close()
				sleepCtx(ctx, 2*time.Second)
				goto next
			case ev, ok := <-sub.Events:
				if !ok {
					log.Info("events channel closed; reconnecting", "ingested", ingested)
					lastDisconnect = time.Now()
					_ = relay.Close()
					goto next
				}
				if c.handle(ctx, ev) {
					ingested++
				} else {
					skipped++
				}
			}
		}
	next:
	}
}

// handle applies in-memory dedup (fast hot path) and saves new events to
// ClickHouse through the shared ingest path.
func (c *Crawler) handle(ctx context.Context, ev *nostr.Event) bool {
	return ingest(ctx, c.store, c.dedup, c.log, ev)
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
		return 60 * time.Second
	}
	return d
}
