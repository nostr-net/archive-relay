package crawler

import (
	"context"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/store"
)

const (
	// fetchOverlap is the re-pull window on incremental ticks: since =
	// last_fetched - fetchOverlap. Generous overlap covers relay propagation
	// delay and backdated events that arrive late.
	fetchOverlap = 24 * time.Hour
	// fetchTimeout bounds one relay fetch; a relay that connects but never
	// sends EOSE stalls the tick without it.
	fetchTimeout = 5 * time.Minute
	// fullSweepAge is how stale last_fetched must get before a pubkey gets a
	// full-history re-pull instead of an incremental one — a completeness
	// safety net for late-arriving old events. Based on persisted state, not a
	// tick counter, so restarts don't trigger a spurious sweep. 24h.
	fullSweepAge = 24 * time.Hour
)

// PriorityCrawler fetches the history of a configured set of pubkeys from a
// dedicated relay list (separate from the firehose -sources), ensuring their
// in-scope events are captured even when the firehose misses them. It shares
// the store + Dedup layer with the firehose Crawler, and records last_fetched
// in crawl_state — which bounds the next tick's `since` filter so each crawl
// is incremental (with a periodic full-history sweep for completeness).
// It does NOT change the stored kind scope — "preserve everything" here means
// all in-scope kinds, completeness-assured.
type PriorityCrawler struct {
	pubkeys  []string
	relays   []string
	store    *store.Store
	dedup    *Dedup
	ctrl     *control.DB
	interval time.Duration
	log      *slog.Logger
}

// NewPriority constructs a PriorityCrawler. pubkeys/relays must be non-empty for
// Run to do anything.
func NewPriority(pubkeys, relays []string, s *store.Store, d *Dedup, ctrl *control.DB,
	interval time.Duration, log *slog.Logger) *PriorityCrawler {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	return &PriorityCrawler{pubkeys: pubkeys, relays: relays, store: s, dedup: d,
		ctrl: ctrl, interval: interval, log: log}
}

// Run crawls each pubkey immediately, then on every interval, until ctx is done.
func (p *PriorityCrawler) Run(ctx context.Context) {
	if len(p.pubkeys) == 0 || len(p.relays) == 0 {
		p.log.Warn("priority crawler disabled: need both pubkeys and relays",
			"pubkeys", len(p.pubkeys), "relays", len(p.relays))
		return
	}
	p.tick(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *PriorityCrawler) tick(ctx context.Context) {
	kinds := store.InScopeKinds()
	for _, pk := range p.pubkeys {
		if ctx.Err() != nil {
			return
		}
		p.fetchPubkey(ctx, pk, kinds)
	}
}

// fetchPubkey pulls one pubkey's events from the first relay that serves it,
// then records last_fetched. Trying relays in order lets a pubkey be completed
// from one relay even if another is down.
func (p *PriorityCrawler) fetchPubkey(ctx context.Context, pubkey string, kinds []int) {
	log := p.log.With("pubkey", pubkey)

	// Incremental bound: since = last_fetched - fetchOverlap (so relay delay
	// and clock skew don't drop events). A pubkey is fetched in full when it
	// has never been crawled or last_fetched is older than fullSweepAge — the
	// persisted completeness sweep for late-arriving old events.
	var since *nostr.Timestamp
	last, err := p.ctrl.LastFetched(ctx, pubkey)
	switch {
	case err != nil:
		log.Warn("last_fetched lookup failed; doing full fetch", "err", err)
	case last == 0 || time.Since(time.Unix(last, 0)) >= fullSweepAge:
		// full fetch: no bound
	default:
		if ts := last - int64(fetchOverlap/time.Second); ts > 0 {
			t := nostr.Timestamp(ts)
			since = &t
		}
	}

	for _, url := range p.relays {
		if ctx.Err() != nil {
			return
		}
		if p.fetchFrom(ctx, url, pubkey, kinds, since, log) {
			_ = p.ctrl.MarkFetched(ctx, pubkey)
			return
		}
	}
	log.Warn("no relay served this pubkey", "relays", p.relays)
}

// fetchFrom connects to one relay, subscribes to the pubkey's in-scope events
// (bounded by `since` on incremental ticks), and ingests everything until EOSE.
// The fetch is bounded by fetchTimeout; a stalled relay reports false so the
// next relay is tried, as does a subscription closed before EOSE or a dropped
// events channel — anything short of a clean EOSE must not advance
// last_fetched past events that were never fetched.
func (p *PriorityCrawler) fetchFrom(parent context.Context, url, pubkey string,
	kinds []int, since *nostr.Timestamp, log *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(parent, fetchTimeout)
	defer cancel()

	relay := nostr.NewRelay(ctx, url)
	if err := relay.Connect(ctx); err != nil {
		log.Warn("connect failed", "relay", url, "err", err)
		return false // try the next relay
	}
	defer relay.Close()

	sub, err := relay.Subscribe(ctx, nostr.Filters{{
		Authors: []string{pubkey}, Kinds: kinds, Since: since,
	}})
	if err != nil {
		log.Warn("subscribe failed", "relay", url, "err", err)
		return false // try the next relay
	}

	ingested, skipped := 0, 0
	for {
		select {
		case <-ctx.Done():
			if parent.Err() == nil {
				// fetchTimeout hit, not shutdown — try the next relay
				log.Warn("fetch timed out", "relay", url, "ingested", ingested)
			}
			// shutdown or timeout: incomplete either way — only a clean EOSE
			// is "done" (the fetchPubkey loop's ctx.Err() guard aborts on shutdown).
			return false
		case <-sub.EndOfStoredEvents:
			log.Info("priority crawl done", "relay", url, "ingested", ingested, "skipped", skipped)
			return true
		case reason := <-sub.ClosedReason:
			log.Warn("subscription closed before EOSE", "relay", url, "reason", reason)
			return false // incomplete — try the next relay
		case ev, ok := <-sub.Events:
			if !ok {
				log.Warn("events channel closed before EOSE", "relay", url, "ingested", ingested)
				return false // incomplete — try the next relay
			}
			if ingest(ctx, p.store, p.dedup, p.log, ev) {
				ingested++
			} else {
				skipped++
			}
		}
	}
}
