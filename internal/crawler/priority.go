package crawler

import (
	"context"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/store"
)

// PriorityCrawler fetches the full history of a configured set of pubkeys from a
// dedicated relay list (separate from the firehose -sources), ensuring their
// in-scope events are captured even when the firehose misses them. It shares the
// store + Dedup layer with the firehose Crawler, and records last_fetched in
// crawl_state for ops visibility. It does NOT change the stored kind scope —
// "preserve everything" here means all in-scope kinds, completeness-assured.
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
	for _, url := range p.relays {
		if ctx.Err() != nil {
			return
		}
		if p.fetchFrom(ctx, url, pubkey, kinds, log) {
			_ = p.ctrl.MarkFetched(ctx, pubkey)
			return
		}
	}
	log.Warn("no relay served this pubkey", "relays", p.relays)
}

// fetchFrom connects to one relay, subscribes to the pubkey's in-scope events,
// ingests everything until EOSE, and reports whether the connection succeeded
// (regardless of how many events came back — an empty history is still "done").
func (p *PriorityCrawler) fetchFrom(ctx context.Context, url, pubkey string, kinds []int, log *slog.Logger) bool {
	relay := nostr.NewRelay(ctx, url)
	if err := relay.Connect(ctx); err != nil {
		log.Warn("connect failed", "relay", url, "err", err)
		return false
	}
	defer relay.Close()

	sub, err := relay.Subscribe(ctx, nostr.Filters{{
		Authors: []string{pubkey}, Kinds: kinds,
	}})
	if err != nil {
		log.Warn("subscribe failed", "relay", url, "err", err)
		return true // connected fine; don't retry other relays on a subscribe error
	}

	ingested, skipped := 0, 0
	for {
		select {
		case <-ctx.Done():
			return true
		case <-sub.EndOfStoredEvents:
			log.Info("priority crawl done", "relay", url, "ingested", ingested, "skipped", skipped)
			return true
		case reason := <-sub.ClosedReason:
			log.Warn("subscription closed", "relay", url, "reason", reason)
			return true
		case ev, ok := <-sub.Events:
			if !ok {
				log.Info("events channel closed", "relay", url, "ingested", ingested)
				return true
			}
			if ingest(ctx, p.store, p.dedup, p.log, ev) {
				ingested++
			} else {
				skipped++
			}
		}
	}
}
