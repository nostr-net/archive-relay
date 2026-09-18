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
	// last_fetched - fetchOverlap. Covers relay propagation delay and
	// backdated events that arrive late. 2h (was 24h).
	fetchOverlap = 2 * time.Hour
	// fetchTimeout bounds one relay fetch; a relay that connects but never
	// sends EOSE stalls the tick without it. Applied per subscription.
	fetchTimeout = 5 * time.Minute
	// fullSweepAge is how stale last_full_sweep must get before a pubkey
	// gets a full-history re-pull (Since=nil). last_fetched is NOT the
	// sweep gate — it refreshes every incremental tick, so using it made
	// the documented periodic sweep dead (plan §1.8 / codex #13).
	fullSweepAge = 24 * time.Hour
)

// relayConn is the go-nostr Relay surface the priority crawler uses. Tests
// inject a fake via PriorityCrawler.dial.
type relayConn interface {
	Connect(ctx context.Context) error
	Subscribe(ctx context.Context, filters nostr.Filters, opts ...nostr.SubscriptionOption) (*nostr.Subscription, error)
	Close() error
}

// relayDialer constructs a (not yet connected) relayConn for url.
// Production uses newUpstreamRelay (AssumeValid=true before Connect).
type relayDialer func(ctx context.Context, url string) relayConn

// pubkeyJob is one pubkey's fetch decision for a tick (since + whether this
// attempt is a full-history sweep). Computed once up front so failover across
// relays does not re-read crawl_state.
type pubkeyJob struct {
	pubkey    string
	since     *nostr.Timestamp
	fullSweep bool
}

// PriorityCrawler fetches the history of a configured set of pubkeys from a
// dedicated relay list (separate from the firehose -sources), ensuring their
// in-scope events are captured even when the firehose misses them. It shares
// the store + Dedup layer with the firehose Crawler, and records last_fetched
// / last_full_sweep in crawl_state — last_fetched bounds the next tick's
// `since` filter; last_full_sweep gates the periodic completeness sweep.
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
	dial     relayDialer
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

// tick visits each relay once: one websocket (AssumeValid set before Connect),
// then every still-incomplete pubkey as a sequential subscription on that conn
// (per-sub fetchTimeout). Sequential-per-conn is intentional — fetchOn's ingest
// loop serializes events from one subscription, so overlapping subs on the same
// conn would not increase throughput. Pubkeys the relay does not complete
// (connect/subscribe/timeout/incomplete EOSE) remain for the next URL,
// preserving per-pubkey failover. The conn is closed before moving on.
func (p *PriorityCrawler) tick(ctx context.Context) {
	kinds := store.InScopeKinds()
	remaining := make([]pubkeyJob, 0, len(p.pubkeys))
	for _, pk := range p.pubkeys {
		if ctx.Err() != nil {
			return
		}
		since, full := p.decideSince(ctx, pk)
		remaining = append(remaining, pubkeyJob{pubkey: pk, since: since, fullSweep: full})
	}
	for _, url := range p.relays {
		if ctx.Err() != nil || len(remaining) == 0 {
			break
		}
		remaining = p.fetchRelay(ctx, url, remaining, kinds)
	}
	for _, job := range remaining {
		p.log.With("pubkey", job.pubkey).Warn("no relay served this pubkey", "relays", p.relays)
	}
}

func (p *PriorityCrawler) dialRelay(ctx context.Context, url string) relayConn {
	if p.dial != nil {
		return p.dial(ctx, url)
	}
	return newUpstreamRelay(ctx, url)
}

// decideSince returns the subscription Since (nil = full history) and whether
// this tick is a full-history sweep. Full sweep when the pubkey has never been
// crawled (last_fetched==0), last_full_sweep is older than fullSweepAge, or
// either crawl_state lookup failed. Incremental ticks only refresh
// last_fetched, so sweeps must not key off it.
func (p *PriorityCrawler) decideSince(ctx context.Context, pubkey string) (since *nostr.Timestamp, fullSweep bool) {
	log := p.log.With("pubkey", pubkey)
	last, err := p.ctrl.LastFetched(ctx, pubkey)
	if err != nil {
		log.Warn("last_fetched lookup failed; doing full fetch", "err", err)
	}
	swept, serr := p.ctrl.LastSwept(ctx, pubkey)
	if serr != nil {
		log.Warn("last_full_sweep lookup failed; doing full fetch", "err", serr)
	}
	fullSweep = err != nil || serr != nil || last == 0 || time.Since(time.Unix(swept, 0)) >= fullSweepAge
	if !fullSweep {
		if ts := last - int64(fetchOverlap/time.Second); ts > 0 {
			t := nostr.Timestamp(ts)
			since = &t
		}
	}
	return since, fullSweep
}

func (p *PriorityCrawler) fetchRelay(ctx context.Context, url string, remaining []pubkeyJob, kinds []int) []pubkeyJob {
	relay := p.dialRelay(ctx, url)
	if err := relay.Connect(ctx); err != nil {
		p.log.Warn("connect failed", "relay", url, "err", err)
		_ = relay.Close()
		return remaining
	}
	defer relay.Close()

	var incomplete []pubkeyJob
	for i, job := range remaining {
		if ctx.Err() != nil {
			return append(incomplete, remaining[i:]...)
		}
		log := p.log.With("pubkey", job.pubkey)
		if p.fetchOn(ctx, relay, url, job.pubkey, kinds, job.since, log) {
			_ = p.ctrl.MarkFetched(ctx, job.pubkey)
			if job.fullSweep {
				_ = p.ctrl.MarkSwept(ctx, job.pubkey)
			}
			continue
		}
		incomplete = append(incomplete, job)
	}
	return incomplete
}

// fetchOn subscribes to one pubkey on an already-connected relay and ingests
// until EOSE. Bounded by fetchTimeout; a stalled/closed subscription reports
// false so the next relay is tried. Anything short of a clean EOSE must not
// advance last_fetched past events that were never fetched.
func (p *PriorityCrawler) fetchOn(parent context.Context, relay relayConn, url, pubkey string,
	kinds []int, since *nostr.Timestamp, log *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(parent, fetchTimeout)
	defer cancel()

	sub, err := relay.Subscribe(ctx, nostr.Filters{{
		Authors: []string{pubkey}, Kinds: kinds, Since: since,
	}})
	if err != nil {
		log.Warn("subscribe failed", "relay", url, "err", err)
		return false
	}

	ingested, skipped := 0, 0
	for {
		select {
		case <-ctx.Done():
			if parent.Err() == nil {
				log.Warn("fetch timed out", "relay", url, "ingested", ingested)
			}
			return false
		case <-sub.EndOfStoredEvents:
			log.Info("priority crawl done", "relay", url, "ingested", ingested, "skipped", skipped)
			return true
		case reason := <-sub.ClosedReason:
			log.Warn("subscription closed before EOSE", "relay", url, "reason", reason)
			return false
		case ev, ok := <-sub.Events:
			if !ok {
				log.Warn("events channel closed before EOSE", "relay", url, "ingested", ingested)
				return false
			}
			if ingest(ctx, p.store, p.dedup, p.log, ev) {
				ingested++
			} else {
				skipped++
			}
		}
	}
}
