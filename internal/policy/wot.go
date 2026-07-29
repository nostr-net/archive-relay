package policy

import (
	"context"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// LookupFollowers returns the follower count for a pubkey. In practice this is
// stats.Service.Followers.
type LookupFollowers func(ctx context.Context, pubkey string) (int64, error)

// WoT is a read-time web-of-trust filter: it drops events whose author has
// fewer than Threshold followers, using a short-TTL cache to avoid hammering
// the follower-counts table. A Threshold of 0 disables filtering entirely.
// This is a read-time filter, not an ingest gate.
type WoT struct {
	Lookup    LookupFollowers
	Threshold int64
	TTL       time.Duration

	cache sync.Map // pubkey -> woTCacheEntry
}

type wotCacheEntry struct {
	count  int64
	expiry time.Time
}

func (w *WoT) Allows(ctx context.Context, pubkey string) bool {
	if w.Threshold == 0 {
		return true
	}
	if e, ok := w.cache.Load(pubkey); ok {
		ce := e.(wotCacheEntry)
		if time.Now().Before(ce.expiry) {
			return ce.count >= w.Threshold
		}
	}
	n, err := w.Lookup(ctx, pubkey)
	if err != nil {
		// on lookup failure, don't filter (fail open) but don't cache
		return true
	}
	ttl := w.TTL
	if ttl == 0 {
		ttl = 30 * time.Second
	}
	w.cache.Store(pubkey, wotCacheEntry{count: n, expiry: time.Now().Add(ttl)})
	return n >= w.Threshold
}

// WrapQuery returns a QueryEvents-shaped function that filters the underlying
// stream through the WoT check.
func (w *WoT) WrapQuery(
	underlying func(context.Context, nostr.Filter) (chan *nostr.Event, error),
) func(context.Context, nostr.Filter) (chan *nostr.Event, error) {
	return func(ctx context.Context, f nostr.Filter) (chan *nostr.Event, error) {
		ch, err := underlying(ctx, f)
		if err != nil || w.Threshold == 0 {
			return ch, err
		}
		out := make(chan *nostr.Event, 64)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-ch:
					if !ok {
						return
					}
					if w.Allows(ctx, ev.PubKey) {
						select {
						case out <- ev:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()
		return out, nil
	}
}
