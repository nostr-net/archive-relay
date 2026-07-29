// Package relay wires the store, scheduler, and policy into a khatru Relay.
package relay

import (
	"context"

	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/policy"
	"github.com/nostr-net/archive-relay/internal/scheduler"
	"github.com/nostr-net/archive-relay/internal/store"
)

// Deps bundles everything the relay needs to wire its hooks.
type Deps struct {
	Store   *store.Store
	Sched   *scheduler.Scheduler       // nil to disable future-dating
	WoT     *policy.WoT                // nil (or Threshold 0) to disable read-time WoT
	Limiter *policy.Limiter            // nil to disable per-IP rate limiting
	Breadth policy.RejectFilterBreadth // zero-value fields disable that limit
}

// New assembles a khatru Relay with all hooks wired to the deps.
func New(d Deps) *khatru.Relay {
	rl := khatru.NewRelay()

	rl.Info.Name = "archive-relay"
	rl.Info.Description = "Selective social-core Nostr archive relay (ClickHouse-backed)."
	rl.Info.Software = "https://github.com/nostr-net/archive-relay"
	rl.Info.Version = "0.1.0"
	rl.Info.SupportedNIPs = []any{1, 9, 11, 12, 15, 45}

	// --- ingress gates ---
	rl.RejectEvent = append(rl.RejectEvent, policy.RejectOutOfScope)
	if d.Limiter != nil {
		rl.RejectEvent = append(rl.RejectEvent, d.Limiter.RejectEvent)
	}

	// --- storage (composed with the scheduler for future-dated events) ---
	save := d.Store.SaveEvent
	if d.Sched != nil {
		save = func(ctx context.Context, evt *nostr.Event) error {
			if d.Sched.ShouldDefer(evt) {
				return d.Sched.Defer(ctx, evt) // park in SQLite; PreventBroadcast stops the rest
			}
			return d.Store.SaveEvent(ctx, evt)
		}
		rl.PreventBroadcast = append(rl.PreventBroadcast, func(_ *khatru.WebSocket, evt *nostr.Event) bool {
			return d.Sched.ShouldDefer(evt)
		})
	}
	rl.StoreEvent = append(rl.StoreEvent, save)
	rl.ReplaceEvent = append(rl.ReplaceEvent, d.Store.ReplaceEvent)
	rl.DeleteEvent = append(rl.DeleteEvent, d.Store.DeleteEvent)

	// --- reads (optionally filtered by read-time WoT) ---
	query := d.Store.QueryEvents
	if d.WoT != nil {
		query = d.WoT.WrapQuery(d.Store.QueryEvents)
	}
	rl.QueryEvents = append(rl.QueryEvents, query)
	rl.CountEvents = append(rl.CountEvents, d.Store.CountEvents)

	// --- egress gates ---
	// per-IP read rate limit (same limiter as publish) + REQ-breadth caps, so a
	// hostile client can't force many large FINAL scans across the tiers.
	if d.Limiter != nil {
		rl.RejectFilter = append(rl.RejectFilter, d.Limiter.RejectFilter)
		rl.RejectCountFilter = append(rl.RejectCountFilter, d.Limiter.RejectFilter)
	}
	rl.RejectFilter = append(rl.RejectFilter, d.Breadth.Reject)
	rl.RejectCountFilter = append(rl.RejectCountFilter, d.Breadth.Reject)

	return rl
}
