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
	Store      *store.Store
	Sched      *scheduler.Scheduler       // nil to disable future-dating
	Limiter    *policy.Limiter            // nil to disable per-IP rate limiting
	Breadth    policy.RejectFilterBreadth // zero-value fields disable that limit
	Access     *policy.Access             // nil to disable NIP-42 + allow-list
	ServiceURL string                     // canonical URL; required when Access is enabled
}

// New assembles a khatru Relay with all hooks wired to the deps.
func New(d Deps) *khatru.Relay {
	rl := khatru.NewRelay()

	rl.Info.Name = "archive-relay"
	rl.Info.Description = "Selective social-core Nostr archive relay (ClickHouse-backed)."
	rl.Info.Software = "https://github.com/nostr-net/archive-relay"
	rl.Info.Version = "0.1.0"
	rl.Info.SupportedNIPs = []any{1, 9, 11, 12, 15, 45}
	if d.ServiceURL != "" {
		rl.ServiceURL = d.ServiceURL
	}
	// Advertise auth-dependent NIPs only when they're actually enforced — a
	// bare serviceURL must not imply NIP-42 support.
	authEnabled := d.Access != nil && d.Access.Enabled()
	if authEnabled {
		rl.Info.SupportedNIPs = append(rl.Info.SupportedNIPs, 42, 86) // AUTH + management RPC
	}

	// --- ingress gates ---
	// auth (if enabled) runs first so an unauthed client gets the AUTH challenge
	// before any scope/rate-limit reason is reported.
	if authEnabled {
		rl.RejectEvent = prepend(rl.RejectEvent, d.Access.RejectEvent)
	}
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

	// --- reads ---
	rl.QueryEvents = append(rl.QueryEvents, d.Store.QueryEvents)
	rl.CountEvents = append(rl.CountEvents, d.Store.CountEvents)

	// --- egress gates ---
	// auth first (auth-required challenge), then per-IP read rate limit, then
	// REQ-breadth caps — so a hostile client can't force large FINAL scans.
	if authEnabled {
		rl.RejectFilter = prepend(rl.RejectFilter, d.Access.RejectFilter)
		rl.RejectCountFilter = prepend(rl.RejectCountFilter, d.Access.RejectFilter)
	}
	if d.Limiter != nil {
		rl.RejectFilter = append(rl.RejectFilter, d.Limiter.RejectFilter)
		rl.RejectCountFilter = append(rl.RejectCountFilter, d.Limiter.RejectFilter)
	}
	rl.RejectFilter = append(rl.RejectFilter, d.Breadth.Reject)
	rl.RejectCountFilter = append(rl.RejectCountFilter, d.Breadth.Reject)

	// --- NIP-86 relay management RPC (allow/ban/list pubkeys) ---
	// khatru serves this automatically on the relay URL when the request sends
	// Content-Type: application/nostr+json+rpc; the caller is authed via NIP-98
	// (an HTTP-signed event), gated to admin pubkeys by RejectAPICall.
	if d.Access != nil {
		rl.ManagementAPI.RejectAPICall = append(rl.ManagementAPI.RejectAPICall, d.Access.AdminGate)
		rl.ManagementAPI.AllowPubKey = func(ctx context.Context, pk, reason string) error {
			return d.Access.AllowPubkey(ctx, pk, reason)
		}
		rl.ManagementAPI.BanPubKey = func(ctx context.Context, pk, _ string) error {
			return d.Access.RevokePubkey(ctx, pk)
		}
		rl.ManagementAPI.ListAllowedPubKeys = d.Access.ListAllowedPubkeys
	}

	return rl
}

// prepend returns a new hook slice with fn at the front.
func prepend[T any](
	hooks []func(context.Context, T) (bool, string),
	fn func(context.Context, T) (bool, string),
) []func(context.Context, T) (bool, string) {
	return append([]func(context.Context, T) (bool, string){fn}, hooks...)
}
