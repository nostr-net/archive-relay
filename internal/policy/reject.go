// Package policy holds the khatru hook functions that gate ingress and reads.
package policy

import (
	"context"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/store"
)

// RejectOutOfScope is the primary ingress gate: drops events whose kind is not
// in the archive scope (the nostrarchives social core: 0,1,3,6,7,16,9735,10002).
// The scope check delegates to store.TierForKind so there's one source of truth.
// ANALYSIS.md §3d① (D8/D9).
func RejectOutOfScope(_ context.Context, evt *nostr.Event) (bool, string) {
	if store.TierForKind(evt.Kind) == store.TierDrop {
		return true, "kind out of scope"
	}
	return false, ""
}

// RejectFilterBreadth caps the size of inbound filters so a hostile REQ can't
// force huge scans. Mirrors eventstore/postgresql's QueryIDsLimit etc.
// Zero means "no limit". Tighten for your threat model.
type RejectFilterBreadth struct {
	MaxIDs     int
	MaxAuthors int
	MaxKinds   int
	MaxTags    int
}

// Reject implements the khatru RejectFilter signature.
func (r RejectFilterBreadth) Reject(_ context.Context, f nostr.Filter) (bool, string) {
	if r.MaxIDs > 0 && len(f.IDs) > r.MaxIDs {
		return true, "too many ids"
	}
	if r.MaxAuthors > 0 && len(f.Authors) > r.MaxAuthors {
		return true, "too many authors"
	}
	if r.MaxKinds > 0 && len(f.Kinds) > r.MaxKinds {
		return true, "too many kinds"
	}
	if r.MaxTags > 0 {
		n := 0
		for _, v := range f.Tags {
			n += len(v)
		}
		if n > r.MaxTags {
			return true, "too many tag values"
		}
	}
	return false, ""
}
