package store

import "github.com/nbd-wtf/go-nostr"

// Tier names. These are both ClickHouse table suffixes and config keys.
const (
	TierPermanent = "permanent"
	TierArchive   = "archive"
	TierSocial    = "social"
	TierTransient = "transient"
	TierDrop      = "drop" // never stored
)

// activeTiers is the set of tiers that receive events (have batchers).
var activeTiers = []string{TierPermanent, TierArchive, TierSocial, TierTransient}

// TierForKind is the exported scope check used by the policy layer and others.
// Returns the tier name (permanent|archive|social|transient|drop).
func TierForKind(kind int) string { return classify(kind) }

// InScopeKinds is the list of kinds this relay stores, used by the crawler to
// build its subscription filter. Mirrors the classify() switch.
func InScopeKinds() []int { return []int{0, 1, 3, 6, 7, 16, 9735, 10002} }

// classify maps a nostr kind to a retention tier, aligned to the social-core
// scope. Unknown kinds → drop.
//
// This is the single source of truth for "what does this relay store". Override
// per-instance via config.Classifier if you want to tune without recompiling.
func classify(kind int) string {
	switch kind {
	case 0, 3, 9735, 10002: // metadata, contacts, zap receipt, relay list
		return TierPermanent
	case 1: // text notes
		return TierArchive
	case 6, 7, 16: // reposts + reactions (stored raw; stats via on-demand/snapshots)
		return TierSocial
	default:
		return TierDrop // ephemeral, DMs, gift wraps, and all out-of-scope NIPs
	}
}

// classifyWithOverride applies an optional config override map on top of the default.
func classifyWithOverride(kind int, override map[int]string) string {
	if t, ok := override[kind]; ok {
		return t
	}
	return classify(kind)
}

// tierForEvent returns the destination tier for an event, honoring config overrides.
func tierForEvent(evt *nostr.Event, override map[int]string) string {
	return classifyWithOverride(evt.Kind, override)
}

// isReplaceableKind reports whether a kind has replaceable/addressable semantics
// (only the latest version per pubkey[/d-tag] is meaningful).
func isReplaceableKind(kind int) bool {
	switch kind {
	case 0, 3, 10002:
		return true
	}
	return false
}
