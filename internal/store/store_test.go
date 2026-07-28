package store

import (
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		kind int
		want string
	}{
		{0, TierPermanent},     // metadata
		{3, TierPermanent},     // contacts
		{1, TierArchive},       // text note
		{6, TierSocial},        // repost
		{7, TierSocial},        // reaction
		{16, TierSocial},       // generic repost
		{9735, TierPermanent},  // zap receipt
		{10002, TierPermanent}, // relay list
		{4, TierDrop},          // legacy DM — out of scope
		{1059, TierDrop},       // gift wrap — out of scope
		{21000, TierDrop},      // ephemeral — NIP-01 forbids storing
		{30023, TierDrop},      // long-form article — not in v1 scope
		{99999, TierDrop},      // unknown
	}
	for _, c := range cases {
		if got := classify(c.kind); got != c.want {
			t.Errorf("classify(%d) = %q, want %q", c.kind, got, c.want)
		}
	}
}

func TestBuildFilterSQL(t *testing.T) {
	since := nostr.Timestamp(1700000000)
	f := nostr.Filter{
		Authors: []string{"abc", "def"},
		Tags: nostr.TagMap{
			"e": []string{"evt1", "evt2"},
			"t": []string{"bitcoin"},
			"d": []string{"myid"},
		},
		Since: &since,
		Limit: 50,
	}

	where, args, tail := buildFilterSQL(f)

	// Must always include the tombstone predicate.
	if !strings.Contains(where, "NOT dictHas('tombstone_dict', id)") {
		t.Errorf("missing tombstone predicate in: %s", where)
	}
	if !strings.Contains(where, "pubkey IN") {
		t.Errorf("missing authors predicate: %s", where)
	}
	if !strings.Contains(where, "hasAny(tag_e") {
		t.Errorf("missing e-tag predicate: %s", where)
	}
	if !strings.Contains(where, "hasAny(tag_t") {
		t.Errorf("missing t-tag predicate: %s", where)
	}
	if !strings.Contains(where, "tag_d = ?") {
		t.Errorf("missing d-tag predicate: %s", where)
	}
	if !strings.Contains(where, "created_at >= ?") {
		t.Errorf("missing since predicate: %s", where)
	}
	if !strings.Contains(tail, "LIMIT 50") {
		t.Errorf("limit not applied: %s", tail)
	}
	// args: 1 pubkey slice + 1 e-tag slice + 1 t-tag slice + 1 d value + 1 since = 5
	if len(args) != 5 {
		t.Errorf("args count = %d, want 5 (got: %v)", len(args), args)
	}
}

func TestBuildFilterSQLLimitClamped(t *testing.T) {
	// absurd limits clamp to defaultQueryLimit
	_, _, tail := buildFilterSQL(nostr.Filter{Limit: 9_999_999})
	if !strings.Contains(tail, "LIMIT 1000") {
		t.Errorf("limit not clamped: %s", tail)
	}
	// limit 0 (unset) also clamps
	_, _, tail = buildFilterSQL(nostr.Filter{})
	if !strings.Contains(tail, "LIMIT 1000") {
		t.Errorf("default limit not applied: %s", tail)
	}
}

func TestBuildFilterSQLEmpty(t *testing.T) {
	where, _, _ := buildFilterSQL(nostr.Filter{})
	// no user conditions → only the tombstone predicate → still present
	if !strings.Contains(where, "NOT dictHas") {
		t.Errorf("empty filter should still get tombstone predicate: %s", where)
	}
}

func TestTiersForFilter(t *testing.T) {
	// kinds spanning two tiers collapse to those tiers only
	tiers := tiersForFilter(nostr.Filter{Kinds: []int{1, 7}}, nil) // archive + social
	if len(tiers) != 2 {
		t.Fatalf("expected 2 tiers, got %v", tiers)
	}
	// out-of-scope kinds contribute nothing
	tiers = tiersForFilter(nostr.Filter{Kinds: []int{1059, 4}}, nil) // both drop
	if len(tiers) != 0 {
		t.Fatalf("expected 0 tiers for all-drop kinds, got %v", tiers)
	}
	// empty kinds → all active tiers
	tiers = tiersForFilter(nostr.Filter{}, nil)
	if len(tiers) != len(activeTiers) {
		t.Fatalf("expected %d tiers for match-all, got %v", len(activeTiers), tiers)
	}
	// override map is honored
	tiers = tiersForFilter(nostr.Filter{Kinds: []int{5}}, map[int]string{5: TierPermanent})
	if len(tiers) != 1 || tiers[0] != TierPermanent {
		t.Fatalf("override not honored, got %v", tiers)
	}
}

func TestReplyTargetNIP10(t *testing.T) {
	cases := []struct {
		name string
		tags nostr.Tags
		want string
	}{
		{"no e-tags", nostr.Tags{{"t", "x"}}, ""},
		{"single positional", nostr.Tags{{"e", "A"}}, "A"},
		{"last positional wins", nostr.Tags{{"e", "A"}, {"e", "B"}}, "B"},
		{"explicit reply marker", nostr.Tags{{"e", "A", "wss://r", "reply"}}, "A"},
		{"reply marker beats earlier positional", nostr.Tags{{"e", "A"}, {"e", "B", "wss://r", "reply"}}, "B"},
		{"root-only is NOT a reply (no positional)", nostr.Tags{{"e", "A", "wss://r", "root"}}, ""},
		{"mention is NOT a reply", nostr.Tags{{"e", "A", "wss://r", "mention"}}, ""},
		{"reply marker chosen over a root marker", nostr.Tags{{"e", "ROOT", "wss://r", "root"}, {"e", "PARENT", "wss://r", "reply"}}, "PARENT"},
	}
	for _, c := range cases {
		if got := replyTarget(c.tags); got != c.want {
			t.Errorf("%s: replyTarget() = %q, want %q", c.name, got, c.want)
		}
	}
}
