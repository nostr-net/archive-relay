package store

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/config"
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
	if !strings.Contains(tail, "id ASC") {
		t.Errorf("missing id ASC tiebreak in: %s", tail)
	}
	if !strings.Contains(tail, collapseLimitBy) {
		t.Errorf("missing LIMIT 1 BY collapse in: %s", tail)
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

func TestBuildFilterSQLCollapseTail(t *testing.T) {
	// Mixed replaceable + regular kinds still use the kind-qualified LIMIT BY
	// expression so kind-0/3/10002 collapse on pubkey and kind-1 on id.
	_, _, tail := buildFilterSQL(nostr.Filter{Kinds: []int{1, 3}, Limit: 25})
	want := " ORDER BY created_at DESC, id ASC LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id) LIMIT 25"
	if tail != want {
		t.Errorf("mixed-kinds tail =\n  %q\nwant\n  %q", tail, want)
	}

	_, _, tail = buildFilterSQL(nostr.Filter{Kinds: []int{0, 3, 10002}, Limit: 10})
	want = " ORDER BY created_at DESC, id ASC LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id) LIMIT 10"
	if tail != want {
		t.Errorf("replaceable-kinds tail =\n  %q\nwant\n  %q", tail, want)
	}
}

func TestSortDescIDTiebreak(t *testing.T) {
	ev := []*nostr.Event{
		{ID: "b", CreatedAt: 100},
		{ID: "c", CreatedAt: 200},
		{ID: "a", CreatedAt: 100},
		{ID: "d", CreatedAt: 50},
		{ID: "aa", CreatedAt: 100},
	}
	sortDesc(ev)
	got := make([]string, len(ev))
	for i, e := range ev {
		got[i] = e.ID
	}
	// created_at DESC, then id ASC: c (200), a, aa, b (100), d (50)
	want := []string{"c", "a", "aa", "b", "d"}
	if len(got) != len(want) {
		t.Fatalf("len=%d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestAcquireReadOverflow(t *testing.T) {
	s := New(&config.Config{}, slog.Default())
	for i := 0; i < readAdmissionCap; i++ {
		s.readSem <- struct{}{}
		s.readWait <- struct{}{}
	}
	_, err := s.acquireRead(context.Background())
	if !errors.Is(err, ErrReadBusy) {
		t.Fatalf("got %v, want ErrReadBusy", err)
	}
}

func TestAcquireReadCtxCancel(t *testing.T) {
	s := New(&config.Config{}, slog.Default())
	for i := 0; i < readAdmissionCap; i++ {
		s.readSem <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.acquireRead(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestRowFromEventNativeTags(t *testing.T) {
	evt := &nostr.Event{
		ID:        "id1",
		PubKey:    "pk1",
		CreatedAt: 1_700_000_000,
		Kind:      1,
		Content:   "hello",
		Sig:       "sig1",
		Tags: nostr.Tags{
			{"e", "eid", "wss://r", "reply"},
			{"p", "pk2"},
			{"t", "btc"},
			{"d", "dval"},
			{"amount", "1000"},
		},
	}
	row := rowFromEvent(evt)
	if len(row) != 9 {
		t.Fatalf("row len=%d, want 9 (tag_* omitted; tags native inserted)", len(row))
	}
	if row[0] != evt.ID || row[1] != evt.PubKey || row[4] != evt.Content || row[5] != evt.Sig {
		t.Fatalf("scalar columns mismatch: %#v", row[:6])
	}
	tagsJSON, ok := row[6].(string)
	if !ok {
		t.Fatalf("tags_raw type %T", row[6])
	}
	var raw nostr.Tags
	if err := json.Unmarshal([]byte(tagsJSON), &raw); err != nil {
		t.Fatalf("tags_raw json: %v", err)
	}
	if !reflect.DeepEqual(raw, evt.Tags) {
		t.Fatalf("tags_raw=%v want %v", raw, evt.Tags)
	}
	tags, ok := row[7].([][]string)
	if !ok {
		t.Fatalf("tags type %T, want [][]string", row[7])
	}
	want := [][]string{
		{"e", "eid", "wss://r", "reply"},
		{"p", "pk2"},
		{"t", "btc"},
		{"d", "dval"},
		{"amount", "1000"},
	}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("native tags=%v want %v", tags, want)
	}
	if row[8] != "eid" {
		t.Fatalf("reply_to=%v want eid", row[8])
	}

	empty := rowFromEvent(&nostr.Event{ID: "e"})
	tags, ok = empty[7].([][]string)
	if !ok || tags == nil || len(tags) != 0 {
		t.Fatalf("empty tags = %#v, want non-nil empty [][]string", empty[7])
	}
}

func TestNativeTagsRoundTrip(t *testing.T) {
	in := nostr.Tags{{"e", "id"}, {"t", "x"}}
	got := nostrTags(nativeTags(in))
	if !reflect.DeepEqual([][]string(nativeTags(got)), [][]string{{"e", "id"}, {"t", "x"}}) {
		t.Fatalf("got %#v want %#v", got, in)
	}
	if nativeTags(nil) == nil {
		t.Fatal("nativeTags(nil) must not return nil")
	}
	if nostrTags(nil) == nil {
		t.Fatal("nostrTags(nil) must not return nil")
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

func TestAcquireReadInternalCallBypassesAdmission(t *testing.T) {
	s := New(&config.Config{}, slog.Default())
	for i := 0; i < readAdmissionCap; i++ {
		s.readSem <- struct{}{}
		s.readWait <- struct{}{}
	}
	// khatru 0.19.1 provides no exported constructor for internal-call ctx
	// (the key is unexported), so the bypass branch is exercised end-to-end by
	// khatru's own deleting/expiration paths. Here we pin the sibling behavior:
	// a saturated semaphore still fails EXTERNAL callers, so the bypass is the
	// only thing keeping NIP-09 alive under load.
	_, err := s.acquireRead(context.Background())
	if !errors.Is(err, ErrReadBusy) {
		t.Fatalf("got %v, want ErrReadBusy", err)
	}
}
