//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// feedQuery issues a pure-feed-shaped REQ (what the relay layer's
// default-since injection produces) and returns the served events in order.
func feedQuery(t *testing.T, s *Store, kinds []int, limit int) []*nostr.Event {
	t.Helper()
	since := nostr.Now() - 60*60 // 1h ago: inside the feed TTL window
	f := nostr.Filter{Kinds: kinds, Since: &since, Limit: limit}
	ch, err := s.QueryEvents(context.Background(), f)
	if err != nil {
		t.Fatalf("feed query: %v", err)
	}
	var out []*nostr.Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func waitForFeed(t *testing.T, s *Store, want int, kinds []int) []*nostr.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got := feedQuery(t, s, kinds, 100)
		if len(got) >= want {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("feed never reached %d events (last: %d)", want, len(feedQuery(t, s, kinds, 100)))
	return nil
}

func TestFeedServeGlobalShape(t *testing.T) {
	s, _, cleanup := setupStore(t)
	defer cleanup()

	sk := "feed00000000000000000000000000000000000000000000000000000000000001"
	old := signEventAt(t, sk, 1, "older", nil, nostr.Now()-1800)
	new1 := signEventAt(t, sk, 1, "newer", nil, nostr.Now()-60)
	new2 := signEventAt(t, sk, 1, "newest", nil, nostr.Now()-10)
	if err := s.SaveEvent(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvent(context.Background(), new1); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvent(context.Background(), new2); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	got := waitForFeed(t, s, 3, []int{1})
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	if got[0].Content != "newest" || got[1].Content != "newer" || got[2].Content != "older" {
		t.Fatalf("wrong order: %v %v %v", got[0].Content, got[1].Content, got[2].Content)
	}
}

func TestFeedReplaceableWinnerAndEqualTSTiebreak(t *testing.T) {
	s, _, cleanup := setupStore(t)
	defer cleanup()

	sk := "feed00000000000000000000000000000000000000000000000000000000000002"
	ts := nostr.Now() - 30
	vOld := signEventAt(t, sk, 3, "old-contacts", nil, ts-100)
	eA := signEventAt(t, sk, 3, "version-a", nil, ts)
	eB := signEventAt(t, sk, 3, "version-b", nil, ts)
	// ids are content hashes: identify the expected winner by ID order,
	// never by content label.
	wantID := eA.ID
	if eB.ID < wantID {
		wantID = eB.ID
	}
	for _, ev := range []*nostr.Event{vOld, eA, eB} {
		if err := s.SaveEvent(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// poll for the STABLE winner: the feed writer coalesces on a 5s cadence,
	// so early polls can see a partial window
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got := feedQuery(t, s, []int{3}, 100)
		if len(got) == 1 && got[0].ID == wantID {
			return // correct winner
		}
		if len(got) > 1 {
			t.Fatalf("winner collapse broken, got %d", len(got))
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("feed winner never stabilized on the lowest id")
}

func TestFeedMixedKindsBothSurvive(t *testing.T) {
	s, _, cleanup := setupStore(t)
	defer cleanup()

	sk := "feed00000000000000000000000000000000000000000000000000000000000003"
	prof := signEventAt(t, sk, 0, "profile", nil, nostr.Now()-20)
	contacts := signEventAt(t, sk, 3, "contacts", nil, nostr.Now()-10)
	note := signEventAt(t, sk, 1, "note", nil, nostr.Now()-5)
	for _, ev := range []*nostr.Event{prof, contacts, note} {
		if err := s.SaveEvent(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	got := waitForFeed(t, s, 3, []int{0, 1, 3})
	kinds := map[int]bool{}
	for _, ev := range got {
		kinds[ev.Kind] = true
	}
	if !kinds[0] || !kinds[1] || !kinds[3] {
		t.Fatalf("all three kinds must survive the feed path, got %+v", kinds)
	}
}

func TestFeedTombstoneHidesAndDupIDDedupes(t *testing.T) {
	s, _, cleanup := setupStore(t)
	defer cleanup()

	sk := "feed00000000000000000000000000000000000000000000000000000000000004"
	target := signEventAt(t, sk, 1, "delete-me", nil, nostr.Now()-40)
	keeper := signEventAt(t, sk, 1, "keep-me", nil, nostr.Now()-30)
	if err := s.SaveEvent(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEvent(context.Background(), keeper); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	// duplicate id (re-ingest) — served exactly once
	if err := s.SaveEvent(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	waitForFeed(t, s, 2, []int{1})

	// NIP-09 delete; writer reloads the dict within its bounded cadence
	if err := s.DeleteEvent(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got := feedQuery(t, s, []int{1}, 100)
		contents := map[string]bool{}
		for _, ev := range got {
			contents[ev.Content] = true
		}
		if !contents["keep-me"] {
			t.Fatalf("keeper vanished: %+v", contents)
		}
		if !contents["delete-me"] {
			return // hidden — done
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("tombstoned event still served from feed after 10s")
}

func TestFeedEligibleUnit(t *testing.T) {
	since := nostr.Now() - 3600
	oldSince := nostr.Now() - nostr.Timestamp(8*24*3600)
	cases := []struct {
		name string
		f    nostr.Filter
		want bool
	}{
		{"pure feed", nostr.Filter{Kinds: []int{1}, Since: &since}, true},
		{"no kinds", nostr.Filter{Since: &since}, true},
		{"authors", nostr.Filter{Authors: []string{"a"}, Since: &since}, false},
		{"ids", nostr.Filter{IDs: []string{"i"}, Since: &since}, false},
		{"tags", nostr.Filter{Tags: nostr.TagMap{"p": []string{"x"}}, Since: &since}, false},
		{"until", nostr.Filter{Until: &since}, false},
		{"no since", nostr.Filter{Kinds: []int{1}}, false},
		{"since too old", nostr.Filter{Kinds: []int{1}, Since: &oldSince}, false},
		{"dropped kind", nostr.Filter{Kinds: []int{4}, Since: &since}, false},
	}
	for _, c := range cases {
		if got := feedEligible(c.f, nil); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestSplitFeedKindsUnit(t *testing.T) {
	a, b := splitFeedKinds(nil, nil)
	if len(a) == 0 || len(b) != 3 {
		t.Fatalf("default split: a=%v b=%v", a, b)
	}
	a, b = splitFeedKinds([]int{1, 3}, nil)
	if len(a) != 1 || a[0] != 1 || len(b) != 1 || b[0] != 3 {
		t.Fatalf("explicit split: a=%v b=%v", a, b)
	}
	a, b = splitFeedKinds([]int{5}, nil) // dropped kind
	if len(a) != 0 || len(b) != 0 {
		t.Fatalf("dropped kinds must vanish: a=%v b=%v", a, b)
	}
}
