package crawler

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/store"
)

type fakeStore struct {
	saved []*nostr.Event
	err   error
}

func (f *fakeStore) SaveEvent(_ context.Context, ev *nostr.Event) error {
	if f.err != nil {
		return f.err
	}
	f.saved = append(f.saved, ev)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func matchingEvent() *nostr.Event {
	ev := &nostr.Event{
		PubKey:    strings.Repeat("ab", 32),
		CreatedAt: 1,
		Kind:      1,
		Tags:      nostr.Tags{},
		Content:   "hello",
	}
	ev.ID = ev.GetID()
	return ev
}

func TestSinceForReconnectBackfillAware(t *testing.T) {
	disc := time.Unix(1_700_000_000, 0)

	if got := sinceForReconnect(false, disc); got != nil {
		t.Errorf("unfinished backfill must use Since=nil, got %v", got)
	}
	if got := sinceForReconnect(true, time.Time{}); got != nil {
		t.Errorf("zero lastDisconnect must use Since=nil, got %v", got)
	}

	got := sinceForReconnect(true, disc)
	if got == nil {
		t.Fatal("completed backfill should set Since")
	}
	want := nostr.Timestamp(disc.Add(-reconnectOverlap).Unix())
	if *got != want {
		t.Errorf("Since = %d, want lastDisconnect-2h = %d", *got, want)
	}
	if reconnectOverlap != 2*time.Hour {
		t.Errorf("reconnectOverlap = %s, want 2h", reconnectOverlap)
	}
}

func TestIngestDropsMismatchedID(t *testing.T) {
	d, _ := testDedup(t)
	saver := &fakeStore{}
	ev := matchingEvent()
	ev.ID = strings.Repeat("00", 32) // valid hex, wrong hash
	if ev.CheckID() {
		t.Fatal("precondition: CheckID should fail for a mismatched id")
	}

	ok := ingestStep(context.Background(), saver, d, testLogger(), ev, func(e *nostr.Event) (idOK, sigOK bool) {
		return e.CheckID(), true // signature forced-ok so CheckID is the seam under test
	})
	if ok {
		t.Error("mismatched id must be dropped")
	}
	if len(saver.saved) != 0 {
		t.Errorf("SaveEvent called %d times, want 0", len(saver.saved))
	}
	if d.DroppedBadID() != 1 {
		t.Errorf("DroppedBadID = %d, want 1", d.DroppedBadID())
	}
	if d.Seen(ev.ID) {
		t.Error("mismatched id must not be marked seen")
	}
}

func TestIngestKeepsMatchingID(t *testing.T) {
	d, _ := testDedup(t)
	saver := &fakeStore{}
	ev := matchingEvent()
	if !ev.CheckID() {
		t.Fatal("precondition: CheckID should pass")
	}

	ok := ingestStep(context.Background(), saver, d, testLogger(), ev, func(e *nostr.Event) (idOK, sigOK bool) {
		return e.CheckID(), true
	})
	if !ok {
		t.Error("matching id should be kept")
	}
	if len(saver.saved) != 1 {
		t.Fatalf("SaveEvent called %d times, want 1", len(saver.saved))
	}
	if !d.Seen(ev.ID) {
		t.Error("kept event should be marked seen")
	}
}

func TestIngestDropsOutOfScopeKindBeforeVerify(t *testing.T) {
	d, _ := testDedup(t)
	saver := &fakeStore{}
	ev := matchingEvent()
	ev.Kind = 4 // DMs — TierDrop
	if store.TierForKind(ev.Kind) != store.TierDrop {
		t.Fatal("precondition: kind 4 is drop")
	}
	ok := ingestStep(context.Background(), saver, d, testLogger(), ev, func(*nostr.Event) (bool, bool) {
		t.Error("verify should not run for out-of-scope kinds")
		return true, true
	})
	if ok || len(saver.saved) != 0 {
		t.Error("out-of-scope event must be dropped without save")
	}
	if d.Seen(ev.ID) {
		t.Error("out-of-scope event must not stay marked seen")
	}
}

func TestIngestDropsInvalidHexID(t *testing.T) {
	d, _ := testDedup(t)
	saver := &fakeStore{}
	ev := matchingEvent()
	ev.ID = "not-hex"
	ok := ingestStep(context.Background(), saver, d, testLogger(), ev, func(e *nostr.Event) (idOK, sigOK bool) {
		return e.CheckID(), true
	})
	if ok {
		t.Error("invalid-hex id must be dropped")
	}
	if len(saver.saved) != 0 {
		t.Errorf("SaveEvent called %d times, want 0", len(saver.saved))
	}
	if d.DroppedBadID() != 1 {
		t.Errorf("DroppedBadID = %d, want 1", d.DroppedBadID())
	}
	if d.Seen(ev.ID) {
		t.Error("invalid-hex id must not poison the seen map")
	}
}

func TestIngestUnmarkOnBatchFull(t *testing.T) {
	d, _ := testDedup(t)
	saver := &fakeStore{err: store.ErrBatchFull}
	ev := matchingEvent()
	ok := ingestStep(context.Background(), saver, d, testLogger(), ev, func(*nostr.Event) (bool, bool) {
		return true, true
	})
	if ok {
		t.Error("batch-full save should not count as ingested")
	}
	if d.Seen(ev.ID) {
		t.Error("batch-full must Unmark so a later re-pull retries")
	}
}

func TestFetchOverlapIsTwoHours(t *testing.T) {
	if fetchOverlap != 2*time.Hour {
		t.Errorf("fetchOverlap = %s, want 2h", fetchOverlap)
	}
}

func TestFullSweepAgeIs24Hours(t *testing.T) {
	if fullSweepAge != 24*time.Hour {
		t.Errorf("fullSweepAge = %s, want 24h", fullSweepAge)
	}
}

func testPriority(t *testing.T, pubkeys, relays []string) *PriorityCrawler {
	t.Helper()
	d, db := testDedup(t)
	return NewPriority(pubkeys, relays, nil, d, db, time.Minute, testLogger())
}

func TestDecideSinceFullSweepWhenNeverFetched(t *testing.T) {
	p := testPriority(t, nil, nil)
	since, full := p.decideSince(context.Background(), "pk")
	if !full {
		t.Error("want full sweep for unknown pubkey")
	}
	if since != nil {
		t.Errorf("since=%v, want nil", since)
	}
}

func TestDecideSinceFullSweepWhenNeverSwept(t *testing.T) {
	p := testPriority(t, nil, nil)
	ctx := context.Background()
	if err := p.ctrl.MarkFetched(ctx, "pk"); err != nil {
		t.Fatal(err)
	}
	since, full := p.decideSince(ctx, "pk")
	if !full {
		t.Error("want full sweep when last_full_sweep is unset")
	}
	if since != nil {
		t.Errorf("since=%v, want nil", since)
	}
}

func TestDecideSinceIncrementalAfterRecentSweep(t *testing.T) {
	p := testPriority(t, nil, nil)
	ctx := context.Background()
	if err := p.ctrl.MarkFetched(ctx, "pk"); err != nil {
		t.Fatal(err)
	}
	if err := p.ctrl.MarkSwept(ctx, "pk"); err != nil {
		t.Fatal(err)
	}
	last, err := p.ctrl.LastFetched(ctx, "pk")
	if err != nil {
		t.Fatal(err)
	}
	since, full := p.decideSince(ctx, "pk")
	if full {
		t.Error("want incremental after recent sweep")
	}
	if since == nil {
		t.Fatal("since is nil")
	}
	want := nostr.Timestamp(last - int64(fetchOverlap/time.Second))
	if *since != want {
		t.Errorf("since=%d, want last-overlap=%d", *since, want)
	}
}

// eoseRelay is a fake relayConn that Connects, immediately EOSEs on Subscribe,
// and records call counts. Used to assert one conn per relay per tick.
type eoseRelay struct {
	connects   *int
	subscribes *int
	closes     *int
}

func (f *eoseRelay) Connect(context.Context) error {
	*f.connects++
	return nil
}

func (f *eoseRelay) Subscribe(context.Context, nostr.Filters, ...nostr.SubscriptionOption) (*nostr.Subscription, error) {
	*f.subscribes++
	eose := make(chan struct{})
	close(eose)
	return &nostr.Subscription{
		Events:            make(chan *nostr.Event),
		EndOfStoredEvents: eose,
		ClosedReason:      make(chan string),
	}, nil
}

func (f *eoseRelay) Close() error {
	*f.closes++
	return nil
}

func TestTickOneConnPerRelayWhenFirstServesAll(t *testing.T) {
	p := testPriority(t, []string{"pk1", "pk2"}, []string{"wss://relay-a.example", "wss://relay-b.example"})
	var connects, subscribes, closes int
	p.dial = func(_ context.Context, url string) relayConn {
		if url != p.relays[0] {
			t.Errorf("dialed %s; first relay should serve all pubkeys", url)
		}
		return &eoseRelay{connects: &connects, subscribes: &subscribes, closes: &closes}
	}
	p.tick(context.Background())
	if connects != 1 {
		t.Errorf("connects = %d, want 1 (one conn per relay per tick)", connects)
	}
	if subscribes != 2 {
		t.Errorf("subscribes = %d, want 2 (one sub per pubkey on the shared conn)", subscribes)
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (conn closed before moving on)", closes)
	}
	ctx := context.Background()
	for _, pk := range []string{"pk1", "pk2"} {
		fetched, err := p.ctrl.LastFetched(ctx, pk)
		if err != nil || fetched == 0 {
			t.Errorf("pubkey %s last_fetched=%d err=%v, want marked", pk, fetched, err)
		}
		swept, err := p.ctrl.LastSwept(ctx, pk)
		if err != nil || swept == 0 {
			t.Errorf("pubkey %s last_swept=%d err=%v, want marked (full sweep)", pk, swept, err)
		}
	}
}
