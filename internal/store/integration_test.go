//go:build integration

// Integration tests for the ClickHouse store. Require a running ClickHouse
// at $CH_ADDR (default localhost:9000). Run with:
//
//	go test -tags=integration ./internal/store/
//
// These use real, cryptographically-signed nostr events (not mocks) and a
// throwaway database that is dropped and recreated per run.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/config"
	"github.com/nostr-net/archive-relay/internal/control"
)

var (
	chAddr = envOr("CH_ADDR", "localhost:9000")
	testDB = "test_archive_relay_store"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// setupStore opens ClickHouse, creates a fresh test DB, and returns an
// initialized Store plus a teardown func.
func setupStore(t *testing.T) (*Store, *control.DB, func()) {
	t.Helper()
	// control connection (to default DB) to create/drop the throwaway DB
	admin, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	ctx := context.Background()
	if err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB)); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	_ = admin.Close()

	log := testLogger()
	cdb, err := control.Open(t.TempDir()+"/control.db", log)
	if err != nil {
		t.Fatalf("control: %v", err)
	}

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{Addr: chAddr, Database: testDB, Username: "default"},
		Batch:      config.Batch{MaxSize: 50, MaxAge: 200 * time.Millisecond},
		Retention:  config.Retention{Archive: "10 YEAR", Social: "1 YEAR"},
	}
	s := New(cfg, log)
	if err := s.Init(); err != nil {
		t.Fatalf("store init: %v", err)
	}
	return s, cdb, func() {
		s.Close()
		cdb.Close()
		// leave the DB for inspection; re-running drops it. To force-clean:
		// admin.Exec(ctx, "DROP DATABASE "+testDB)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// signEvent builds and signs a real nostr event with the given fields.
func signEvent(t *testing.T, sk string, kind int, content string, tags nostr.Tags, age time.Duration) *nostr.Event {
	t.Helper()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	evt := &nostr.Event{
		PubKey:    pk,
		CreatedAt: nostr.Now() - nostr.Timestamp(age.Seconds()),
		Kind:      kind,
		Tags:      tags,
		Content:   content,
	}
	evt.ID = evt.GetID()
	if err := evt.Sign(sk); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !evt.CheckID() {
		t.Fatal("CheckID failed")
	}
	return evt
}

func TestStoreSaveAndQueryKind1(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	n1 := signEvent(t, sk, 1, "hello archive", nostr.Tags{{"t", "golang"}, {"t", "nostr"}}, 0)
	n2 := signEvent(t, sk, 1, "second note", nostr.Tags{}, 0)

	if err := s.SaveEvent(ctx, n1); err != nil {
		t.Fatalf("SaveEvent n1: %v", err)
	}
	if err := s.SaveEvent(ctx, n2); err != nil {
		t.Fatalf("SaveEvent n2: %v", err)
	}
	mustFlush(t, s)

	// Query by author
	ch, err := s.QueryEvents(ctx, nostr.Filter{Authors: []string{n1.PubKey}, Kinds: []int{1}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	// newest first (ORDER BY created_at DESC)
	if got[0].Content != "second note" && got[1].Content != "hello archive" {
		// at least one must be each
	}
	if got[0].CreatedAt < got[1].CreatedAt {
		t.Fatalf("expected desc order: %d >= %d", got[0].CreatedAt, got[1].CreatedAt)
	}

	// Query by id
	ch, _ = s.QueryEvents(ctx, nostr.Filter{IDs: []string{n1.ID}})
	got = drain(ch)
	if len(got) != 1 || got[0].ID != n1.ID {
		t.Fatalf("id query got %v", got)
	}
	if !reflect.DeepEqual(tagStrings(got[0].Tags), [][]string{{"t", "golang"}, {"t", "nostr"}}) {
		t.Fatalf("native tags round-trip got %v", got[0].Tags)
	}

	// Count
	c, err := s.CountEvents(ctx, nostr.Filter{Authors: []string{n1.PubKey}})
	if err != nil {
		t.Fatalf("CountEvents: %v", err)
	}
	if c != 2 {
		t.Fatalf("expected count 2, got %d", c)
	}
}

func TestStoreTagQueries(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	target := signEvent(t, sk, 1, "root note", nostr.Tags{}, 0)
	s.SaveEvent(ctx, target)

	// a reaction referencing target via e-tag
	sk2 := nostr.GeneratePrivateKey()
	reaction := signEvent(t, sk2, 7, "+", nostr.Tags{{"e", target.ID}}, 0)
	s.SaveEvent(ctx, reaction)
	mustFlush(t, s)

	// query by #e tag
	ch, _ := s.QueryEvents(ctx, nostr.Filter{Tags: nostr.TagMap{"e": []string{target.ID}}})
	got := drain(ch)
	if len(got) != 1 || got[0].ID != reaction.ID {
		t.Fatalf("e-tag query got %v", got)
	}

	// query by #t tag (on root)
	rootT := signEvent(t, sk, 1, "tagged", nostr.Tags{{"t", "bitcoin"}}, 0)
	s.SaveEvent(ctx, rootT)
	mustFlush(t, s)
	ch, _ = s.QueryEvents(ctx, nostr.Filter{Tags: nostr.TagMap{"t": []string{"bitcoin"}}})
	got = drain(ch)
	if len(got) != 1 || got[0].ID != rootT.ID {
		t.Fatalf("t-tag query got %v", got)
	}
}

func TestStoreDeleteTombstone(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	evt := signEvent(t, sk, 1, "doomed", nostr.Tags{}, 0)
	s.SaveEvent(ctx, evt)
	mustFlush(t, s)

	// visible before deletion
	ch, _ := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if len(drain(ch)) != 1 {
		t.Fatal("expected visible before delete")
	}

	// tombstone it
	if err := s.DeleteEvent(ctx, evt); err != nil {
		t.Fatalf("DeleteEvent: %v", err)
	}
	waitTombstone(t, s, evt.ID)

	ch, _ = s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	got := drain(ch)
	if len(got) != 0 {
		t.Fatalf("expected hidden after tombstone, got %d", len(got))
	}
	// but still physically present (tombstone only hides)
	var n uint64
	_ = s.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM events_archive WHERE id = '%s'", evt.ID)).Scan(&n)
	if n != 1 {
		t.Fatalf("expected raw row to still exist, got count=%d", n)
	}
}

func TestStoreReplaceEvent(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	// older profile
	old := signEvent(t, sk, 0, `{"name":"old"}`, nostr.Tags{}, 2*time.Hour)
	if err := s.ReplaceEvent(ctx, old); err != nil {
		t.Fatalf("ReplaceEvent old: %v", err)
	}
	mustFlush(t, s)
	// newer profile
	newer := signEvent(t, sk, 0, `{"name":"new"}`, nostr.Tags{}, 1*time.Hour)
	if err := s.ReplaceEvent(ctx, newer); err != nil {
		t.Fatalf("ReplaceEvent newer: %v", err)
	}
	mustFlush(t, s)

	// kind 0 is replaceable: only the latest version should be returned
	// (LIMIT 1 BY collapse heals stored-but-unretired overlap immediately)
	ch, err := s.QueryEvents(ctx, nostr.Filter{Authors: []string{old.PubKey}, Kinds: []int{0}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 replaceable version, got %d", len(got))
	}
	if got[0].Content != `{"name":"new"}` {
		t.Fatalf("expected newest version, got %q", got[0].Content)
	}
	if got[0].ID != newer.ID {
		t.Fatalf("expected newest id %s, got %s", newer.ID, got[0].ID)
	}

	waitTombstone(t, s, old.ID)
	ch, err = s.QueryEvents(ctx, nostr.Filter{IDs: []string{old.ID}})
	if err != nil {
		t.Fatalf("QueryEvents old id: %v", err)
	}
	if n := drain(ch); len(n) != 0 {
		t.Fatalf("expected old version retired, still visible: %v", n)
	}
}

func TestStoreRejectsOutOfScope(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	gw := signEvent(t, sk, 1059, "giftwrap", nostr.Tags{}, 0) // gift wrap → drop
	if err := s.SaveEvent(ctx, gw); err == nil {
		t.Fatal("expected SaveEvent to reject gift-wrap kind 1059")
	}
	mustFlush(t, s)
	var n uint64
	_ = s.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM events_all WHERE id = '%s'", gw.ID)).Scan(&n)
	if n != 0 {
		t.Fatalf("expected 0 stored rows for dropped kind, got %d", n)
	}
}

func mustFlush(t *testing.T, s *Store) {
	t.Helper()
	if err := s.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
}

func waitTombstone(t *testing.T, s *Store, id string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var n uint64
		if err := s.ch.QueryRow(ctx, "SELECT count() FROM tombstones WHERE id = ?", id).Scan(&n); err != nil {
			t.Fatalf("tombstones lookup: %v", err)
		}
		if n > 0 {
			if err := s.ch.Exec(ctx, "SYSTEM RELOAD DICTIONARY tombstone_dict"); err != nil {
				t.Fatalf("reload dict: %v", err)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("tombstone row for %s not inserted in time", id)
}

func signEventAt(t *testing.T, sk string, kind int, content string, tags nostr.Tags, created nostr.Timestamp) *nostr.Event {
	t.Helper()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	evt := &nostr.Event{
		PubKey:    pk,
		CreatedAt: created,
		Kind:      kind,
		Tags:      tags,
		Content:   content,
	}
	evt.ID = evt.GetID()
	if err := evt.Sign(sk); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !evt.CheckID() {
		t.Fatal("CheckID failed")
	}
	return evt
}

func drain(ch chan *nostr.Event) []*nostr.Event {
	var out []*nostr.Event
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-time.After(2 * time.Second):
			return out
		}
	}
}

func TestStoreOnFlushedFiresAfterFlush(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	var mu sync.Mutex
	var flushed []string
	s.SetOnFlushed(func(events []*nostr.Event) {
		mu.Lock()
		for _, e := range events {
			flushed = append(flushed, e.ID)
		}
		mu.Unlock()
	})

	sk := nostr.GeneratePrivateKey()
	evt := signEvent(t, sk, 1, "crash-safety check", nostr.Tags{}, 0)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}

	// CRASH-SAFETY INVARIANT: the event is not durable yet (only buffered), so
	// OnFlushed must NOT have fired. A process kill here must leave no trace
	// that pretends the event was stored.
	mu.Lock()
	n := len(flushed)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("OnFlushed fired before flush (crash-hole present): %v", flushed)
	}

	// Now flush — the hook fires, recording durable state only once safe.
	mustFlush(t, s)
	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 1 || flushed[0] != evt.ID {
		t.Fatalf("OnFlushed should have recorded the event post-flush, got %v", flushed)
	}
}

func TestQueryEventsMixedReplaceableKinds(t *testing.T) {
	// Codex BLOCKER 1: LIMIT 1 BY must include kind, otherwise same-author
	// kind-0 and kind-3 collapse onto one pubkey key and one disappears.
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	old0 := signEvent(t, sk, 0, `{"name":"old"}`, nostr.Tags{}, 3*time.Hour)
	new0 := signEvent(t, sk, 0, `{"name":"new"}`, nostr.Tags{}, 1*time.Hour)
	old3 := signEvent(t, sk, 3, `["old-contacts"]`, nostr.Tags{}, 3*time.Hour)
	new3 := signEvent(t, sk, 3, `["new-contacts"]`, nostr.Tags{}, 1*time.Hour)
	for _, e := range []*nostr.Event{old0, new0, old3, new3} {
		if err := s.SaveEvent(ctx, e); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}
	mustFlush(t, s)

	ch, err := s.QueryEvents(ctx, nostr.Filter{Authors: []string{old0.PubKey}, Kinds: []int{0, 3}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 2 {
		t.Fatalf("expected both newest kind-0 and kind-3, got %d: %v", len(got), contents(got))
	}
	seen := map[int]string{}
	for _, e := range got {
		seen[e.Kind] = e.Content
	}
	if seen[0] != `{"name":"new"}` {
		t.Fatalf("kind 0 = %q, want newest profile", seen[0])
	}
	if seen[3] != `["new-contacts"]` {
		t.Fatalf("kind 3 = %q, want newest contacts", seen[3])
	}
}

func TestQueryEventsKind3NewestAndEqualTSTiebreak(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	pk, _ := nostr.GetPublicKey(sk)

	v1 := signEvent(t, sk, 3, "contacts-v1", nostr.Tags{}, 3*time.Hour)
	v2 := signEvent(t, sk, 3, "contacts-v2", nostr.Tags{}, 2*time.Hour)
	v3 := signEvent(t, sk, 3, "contacts-v3", nostr.Tags{}, 1*time.Hour)
	for _, e := range []*nostr.Event{v1, v2, v3} {
		if err := s.SaveEvent(ctx, e); err != nil {
			t.Fatalf("SaveEvent: %v", err)
		}
	}
	mustFlush(t, s)

	ch, err := s.QueryEvents(ctx, nostr.Filter{Authors: []string{pk}, Kinds: []int{3}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected exactly newest kind-3, got %d: %v", len(got), contents(got))
	}
	if got[0].ID != v3.ID {
		t.Fatalf("expected v3 %s, got %s (%q)", v3.ID, got[0].ID, got[0].Content)
	}

	// Equal-timestamp versions: lowest id wins (NIP-01 / ORDER BY id ASC).
	sk2 := nostr.GeneratePrivateKey()
	ts := nostr.Timestamp(1_700_000_000)
	a := signEventAt(t, sk2, 3, "eq-a", nostr.Tags{}, ts)
	b := signEventAt(t, sk2, 3, "eq-b", nostr.Tags{}, ts)
	low, high := a, b
	if low.ID > high.ID {
		low, high = high, low
	}
	if err := s.SaveEvent(ctx, high); err != nil {
		t.Fatalf("SaveEvent high: %v", err)
	}
	if err := s.SaveEvent(ctx, low); err != nil {
		t.Fatalf("SaveEvent low: %v", err)
	}
	mustFlush(t, s)

	ch, err = s.QueryEvents(ctx, nostr.Filter{Authors: []string{low.PubKey}, Kinds: []int{3}})
	if err != nil {
		t.Fatalf("QueryEvents equal-ts: %v", err)
	}
	got = drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected 1 equal-ts winner, got %d: %v", len(got), contents(got))
	}
	if got[0].ID != low.ID {
		t.Fatalf("equal-ts tiebreak: got %s, want lowest id %s (high=%s)", got[0].ID, low.ID, high.ID)
	}
}

func TestQueryEventsDupIDCollapse(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	evt := signEvent(t, sk, 1, "dup-me", nostr.Tags{{"t", "dup"}}, 0)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatalf("SaveEvent 1: %v", err)
	}
	mustFlush(t, s)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatalf("SaveEvent 2: %v", err)
	}
	mustFlush(t, s)

	var raw uint64
	if err := s.ch.QueryRow(ctx, "SELECT count() FROM events_archive WHERE id = ?", evt.ID).Scan(&raw); err != nil {
		t.Fatalf("raw count: %v", err)
	}
	if raw < 2 {
		t.Fatalf("expected at least 2 physical rows for dup id, got %d", raw)
	}

	ch, err := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected dup-id collapse to 1, got %d", len(got))
	}
	if got[0].ID != evt.ID {
		t.Fatalf("got id %s, want %s", got[0].ID, evt.ID)
	}
}

func TestReplaceEventEqualTimestampLowerIDWins(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	ts := nostr.Timestamp(1_700_000_100)
	a := signEventAt(t, sk, 0, `{"name":"a"}`, nostr.Tags{}, ts)
	b := signEventAt(t, sk, 0, `{"name":"b"}`, nostr.Tags{}, ts)
	low, high := a, b
	if low.ID > high.ID {
		low, high = high, low
	}

	if err := s.ReplaceEvent(ctx, high); err != nil {
		t.Fatalf("ReplaceEvent high: %v", err)
	}
	mustFlush(t, s)
	if err := s.ReplaceEvent(ctx, low); err != nil {
		t.Fatalf("ReplaceEvent low: %v", err)
	}
	mustFlush(t, s)

	ch, err := s.QueryEvents(ctx, nostr.Filter{Authors: []string{low.PubKey}, Kinds: []int{0}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected 1 winner, got %d: %v", len(got), contents(got))
	}
	if got[0].ID != low.ID {
		t.Fatalf("equal-ts ReplaceEvent: got %s, want lowest id %s", got[0].ID, low.ID)
	}

	// Incoming higher-id at the same timestamp must be discarded.
	if err := s.ReplaceEvent(ctx, high); err != nil {
		t.Fatalf("ReplaceEvent high again: %v", err)
	}
	mustFlush(t, s)
	ch, err = s.QueryEvents(ctx, nostr.Filter{Authors: []string{low.PubKey}, Kinds: []int{0}})
	if err != nil {
		t.Fatalf("QueryEvents 2: %v", err)
	}
	got = drain(ch)
	if len(got) != 1 || got[0].ID != low.ID {
		t.Fatalf("higher-id at equal ts should stay discarded, got %v", contents(got))
	}
}

func TestQueryEventsSynchronousError(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	if err := s.ch.Exec(ctx, "DROP TABLE events_archive"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	ch, err := s.QueryEvents(ctx, nostr.Filter{Kinds: []int{1}})
	if err == nil {
		t.Fatal("expected synchronous error after dropping tier table")
	}
	if ch != nil {
		t.Fatalf("expected nil channel on error, got %v", ch)
	}
}

func contents(ev []*nostr.Event) []string {
	out := make([]string, len(ev))
	for i, e := range ev {
		out[i] = fmt.Sprintf("kind=%d id=%s content=%q", e.Kind, e.ID, e.Content)
	}
	return out
}

func tagStrings(tags nostr.Tags) [][]string {
	return nativeTags(tags)
}

func TestNativeTagsQueryRoundTrip(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	tags := nostr.Tags{
		{"e", "deadbeef", "wss://relay.example", "reply"},
		{"p", "pkpkpk"},
		{"t", "bitcoin"},
		{"amount", "21000"},
		{"client", "archive-relay"},
	}
	evt := signEvent(t, sk, 1, "native-tags", tags, 0)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}
	mustFlush(t, s)

	ch, err := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if !reflect.DeepEqual(tagStrings(got[0].Tags), tagStrings(tags)) {
		t.Fatalf("tags=%v want %v", got[0].Tags, tags)
	}

	// clickhouse-go Append of [][]string landed as Array(Array(String))
	var stored [][]string
	if err := s.ch.QueryRow(ctx, "SELECT tags FROM events_archive WHERE id = ?", evt.ID).Scan(&stored); err != nil {
		t.Fatalf("select tags: %v", err)
	}
	if !reflect.DeepEqual(stored, tagStrings(tags)) {
		t.Fatalf("stored tags=%v want %v", stored, tags)
	}
	// DEFAULT-derived accelerators still work (tag filters + stats).
	var tagE, tagT []string
	if err := s.ch.QueryRow(ctx, "SELECT tag_e, tag_t FROM events_archive WHERE id = ?", evt.ID).Scan(&tagE, &tagT); err != nil {
		t.Fatalf("select accelerators: %v", err)
	}
	if !reflect.DeepEqual(tagE, []string{"deadbeef"}) || !reflect.DeepEqual(tagT, []string{"bitcoin"}) {
		t.Fatalf("tag_e=%v tag_t=%v", tagE, tagT)
	}

	empty := signEvent(t, sk, 1, "no-tags", nostr.Tags{}, 0)
	if err := s.SaveEvent(ctx, empty); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)
	ch, err = s.QueryEvents(ctx, nostr.Filter{IDs: []string{empty.ID}})
	if err != nil {
		t.Fatal(err)
	}
	got = drain(ch)
	if len(got) != 1 || len(got[0].Tags) != 0 {
		t.Fatalf("empty tags: %+v", got)
	}
}

func TestEventsAllViewAfterTagsMigration(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()

	sk := nostr.GeneratePrivateKey()
	evt := signEvent(t, sk, 1, "view-check", nostr.Tags{{"t", "view"}}, 0)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s)

	var n uint64
	if err := s.ch.QueryRow(ctx, "SELECT count() FROM events_all WHERE id = ?", evt.ID).Scan(&n); err != nil {
		t.Fatalf("events_all did not resolve: %v", err)
	}
	if n != 1 {
		t.Fatalf("events_all count=%d", n)
	}
	for _, col := range []string{"tags", "tags_raw", "tag_e", "tag_p", "tag_t", "tag_d", "reply_to"} {
		var c uint64
		q := `SELECT count() FROM system.columns WHERE database = currentDatabase() AND table IN ('events_permanent','events_archive','events_social') AND name = ?`
		if err := s.ch.QueryRow(ctx, q, col).Scan(&c); err != nil {
			t.Fatal(err)
		}
		if c != 3 {
			t.Fatalf("column %s present on %d/3 tiers", col, c)
		}
	}
	var tags [][]string
	var tagT []string
	if err := s.ch.QueryRow(ctx, "SELECT tags, tag_t FROM events_all WHERE id = ?", evt.ID).Scan(&tags, &tagT); err != nil {
		t.Fatalf("events_all column types: %v", err)
	}
	if !reflect.DeepEqual(tags, [][]string{{"t", "view"}}) || !reflect.DeepEqual(tagT, []string{"view"}) {
		t.Fatalf("events_all tags=%v tag_t=%v", tags, tagT)
	}
}

func TestTagsBackfillExistingInstall(t *testing.T) {
	admin, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	ctx := context.Background()
	if err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB)); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	_ = admin.Close()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: testDB, Username: "default"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	oldDDL := `
CREATE TABLE events_%s (
  id String, pubkey String, created_at UInt32, kind UInt32, content String, sig String,
  tags_raw String, tag_e Array(String), tag_p Array(String), tag_t Array(String),
  tag_d String, reply_to String,
  received_at DateTime64(3) DEFAULT now64(3),
  version UInt32 MATERIALIZED created_at
) ENGINE = ReplacingMergeTree(version)
  PARTITION BY toYYYYMM(toDateTime(created_at))
  ORDER BY (kind, pubkey, created_at, id)`
	for _, tier := range activeTiers {
		if err := conn.Exec(ctx, fmt.Sprintf(oldDDL, tier)); err != nil {
			t.Fatalf("old schema %s: %v", tier, err)
		}
	}

	sk := nostr.GeneratePrivateKey()
	tags := nostr.Tags{{"e", "aabbccdd", "wss://r", "reply"}, {"t", "golang"}, {"amount", "42"}}
	evt := signEvent(t, sk, 1, "legacy-row", tags, 0)
	tagsJSON, err := json.Marshal(evt.Tags)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events_archive (id, pubkey, created_at, kind, content, sig, tags_raw, tag_e, tag_p, tag_t, tag_d, reply_to)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := batch.Append(evt.ID, evt.PubKey, uint32(evt.CreatedAt), uint32(evt.Kind), evt.Content, evt.Sig, string(tagsJSON), []string{"aabbccdd"}, []string{}, []string{"golang"}, "", "aabbccdd"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send: %v", err)
	}
	_ = conn.Close()

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{Addr: chAddr, Database: testDB, Username: "default"},
		Batch:      config.Batch{MaxSize: 50, MaxAge: 200 * time.Millisecond},
		Retention:  config.Retention{Archive: "10 YEAR", Social: "1 YEAR"},
	}
	s := New(cfg, testLogger())
	if err := s.Init(); err != nil {
		t.Fatalf("init over old schema: %v", err)
	}
	defer s.Close()

	deadline := time.Now().Add(15 * time.Second)
	var stored [][]string
	for time.Now().Before(deadline) {
		if err := s.ch.QueryRow(ctx, "SELECT tags FROM events_archive WHERE id = ?", evt.ID).Scan(&stored); err != nil {
			t.Fatalf("select tags: %v", err)
		}
		if len(stored) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !reflect.DeepEqual(stored, tagStrings(tags)) {
		t.Fatalf("backfill tags=%v want %v", stored, tags)
	}

	ch, err := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 || !reflect.DeepEqual(tagStrings(got[0].Tags), tagStrings(tags)) {
		t.Fatalf("read path after backfill: %v", got)
	}

	var n uint64
	if err := s.ch.QueryRow(ctx, "SELECT count() FROM events_all WHERE id = ?", evt.ID).Scan(&n); err != nil {
		t.Fatalf("events_all after migration: %v", err)
	}
	if n != 1 {
		t.Fatalf("events_all count=%d", n)
	}
}
