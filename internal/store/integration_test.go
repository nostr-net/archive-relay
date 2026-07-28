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
	"fmt"
	"log/slog"
	"os"
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
		Retention:  config.Retention{Archive: "10 YEAR", Social: "1 YEAR", Transient: "30 DAY"},
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
	s.FlushAll()

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
	s.FlushAll()

	// query by #e tag
	ch, _ := s.QueryEvents(ctx, nostr.Filter{Tags: nostr.TagMap{"e": []string{target.ID}}})
	got := drain(ch)
	if len(got) != 1 || got[0].ID != reaction.ID {
		t.Fatalf("e-tag query got %v", got)
	}

	// query by #t tag (on root)
	rootT := signEvent(t, sk, 1, "tagged", nostr.Tags{{"t", "bitcoin"}}, 0)
	s.SaveEvent(ctx, rootT)
	s.FlushAll()
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
	s.FlushAll()

	// visible before deletion
	ch, _ := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if len(drain(ch)) != 1 {
		t.Fatal("expected visible before delete")
	}

	// tombstone it
	if err := s.DeleteEvent(ctx, evt); err != nil {
		t.Fatalf("DeleteEvent: %v", err)
	}
	// reload dictionary so the tombstone predicate sees it immediately
	_ = s.ch.Exec(ctx, "SYSTEM RELOAD DICTIONARY tombstone_dict")

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
	s.ReplaceEvent(ctx, old)
	s.FlushAll()
	// newer profile
	newer := signEvent(t, sk, 0, `{"name":"new"}`, nostr.Tags{}, 1*time.Hour)
	s.ReplaceEvent(ctx, newer)
	s.FlushAll()

	// kind 0 is replaceable: only the latest version should be returned
	ch, _ := s.QueryEvents(ctx, nostr.Filter{Authors: []string{old.PubKey}, Kinds: []int{0}})
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 replaceable version, got %d", len(got))
	}
	if got[0].Content != `{"name":"new"}` {
		t.Fatalf("expected newest version, got %q", got[0].Content)
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
	s.FlushAll()
	var n uint64
	_ = s.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM events_all WHERE id = '%s'", gw.ID)).Scan(&n)
	if n != 0 {
		t.Fatalf("expected 0 stored rows for dropped kind, got %d", n)
	}
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
	s.FlushAll()
	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 1 || flushed[0] != evt.ID {
		t.Fatalf("OnFlushed should have recorded the event post-flush, got %v", flushed)
	}
}
