//go:build integration

// Package e2e is the end-to-end test: it starts the full relay (store +
// scheduler + WoT + stats + REST API) in-process and exercises it over a real
// websocket with a real go-nostr client. No mocks: real signed events, real
// ClickHouse, real SQLite, real HTTP.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/api"
	"github.com/nostr-net/archive-relay/internal/config"
	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/policy"
	"github.com/nostr-net/archive-relay/internal/relay"
	"github.com/nostr-net/archive-relay/internal/scheduler"
	"github.com/nostr-net/archive-relay/internal/stats"
	"github.com/nostr-net/archive-relay/internal/store"
)

var (
	chAddr = envOr("CH_ADDR", "localhost:9000")
	testDB = "test_archive_relay_e2e"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func resetDB(t *testing.T) {
	t.Helper()
	admin, _ := clickhouse.Open(&clickhouse.Options{Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: "default"}})
	ctx := context.Background()
	_ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close()
}

// harness is a fully wired relay + REST API running on a random port.
type harness struct {
	store  *store.Store
	stats  *stats.Service
	sched  *scheduler.Scheduler
	wsURL  string
	apiURL string
	srv    *http.Server
	cancel context.CancelFunc
}

func newHarness(t *testing.T, schedBuffer time.Duration, wotThreshold int64) *harness {
	t.Helper()
	resetDB(t)
	log := newLogger()
	cdb, err := control.Open(t.TempDir()+"/c.db", log)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		ClickHouse: config.ClickHouse{Addr: chAddr, Database: testDB, Username: "default"},
		Batch:      config.Batch{MaxSize: 50, MaxAge: 200 * time.Millisecond},
	}
	s := store.New(cfg, log)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	svc := stats.New(s.CH(), log)

	ctx, cancel := context.WithCancel(context.Background())
	wot := &policy.WoT{Lookup: svc.Followers, Threshold: wotThreshold, TTL: time.Second}
	var krl *khatru.Relay
	sched := scheduler.New(cdb.Conn(),
		func(ctx context.Context, evt *nostr.Event) error { _, err := krl.AddEvent(ctx, evt); return err },
		schedBuffer, log)
	krl = relay.New(relay.Deps{Store: s, Sched: sched, WoT: wot})
	go sched.Run(ctx)
	api.NewHandler(svc, s, nil, log).Register(krl.Router())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	srv := &http.Server{Handler: krl}
	go srv.Serve(ln)
	return &harness{
		store: s, stats: svc, sched: sched,
		wsURL: "ws://" + addr, apiURL: "http://" + addr,
		srv: srv, cancel: cancel,
	}
}

func (h *harness) close() {
	h.cancel()
	_ = h.srv.Close()
	h.store.Close()
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// sign builds and signs a real event.
func sign(t *testing.T, sk string, kind int, content string, tags nostr.Tags, createdAt nostr.Timestamp) *nostr.Event {
	t.Helper()
	pk, _ := nostr.GetPublicKey(sk)
	e := &nostr.Event{PubKey: pk, CreatedAt: createdAt, Kind: kind, Tags: tags, Content: content}
	e.ID = e.GetID()
	if err := e.Sign(sk); err != nil {
		t.Fatal(err)
	}
	return e
}

func connectClient(t *testing.T, url string) *nostr.Relay {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := nostr.NewRelay(context.Background(), url)
	if err := r.Connect(ctx); err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	return r
}

func publish(t *testing.T, r *nostr.Relay, evt *nostr.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Publish(ctx, *evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// queryOnce subscribes and collects until EOSE or timeout.
func queryOnce(t *testing.T, r *nostr.Relay, f nostr.Filter) []*nostr.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := r.Subscribe(ctx, nostr.Filters{f})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	var out []*nostr.Event
	for {
		select {
		case ev, ok := <-sub.Events:
			if ok {
				out = append(out, ev)
			}
		case <-sub.EndOfStoredEvents:
			return out
		case <-time.After(3 * time.Second):
			return out
		}
	}
}

func TestE2E_PublishQueryEngageDefer(t *testing.T) {
	h := newHarness(t, 1*time.Second, 0) // scheduler buffer 1s, WoT off
	defer h.close()

	client := connectClient(t, h.wsURL)
	defer client.Close()
	sk := nostr.GeneratePrivateKey()

	// 1) publish a kind-1 note; query it back over ws
	note := sign(t, sk, 1, "hello e2e", nostr.Tags{{"t", "test"}}, nostr.Now())
	publish(t, client, note)
	h.store.FlushAll()
	got := queryOnce(t, client, nostr.Filter{IDs: []string{note.ID}})
	if len(got) != 1 {
		t.Fatalf("expected to query back our note, got %d", len(got))
	}

	// 2) publish a reaction; engagement should show via the REST API after refresh
	reacter := nostr.GeneratePrivateKey()
	publish(t, client, sign(t, reacter, 7, "👍", nostr.Tags{{"e", note.ID}}, nostr.Now()))
	h.store.FlushAll()
	if err := h.stats.RefreshNoteMonthly(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng := apiGet(t, h.apiURL+"/v1/note/"+note.ID)
	if int(eng["reaction"].(float64)) != 1 {
		t.Errorf("engagement reaction = %v, want 1", eng["reaction"])
	}

	// 3) future-dated event is deferred: not queryable immediately, published when due
	future := sign(t, sk, 1, "from the future", nil, nostr.Timestamp(time.Now().Unix()+3))
	publish(t, client, future)
	h.store.FlushAll()
	if len(queryOnce(t, client, nostr.Filter{IDs: []string{future.ID}})) != 0 {
		t.Fatal("future-dated event should be deferred (not queryable yet)")
	}
	// wait past the due time + scheduler tick
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(queryOnce(t, client, nostr.Filter{IDs: []string{future.ID}})) > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.store.FlushAll()
	if len(queryOnce(t, client, nostr.Filter{IDs: []string{future.ID}})) == 0 {
		t.Fatal("future-dated event was never published after its due time")
	}
}

func TestE2E_WoTFilter(t *testing.T) {
	// threshold 1: only authors with >=1 follower pass the read filter
	h := newHarness(t, time.Minute, 1)
	defer h.close()

	client := connectClient(t, h.wsURL)
	defer client.Close()
	popular := nostr.GeneratePrivateKey()
	nobody := nostr.GeneratePrivateKey()
	fan := nostr.GeneratePrivateKey()
	nobodyPK, _ := nostr.GetPublicKey(nobody)

	// fan follows `popular` via kind-3
	popPK, _ := nostr.GetPublicKey(popular)
	publish(t, client, sign(t, fan, 3, "", nostr.Tags{{"p", popPK}}, nostr.Now()))
	h.store.FlushAll()
	if err := h.stats.RefreshFollowers(context.Background()); err != nil {
		t.Fatal(err)
	}

	// both publish a note
	popNote := sign(t, popular, 1, "popular post", nil, nostr.Now())
	nobodyNote := sign(t, nobody, 1, "nobody post", nil, nostr.Now())
	publish(t, client, popNote)
	publish(t, client, nobodyNote)
	h.store.FlushAll()

	// query all kind-1: only the popular author should come back (WoT threshold 1)
	got := queryOnce(t, client, nostr.Filter{Kinds: []int{1}, Limit: 100})
	seen := map[string]bool{}
	for _, ev := range got {
		seen[ev.PubKey] = true
	}
	if !seen[popPK] {
		t.Errorf("popular author's note was filtered out by WoT")
	}
	if seen[nobodyPK] {
		t.Errorf("nobody author's note should have been filtered out by WoT; got: %+v", seen)
	}
}

// --- helpers ---

func apiGet(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}
