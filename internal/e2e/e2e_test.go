//go:build integration

// Package e2e is the end-to-end test: it starts the full relay (store +
// scheduler + stats + REST API) in-process and exercises it over a real
// websocket with a real go-nostr client. No mocks: real signed events, real
// ClickHouse, real SQLite, real HTTP.
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
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
	access *policy.Access // nil when auth is off
	wsURL  string
	apiURL string
	srv    *http.Server
	cancel context.CancelFunc
}

func newHarness(t *testing.T, schedBuffer time.Duration) *harness {
	return buildHarness(t, schedBuffer, nil, nil)
}

// buildHarness wires the full relay. A non-nil allow/admin enables the NIP-42
// + allow-list gate (ServiceURL is set to the real ws URL so khatru's AUTH
// validation sees the same URL the client signs).
func buildHarness(t *testing.T, schedBuffer time.Duration, allow, admin []string) *harness {
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

	// listen first so the ServiceURL (part of the signed AUTH event) matches
	// what clients actually dial.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	var access *policy.Access
	if allow != nil || admin != nil {
		access, err = policy.NewAccess(true, allow, admin, "ws://"+addr, cdb, log)
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var krl *khatru.Relay
	sched := scheduler.New(cdb,
		func(ctx context.Context, evt *nostr.Event) error { _, err := krl.AddEvent(ctx, evt); return err },
		schedBuffer, log)
	krl = relay.New(relay.Deps{Store: s, Sched: sched, Access: access, ServiceURL: "ws://" + addr})
	go sched.Run(ctx)
	api.NewHandler(svc, s, nil, access, log).Register(krl.Router())

	srv := &http.Server{Handler: krl}
	go srv.Serve(ln)
	return &harness{
		store: s, stats: svc, sched: sched, access: access,
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
	h := newHarness(t, 1*time.Second) // scheduler buffer 1s
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

// TestE2E_Auth exercises the real auth paths end-to-end: NIP-42 challenge →
// AUTH → gated reads/writes over the websocket, NIP-86 management over the
// HTTP RPC (NIP-98 signed), and the NIP-98 gate on the REST API.
func TestE2E_Auth(t *testing.T) {
	allowedSK := nostr.GeneratePrivateKey()
	allowedPK, _ := nostr.GetPublicKey(allowedSK)
	adminSK := nostr.GeneratePrivateKey()
	adminPK, _ := nostr.GetPublicKey(adminSK)
	h := buildHarness(t, time.Second, []string{allowedPK}, []string{adminPK})
	defer h.close()

	// --- unauthed websocket client: REQ is refused with auth-required ---
	anon := connectClient(t, h.wsURL)
	defer anon.Close()
	if reason := subscribeClosedReason(t, anon); !strings.HasPrefix(reason, "auth-required:") {
		t.Fatalf("unauthed REQ: got %q, want auth-required:", reason)
	}
	evt := sign(t, allowedSK, 1, "must not land unauthenticated", nil, nostr.Now())
	if err := anon.Publish(context.Background(), *evt); err == nil ||
		!strings.Contains(err.Error(), "auth-required") {
		t.Fatalf("unauthed publish should fail with auth-required, got %v", err)
	}

	// --- NIP-42: authenticate as the allowed key, then read+write work ---
	authAs(t, anon, allowedSK)
	publish(t, anon, evt)
	h.store.FlushAll()
	if got := queryOnce(t, anon, nostr.Filter{IDs: []string{evt.ID}}); len(got) != 1 {
		t.Fatalf("authed allowed key should read back its event, got %d", len(got))
	}

	// --- authed but not allow-listed: restricted ---
	strangerSK := nostr.GeneratePrivateKey()
	stranger := connectClient(t, h.wsURL)
	defer stranger.Close()
	authAs(t, stranger, strangerSK)
	if reason := subscribeClosedReason(t, stranger); !strings.HasPrefix(reason, "restricted:") {
		t.Fatalf("non-allowlisted authed REQ: got %q, want restricted:", reason)
	}
	if err := stranger.Publish(context.Background(), *sign(t, strangerSK, 1, "nope", nil, nostr.Now())); err == nil ||
		!strings.Contains(err.Error(), "restricted") {
		t.Fatalf("non-allowlisted publish should fail with restricted, got %v", err)
	}

	// --- REST API is NIP-98-gated too (health stays open for probes) ---
	if code := apiStatus(t, h.apiURL+"/v1/health", ""); code != 200 {
		t.Errorf("/v1/health should stay public, got %d", code)
	}
	if code := apiStatus(t, h.apiURL+"/v1/events", ""); code != 401 {
		t.Errorf("unauthed /v1/events should be 401, got %d", code)
	}
	if code := apiStatus(t, h.apiURL+"/v1/events",
		nip98Auth(t, strangerSK, h.apiURL+"/v1/events", "GET")); code != 403 {
		t.Errorf("non-allowlisted /v1/events should be 403, got %d", code)
	}
	if code := apiStatus(t, h.apiURL+"/v1/events",
		nip98Auth(t, allowedSK, h.apiURL+"/v1/events", "GET")); code != 200 {
		t.Errorf("allow-listed /v1/events should be 200, got %d", code)
	}

	// --- NIP-86 management RPC: admin allowpubkey grants access ---
	newSK := nostr.GeneratePrivateKey()
	newPK, _ := nostr.GetPublicKey(newSK)

	// a non-admin caller is rejected
	resp := nip86Call(t, h, strangerSK, `{"method":"allowpubkey","params":["`+newPK+`","intruder"]}`)
	if !strings.Contains(resp.Error, "unauthorized") {
		t.Errorf("non-admin NIP-86 call should be unauthorized, got %q", resp.Error)
	}
	if h.access.Allowed(newPK) {
		t.Error("non-admin call must not mutate the allow-list")
	}
	// the admin caller is accepted and the key becomes usable
	resp = nip86Call(t, h, adminSK, `{"method":"allowpubkey","params":["`+newPK+`","paid"]}`)
	if resp.Error != "" {
		t.Fatalf("admin allowpubkey failed: %q", resp.Error)
	}
	if !h.access.Allowed(newPK) {
		t.Fatal("allowpubkey should enroll the key immediately")
	}

	newbie := connectClient(t, h.wsURL)
	defer newbie.Close()
	authAs(t, newbie, newSK)
	publish(t, newbie, sign(t, newSK, 1, "hello from a paying customer", nil, nostr.Now()))
	h.store.FlushAll()
	if got := queryOnce(t, newbie, nostr.Filter{Authors: []string{newPK}}); len(got) != 1 {
		t.Fatalf("NIP-86-enrolled key should read+write, got %d events", len(got))
	}
}

// subscribeClosedReason sends a REQ and returns the relay's CLOSED reason.
func subscribeClosedReason(t *testing.T, r *nostr.Relay) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := r.Subscribe(ctx, nostr.Filters{{Kinds: []int{1}, Limit: 1}})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case reason := <-sub.ClosedReason:
		return reason
	case <-time.After(5 * time.Second):
		t.Fatal("no CLOSED reason within 5s")
		return ""
	}
}

// authAs performs the NIP-42 handshake: provoke the relay's AUTH challenge
// (khatru sends it in reply to an auth-required rejection), then sign + send
// AUTH. Retries until the asynchronously-delivered challenge has landed.
func authAs(t *testing.T, r *nostr.Relay, sk string) {
	t.Helper()
	_ = subscribeClosedReason(t, r) // triggers "auth-required:" → AUTH challenge
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	for range 20 {
		if err = r.Auth(ctx, func(e *nostr.Event) error { return e.Sign(sk) }); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("NIP-42 auth failed: %v", err)
}

// nip98Auth builds a NIP-98 Authorization header for an HTTP request.
func nip98Auth(t *testing.T, sk, url, method string) string {
	t.Helper()
	pk, _ := nostr.GetPublicKey(sk)
	evt := &nostr.Event{
		PubKey: pk, Kind: nostr.KindHTTPAuth, CreatedAt: nostr.Now(),
		Tags: nostr.Tags{{"u", url}, {"method", method}},
	}
	evt.ID = evt.GetID()
	if err := evt.Sign(sk); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(evt)
	return "Nostr " + base64.StdEncoding.EncodeToString(raw)
}

// nip86Resp is the NIP-86 RPC envelope reply.
type nip86Resp struct {
	Result any    `json:"result"`
	Error  string `json:"error"`
}

// nip86Call posts a management RPC to the relay URL with a NIP-98 signed
// admin auth event (u tag = the relay base URL, payload = sha256 of the body,
// as khatru requires).
func nip86Call(t *testing.T, h *harness, sk, body string) nip86Resp {
	t.Helper()
	hash := sha256.Sum256([]byte(body))
	pk, _ := nostr.GetPublicKey(sk)
	auth := &nostr.Event{
		PubKey: pk, Kind: nostr.KindHTTPAuth, CreatedAt: nostr.Now(),
		Tags: nostr.Tags{
			{"u", h.wsURL}, // khatru normalizes ServiceURL (ws://) as the base URL
			{"method", "POST"},
			{"payload", hex.EncodeToString(hash[:])},
		},
	}
	auth.ID = auth.GetID()
	if err := auth.Sign(sk); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(auth)

	req, err := http.NewRequest("POST", h.apiURL+"/", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/nostr+json+rpc")
	req.Header.Set("Authorization", "Nostr "+base64.StdEncoding.EncodeToString(raw))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out nip86Resp
	_ = json.NewDecoder(res.Body).Decode(&out)
	return out
}

// apiStatus GETs a URL with an optional Authorization header and returns the
// HTTP status code.
func apiStatus(t *testing.T, url, auth string) int {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
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
