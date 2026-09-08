package policy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
)

func newAccess(t *testing.T, enabled bool, allow, admin []string) (*Access, *control.DB) {
	return newAccessURL(t, enabled, allow, admin, "")
}

func newAccessURL(t *testing.T, enabled bool, allow, admin []string, serviceURL string) (*Access, *control.DB) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := control.Open(filepath.Join(t.TempDir(), "c.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a, err := NewAccess(enabled, allow, admin, serviceURL, db, log)
	if err != nil {
		t.Fatal(err)
	}
	return a, db
}

func TestNewAccessMergesStaticAndDB(t *testing.T) {
	a, db := newAccess(t, true, []string{"config-pk"}, nil)
	ctx := context.Background()

	if err := db.AllowPubkey(ctx, "db-pk", "customer"); err != nil {
		t.Fatal(err)
	}
	a.Refresh(ctx) // pick up the DB row

	if !a.Allowed("config-pk") {
		t.Error("static config key should be allowed")
	}
	if !a.Allowed("db-pk") {
		t.Error("dynamic DB key should be allowed")
	}
	if a.Allowed("random") {
		t.Error("unknown key should not be allowed")
	}
}

func TestAccessMutationsUpdateCacheAndDB(t *testing.T) {
	a, db := newAccess(t, true, nil, []string{"admin-pk"})
	ctx := context.Background()

	if !a.IsAdmin("admin-pk") {
		t.Error("admin key from config should be an admin")
	}
	if a.IsAdmin("nope") {
		t.Error("non-configured key should not be an admin")
	}

	// allow via the in-process mutator (the path NIP-86 uses) -> immediately allowed
	if err := a.AllowPubkey(ctx, "alice", "paid"); err != nil {
		t.Fatal(err)
	}
	if !a.Allowed("alice") {
		t.Error("AllowPubkey should make the key allowed immediately (cache)")
	}
	// ...and persisted to the DB
	set, _ := db.LoadAllowedSet(ctx)
	if _, ok := set["alice"]; !ok {
		t.Error("AllowPubkey should persist to the DB")
	}

	// revoke -> removed from both
	if err := a.RevokePubkey(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	if a.Allowed("alice") {
		t.Error("RevokePubkey should drop the key from the cache")
	}
	set, _ = db.LoadAllowedSet(ctx)
	if _, ok := set["alice"]; ok {
		t.Error("RevokePubkey should remove the DB row")
	}
}

func TestRevokeCannotRemoveStaticKey(t *testing.T) {
	// a static (config) key stays allowed even if someone revokes it from the DB
	a, _ := newAccess(t, true, []string{"config-pk"}, nil)
	ctx := context.Background()

	if err := a.RevokePubkey(ctx, "config-pk"); err != nil {
		t.Fatal(err)
	}
	if !a.Allowed("config-pk") {
		t.Error("a static config key must remain allowed after revoke (config wins)")
	}
}

func TestListAllowedPubkeysReturnsUnion(t *testing.T) {
	a, _ := newAccess(t, true, []string{"config-pk"}, nil)
	ctx := context.Background()
	if err := a.AllowPubkey(ctx, "db-pk", "customer"); err != nil {
		t.Fatal(err)
	}

	rows, err := a.ListAllowedPubkeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, r := range rows {
		seen[r.PubKey] = r.Reason
	}
	if _, ok := seen["config-pk"]; !ok {
		t.Errorf("static key missing from list: %+v", seen)
	}
	if seen["config-pk"] != "config" {
		t.Errorf("static key reason = %q, want config", seen["config-pk"])
	}
	if seen["db-pk"] != "customer" {
		t.Errorf("db key reason = %q, want customer", seen["db-pk"])
	}
}

func TestGateDisabledAllowsAll(t *testing.T) {
	a, _ := newAccess(t, false, nil, nil)
	if rej, reason := a.gate(context.Background()); rej {
		t.Errorf("disabled access should not reject, got %q", reason)
	}
}

func TestGateBypassesInternalCalls(t *testing.T) {
	// enabled access, but a context with no websocket connection (server-side
	// call, e.g. the scheduler publishing a due event) must be allowed through.
	a, _ := newAccess(t, true, []string{"someone"}, nil)
	if rej, reason := a.gate(context.Background()); rej {
		t.Errorf("internal call (no connection) should bypass auth, got %q", reason)
	}
}

// nip98Header builds a signed Authorization header for the given request.
func nip98Header(t *testing.T, sk, url, method string) string {
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

func TestHTTPAuth(t *testing.T) {
	sk := nostr.GeneratePrivateKey()
	pk, _ := nostr.GetPublicKey(sk)
	a, _ := newAccessURL(t, true, []string{pk}, nil, "http://x")

	ok := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ok = true; w.WriteHeader(200) })
	handler := a.HTTPAuth(next)

	newReq := func(hdr string) *httptest.ResponseRecorder {
		ok = false
		r := httptest.NewRequest("GET", "http://x/v1/events", nil)
		if hdr != "" {
			r.Header.Set("Authorization", hdr)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec
	}

	if rec := newReq(""); rec.Code != 401 || ok {
		t.Errorf("no auth header: code = %d, want 401", rec.Code)
	}
	if rec := newReq("Bearer xyz"); rec.Code != 401 || ok {
		t.Errorf("wrong scheme: code = %d, want 401", rec.Code)
	}
	// valid NIP-98 for a different path must not pass
	bad := nip98Header(t, sk, "http://x/v1/other", "GET")
	if rec := newReq(bad); rec.Code != 401 || ok {
		t.Errorf("wrong u path: code = %d, want 401", rec.Code)
	}
	// ...nor a NIP-98 minted for a different service (foreign u host)
	foreign := nip98Header(t, sk, "https://other-relay.example/v1/events", "GET")
	if rec := newReq(foreign); rec.Code != 401 || ok {
		t.Errorf("foreign u host: code = %d, want 401", rec.Code)
	}
	// a 401 must carry WWW-Authenticate so clients can negotiate the scheme
	if got := newReq("").Header().Get("WWW-Authenticate"); got != "Nostr" {
		t.Errorf("WWW-Authenticate = %q, want Nostr", got)
	}
	// valid NIP-98 from a non-allow-listed key
	stranger := nostr.GeneratePrivateKey()
	blocked := nip98Header(t, stranger, "http://x/v1/events", "GET")
	if rec := newReq(blocked); rec.Code != 403 || ok {
		t.Errorf("non-allowed key: code = %d, want 403", rec.Code)
	}
	// valid NIP-98 from an allowed key
	good := nip98Header(t, sk, "http://x/v1/events", "GET")
	if rec := newReq(good); rec.Code != 200 || !ok {
		t.Errorf("allowed key: code = %d, want 200", rec.Code)
	}
}

func TestHTTPAuthDisabledPassesThrough(t *testing.T) {
	a, _ := newAccess(t, false, nil, nil)
	ok := false
	rec := httptest.NewRecorder()
	a.HTTPAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ok = true })).ServeHTTP(
		rec, httptest.NewRequest("GET", "/v1/events", nil))
	if !ok {
		t.Error("disabled access should pass HTTP requests through")
	}
}

func TestAdminGateRejectsNonAdmin(t *testing.T) {
	// a plain ctx has no authed pubkey -> not an admin -> rejected
	a, _ := newAccess(t, true, nil, []string{"admin-pk"})
	if rej, msg := a.AdminGate(context.Background(), nil); !rej {
		t.Errorf("AdminGate should reject when no admin pubkey is authed, got %q", msg)
	}
}
