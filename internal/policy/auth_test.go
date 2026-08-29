package policy

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/nostr-net/archive-relay/internal/control"
)

func newAccess(t *testing.T, enabled bool, allow, admin []string) (*Access, *control.DB) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := control.Open(filepath.Join(t.TempDir(), "c.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a, err := NewAccess(enabled, allow, admin, db, log)
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

func TestAdminGateRejectsNonAdmin(t *testing.T) {
	// a plain ctx has no authed pubkey -> not an admin -> rejected
	a, _ := newAccess(t, true, nil, []string{"admin-pk"})
	if rej, msg := a.AdminGate(context.Background(), nil); !rej {
		t.Errorf("AdminGate should reject when no admin pubkey is authed, got %q", msg)
	}
}
