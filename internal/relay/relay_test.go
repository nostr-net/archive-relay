package relay

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/policy"
)

func TestAdvertisedNIPsAccurate(t *testing.T) {
	rl := New(Deps{Breadth: policy.RejectFilterBreadth{MaxKinds: 20}})
	got := map[int]bool{}
	for _, n := range rl.Info.SupportedNIPs {
		got[n.(int)] = true
	}
	for _, want := range []int{1, 9, 11, 12, 15, 45} {
		if !got[want] {
			t.Errorf("NIP %d should be advertised", want)
		}
	}
	// search (50) and hungry (77) are NOT implemented and must not be advertised
	if got[50] {
		t.Error("NIP-50 (search) must not be advertised — it is not implemented")
	}
	if got[77] {
		t.Error("NIP-77 (hungry) must not be advertised — it is not implemented")
	}
}

func TestScopeGateIsWiredAndRejectsDMs(t *testing.T) {
	rl := New(Deps{Breadth: policy.RejectFilterBreadth{}})
	if len(rl.RejectEvent) == 0 {
		t.Fatal("RejectEvent hooks should be wired")
	}
	var rejected bool
	for _, fn := range rl.RejectEvent {
		if r, _ := fn(context.Background(), &nostr.Event{Kind: 4}); r {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Error("a kind-4 (DM) event should be rejected by the wired scope gate")
	}
}

func TestBreadthHookWired(t *testing.T) {
	breadth := policy.RejectFilterBreadth{MaxKinds: 2}
	rl := New(Deps{Breadth: breadth})

	overLimit := nostr.Filter{Kinds: []int{1, 2, 3}}
	var found bool
	for _, fn := range rl.RejectFilter {
		if r, _ := fn(context.Background(), overLimit); r {
			found = true
			break
		}
	}
	if !found {
		t.Error("no RejectFilter hook rejected an over-limit kinds filter; breadth hook not wired")
	}
}

// --- auth + NIP-86 ---

func newEnabledAccess(t *testing.T) *policy.Access {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := control.Open(filepath.Join(t.TempDir(), "c.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a, err := policy.NewAccess(true, []string{"allowed-pk"}, []string{"admin-pk"}, db, log)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAuthHooksWiredWhenEnabled(t *testing.T) {
	a := newEnabledAccess(t)
	rl := New(Deps{Access: a, ServiceURL: "wss://relay.example.com"})

	// NIP-42 advertised and ServiceURL propagated (khatru needs it for validation)
	if rl.ServiceURL != "wss://relay.example.com" {
		t.Errorf("ServiceURL = %q, want wss://relay.example.com", rl.ServiceURL)
	}
	has42 := false
	for _, n := range rl.Info.SupportedNIPs {
		if n.(int) == 42 {
			has42 = true
		}
	}
	if !has42 {
		t.Error("NIP-42 should be advertised when ServiceURL is set")
	}

	// NIP-86 management handlers wired
	if rl.ManagementAPI.AllowPubKey == nil || rl.ManagementAPI.ListAllowedPubKeys == nil ||
		rl.ManagementAPI.BanPubKey == nil || len(rl.ManagementAPI.RejectAPICall) == 0 {
		t.Error("NIP-86 management API + admin gate should be wired when Access is set")
	}
}

func TestAuthHooksAbsentWhenAccessNil(t *testing.T) {
	rl := New(Deps{Breadth: policy.RejectFilterBreadth{}})
	if rl.ManagementAPI.AllowPubKey != nil {
		t.Error("ManagementAPI should not be wired when Access is nil")
	}
	has42 := false
	for _, n := range rl.Info.SupportedNIPs {
		if n.(int) == 42 {
			has42 = true
		}
	}
	if has42 {
		t.Error("NIP-42 should not be advertised without ServiceURL")
	}
}

func TestManagementAPIAllowEnrolls(t *testing.T) {
	a := newEnabledAccess(t)
	rl := New(Deps{Access: a, ServiceURL: "wss://relay.example.com"})

	if err := rl.ManagementAPI.AllowPubKey(context.Background(), "new-customer", "paid"); err != nil {
		t.Fatal(err)
	}
	if !a.Allowed("new-customer") {
		t.Error("AllowPubKey via the NIP-86 handler should enroll the pubkey")
	}
	if !a.Allowed("allowed-pk") {
		t.Error("static config key should still be allowed")
	}
}
