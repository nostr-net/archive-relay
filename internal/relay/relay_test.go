package relay

import (
	"context"
	"testing"

	"github.com/nbd-wtf/go-nostr"

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

	// find a RejectFilter hook that rejects an over-limit filter; the breadth
	// hook is the one that cares about kinds.
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
