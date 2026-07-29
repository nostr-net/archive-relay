package policy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// --- scope gate ---

func TestRejectOutOfScope(t *testing.T) {
	inScope := []int{0, 1, 3, 6, 7, 16, 9735, 10002}
	for _, k := range inScope {
		if rej, _ := RejectOutOfScope(context.Background(), &nostr.Event{Kind: k}); rej {
			t.Errorf("kind %d should be in scope (not rejected)", k)
		}
	}
	for _, k := range []int{4, 5, 1059, 21000, 30023, 99999} {
		if rej, _ := RejectOutOfScope(context.Background(), &nostr.Event{Kind: k}); !rej {
			t.Errorf("kind %d should be rejected as out of scope", k)
		}
	}
}

// --- filter breadth caps ---

func TestRejectFilterBreadthAtBoundary(t *testing.T) {
	r := RejectFilterBreadth{MaxIDs: 2, MaxAuthors: 2, MaxKinds: 2, MaxTags: 2}

	// exactly the limit is allowed
	if rej, _ := r.Reject(context.Background(), nostr.Filter{IDs: []string{"a", "b"}}); rej {
		t.Error("IDs at the limit should pass")
	}
	// one over the limit is rejected
	if rej, _ := r.Reject(context.Background(), nostr.Filter{IDs: []string{"a", "b", "c"}}); !rej {
		t.Error("IDs over the limit should be rejected")
	}
	if rej, _ := r.Reject(context.Background(), nostr.Filter{Authors: []string{"a", "b", "c"}}); !rej {
		t.Error("authors over the limit should be rejected")
	}
	if rej, _ := r.Reject(context.Background(), nostr.Filter{Kinds: []int{1, 2, 3}}); !rej {
		t.Error("kinds over the limit should be rejected")
	}
	// total tag values across all keys are summed
	f := nostr.Filter{Tags: nostr.TagMap{"e": {"a", "b"}, "p": {"c"}}}
	if rej, _ := r.Reject(context.Background(), f); !rej {
		t.Error("tag values over the limit should be rejected")
	}
}

func TestRejectFilterBreadthZeroDisables(t *testing.T) {
	r := RejectFilterBreadth{} // all zero => no limits
	if rej, _ := r.Reject(context.Background(), nostr.Filter{
		IDs: make([]string, 10_000), Authors: make([]string, 10_000),
	}); rej {
		t.Error("zero-valued breadth policy should not reject")
	}
}

// --- per-IP limiter ---

func TestLimiterPerIPWindow(t *testing.T) {
	l := NewLimiter(2)
	if !l.Allow("1.1.1.1") || !l.Allow("1.1.1.1") {
		t.Fatal("first two requests from the same IP should be allowed")
	}
	if l.Allow("1.1.1.1") {
		t.Error("third request from the same IP should be rejected (over the limit)")
	}
	// a different IP has its own independent window
	if !l.Allow("2.2.2.2") {
		t.Error("a different IP should not be affected by another IP's limit")
	}
}

func TestLimiterDisabledWhenZero(t *testing.T) {
	l := NewLimiter(0)
	for i := 0; i < 1000; i++ {
		if !l.Allow("1.1.1.1") {
			t.Fatal("a zero per-minute limiter should always allow")
		}
	}
}

func TestLimiterHTTPMiddlewareEnforces(t *testing.T) {
	l := NewLimiter(1)
	hits := atomic.Int32{}
	h := l.HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
	}))

	req := httptest.NewRequest("GET", "/x", nil)
	req.RemoteAddr = "1.1.1.1:1234"

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("first request: code=%d, want 200", rec.Code)
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: code=%d, want 429", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Error("429 response should carry a Retry-After header")
	}
	if hits.Load() != 1 {
		t.Errorf("downstream handler hit %d times, want 1 (second was rate-limited)", hits.Load())
	}
}

// --- client IP extraction (XFF + RemoteAddr) ---

func TestClientIP(t *testing.T) {
	// bare RemoteAddr, no proxy header
	r := httptest.NewRequest("GET", "/x", nil)
	r.RemoteAddr = "9.9.9.9:4321"
	if got := ClientIP(r); got != "9.9.9.9" {
		t.Errorf("ClientIP without XFF = %q, want 9.9.9.9", got)
	}
	// XFF: first public (non-private) hop wins, so private 10.x is skipped
	r2 := httptest.NewRequest("GET", "/x", nil)
	r2.Header.Set("X-Forwarded-For", "10.0.0.1, 8.8.8.8")
	if got := ClientIP(r2); got != "8.8.8.8" {
		t.Errorf("ClientIP with XFF = %q, want 8.8.8.8", got)
	}
	// XFF with only private hops falls back to RemoteAddr
	r3 := httptest.NewRequest("GET", "/x", nil)
	r3.RemoteAddr = "7.7.7.7:1"
	r3.Header.Set("X-Forwarded-For", "192.168.1.5")
	if got := ClientIP(r3); got != "7.7.7.7" {
		t.Errorf("ClientIP with only-private XFF = %q, want 7.7.7.7", got)
	}
}

// --- web-of-trust read-time filter ---

func TestWoTThreshold(t *testing.T) {
	lookup := func(_ context.Context, pk string) (int64, error) {
		if pk == "popular" {
			return 5, nil
		}
		return 0, nil
	}

	t.Run("filters below threshold", func(t *testing.T) {
		w := &WoT{Lookup: lookup, Threshold: 1, TTL: time.Minute}
		if !w.Allows(context.Background(), "popular") {
			t.Error("author at/above threshold should be allowed")
		}
		if w.Allows(context.Background(), "nobody") {
			t.Error("author below threshold should be filtered out")
		}
	})

	t.Run("threshold 0 disables", func(t *testing.T) {
		w := &WoT{Lookup: lookup, Threshold: 0}
		if !w.Allows(context.Background(), "nobody") {
			t.Error("Threshold 0 must disable the filter entirely")
		}
	})

	t.Run("caches within TTL", func(t *testing.T) {
		calls := atomic.Int64{}
		cachingLookup := func(ctx context.Context, pk string) (int64, error) {
			calls.Add(1)
			return lookup(ctx, pk)
		}
		w := &WoT{Lookup: cachingLookup, Threshold: 1, TTL: time.Minute}
		_ = w.Allows(context.Background(), "popular")
		_ = w.Allows(context.Background(), "popular")
		_ = w.Allows(context.Background(), "popular")
		if got := calls.Load(); got != 1 {
			t.Errorf("lookup called %d times, want 1 (cached within TTL)", got)
		}
	})
}
