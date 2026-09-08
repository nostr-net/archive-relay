package policy

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"
)

// Limiter is a per-IP fixed-window rate limiter shared by the EVENT-publish
// hook and the REST API middleware. Essential for a public relay — without it
// a single hostile IP can fill the batcher/disk with EVENT spam (a scanner was
// already hitting :3334 during the test run).
//
// PerMinute <= 0 disables the limiter. A lazy GC evicts idle IP entries so the
// map doesn't grow unbounded under a spoofed-source flood.
type Limiter struct {
	PerMinute int

	mu     sync.Mutex
	counts map[string]*rlWindow
	lastGC time.Time
}

type rlWindow struct {
	start time.Time
	count int
}

// NewLimiter constructs a Limiter allowing PerMinute requests per IP.
func NewLimiter(perMinute int) *Limiter {
	return &Limiter{PerMinute: perMinute, counts: map[string]*rlWindow{}}
}

// Allow reports whether ip may proceed. Safe for concurrent use.
func (l *Limiter) Allow(ip string) bool {
	if l.PerMinute <= 0 || ip == "" {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.counts[ip]
	if w == nil || now.Sub(w.start) >= time.Minute {
		w = &rlWindow{start: now}
		l.counts[ip] = w
	}
	w.count++
	allowed := w.count <= l.PerMinute
	// lazy GC: sweep expired entries once per minute to bound memory
	if now.Sub(l.lastGC) >= time.Minute {
		for ip, w := range l.counts {
			if now.Sub(w.start) >= time.Minute {
				delete(l.counts, ip)
			}
		}
		l.lastGC = now
	}
	return allowed
}

// RejectEvent implements the khatru RejectEvent hook signature.
func (l *Limiter) RejectEvent(ctx context.Context, _ *nostr.Event) (bool, string) {
	if !l.Allow(khatru.GetIP(ctx)) {
		return true, "rate-limited: too many events from this IP"
	}
	return false, ""
}

// RejectFilter implements the khatru RejectFilter / RejectCountFilter hook
// signature, extending per-IP rate limiting to the read path so a hostile
// client cannot drive many expensive FINAL scans across the tiers.
func (l *Limiter) RejectFilter(ctx context.Context, _ nostr.Filter) (bool, string) {
	if !l.Allow(khatru.GetIP(ctx)) {
		return true, "rate-limited: too many reads from this IP"
	}
	return false, ""
}

// HTTP returns middleware that enforces the same per-IP limit on REST routes.
func (l *Limiter) HTTP(next http.Handler) http.Handler {
	return l.HTTPWhen(func(*http.Request) bool { return true }, next)
}

// HTTPWhen enforces the per-IP limit only on requests matching cond. Used to
// rate-limit the NIP-86 RPC endpoint, which khatru dispatches by Content-Type
// on any path — outside the /v1/* route-level wrapping.
func (l *Limiter) HTTPWhen(cond func(*http.Request) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cond(r) && !l.Allow(ClientIP(r)) {
			w.Header().Set("Retry-After", "60")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ClientIP extracts the caller's IP from an HTTP request, honoring
// X-Forwarded-For (the first public global-unicast hop) when behind a proxy.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, v := range strings.Split(xff, ",") {
			ip := strings.TrimSpace(v)
			// IsGlobalUnicast already excludes loopback/link-local; IsPrivate
			// additionally skips RFC1918/fc00 addresses.
			if p := net.ParseIP(ip); p != nil && p.IsGlobalUnicast() && !p.IsPrivate() {
				return ip
			}
		}
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}
