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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.Allow(ClientIP(r)) {
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
			if parsed := net.ParseIP(ip); parsed != nil && parsed.IsGlobalUnicast() && !isPrivate(ip) {
				return ip
			}
		}
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

func isPrivate(ip string) bool {
	p := net.ParseIP(ip)
	if p == nil {
		return false
	}
	return p.IsPrivate() || p.IsLoopback() || p.IsLinkLocalUnicast()
}
