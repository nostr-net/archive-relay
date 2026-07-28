// Package api exposes REST endpoints over the stats snapshot tables and the
// live store: engagement, daily rollups, DAU, follower counts, and basic event
// queries. Mounted on the khatru relay's HTTP mux.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/policy"
	"github.com/nostr-net/archive-relay/internal/stats"
	"github.com/nostr-net/archive-relay/internal/store"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	stats   *stats.Service
	store   *store.Store
	limiter *policy.Limiter // optional per-IP REST rate limit
	log     *slog.Logger
}

// NewHandler constructs the API handler. limiter may be nil.
func NewHandler(s *stats.Service, st *store.Store, limiter *policy.Limiter, log *slog.Logger) *Handler {
	return &Handler{stats: s, store: st, limiter: limiter, log: log}
}

// Register mounts the API routes on the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	register := func(pattern string, fn http.HandlerFunc) {
		var handler http.Handler = http.HandlerFunc(fn)
		if h.limiter != nil {
			handler = h.limiter.HTTP(handler)
		}
		mux.Handle(pattern, handler)
	}
	register("/v1/health", h.health)
	register("/v1/stats/daily", h.daily)
	register("/v1/stats/dau", h.dau)
	register("/v1/note/", h.noteEngagement)
	register("/v1/pubkey/", h.followers)
	register("/v1/events", h.events)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	chOK := h.store.CH().Ping(ctx) == nil
	var events uint64
	if chOK {
		_ = h.store.CH().QueryRow(ctx, "SELECT count() FROM events_all").Scan(&events)
	}
	code := http.StatusOK
	if !chOK {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"ok": chOK, "clickhouse": chOK, "events": events})
}

func (h *Handler) daily(w http.ResponseWriter, r *http.Request) {
	days := queryInt(r, "days", 30)
	rows, err := h.stats.Daily(r.Context(), days)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (h *Handler) dau(w http.ResponseWriter, r *http.Request) {
	days := queryInt(r, "days", 30)
	rows, err := h.stats.DAU(r.Context(), days)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (h *Handler) noteEngagement(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v1/note/"):]
	if id == "" {
		http.Error(w, "missing note id", http.StatusBadRequest)
		return
	}
	eng, err := h.stats.Engagement(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, eng)
}

func (h *Handler) followers(w http.ResponseWriter, r *http.Request) {
	pk := r.URL.Path[len("/v1/pubkey/"):]
	if pk == "" {
		http.Error(w, "missing pubkey", http.StatusBadRequest)
		return
	}
	n, err := h.stats.Followers(r.Context(), pk)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pubkey": pk, "followers": n})
}

// events is a minimal event query endpoint (clients usually use the ws relay,
// but a REST mirror is handy).
func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := nostr.Filter{
		Kinds:   parseInts(q["kind"]),
		Authors: q["author"],
		IDs:     q["id"],
		Limit:   queryInt(r, "limit", 100),
	}
	ch, err := h.store.QueryEvents(r.Context(), f)
	if err != nil {
		writeErr(w, err)
		return
	}
	var out []*nostr.Event
	for ev := range ch {
		out = append(out, ev)
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func queryInt(r *http.Request, key string, def int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return def
	}
	return n
}

func parseInts(ss []string) []int {
	var out []int
	for _, s := range ss {
		if n, err := strconv.Atoi(s); err == nil {
			out = append(out, n)
		}
	}
	return out
}
