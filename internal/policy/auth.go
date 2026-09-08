package policy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip86"

	"github.com/nostr-net/archive-relay/internal/control"
)

// Access enforces NIP-42 AUTH + a pubkey allow-list on the relay, and backs the
// NIP-86 management RPC for the dynamic part of that list.
//
// The effective allow-set is the UNION of:
//   - a static config set (operator/friend keys — read-only via NIP-86), and
//   - the dynamic allowed_pubkeys SQLite table (paying customers, mutated via
//     NIP-86, later the freedompay webhook).
//
// The set is cached in memory for O(1) auth checks and rebuilt on a periodic
// Refresh (so external DB writes are picked up) and after every in-process
// mutation.
type Access struct {
	enabled     bool
	admin       map[string]struct{} // NIP-86 management callers
	static      map[string]struct{} // config allow-list (not in the DB)
	serviceHost string              // hostname of the configured serviceURL ("" = unbound)

	mu      sync.RWMutex
	allowed map[string]struct{} // effective union (static ∪ dynamic)

	ctrl *control.DB
	log  *slog.Logger
}

// NewAccess loads the static config sets and primes the union from the DB.
// serviceURL is the canonical relay URL; its hostname binds the NIP-98 `u` tag
// on HTTP requests to this relay (empty disables host binding).
func NewAccess(enabled bool, allow, admin []string, serviceURL string,
	ctrl *control.DB, log *slog.Logger) (*Access, error) {
	host := ""
	if u, err := url.Parse(serviceURL); err == nil {
		host = u.Hostname()
	}
	a := &Access{
		enabled:     enabled,
		admin:       toSet(admin),
		static:      toSet(allow),
		serviceHost: host,
		allowed:     make(map[string]struct{}),
		ctrl:        ctrl,
		log:         log,
	}
	dyn, err := ctrl.LoadAllowedSet(context.Background())
	if err != nil {
		return nil, err
	}
	a.rebuild(dyn)
	return a, nil
}

func (a *Access) rebuild(dynamic map[string]struct{}) {
	uni := make(map[string]struct{}, len(a.static)+len(dynamic))
	for k := range a.static {
		uni[k] = struct{}{}
	}
	for k := range dynamic {
		uni[k] = struct{}{}
	}
	a.mu.Lock()
	a.allowed = uni
	a.mu.Unlock()
}

// Refresh reloads the dynamic set from the DB and rebuilds the union. Call on a
// ticker (e.g. 30s) so external writes (a billing script, the freedompay
// webhook) are picked up without restart.
func (a *Access) Refresh(ctx context.Context) {
	dyn, err := a.ctrl.LoadAllowedSet(ctx)
	if err != nil {
		a.log.Warn("access refresh failed", "err", err)
		return
	}
	a.rebuild(dyn)
}

// Enabled reports whether the NIP-42 + allow-list gate is active.
func (a *Access) Enabled() bool { return a.enabled }

// IsAdmin reports whether pk may call NIP-86 management methods.
func (a *Access) IsAdmin(pk string) bool { _, ok := a.admin[pk]; return ok }

// Allowed reports whether pk is in the effective allow-set.
func (a *Access) Allowed(pk string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.allowed[pk]
	return ok
}

// gate is the core publish/read auth decision. Returns (reject, reason).
//   - auth off: allow everything.
//   - no websocket connection (server-side call, e.g. scheduler.AddEvent):
//     allow — never block the relay's own ingestion.
//   - no authed pubkey: reject "auth-required:" so khatru sends the AUTH
//     challenge and the client can authenticate + retry.
//   - authed but not allow-listed: reject "restricted:".
func (a *Access) gate(ctx context.Context) (bool, string) {
	if !a.enabled {
		return false, ""
	}
	if khatru.GetConnection(ctx) == nil {
		return false, "" // internal/server-side call
	}
	pk := khatru.GetAuthed(ctx)
	if pk == "" {
		return true, "auth-required: this relay requires authentication"
	}
	if !a.Allowed(pk) {
		return true, "restricted: your public key is not on the allow-list"
	}
	return false, ""
}

// RejectEvent adapts gate to the khatru RejectEvent hook signature.
func (a *Access) RejectEvent(ctx context.Context, _ *nostr.Event) (bool, string) {
	return a.gate(ctx)
}

// RejectFilter adapts gate to the khatru RejectFilter / RejectCountFilter hook
// signature (reads).
func (a *Access) RejectFilter(ctx context.Context, _ nostr.Filter) (bool, string) {
	return a.gate(ctx)
}

// httpAuthWindow is the allowed clock skew for a NIP-98 auth event.
const httpAuthWindow = 60 // seconds

// HTTPAuth gates an HTTP endpoint behind the same allow-list, using NIP-98:
// the client sends `Authorization: Nostr <base64 kind-27235 event>` whose `u`
// tag must point at the request path and whose pubkey must be allow-listed.
// When the Access is disabled it passes everything through.
func (a *Access) HTTPAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.enabled {
			next.ServeHTTP(w, r)
			return
		}
		pk, err := a.httpAuthPubkey(r)
		if err != nil {
			// WWW-Authenticate lets clients auto-negotiate the Nostr scheme.
			w.Header().Set("WWW-Authenticate", "Nostr")
			http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
			return
		}
		if !a.Allowed(pk) {
			http.Error(w, "forbidden: pubkey is not on the allow-list", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// httpAuthPubkey extracts and validates the NIP-98 auth event from the request,
// returning the signing pubkey. Checks: Authorization scheme, kind 27235,
// `u` tag path matches the request path, `u` tag host matches the configured
// serviceURL host when both are known (the relay may sit behind a TLS proxy, so
// an unconfigured host skips the check rather than false-rejecting), `method`
// tag matches when present, created_at within httpAuthWindow, and a valid
// signature. (No `payload` check: the REST API is GET-only, so there's no body
// to bind.)
func (a *Access) httpAuthPubkey(r *http.Request) (string, error) {
	b64, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Nostr ")
	if !ok {
		return "", errors.New("missing 'Authorization: Nostr <base64 event>'")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return "", errors.New("invalid base64 auth event")
	}
	var evt nostr.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		return "", errors.New("invalid auth event JSON")
	}
	if evt.Kind != nostr.KindHTTPAuth {
		return "", errors.New("auth event must be kind 27235")
	}
	uTag := evt.Tags.Find("u")
	if uTag == nil {
		return "", errors.New("auth event missing 'u' tag")
	}
	parsed, err := url.Parse(uTag[1])
	if err != nil || parsed.Path != r.URL.Path {
		return "", errors.New("'u' tag does not match request path")
	}
	if h := parsed.Hostname(); h != "" && a.serviceHost != "" &&
		!strings.EqualFold(h, a.serviceHost) {
		return "", errors.New("'u' tag host does not match this relay")
	}
	if mTag := evt.Tags.Find("method"); mTag != nil && !strings.EqualFold(mTag[1], r.Method) {
		return "", errors.New("'method' tag does not match request method")
	}
	now := nostr.Now()
	if evt.CreatedAt > now+httpAuthWindow || evt.CreatedAt < now-httpAuthWindow {
		return "", errors.New("auth event is stale or future-dated")
	}
	if ok, _ := evt.CheckSignature(); !ok {
		return "", errors.New("invalid auth event signature")
	}
	return evt.PubKey, nil
}

// --- mutations (used by NIP-86; update DB + cache together) ---

// AllowPubkey grants access and updates the in-memory cache immediately.
func (a *Access) AllowPubkey(ctx context.Context, pk, note string) error {
	if err := a.ctrl.AllowPubkey(ctx, pk, note); err != nil {
		return err
	}
	a.mu.Lock()
	a.allowed[pk] = struct{}{}
	a.mu.Unlock()
	return nil
}

// RevokePubkey removes DB-granted access and drops it from the cache. A pubkey
// that's also in the static config set remains allowed (config wins).
func (a *Access) RevokePubkey(ctx context.Context, pk string) error {
	if err := a.ctrl.RevokePubkey(ctx, pk); err != nil {
		return err
	}
	a.mu.Lock()
	if _, isStatic := a.static[pk]; !isStatic {
		delete(a.allowed, pk)
	}
	a.mu.Unlock()
	return nil
}

// ListAllowedPubkeys returns the full effective allow-list (DB rows + static
// config keys) for NIP-86 listAllowedPubKeys. The "reason" field carries the
// note (DB rows) or "config" (static keys).
func (a *Access) ListAllowedPubkeys(ctx context.Context) ([]nip86.PubKeyReason, error) {
	rows, err := a.ctrl.ListAllowed(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]nip86.PubKeyReason, 0, len(rows)+len(a.static))
	for _, r := range rows {
		out = append(out, nip86.PubKeyReason{PubKey: r.Pubkey, Reason: r.Note})
	}
	for pk := range a.static {
		out = append(out, nip86.PubKeyReason{PubKey: pk, Reason: "config"})
	}
	return out, nil
}

// AdminGate implements khatru's RejectAPICall hook: only configured admin
// pubkeys (authed via NIP-98) may call NIP-86 management methods.
func (a *Access) AdminGate(ctx context.Context, _ nip86.MethodParams) (bool, string) {
	if !a.IsAdmin(khatru.GetAuthed(ctx)) {
		return true, "unauthorized: admin pubkey required"
	}
	return false, ""
}

func toSet(xs []string) map[string]struct{} {
	m := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if x != "" {
			m[x] = struct{}{}
		}
	}
	return m
}
