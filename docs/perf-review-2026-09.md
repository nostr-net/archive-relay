# archive-relay performance review (2026-09-18)

> Validated by two independent reviewers — see `docs/perf-review-grok.md` and
> `docs/perf-review-pi.md` for per-finding verdicts, and
> `docs/perf-plan-2026-09.md` for the reconciled plan. Notable corrections to
> this document: F2's d-tag mechanism is wrong in direction (addressable kinds
> get *no* retirement, not wrong retirement), F4 must NOT be fixed by
> drop-oldest (silent loss of ACKed events), F1 is best fixed on the read side
> (not by routing the crawler through ReplaceEvent), F11/F21 are overstated.

Codebase: ~5.3k lines of Go. khatru relay + ClickHouse (events/stats) + embedded
SQLite (control plane: dedup, scheduler, allow-list) + firehose/priority crawlers.

Findings below are grouped by subsystem. Each claim references the code it is
based on. Severity: HIGH / MED / LOW.

## Write path

### F1 (HIGH) — Replaceable events ingested via crawler are never deduplicated
`internal/crawler/ingest.go` calls `store.SaveEvent` directly, bypassing
`store.ReplaceEvent`. Every crawled version of kind 0/3/10002 is kept forever in
`events_permanent`; ReplacingMergeTree cannot collapse versions because
`created_at` is part of ORDER BY. Only client-published events (khatru
`ReplaceEvent` hook) retire superseded versions.
Impact: unbounded growth of the "permanent" tier (contact lists churn often),
stale versions served on reads.
Refs: internal/crawler/ingest.go:31, internal/store/store.go:154-194,
internal/store/classifier.go:57-63.

### F2 (HIGH) — ReplaceEvent does a full QueryEvents per event
For each kind-0/3/10002 it runs FINAL query reading all 12 columns, JSON
unmarshals tags_raw per row, sorts, round-trips a channel — only to compare
id/created_at. A dedicated `SELECT id, created_at ... WHERE pubkey=? AND kind=?`
is much cheaper. Also: no d-tag handling — if addressable kinds (30000-39999)
are enabled via classifier override, ReplaceEvent would retire unrelated
d-values; `isReplaceableKind` doesn't cover that range.
Refs: internal/store/store.go:160-194, classifier.go:57-63.

### F3 (HIGH) — Tombstone writes: per-row INSERT + full dict reload per call
`retireIDs` does one `Exec` per id, then `SYSTEM RELOAD DICTIONARY
tombstone_dict` rebuilds the entire hashed dictionary on every NIP-09/replace.
O(tombstone table) per reload; reloads uncoalesced. Also `tombstones` is never
pruned — tombstones whose events already expired via tier TTL could be dropped.
Fix direction: single PrepareBatch insert; debounce/coalesce reloads (or rely on
LIFETIME 60s); periodic prune where data already TTL-expired.
Refs: internal/store/store.go:136-152, internal/store/schema.go:75-82.

### F4 (HIGH) — Batcher retry path can grow buffer unboundedly during CH outage
On flush failure, batch is prepended back to `buf`; every subsequent tick
`drain()` pulls the whole input channel into `buf` again, so producers never hit
ErrBatchFull while `buf` grows ~2×maxSize per failed retry, unbounded across a
long outage → OOM. Also no backoff: retries the whole growing batch every tick.
Fix: cap retained backlog (stop draining when buf ≥ threshold, or drop-oldest +
log) and exponential backoff on consecutive failures.
Refs: internal/store/batcher.go:86-113.

### F5 (MED) — Dead ngrambf_v1 index on content
`idx_content` costs insert CPU + disk on every part of every tier; no code path
ever emits a content predicate (no NIP-50 search). Drop unless NIP-50 planned.
Refs: internal/store/schema.go:32, internal/store/filter_sql.go.

## Read path

### F6 (HIGH) — Per-row JSON unmarshal of tags_raw on every read
`scanEvent` runs `json.Unmarshal` on full tags JSON per row — hottest CPU cost on
the read path. Options: (a) store tags natively `Array(Array(String))`, decode in
CH, reconstruct nostr.Tags without JSON (tag_e/p/t/d could be MATERIALIZED from
it); (b) faster JSON lib (json-iterator/sonic already indirect deps).
Refs: internal/store/store.go:303-323.

### F7 (MED) — scanEvent SELECTs accelerator columns it discards
SELECT reads tag_e/tag_p/tag_t/tag_d/reply_to then throws them away. Selecting
only id,pubkey,created_at,kind,content,sig,tags_raw cuts read I/O.
Refs: internal/store/store.go:217-219, 303-323.

### F8 (MED) — Unbounded-time queries can't use primary index
ORDER BY (kind, pubkey, created_at, id) serves {kinds,authors} filters well, but
{kinds:[1], limit:N} with no since/until (canonical global feed) must scan all
kind-1 rows; {authors:[pk]} without kinds misses the PK prefix. Options: bound
time range at policy layer, a recent-events projection, or accept as archive
trade-off.
Refs: internal/store/schema.go:51, internal/store/filter_sql.go.

### F9 (LOW) — `tier scan` logged at Info per tier per query
3 stderr writes per REQ on hot path. Demote to Debug.
Refs: internal/store/store.go:236.

### F10 (LOW) — Tier queries sequential + per-tier errors swallowed
A failed tier `continue`s → partial results returned as complete. Run tiers in
parallel; surface error.
Refs: internal/store/store.go:213-237.

## Schema / ClickHouse

### F11 (MED) — Hex String ids waste ~half the key storage
id/pubkey String(64 hex) → FixedString(32) via unhex(); sig → FixedString(64).
Halves key storage, speeds PK/bloom/dictHas. Migration needed.
Refs: internal/store/schema.go:13-33.

### F12 (MED) — One CH conn pool for writes + reads + heavy stats
Stats refresh scans can starve REQ queries. Separate driver.Conn for stats,
tune MaxOpenConns, or CH-side quotas.
Refs: internal/store/store.go:39-48.

### F13 (MED) — RefreshFollowers TRUNCATE→rebuild non-atomic, every 5 min
Reads return 0s mid-refresh; full `FINAL WHERE kind=3` scan each run. Use
staging table + EXCHANGE TABLES; consider longer interval / incremental.
Also: stats refreshes don't exclude tombstoned events (correctness nit).
Refs: internal/stats/stats.go:71-84.

## Crawler / ingest

### F14 (MED) — Firehose reconnect re-pulls full history
Subscription has no `since`; every reconnect re-downloads all history (dedup
catches dupes but bandwidth + sig-verify still paid). Track per-source
high-water mark; `since = hwm - overlap` on reconnect; full backfill only cold.
Refs: internal/crawler/crawler.go:75-98.

### F15 (MED) — Signature verify single-threaded per source
~50µs+ CPU per event in the one goroutine per relay; a fast source can saturate
a core and backpressure the ws. Worker pool for verify→enqueue.
Refs: internal/crawler/ingest.go:24.

### F16 (MED) — Dedup memory + lock overhead
map[string]struct{} × 2M × 2 generations ≈ 400MB peak at rotation; every event
takes ≥2 lock ops (Seen+Mark, Unmark on retry). uint64 key from first 8 bytes of
decoded id → ~30-40MB; combined CheckAndMark → 1 lock; OnFlushed should MarkMany
under one lock instead of per-event.
Refs: internal/crawler/dedup.go:65-107.

### F17 (LOW) — seen_events prune: no index on created_at
Hourly `DELETE WHERE created_at < ?` = full scan over a table growing one row
per ingested event. Add index or prune by rowid window.
Refs: internal/control/control.go:138-146.

### F18 (LOW) — Priority crawler fully serial + reconnect-per-pubkey
N pubkeys × M relays, sequential, up to 5-min timeout each → tick can exceed
interval. Parallelize pubkeys (bounded); reuse connection per relay per tick.
Refs: internal/crawler/priority.go:78-149.

## Server / policy

### F19 (MED) — http.Server has no timeouts
No ReadHeaderTimeout → slowloris on a public port. Safe to add (header-read
bound only, ws unaffected).
Refs: cmd/archive-relay/main.go:154.

### F20 (LOW) — REST /v1/events bypasses breadth caps
RejectFilterBreadth only wraps khatru WS hooks; REST accepts unbounded
?id=/?author= lists. Apply same clamps.
Refs: internal/api/api.go:123-141.

### F21 (LOW) — Rate limiter: single global mutex + fixed window
counts-map mutex contends across all IPs at high rps (shard it); fixed window
allows 2× burst at boundary (minor).
Refs: internal/policy/ratelimit.go:41-65.

## Open questions for reviewers
- F1: is bypassing ReplaceEvent on ingest a deliberate throughput trade-off, or
  an oversight? Crawled contact lists accumulate all versions.
- F8: acceptable to require since/until on firehose-scale tier scans?
- F11: worth a schema migration now, or batch with a later migration?
- F3: is immediate tombstone visibility (dict reload) required vs ≤60s LIFETIME?
