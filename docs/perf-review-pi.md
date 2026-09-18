All source verified against the audit. Here's the full review.

---

# 1. Finding-by-finding validation

## Write path

**F1 — crawler bypasses ReplaceEvent → version accumulation: CORRECT (and understated).**
`internal/crawler/ingest.go:34` calls `s.SaveEvent` directly; `ReplaceEvent` is only wired as a khatru hook (`internal/relay/relay.go:69`), and khatru only routes *client-published* non-regular kinds through it (`khatru@0.19.1/adding.go:80–100`). Since `ORDER BY (kind, pubkey, created_at, id)` (schema.go:51) includes `created_at`+`id`, distinct versions never share a sort key, so `ReplacingMergeTree`/`FINAL` collapses nothing — every crawled kind-0/3/10002 version lives forever. It's understated in one respect: it's not just growth, it's a **protocol violation on reads** — a REQ `{authors:X, kinds:3}` returns N stale contact-list versions (QueryEvents has no `LIMIT 1 BY pubkey`). It also feeds F13: `RefreshFollowers` scans that ever-growing pile every 5 min.

**F2 — ReplaceEvent does a full QueryEvents per event: PARTIALLY CORRECT.**
The cost claim is right: `store.go:160–165` runs the full 12-column `tierColumns` SELECT + per-row `json.Unmarshal` (scanEvent) + channel round-trip + `sortDesc`, to compare two scalars. A `SELECT id, created_at … WHERE pubkey=? AND kind=?` is strictly better. The d-tag parenthetical is **wrong in direction**: `isReplaceableKind` (classifier.go:57–63) returns *false* for 30000–39999 and for 10000–19999 (except 10002), so `ReplaceEvent` falls through to `SaveEvent` — addressable/replaceable kinds enabled via override get **no retirement at all** (full accumulation), not "retiring unrelated d-values". The gap is real, the mechanism as described isn't. Also missed: with a crawler backlog >100 versions, the `Limit: 100` in the replace query means retirement is incomplete anyway — another argument for read-side filtering (see §3).

**F3 — tombstone per-row INSERT + per-call dict reload: CORRECT.**
`store.go:141–147`: one `Exec("INSERT INTO tombstones …")` per id — in ClickHouse each `INSERT` statement is its own part, so a delete/replace burst is a parts-churn hazard ("too many parts" → inserts start failing → stale versions survive). `SYSTEM RELOAD DICTIONARY` per call at store.go:150 rebuilds the whole hashed dict. No prune of `tombstones` anywhere (only DDL in schema.go:67–82). All confirmed.

**F4 — batcher retry grows unboundedly during CH outage: CORRECT.**
Verified in `batcher.go:62–96`: `flush()` calls `drain()` *first*, then on send failure does `buf = append(batch, buf...)`. Because the worker keeps draining `in` into an ever-growing `buf` even while failing, the channel never fills and `enqueue` never returns `ErrBatchFull` — the load-shed valve is disabled exactly when it's needed. Growth per tick is bounded only by ingest rate; across an outage it's unbounded. No backoff (retries the full batch every `maxAge` tick, 5s default). This is the one finding that can take the whole relay down.

**F5 — dead ngrambf index on content: CORRECT.**
schema.go:32 defines `idx_content`; `filter_sql.go` emits predicates only on id/pubkey/kind/tags/time — no content predicate anywhere, and stats use `tags_raw` regex (stats.go:110), not `content`. Pure insert-CPU/disk/merge tax.

## Read path

**F6 — per-row JSON unmarshal of tags_raw: CORRECT.**
store.go:314 `json.Unmarshal([]byte(tagsRaw), &tags)` per row. It's the only real per-row work besides `Scan`, so "hottest CPU on read path" is fair; the same marshal exists on the *write* path (`rowFromEvent`, batcher.go:212) which the audit didn't mention — the fix should kill both.

**F7 — SELECT includes discarded accelerator columns: CORRECT.**
store.go:218 selects `tierColumns` (12 cols); scanEvent (303–323) reads `tagE/tagP/tagT/tagD/replyTo` into locals and drops them. In a columnar store that's real wasted I/O — `tag_p` on kind-3s is large. Fix is a second column-list const for SELECT; `tierColumns` stays for INSERT.

**F8 — unbounded-time queries can't use the PK: CORRECT.**
ORDER BY `(kind, pubkey, created_at, id)` (schema.go:51): `{kinds:[1], limit:N}` with no time bound must read `created_at` for every kind-1 row across all monthly partitions to satisfy `ORDER BY created_at DESC` (filter_sql.go:96–99); `{authors:[pk]}` without kinds misses the prefix entirely (and there's no bloom index on pubkey). Both confirmed.

**F9 — Info log per tier per query: CORRECT.** store.go:236. Trivial.

**F10 — sequential tiers + swallowed errors: CORRECT observation, questionable fix.**
store.go:226–229: `continue` on query error → partial results streamed as complete. Real correctness issue. But "run tiers in parallel" triples concurrent CH queries per REQ under many subscribers — sequential-per-REQ with proper error surfacing is the safer fix (see §3).

## Schema / ClickHouse

**F11 — hex strings waste half the key storage: OVERSTATED.**
Directionally right, but the "halves storage" math ignores that LZ4/ZSTD compresses hex ~2×, so on-disk savings are far less than 50%. The *real* win is uncompressed RAM for the primary index + bloom filters and comparison CPU. Against that: a full rewrite migration (new tables, `unhex()` backfill, `hex()` at every read site, tombstone dict key type, all `IN`-list bindings). Not worth it now; bundle with the F6 native-tags migration if that ever happens, and measure actual index RAM first.

**F12 — one shared pool: PARTIALLY CORRECT.**
True that writes, reads, and stats share one `driver.Conn` (store.go:41–66; stats/api use `s.CH()`). But it is not one connection — clickhouse-go v2's native conn is an internal pool (default `MaxOpenConns = 10`, `MaxIdleConns = 5`, clickhouse_options.go:412–417), so concurrent queries do parallelize. Starvation is real but mostly via CH CPU/IO from the 5-min `RefreshFollowers` FINAL scan, not pool exhaustion. Separate conn for stats is still worth doing (one line).

**F13 — RefreshFollowers TRUNCATE→rebuild non-atomic: CORRECT.**
stats.go:71–84. Readers see 0s (or an empty table for up to 5 min if the INSERT fails after TRUNCATE). Tombstone-exclusion nit also confirmed: `events_all` and all refresh queries lack `NOT dictHas('tombstone_dict', id)` — deleted events keep counting in engagement/DAU/followers forever. And the scan grows unboundedly thanks to F1.

## Crawler / ingest

**F14 — reconnect re-pulls full history: CORRECT.**
crawler.go:76: `filter := nostr.Filter{Kinds: kinds}` — no `Since`, no persisted high-water mark; every reconnect re-downloads the relay's full stored history. In-memory dedup (4M ids ≈ 13 min at 5k events/s!) doesn't cover a long gap, so aged-out dupes also get sig-verified and re-inserted into CH (harmlessly — same sort key, FINAL collapses — but bandwidth and CPU are paid).

**F15 — sig verify single-threaded per source: CORRECT.**
`ingest.go:27` `ev.CheckSignature()` runs inline in the one goroutine per source (crawler.go:117–121) and in the serial priority crawler. ~50–100µs/event → a fast source saturates a core and backpressures the websocket. Dedup-before-verify helps, but a big relay's startup backlog pushes >10k fresh events/s through one goroutine.

**F16 — dedup memory + locks: CORRECT (lock part is minor).**
dedup.go:65–107: two generations × 2M `map[string]struct{}` with 64-byte string keys ≈ 300–400MB at rotation — plausible and confirmed structurally. Two lock ops per event (Seen RLock + Mark Lock): real but tiny critical sections; the memory is the actual problem. `OnFlushed` does per-event `Mark` under per-event Lock (dedup.go:97–99) — `MarkMany` is a free win.

**F17 — seen_events prune full-scans: PARTIALLY CORRECT (finding right, fix suboptimal).**
control.go:138–146: `DELETE … WHERE created_at < ?` with no index on `created_at` → hourly full scan. But adding a `created_at` index taxes every insert on the hottest SQLite write path. `LoadRecentSeen` already uses rowid as the recency proxy — prune by rowid window instead (index-free, O(deleted), bounds table size by count). See §3.

**F18 — priority crawler serial + reconnect-per-pubkey: CORRECT, plus a bigger miss.**
priority.go:101–133: pubkeys sequential, relays tried serially, 5-min `fetchTimeout` each → a tick can badly overrun the 10-min interval. The audit missed the worse waste: `fetchOverlap = 24h` (priority.go:21) on a 10-min cadence means **every tick re-pulls a full day of every pubkey's events** — 144 redundant day-pulls per pubkey per day, all sig-verified when outside the dedup window.

## Server / policy

**F19 — no http.Server timeouts: CORRECT.** main.go:154. `ReadHeaderTimeout` + `IdleTimeout` are safe for the WS endpoint (hijacked conns are exempt); do NOT add Read/WriteTimeout (would kill long-lived REQ streams).

**F20 — REST bypasses breadth caps: CORRECT.**
api.go:123–141: `q["id"]`/`q["author"]` unbounded; `RejectFilterBreadth` is wired only on khatru hooks (relay.go:76–77). Store-side `limit` is capped at 1000, but a million-element `id IN (?)` list is still a giant query. One-line clamp or reuse `breadth.Reject`.

**F21 — rate limiter global mutex + fixed window: OVERSTATED.**
ratelimit.go:41–65: description accurate, but the critical section is ~100ns — at thousands of rps a single mutex is fine; contention matters at 100k+ ops/s. The genuinely ugly part is the GC sweep *inside* the lock iterating all IPs (line 55–61). Fixed-window 2× burst: real, minor. Sharding is complexity before evidence; profile first (pprof is already wired).

---

# 2. Challenging the proposed fixes

**Tombstone reloads (F3).** "Rely on LIFETIME 60s" breaks the delete contract — a client gets `deleted: true` and expects the next query to be clean; 60s of resurrection is a bug report waiting. Better: batch + **debounced** reload. Route all tombstone writes through one path that (a) inserts via `PrepareBatch` (kills the parts churn), (b) reloads the dict at most once per ~2s when dirty (bounded staleness, still feels instant, coalesces bursts). Prune: age-based pruning is *wrong* — it would resurrect tombstoned permanent-tier events; only an anti-join against the tier tables (`id NOT IN (SELECT id FROM events_…)` per tier, monthly, off-peak) is safe.

**tags_raw JSON decode (F6).** Option (b) — pull in sonic/json-iterator — is the weak fix: adds a direct dependency for a one-call site, keeps the write-path marshal, keeps double storage. Option (a) — native `Array(Array(String))` — is strictly better than advertised: make `tag_e/p/t/d` `MATERIALIZED` from it (`arrayMap/arrayFilter`), which deletes the Go-side extraction loop in `rowFromEvent` *and* the per-row `json.Marshal` on the write path *and* F7's wasted columns, and upgrades the arbitrary-tag-key fallback from a substring scan to an exact `arrayExists` match. The audit undersold it; make it the centerpiece of the next schema migration (with F11 bundled in, since both need a table rewrite).

**Dedup memory (F16).** Agreed on `uint64` key (first 16 hex chars → `ParseUint`; collisions at 4M live keys ≈ 4×10⁻⁷, consequence = one skipped event later recovered by a priority sweep — acceptable, and it's an *archive*, drop-on-collision should be logged). Combined `CheckAndMark` and batched `MarkMany`: yes, free. Don't bother with sharded maps.

**Batcher backpressure (F4).** The audit's "drop-oldest + log" option is **dangerous**: for client-published events, `enqueue` already returned success and khatru sent `OK:true` — silently dropping is data loss with a lying ACK. The correct fix is the other option: **stop draining when the retained backlog hits a cap** (e.g. 4×maxSize). Then the channel fills, `enqueue` returns `ErrBatchFull`, khatru honestly rejects, the crawler unmarks dedup and re-pulls later — the existing contracts all hold. Plus exponential backoff (1s→30s cap, reset on success) so a dead CH isn't hammered every 5s with a giant batch.

**Replaceable-version handling on ingest (F1/F2) — the audit's implied "route crawler through ReplaceEvent" is the wrong fix.** That adds a CH round-trip query per crawled kind-0/3/10002 event on the firehose hot path — exactly the cost F2 complains about, applied at crawler volume. And it can't heal the backlog already accumulated. The right shape is **read-time**: for replaceable kinds, add `LIMIT 1 BY pubkey` (plus `tag_d` scoping if addressable kinds are ever enabled) to the per-tier tail. One line in `buildFilterSQL`/QueryEvents; fixes stale-version serving *and* the existing mess *and* the `Limit:100` retirement incompleteness in F2, with zero ingest cost. Then a slow periodic compaction job (tombstone stale versions in bulk via the fixed F3 path) bounds disk growth. Important interaction: don't bulk-tombstone the backlog *before* F3's batching exists, and note each tombstone row also lives in the dict's RAM — prefer read-side filtering as the primary mechanism, tombstones for client-driven replaces only.

**Global-feed query (F8).** "Bound time range at policy layer" silently changes archive semantics — a legitimate trade-off, but there's a lazier native option first: a **ClickHouse projection** (`ADD PROJECTION p_feed (SELECT … ORDER BY (kind, created_at))` on `events_archive`) — zero app code, CH maintains it, and the planner can serve `kind=1 ORDER BY created_at DESC LIMIT n` from it. Test whether the planner picks it up (projection + `LIMIT` pushdown works on recent CH versions); if it doesn't, fall back to a server-side `since` default (e.g. 90d) when the filter has kinds but no time bound and no authors/ids. For `{authors}` without kinds, add a `bloom_filter` index on `pubkey` rather than touching the ORDER BY — reordering the key to lead with pubkey would wreck the (dominant) kinds-first scans.

**F10 parallel tiers:** skip the parallelism; surface errors instead (close the channel early / log-and-fail the REQ). Parallel-per-REQ × many subscribers multiplies CH load 3× for latency you mostly don't need.

**F17:** index on `created_at` is the wrong tool — prune by rowid count (below).

---

# 3. Concrete solutions for the top issues

1. **F4 — batcher backlog cap + backoff** (`internal/store/batcher.go`, ~15 lines). In `flush()`: on error, keep at most `4×maxSize` (drop-oldest is fine *only* for the tail beyond the cap, with an error log — and since the channel then backs up, producers get `ErrBatchFull` before that point anyway). Track consecutive failures; sleep `min(30s, 1s<<fails)` between retries instead of the raw tick. No contract changes; OOM path eliminated.

2. **F1+F2 — read-side replaceable filtering + slim replace check.** In `QueryEvents`, when the filter's kinds are all in `{0,3,10002}` (or, if addressable kinds get enabled, `IsAddressableKind`), append `LIMIT 1 BY pubkey[, tag_d]` before the final LIMIT. Then make `ReplaceEvent`'s probe a two-column query (`SELECT id, created_at FROM events_<tier> FINAL WHERE pubkey=? AND kind=? AND NOT dictHas(...) ORDER BY created_at DESC, id LIMIT 50`) — no channel/sort/tags decode. Optional monthly compaction job retires stale crawler versions in bulk through the batched tombstone path.

3. **F3 — batched tombstones + debounced reload** (`store.go:retireIDs`, ~25 lines). One `PrepareBatch` insert for the whole `ids` slice; a tiny "dirty" goroutine reloads `tombstone_dict` at most once per 2s. Monthly anti-join prune (never age-based). Kills parts churn and O(table)-per-delete.

4. **M1 (missed) — `/v1/health` full-table count()** (`api.go:64`, one line). `SELECT count() FROM events_all` per probe is a self-inflicted full scan of billions of rows on every healthcheck. Replace with `SELECT sum(rows) FROM system.parts WHERE active AND database=? AND table LIKE 'events_%'` — instant, approximate, good enough for health.

5. **F6+F7 — column pruning now, native tags at next migration.** Immediate: `selectColumns = "id, pubkey, created_at, kind, content, sig, tags_raw"` for the read path (F7, trivial). Migration: `tags Array(Array(String))` with `tag_e/p/t/d` MATERIALIZED from it — removes per-row JSON decode on reads, per-event JSON marshal on writes, and the Go tag-extraction loop; backfill via `JSONExtract` from `tags_raw`. Bundle F5 (drop `idx_content`) and, if index RAM justifies it, F11 (`FixedString` keys) into the same one-time rewrite.

6. **F8 — projection first, since-default second.** `ALTER TABLE events_archive ADD PROJECTION p_feed (SELECT * ORDER BY (kind, created_at)) PARTITION BY …; MATERIALIZE PROJECTION`. Verify with `EXPLAIN` that the global-feed query hits it. Fallback: `since = now − 90d` injected in `buildFilterSQL` when kinds-only/no-authors/no-time. Add `INDEX idx_pubkey pubkey TYPE bloom_filter(0.01)` for author-without-kind lookups.

7. **F15+F16 — ingest worker pool + dedup compaction.** Bounded verify pool (e.g. `GOMAXPROCS` workers) between the source loop and `ingest`, preserving dedup-before-verify ordering. Dedup: `map[uint64]struct{}` keyed by `ParseUint(id[:16],16,64)`, combined `CheckAndMark`, batched `MarkMany` in `OnFlushed`. ~400MB → ~50MB, 2 locks → 1.

8. **F13+F12 — stats isolation.** `EXCHANGE TABLES author_follower_counts AND author_follower_counts_staging` around the rebuild (atomic, no zero-window); open a second `clickhouse.Open` conn for the stats service; add `NOT dictHas('tombstone_dict', id)` to the refresh queries.

Runners-up: F14 (persisted per-source HWM + 6h overlap + daily full sweep), F18 (bounded pubkey parallelism + shrink `fetchOverlap` 24h→1–2h), F19 (`ReadHeaderTimeout: 10s`, `IdleTimeout: 120s`), F20 (clamp `len(q["id"])`/`len(q["author"])` or reuse `breadth.Reject`), F9 (Debug), F17 (rowid prune: `DELETE FROM seen_events WHERE rowid <= (SELECT rowid FROM (SELECT rowid FROM seen_events ORDER BY rowid DESC LIMIT 1 OFFSET ?))`).

---

# 4. What the audit missed

- **M1 — `/v1/health` runs `SELECT count() FROM events_all` on every probe** (api.go:64). Full scan of all tiers, no cache. With a k8s liveness probe at 10–30s intervals this is a permanent background DoS. Highest severity-per-line-of-fix ratio in the codebase.
- **M2 — `seen_events` is the real firehose bottleneck, not the in-memory map.** At 5k events/s × 7-day retention ≈ **3B rows / ~250GB in SQLite** (64-char TEXT PK), with hourly full-scan deletes (F17) and a file that never shrinks (no VACUUM anywhere). F16's 400MB map is noise next to this. Fix: count-bounded rowid pruning + retention cut to ~48h (the in-memory window is only ~13 min at that rate; CH FINAL absorbs the rest).
- **M3 — write-path `json.Marshal` per event** (batcher.go:212). Same CPU class as F6 but on ingest; the native-array fix kills both. Audit only saw the read side.
- **M4 — in-memory dedup window collapses at firehose rate.** 2×2M generations = ~13 minutes at 5k/s, not hours — so F14's reconnect re-pulls mostly *miss* memory dedup and re-verify + re-insert. Raises F14's priority above what the audit implies.
- **M5 — F1 fix / tombstone interplay:** bulk-tombstoning the crawler backlog (the obvious F1 "fix") migrates the rows into `tombstones`, which the HASHED dict holds fully in RAM, unpruned — read-side `LIMIT 1 BY` avoids creating that problem.
- **M6 — single worker per tier batcher:** while `flush()` blocks on the network (up to 30s timeout), nobody drains `in` (cap = 2×maxSize = 10k = ~2s of firehose). Survivable, but worth widening the channel cap when touching F4.
- **M7 — khatru per-publish "deleted?" probes** (adding.go:29–45): every client EVENT triggers a `QueryEvents({kinds:[5], #e})` — kind 5 maps to TierDrop so tiers are empty and it's cheap, but it's a goroutine + channel + filter build per publish, and NIP-09-by-query can never match since kind-5 events aren't stored (deletion works only via the DeleteEvent hook at delete time — correct, but worth knowing).
- **M8 — `sortDesc` + full materialization before first event out** bounds REQ latency at (slowest tier) + sort of ≤3×1000 events — acceptable, but at many-subscriber scale consider streaming the common single-tier case.

---

# 5. Prioritized order

1. **F4 batcher cap + backoff** — the only finding that OOMs the process; ~15 lines, no contract change.
2. **M1 health-check count()** — one line, removes a constant self-DoS.
3. **F1+F2 read-side replaceable filtering + slim replace probe** — protocol correctness (stale versions served today) *and* unbounded growth; heals existing data with no migration.
4. **F3 batched tombstones + debounced reload** — parts churn + dict reload CPU; prerequisite for any future tombstone-based compaction.
5. **F6/F7 column pruning now; native `Array(Array(String))` tags as the next (only) schema migration** — biggest CPU win on both read and write paths at subscriber+firehose scale; fold F5 (drop `idx_content`) and maybe F11 in.
6. **F8 global feed** — projection test → `since`-default fallback — the canonical query that will fall over first as `events_archive` grows.
7. **F14/F18 reconnect economics** — per-source HWM, shrink priority `fetchOverlap` (24h → 1–2h), bounded parallelism; mostly bandwidth/CPU, not correctness.
8. **F13/F12 stats isolation** — EXCHANGE TABLES, second CH conn, tombstone exclusion in refreshes.
9. **F15/F16 verify pool + dedup key compaction; M2 seen_events count-bounded prune** — the ingest-CPU/memory cluster, done together.
10. **Hygiene tail:** F19 timeouts, F20 REST clamps, F9 log demote, F10 error surfacing (not parallelism). **Leave alone:** F11 until measured, F21 until profiled (the mutex isn't your bottleneck at these rates).

The audit is solid — 14 of 21 findings fully hold, and the overstated ones (F11, F21) are directionally fine. Its two real errors are the F2 d-tag mechanism (the gap is the opposite of what's described) and the F4 "drop-oldest" suggestion (silent data loss for acknowledged client events). The biggest strategic correction: for replaceable-event handling, fix it **on read**, not on ingest — every ingest-side fix multiplies a CH query onto the firehose hot path, while read-side `LIMIT 1 BY` is one line, correct for the accumulated backlog, and free when nobody's reading.
