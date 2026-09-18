I'll start with the audit document, then verify each F1–F21 claim against the actual Go source rather than taking the write-up at face value.I'll pull the cited files and surrounding write/read/crawler paths so each finding can be checked against the real code.Next I’ll read crawler, stats, API, policy, and main, plus scan for search/content usage and other write-path callers.I’ll check relay wiring, policy, tests, and remaining hot-path details so the verdicts aren’t based on the audit notes alone.I want khatru’s StoreEvent vs ReplaceEvent split, clickhouse-go pool defaults, and a few more hot-path details before scoring the findings.I have enough of the real paths to score F1–F21 against the code. Below is a skeptical read: several diagnoses are right, a few implied fixes would make ingest worse, and FINAL is doing less than the comments claim.

---

# Performance audit validation

Code matches the write-up on most *facts*. Severity and fix directions are where it slips: a few items are overrated given current call rates, F2’s d-tag claim is wrong, and the implied F1 fix (`ReplaceEvent` on the firehose) would be disastrous.

`FINAL` does **not** implement replaceable semantics. `ORDER BY (kind, pubkey, created_at, id)` plus `version = created_at` only collapses *exact duplicate rows of the same event* (same sort key). Different kind-0/3 versions have different `created_at`/`id`, so they never merge. The comment on `QueryEvents` is misleading:

```196:218:internal/store/store.go
// QueryEvents streams events matching the filter, querying each relevant tier
// with FINAL (so replaceable/addressable dedup is correct) and merging in Go.
// ...
			q := fmt.Sprintf("SELECT %s FROM events_%s FINAL WHERE %s%s",
				tierColumns, t, where, tail)
```

---

## 1. Finding verdicts

| ID | Verdict | One-line reason |
|---|---|---|
| **F1** | **CORRECT** (implied fix is wrong) | Crawler calls `SaveEvent` only (`ingest.go:31`); khatru `ReplaceEvent` is client-publish only (`relay.go:69`). RMT cannot collapse versions because `created_at` is in the sort key (`schema.go:49-51`). |
| **F2** | **PARTIALLY CORRECT** | Full `QueryEvents` per replaceable publish is real (`store.go:165-167`). The d-tag scare is false: `isReplaceableKind` is only 0/3/10002 (`classifier.go:57-63`); classifier overrides of 3xxxx still fall through to `SaveEvent` (`store.go:161-162`). |
| **F3** | **CORRECT, OVERSTATED as HIGH** | Per-id `Exec` + `SYSTEM RELOAD DICTIONARY` is real (`store.go:142-150`). Today this is NIP-09 + client `ReplaceEvent` only — not the firehose. Becomes HIGH the moment you start retiring crawled versions. |
| **F4** | **CORRECT** | Failed `flush()` `drain()`s the whole channel into `buf`, then retries; `ErrBatchFull` only holds while `in` is full (`batcher.go:86-113`, cap `maxSize*2` at `:47`). ~2×`maxSize` extra events per tick, unbounded. |
| **F5** | **CORRECT** | `idx_content ngrambf_v1` is on every tier (`schema.go:32`). `buildFilterSQL` never emits a content/`Search` predicate; NIP-50 is explicitly not advertised (`relay.go:33`, `relay_test.go:27-29`). |
| **F6** | **CORRECT** | `scanEvent` `json.Unmarshal`s `tags_raw` per row (`store.go:312-313`). Kind-3 contact lists make this the real CPU cost, not generic notes. |
| **F7** | **CORRECT** | `SELECT` uses `tierColumns` including `tag_e/p/t/d, reply_to` (`store.go:217-218`, `schema.go:11`); `scanEvent` drops them after scan (`:314-322`). Kind-3 reads the tags twice (JSON + arrays). |
| **F8** | **CORRECT** | PK `(kind, pubkey, created_at, id)` (`schema.go:51`) is good for `{kinds, authors}`; `{kinds:[1], limit:N}` with no time bound must scan every kind-1 granule and sort (`filter_sql.go:95`). |
| **F9** | **CORRECT** | `s.log.Info("tier scan", ...)` per tier per REQ (`store.go:236`). |
| **F10** | **PARTIALLY CORRECT** | Errors are swallowed (`store.go:220-222`) — that is a correctness bug. Parallelizing 3 `FINAL` scans is not a free win: most filters hit one tier (`tiersForFilter`); match-all would triple CH load. |
| **F11** | **CORRECT, not worth doing now** | Hex `String` vs `FixedString`/`unhex` is real (`schema.go:13-20`). Tags+content dominate; migration touches dict keys, blooms, every query. |
| **F12** | **CORRECT** | One `clickhouse.Open` (`store.go:39-48`); stats uses `s.CH()` (`main.go:89`). Driver default `MaxOpenConns = MaxIdleConns+5 = 10`. |
| **F13** | **CORRECT** | `TRUNCATE` then `INSERT` (`stats.go:72-83`); empty table is visible. Query is `FINAL WHERE kind=3` with no `dictHas`. |
| **F14** | **PARTIALLY CORRECT / OVERSTATED** | Filter is `{Kinds: kinds}` with no `since` (`crawler.go:77-92`). Reconnect replays the *upstream’s EOSE backlog*, not infinite history. Bandwidth is real; sig-verify is paid in go-nostr *before* `Seen()` (see F15). |
| **F15** | **PARTIALLY CORRECT** | One goroutine per source is real. `ingest` `CheckSignature` (`ingest.go:24`) is a **second** verify: `nostr.Relay` already verifies unless `AssumeValid` (`go-nostr/relay.go:282-288`). A worker pool in `ingest` does not unblock the WS read loop. |
| **F16** | **PARTIALLY CORRECT / OVERSTATED** | 2×`memCap` string-key maps are real (`dedup.go:19,32-34,76-82`). ~400MB is plausible. `uint64` from 8 decoded bytes is the wrong trade for an archive; `OnFlushed` already batches SQLite (`:98-107`) — the extra per-id `Mark` loop is waste, not the main cost. |
| **F17** | **CORRECT, UNDERSTATED** | `seen_events(id PK)` + hourly `DELETE WHERE created_at < ?` (`control.go:32-35,138-146`) is a full scan. At firehose the *table size* (7d × ingest rate) is the problem, not the missing index. |
| **F18** | **CORRECT** | Serial pubkeys, new connect per relay (`priority.go:78-140`), 5min timeout (`:21,132`). Fine for tens of keys; not for thousands. |
| **F19** | **CORRECT** | `http.Server{Addr, Handler}` only (`main.go:154`). `ReadHeaderTimeout` is the right knob; do **not** set `WriteTimeout` (long-lived WS). |
| **F20** | **CORRECT** | Breadth caps are khatru hooks only (`relay.go:87-88`, `reject.go:32-52`). REST `/v1/events` builds a filter from unbounded query params (`api.go:123-131`). URL length + the 600/min limiter bound practical abuse. |
| **F21** | **CORRECT as LOW** | One mutex + fixed window (`ratelimit.go:41-65`). Crawler ingest does not hit this. Not a firehose issue. |

### Open questions from the audit

- **F1 — deliberate or oversight?** Both. `ingest` is intentionally the cheap path (calling `ReplaceEvent` at firehose rates would be F2×every kind-0/3). What is missing is *any other* collapse: not on write, not on read (`LIMIT 1 BY`), not in a background job. Client publishes are cleaned; crawled versions accumulate and are served (`QueryEvents` returns up to `limit` historical profiles/contact lists).
- **F8 — require `since`/`until`?** Do not hard-reject without a default. Inject a recent window, or add a `(kind, created_at)` projection. A naked `{kinds:[1], limit:20}` is every client’s first REQ.
- **F11 — migrate now?** No. Bundle with a tags-native schema change later.
- **F3 — immediate dict reload?** No. `LIFETIME MIN 0 MAX 60` (`schema.go:81`) already converges in ≤60s. Immediate reload is not required for an archive; it becomes toxic if replaceable retirement volume grows.

---

## 2. Challenge the proposed fixes

### Replaceable versions on ingest (F1)

**Do not call `ReplaceEvent` from the crawler.** That is one `FINAL` query + JSON decode + tombstone reload per kind-0/3/10002 at firehose rate. Kind-3 churn would dominate CH CPU.

Also `ReplaceEvent` uses `Limit: 100` (`store.go:166`). After the crawler has stored hundreds of versions, a client publish only sees the 100 newest and cannot retire the rest.

Better, in order of blast radius:

1. **Read-path collapse (correctness, no write change):** for kinds 0/3/10002, append `LIMIT 1 BY (kind, pubkey)` (NIP-01 tie-break: `ORDER BY created_at DESC, id ASC`). Clients stop seeing stale versions immediately. Storage still grows.
2. **Background compact (storage):** periodic `INSERT INTO tombstones SELECT ... WHERE row_number() OVER (PARTITION BY kind, pubkey ORDER BY created_at DESC, id ASC) > 1` from `events_permanent`, then **one** dict reload. Keep this off the ingest path.
3. **Schema (only if you want CH-native replace):** a *separate* replaceable table with `ORDER BY (kind, pubkey)` and `ReplacingMergeTree(created_at)`. You cannot change the current PK: 9735 lives in `permanent` and is not replaceable.

Do not put 9735 in that replaceable table.

### Tombstone dictionary reloads (F3)

`PrepareBatch` for the inserts is fine. `SYSTEM RELOAD DICTIONARY` per call is the actual cost, and coalescing reloads is right — **relying on LIFETIME alone is also fine**.

Do **not** rebuild HASHED on every NIP-09. Better:

- Insert tombstones in one batch.
- Debounce reload (e.g. 1s / 64 ids) **or skip reload entirely**.
- If you need <60s hide: keep a small in-process set of recent ids and drop them in `scanEvent` (and in SQL as `id NOT IN (?)` when the set is non-empty). Dict is the bulk filter; memory covers the gap.
- Prune only tombstones whose *event* has TTL-expired. Permanent ids (0/3/9735/10002) must keep tombstones forever. Blind prune re-exposes deleted events.

### `tags_raw` JSON decode (F6)

`json-iterator`/`sonic` is a 5-line palliative (sonic is already an indirect dep). It does not fix kind-3.

**Do not** reconstruct tags from `tag_e/p/t/d` — you drop `amount`, `client`, `nonce`, markers, etc.

Right long-term column: `tags Array(Array(String))`, scan into `nostr.Tags` (`[][]string`) with no JSON. That is a schema migration; do it with F11, not as a drive-by. Short-term: stop `SELECT`ing the accelerator columns (F7) so you at least stop reading tags twice.

### Dedup memory (F16)

`uint64` from first 8 bytes of the decoded id is ~2⁻⁶³ collision per pair, tiny, but a collision **silently drops a different event** from an archive. Not worth it.

Use `map[[32]byte]struct{}` (decode hex once). Same algorithm, ~half the key bytes, zero collision. Combined `CheckAndMark` under one lock is a micro-optimization; the duplicate path is already `RLock` only (`dedup.go:65-72`), which is the common case across overlapping sources.

The bigger issue is `seen_events` at firehose (F17), not the in-memory map.

### Batcher backpressure (F4)

**Do not drop-oldest.** This is an archive; the designed valve is already `ErrBatchFull` → crawler `Unmark` (`ingest.go:32-34`).

The bug is that `flush()` always `drain()`s, emptying `in` and reopening the valve.

Fix:

```text
on failure: keep `pending` (the failed slice), do not drain
on next tick: retry only pending, capped at maxSize
only drain into a new batch when pending is empty
if pending is non-empty, leave `in` untouched → channel fills → ErrBatchFull works
exponential backoff between retries (cap ~30s, matching flush timeout)
```

Also cap each `PrepareBatch` at `maxSize`. After a 10-minute outage, retrying a 1M-event slice against a 30s timeout (`batcher.go:154`) will never catch up.

Two related bugs the audit missed:

- `FlushAll` always `reply <- nil` (`batcher.go:127-129`) even when flush failed.
- `case <-b.stop: flush(); return` (`:117-119`) drops `buf` on failure — data loss on shutdown during an outage.
- `PrepareBatch` is not `Abort()`ed if `Append` fails (`:158-167`) — native conn can stay leased.

### Global-feed query (F8)

Requiring `since`/`until` as a hard reject surprises every `nak req -k 1`. Better:

1. **Policy default:** if `IDs` empty and `Since` nil, set `Since = now - 48h` (configurable). Log/notice once. Authors+kinds queries stay unbounded in time (PK works).
2. **Projection** (the real CH fix):  
   `PROJECTION proj_kind_time (SELECT ... ORDER BY (kind, created_at, id))`  
   Global `{kinds:[1], since, until, limit}` can jump the time axis. PK stays optimal for `{kinds, authors}`.
3. Do **not** change the table `ORDER BY` to `(kind, created_at, …)` — that wrecks the author-profile path.

`FINAL` on these scans is the other multiplier (missed by the audit). Drop it for id-keyed duplicate collapse; handle exact dupes with `LIMIT 1 BY id` only if overlapping crawlers make it visible, or accept pre-merge dupes.

---

## 3. Best solutions for the top issues

Minimal blast radius, implementable, with “don’t do this” flags.

### 1. F4 — cap the retry buffer (do this first)

Split `pending` vs new drain as above. No drop-oldest. Surface flush errors on `FlushAll`. Abort failed batches.

**Don’t:** call `ReplaceEvent` here; don’t grow the INSERT until CH recovers.

### 2. F8 + `FINAL` — make REQ cheap

- Inject default `since` for author-less, id-less filters.
- Add `(kind, created_at)` projection on `events_archive` (kind 1 is the table that hurts).
- Stop advertising `FINAL` as replaceable-dedup. Use `FINAL`/`LIMIT 1 BY id` only if you measure duplicate ids in results.

**Don’t:** require since/until with no default. **Don’t:** parallelize tiers (F10) until queries are cheap; that amplifies load.

### 3. F1 — collapse replaceable versions off the ingest path

- Phase A: `LIMIT 1 BY (kind, pubkey)` on read for 0/3/10002 (and `ORDER BY created_at DESC, id ASC`).
- Phase B: hourly compact job → tombstones, one reloads debounce (depends on F3).

**Don’t:** `ReplaceEvent` in `ingest`. **Don’t:** change `events_permanent` PK while 9735 lives there.

### 4. F12 — split pools + bound concurrency

- Write conn (batchers) vs read conn (REQ/COUNT/API) vs stats conn.
- Set `MaxOpenConns` explicitly (e.g. writes 4, reads 16, stats 2).
- Semaphore on `QueryEvents`/`CountEvents` (e.g. 8 in flight) so a REQ stampede cannot eat the read pool; fail with a closed/notice rather than 5s acquire timeout (`DialTimeout: 5s` at `store.go:46`).

Stats `FINAL` scans every 5 min on the same 10-conn default pool will stall live REQ. This is a real firehose-adjacent outage mode.

### 5. F7 + F5 — cheap schema/read wins

- Separate `selectColumns` = `id, pubkey, created_at, kind, content, sig, tags_raw`.
- `ALTER TABLE ... DROP INDEX idx_content` on all three tiers (NIP-50 is not implemented; the test forbids advertising it).

**Don’t:** drop accelerators from the table, only from the SELECT. Tag filters still need `hasAny(tag_*)`.

### 6. F3 — make tombstones cheap before compact jobs exist

PrepareBatch inserts; debounce or skip `SYSTEM RELOAD`; in-process recent-id filter if you care about sub-minute NIP-09.

**Don’t:** reload-per-id. **Don’t:** prune permanent-tier tombstones.

### 7. F14/F15 — crawler reconnect + verify

- Per-source high-water mark: `since = hwm - 2min` after the first EOSE; full filter only on cold start.
- `nostr.NewRelay` with `AssumeValid: true` for configured firehose URLs (you already chose to ingest them). Keep `CheckSignature` in `ingest` for **new** ids only — that is the right single verify.
- Optional: small worker pool **after** `Seen()` for new-id verify if one source exceeds ~1 core. Do not pool before `Seen()`.

**Don’t:** verify twice. **Don’t:** start a worker pool as the first step.

### 8. F16/F17 — stop using SQLite as a firehose log

`seen_events` at 1k/s × 7d ≈ 600M rows. Hourly `DELETE` without an index is a long writer lock on the same DB as auth/scheduler.

Better:

- In-memory `[32]byte` set as now (maybe smaller `memCap`).
- Drop durable `seen_events` for the firehose, **or** cap it (e.g. last 2M rowids, prune by `rowid` not `created_at`).
- Warm from CH: `SELECT id FROM events_* WHERE received_at > now()-1d ORDER BY received_at DESC LIMIT memCap` — restart-safe without a growing SQLite table.

`OnFlushed` currently runs **on the batcher goroutine** (`batcher.go:110-112` → `dedup.go:98-107`). A 5k-row SQLite txn stalls inserts. Make it async: losing a `seen_events` write only causes re-ingest, which RMT already collapses for identical ids.

**Don’t:** `uint64` keys. **Don’t:** add only a `created_at` index and call it solved.

### Fixes that hurt correctness or ops simplicity

| Suggestion | Why not |
|---|---|
| `ReplaceEvent` on crawler ingest | Turns F2+F3 into the firehose |
| Drop-oldest on batcher overflow | Silent archive data loss; `ErrBatchFull` already exists |
| `uint64` dedup keys | Rare collisions omit events forever |
| `WriteTimeout` on `http.Server` | Kills WS |
| Parallel tier `FINAL` without a pool/semaphore | Multiplies CH load |
| Reconstruct tags from accelerator columns | Drops non-e/p/t/d tags |
| Change PK to `(kind, created_at)` | Breaks `{kinds, authors}` |
| Prune all tombstones after event TTL blindly | Permanent-tier deletes reappear |
| Hex→binary migration *now* | High-risk, low share of row size |

---

## 4. What the audit missed (firehose-scale)

**Ingest / write**

1. **Double schnorr verify** — go-nostr subscription verifies every frame; `ingest` verifies again for unseen ids. Reconnect replay pays the first verify for events `Seen()` would skip.
2. **`OnFlushed` blocks the only batcher goroutine** on SQLite. At `maxSize=5000` this is the stall between CH inserts.
3. **`config.example.yaml` has `maxAge: 1s`** while `config.go:111-114` documents 5s as the production setting. 1s ≈ one CH part/s/tier — the small-inserts anti-pattern the comments warn about.
4. **`json.Marshal(evt.Tags)` on every insert** (`batcher.go:189`) — kind-3 write CPU, symmetric to F6.
5. **9735 in `permanent` with empty TTL** (`classifier.go:31-32`, `config.example.yaml:26`). Zap receipts at firehose volume may dwarf kind-3 version bloat. Product question, but it is a storage bomb the audit skipped.
6. **No `bloom_filter` on `pubkey`.** `idx_id` exists; authors-without-kinds cannot use the PK prefix `(kind, …)` and has no skip index.

**Read / CH**

7. **`FINAL` on every REQ/COUNT** is the expensive part of F8, and it does not implement NIP-01 replaceable semantics. Overlapping crawlers produce duplicate *ids* that RMT will merge eventually; paying `FINAL` on a 10-year kind-1 table for that is the wrong default.
8. **Default pool of 10, `DialTimeout` 5s, `max_execution_time` 120s.** One global-feed `FINAL` can pin a conn for 2 minutes; a handful of REQs plus `RefreshFollowers` exhaust the pool. No query semaphore, no `max_memory_usage` / `max_threads` per query.
9. **`events_all` is `UNION ALL` without `FINAL` or `dictHas`** (`schema.go:58-64`). Hourly stats (`stats.go:104,126,142`) count tombstoned rows. Followers is the only stats query that uses `FINAL`.
10. **`/v1/health` does `SELECT count() FROM events_all`** (`api.go:64`). Usually metadata-cheap per MergeTree; still a needless full-view query on a public liveness probe (and it is `public: true`).
11. **Cross-tier over-fetch:** each tier query already has `LIMIT N`, then Go merges  up to `3N` and truncates (`store.go:217-241`). Fine at N=1000, wasteful with F6/F7 still on.

**Live subscribers**

12. **khatru `notifyListeners` is O(listeners) per published EVENT** (`listener.go:136-149`), under `clientsMutex` for add/remove. Crawler ingest does **not** broadcast (it bypasses khatru). This hits client publishes and scheduler replays only — not firehose — unless you later pipe crawler events into `AddEvent`.
13. **No cap on concurrent `QueryEvents` goroutines.** Each REQ is `go func` + one CH query per tier (`store.go:210`). Many subscribers × EOSE scans is the read-side equivalent of F4.

**Correctness adjacent to perf**

14. **`FlushAll` / shutdown ignore flush errors** (above).
15. **Replaceable read comment vs reality** — serving many kind-0/3 versions also inflates F6/F7 (huge tags JSON).

---

## 5. What to do first

Order is crash risk, then ongoing cost, then cheap wins. Not the audit’s HIGH/MED labels.

| Priority | Work | Why first |
|---|---|---|
| **P0** | **F4 batcher pending/backoff/cap** | CH blip → OOM is the page-at-3am bug. Small, local to `batcher.go`. |
| **P0** | **F19 `ReadHeaderTimeout`** | One field. Do not set `WriteTimeout`. |
| **P1** | **Default `since` + drop unnecessary `FINAL` + query semaphore + split CH pools (F8, F12, missed)** | This is live REQ latency/timeouts at archive scale. Projection can follow once filters have a time bound you can index. |
| **P1** | **F1 read-side `LIMIT 1 BY (kind, pubkey)`** | Stops serving stale profiles/contact lists *today* without touching ingest. |
| **P2** | **F7 select list + F5 drop `idx_content` + F9 Debug** | Low-risk CPU/IO; F5 helps ingest CPU on every part. |
| **P2** | **F3 debounce/skip dict reload** | Must land before any compact job starts writing tombstones in bulk. |
| **P2** | **Background replaceable compact (rest of F1)** | Reclaims permanent-tier kind-3. Depends on P2 tombstone path. |
| **P3** | **Crawler `since` HWM + `AssumeValid` (F14/F15)** | Cuts reconnect bandwidth and duplicate schnorr. |
| **P3** | **Async `OnFlushed` + cap/drop firehose `seen_events` (F16/F17)** | Unblocks the batcher; prevents a multi-GB SQLite file. `[32]byte` keys if you touch `dedup.go`. |
| **P3** | **F13 staging + `EXCHANGE TABLES`; exclude tombstones** | Visible 0-follower window every 5 min. |
| **P4** | **F6 native `Array(Array(String))`** | Real, but a migration. Pair with F11 if ever. |
| **P4** | **F10 fail the REQ on tier error** (not parallel scans) | Correctness. |
| **P4** | **F18 / F20 / F21 / F11** | Only if priority-key count, REST abuse, or a planned schema rev. |

**F11, sonic-for-tags, sharded rate limiter, and parallel tier scans can wait.** They are not what breaks at thousands of events/s.

If you only do three things: **fix the batcher retry (F4), stop unbounded/FINAL global scans (F8+pool), and collapse replaceable kinds on read (F1) without calling `ReplaceEvent` on ingest.**
