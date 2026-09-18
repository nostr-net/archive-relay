# Performance improvement plan — archive-relay (v2)

Date: 2026-09-18, rev 2 (incorporates codex review)
Basis: audit `docs/perf-review-2026-09.md`; independent validations by grok
(`docs/perf-review-grok.md`), pi (`docs/perf-review-pi.md`), and codex
(`docs/perf-plan-codex.md`). All contested claims re-verified against source
and, where possible, against the live ClickHouse 26.9.1 instance.

## 0. Codex review disposition

Codex produced 22 findings (2 BLOCKER, ~16 MAJOR, rest MINOR/NIT). Disposition:

| # | Claim | Disposition |
|---|---|---|
| 1 | `LIMIT 1 BY` key merges different replaceable kinds (BLOCKER) | **ACCEPTED** — verified; fix = `LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id)` (tested on CH 26.9.1) |
| 2 | HASHED dict requires UInt64 key, String breaks (BLOCKER) | **REFUTED** — empirically verified on CH 26.9.1: `HASHED()` + `id String` + `dictHas` works. (Historical UInt64 restriction was lifted.) Add a startup dict sanity check anyway (cheap insurance) |
| 3 | Collapse runs after filtering → "latest matching" vs "latest" semantics | **ACCEPTED** — document semantic + tests |
| 4 | Go merge lacks id tiebreak → nondeterministic limit boundary | **ACCEPTED** — sort by `(created_at DESC, id ASC)` in Go too |
| 5 | "Don't drain" insufficient — `case evt := <-b.in` still consumes | **ACCEPTED** — needs a worker state machine, not a flag in flush() |
| 6 | Shutdown loss not recoverable; producers not joined; enqueue has no closed check | **ACCEPTED** — add shutdown sequencing |
| 7 | ReplaceEvent retires before save → full batcher hides old, drops new | **ACCEPTED** — reorder: save first, retire after |
| 8 | Default-since breaks `{until}` and tag-only queries | **ACCEPTED** — inject only for the pure global-feed shape, at `OverwriteFilter`, not in `buildFilterSQL` |
| 9 | Projections can't serve `FINAL` queries → P1 experiment is wasted | **ACCEPTED** — couple FINAL-removal and projection tests; do FINAL first |
| 10 | Closing channel ≠ error to client; `rows.Err()` unchecked; scanEvent must match pruned SELECT | **ACCEPTED** — collect-then-return with synchronous error |
| 11 | Semaphore needs admission policy, not unbounded waiters | **ACCEPTED** — acquire pre-spawn, bounded, release after DB work |
| 12 | `CheckSignature` ignores `evt.ID` → ID-poisoning gap | **ACCEPTED** — verified (`signature.go:15-41`); add `CheckID()` for unseen events |
| 13 | Disconnect time isn't a completeness watermark; priority "full sweep" never fires | **ACCEPTED** — `fullSweepAge` never triggers because every tick refreshes `last_fetched`; needs a separate `last_full_sweep` column |
| 14 | Async OnFlushed can recreate backpressure | **ACCEPTED** — sync MarkMany + bounded drop-with-metric durable queue |
| 15 | Rowid prune off-by-one; O(cap) cutoff; prune loop tied to sources; file never shrinks | **ACCEPTED** — `rowid <=`, own schedule, note VACUUM |
| 16 | Trailing debounce starves; khatru calls DeleteEvent per tag → singleton inserts anyway | **ACCEPTED** — tombstone writer owns all tombstone I/O (buffer→batch→reload ≤1/2s) |
| 17 | Tombstoning ≠ physical compaction; anti-join prune unsafe | **ACCEPTED** — reframe deferred job; add to do-NOT-do |
| 18 | COUNT/followers/zap-sats don't share winner semantics | **ACCEPTED** — define semantics; add id tiebreak to followers query |
| 19 | EXCHANGE TABLES prerequisites (Atomic engine, staging lifecycle) | **ACCEPTED** |
| 20 | Bloom index needs MATERIALIZE INDEX; tags-migration consumer checklist; 9735 tier move strands old rows | **ACCEPTED** |
| 21 | Read/WriteTimeout and maxAge claims too absolute; health count is physical rows | **ACCEPTED** — wording softened |
| 22 | Reorder: dict sanity + error surfacing + collapse into P0; admission before projections | **ACCEPTED** — phases re-cut below |

## 1. Chosen solutions (v2)

### 1.1 F1 — collapse replaceable versions on READ (revised)

In `QueryEvents`, per-tier tail becomes:

```sql
ORDER BY created_at DESC, id ASC
LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id)
LIMIT <n>
```

- `kind` is part of the group key — without it, an author's kind-0 profile and
  kind-3 contacts share the `pubkey` key and one disappears (codex BLOCKER 1;
  verified empirically: corrected key returns newest per `(kind,pubkey)`,
  collapses exact `id` dupes, preserves mixed-kind results).
- Semantic being adopted: **"latest matching version"** — collapse happens
  after WHERE, so a tag/`until`-filtered query can legitimately return an older
  version when the newest doesn't match. This is consistent, documentable, and
  matches what most relays do. If "latest overall, then filter" is ever
  required, winner selection must move into a subquery — revisit only if a
  client needs it.
- Deleting the newest version exposes the previous untombstoned version — this
  is *desired* archive behavior (NIP-09 deletes a specific version, not the
  pubkey's whole history).
- If addressable kinds (30000–39999) are enabled later, extend the key with
  `tag_d` for those kinds and fix `isReplaceableKind`.
- Go-side merge (`sortDesc`) must tiebreak `id ASC` for a deterministic global
  limit boundary across tiers (codex #4).
- Mixed-kind filters: `LIMIT 1 BY` is correct per-kind, so `{kinds:[1,3]}`
  returns all kind-1 notes + latest kind-3 — verified.

Rejected (unchanged from v1): routing crawler through `ReplaceEvent`;
bulk-tombstoning the backlog.

### 1.2 F4 — batcher rework: worker state machine (P0)

Codex #5/#6 sharpened this into a real redesign:

- Worker states: `normal` (consume `in`, flush on size/tick), `retry` (hold
  `pending`, do NOT consume `in` — the select case for `in` must be disabled,
  not just the `drain()` helper), `stopped`.
- `pending` capped at `maxSize` per send attempt; an oversized retained
  backlog splits into `maxSize` chunks; partial-send success keeps the rest.
- Backoff `min(30s, 1s<<fails)` on a retry timer; `stop` and `flushReq` remain
  selectable during backoff (no sleeping through them).
- `enqueue` gets a closed check — after `stop`, return `ErrBatchFull`, never
  accept into a dead worker.
- `FlushAll(ctx) error` propagates flush errors through `Store.FlushAll`.
- Shutdown contract (codex #6): producers are ctx-canceled first; then batchers
  flush with an explicit deadline. Document honestly: crawler events re-ingest
  via dedup, but a client-published event that was ACKed and then lost in a
  shutdown-during-outage is a real (small) loss window — durable spooling or
  post-persistence ACK would be needed to close it fully.

### 1.3 F3 — tombstone writer (revised)

Codex #16: a trailing-edge debounce starves under continuous writes, and khatru
invokes `DeleteEvent` per deletion tag, so batching inside `retireIDs` alone
still produces singleton inserts on the NIP-09 path.

**Design:** a single tombstone writer goroutine owns all tombstone I/O:

- `retireIDs`/`DeleteEvent`/`ReplaceEvent` push ids into a buffered channel.
- Writer coalesces: flush a `PrepareBatch` every ~250ms or 1000 ids, then mark
  the dict dirty. Solves per-tag singletons AND batch inserts.
- A reload worker reloads `tombstone_dict` at a bounded rate (≤1 per ~2s) while
  dirty — periodic, not trailing-edge. Verify `SYSTEM RELOAD` actually applied
  (it can report success on update failure); reload once at shutdown.
- Optional: a small in-process overlay of just-tombstoned ids checked in
  `scanEvent`, covering the sub-2s visibility gap if "delete→invisible must be
  instant" matters. Decide via open question §4.
- Startup sanity check (replacement for codex's refuted blocker): after
  `initSchema`, assert `dictHas` executes (one probe query) so a broken dict
  fails loudly instead of silently serving tombstoned events.
- Prune: NEVER age-based AND never infer safety from row absence (a tombstone
  has no tier; upstream replay can re-add a deleted event). Defer all pruning.

### 1.4 F8/F11/M10 — global-feed cost (revised)

- Inject default `since` via khatru `OverwriteFilter` (has filter + client
  context, runs before RejectFilter) — NOT inside `buildFilterSQL`, which also
  feeds COUNT and REST. Condition: only when `IDs` empty AND `Authors` empty
  AND `Tags` empty AND `Since` nil AND `Until` nil — the pure global-feed
  shape. `{until:…}` historical pagination and tag-only queries stay untouched.
  Config knob `policy.defaultSinceHours` (e.g. 48, 0=disabled).
- `INDEX idx_pubkey pubkey TYPE bloom_filter(0.01) GRANULARITY 4` — emit both
  `ADD INDEX` + `MATERIALIZE INDEX` for existing parts and update fresh DDL.
- **Sequencing fix (codex #9):** ClickHouse projections do not serve queries
  with `FINAL`. So: FIRST run the FINAL-removal evaluation (with §1.1's
  `LIMIT 1 BY` providing dup collapse, measure `EXPLAIN`+a/b on real data);
  ONLY if FINAL is dropped does the `p_feed` projection experiment make sense.
  Moved both to a paired P2/P4 item.
- Keep ORDER BY `(kind, pubkey, created_at, id)` — do not reorder.

### 1.5 F12/M11 — pools + admission (revised)

- Three conns (write ~4 / read ~16 / stats ~2 `MaxOpenConns`), as before.
- Query admission: acquire a bounded semaphore **before** spawning the
  QueryEvents goroutine; bounded waiters with ctx-cancel; release when the
  CH rows are drained (not when the client drains the output channel).
  Route the ReplaceEvent probe and stats-API reads through their own bounds.
  Overflow → synchronous error → khatru NOTICE (see §1.7).

### 1.6 F2 — slim probe + save-then-retire (revised)

- Two-column probe as v1: `SELECT id, created_at … WHERE pubkey=? AND kind=?
  AND NOT dictHas(...) ORDER BY created_at DESC, id ASC LIMIT 50`.
- **Reorder (codex #7): store first, retire after.** Current code tombstones
  old versions before enqueue; once the batcher reliably returns
  `ErrBatchFull`, that sequence hides the old version and then drops the new
  one — leaving the author with nothing served. Save the new version first,
  then retire the superseded ids. (A stored-but-unretired overlap is healed
  by §1.1's read collapse; a retired-but-unstored gap is not.)

### 1.7 F5/F7/F9/F10/F20 — read-path hygiene (revised)

- `selectColumns` (7 cols) for reads — **and update `scanEvent` to match**
  (it currently scans 12 destinations; codex #10).
- Drop `idx_content` (DDL + `DROP INDEX` on existing installs).
- `tier scan` → Debug.
- Error surfacing (codex #10): `QueryEvents` currently returns `(chan, nil)`
  before any SQL runs, so per-tier failures can only ever yield silent partial
  results — khatru treats channel close as success and REST returns 200.
  Since the code already collects-then-sorts, restructure to run the tier
  queries eagerly (bounded), check `rows.Err()` per tier, and return a real
  error synchronously on failure → khatru sends NOTICE. Then stream the
  collected results through the buffered channel.
- `/v1/events`: clamp `id`/`author` to `policy.MaxIDs/MaxAuthors`.

### 1.8 F14/F15/M7 — crawler economics (revised)

- `relay.AssumeValid = true` (field assignment before `Connect`; no ctor arg
  in v0.52.3 — codex #12) on firehose + priority crawlers; keep the single
  `CheckSignature` in `ingest` for unseen ids.
- **Add `evt.CheckID()` for unseen events** (codex #12): `CheckSignature`
  ignores `evt.ID` — a validly-signed event with a mismatched id field would
  poison ID-based dedup/storage. Check ID↔body consistency before `Mark`.
- Reconnect watermark (codex #13): only use `since = lastDisconnect − overlap`
  AFTER a source completed its first EOSE. A disconnect mid-backfill must
  restart the full backfill, not switch to recent-only. Track
  `backfillComplete` per source in memory.
- Priority crawler (codex #13): `fullSweepAge` never fires — `MarkFetched`
  refreshes `last_fetched` every tick, so `time.Since(last)` never reaches
  24h. The "periodic full sweep" is a documented-but-dead feature. Fix by
  persisting a separate `last_full_sweep` column in `crawl_state` and sweeping
  when THAT goes stale. Also: `fetchOverlap` 24h→1–2h; bounded pubkey
  parallelism; one conn per relay per tick with per-subscription (not
  per-conn) timeout contexts.
- Worker pool for verify: only if profiling shows a core saturated — do
  `AssumeValid` first.

### 1.9 F16/F17/M2/M6 — dedup & seen_events (revised)

- `map[[32]byte]struct{}` keys (hex-decode once; validate hex before
  conversion — codex #12). `CheckAndMark` under one lock; `MarkMany` sync in
  `OnFlushed`.
- OnFlushed split (codex #14): in-memory `MarkMany` stays synchronous on the
  flush worker (cheap). The durable `seen_events` write goes to a bounded
  queue of compact `(id,created_at)` records consumed by a separate goroutine;
  on queue-full, drop with a metric — durable dedup is an optimization,
  re-ingest is the safety net. Drain/discard the queue before `cdb.Close()`.
- `seen_events` prune (codex #15): `rowid <= (SELECT rowid … ORDER BY rowid
  DESC LIMIT 1 OFFSET cap)` — `<=`, not `<` (off-by-one leaves cap+1).
  Bound each delete txn. Run the pruner on its own ticker — today it lives in
  `crawler.Run`, so zero sources = no pruning. Note the file doesn't shrink
  after DELETE: enable `auto_vacuum=INCREMENTAL` at open, or periodic VACUUM
  off-hours. At 5k ev/s an hourly prune still lets ~18M rows accumulate —
  tighten cadence or cap with the rate in mind.

### 1.10 F13/M13 — stats (revised)

- Followers rebuild: staging table (same schema) → `EXCHANGE TABLES
  author_follower_counts AND author_follower_counts_staging`. Requires Atomic
  DB engine (default on modern CH — verify at init). Truncate staging BEFORE
  each build; serialize refreshes so two runs can't interleave; after swap,
  staging holds the old snapshot — next build truncates it first.
- Add `NOT dictHas('tombstone_dict', id)` to refresh queries — but note this
  only repairs FUTURE buckets; frozen historical buckets keep tombstoned
  contributions (document; a one-time backfill can repair if it matters).
- Winner semantics (codex #18): the followers query gets the same
  `created_at DESC, id ASC` tiebreak. COUNT still counts every stored version
  — document as a known divergence (NIP-45 counts are approximate anyway), or
  dedupe winners in COUNT via the same `LIMIT BY`-style `uniqExact` on
  `(kind, if(replaceable, pubkey, id))`. Zap `sats` sums undeduplicated rows —
  dedupe zap events before summing `amount`.

### 1.11 M1/M8/F19 — ops trivia (revised wording)

- `/v1/health`: `system.parts` sum over explicit table list, labeled as
  physical rows (includes dupes/tombstoned — it's a size gauge, not a count).
- `http.Server`: `ReadHeaderTimeout: 10s`, `IdleTimeout: 120s`. Keep
  Read/WriteTimeout unset — hijacked WS conns are exempt anyway, but there's
  no reason to set them.
- `config.example.yaml` `maxAge` 1s→5s — with the caveat that `maxSize` still
  triggers flushes at high throughput, so parts/sec isn't solely governed by
  `maxAge` (codex #21).

### 1.12 F6 (+M3, F11) — native tags migration (checklist added)

As v1, plus the consumer checklist codex required: update `tierColumns`/
INSERT, `scanEvent`, the `events_all` view, and stats' `tags_raw` `extract()`
for zap amounts; keep NIP-10 `reply_to` derivation logic (it's a separate
column, not covered by tag accelerators); backfill via `JSONExtract`;
rollback strategy = keep `tags_raw` during transition (dual-write) then drop.
Bundle F11 only if index-RAM measurement justifies.

### 1.13 Deferred decisions (updated)

- **M9 / 9735 tier move**: moving it to `social` requires relocating existing
  rows AND switching write+read routing atomically — changing `classifier`
  alone makes old permanent-tier rows invisible to kind-9735 queries
  (tiersForFilter would stop reading `permanent`). Plan it as a data migration
  or just add a `permanent` TTL instead.
- **Bulk retirement job**: reframe as *logical* retirement only — tombstones
  hide but don't reclaim disk (codex #17). Physical reclamation is a separate
  design (ALTER DELETE/partition ops). Only worth doing if dict RAM + read
  overhead justify it after §1.1 lands.
- **FINAL removal + projection**: paired experiment (§1.4).
- F11, F21: measure-first, as before.

## 2. Phased plan (re-cut per codex #22)

### P0 — crash + correctness (the page-at-3am set)
1. **F4 batcher state machine** (§1.2): pending/backoff/no-drain-while-pending,
   closed check, `FlushAll error`, shutdown sequencing. Acceptance: 10-min CH
   outage under ingest → flat memory, `ErrBatchFull` fires, `stop` mid-backoff
   still flushes-or-logs cleanly.
2. **F1 read collapse** (§1.1): `LIMIT 1 BY kind, if(...)`, `id ASC` tiebreak
   in SQL and Go merge. Acceptance: same-author `{kinds:[0,3]}` returns BOTH
   newest versions; multi-version kind-3 returns exactly newest; dupes collapse.
3. **F10 error surfacing** (§1.7): eager collect + `rows.Err()` + synchronous
   error. Acceptance: killed CH mid-REQ → client gets NOTICE, not silent EOS.
4. **Dict sanity check** at startup (§1.3): one `dictHas` probe post-schema.
5. **M1 health** → `system.parts`; **F19** `ReadHeaderTimeout`;
   **M8** `maxAge` example fix. (one-liners)

### P1 — read path at scale
6. **F8 default-since** via `OverwriteFilter`, pure-feed-shape only (§1.4) +
   `idx_pubkey` with `MATERIALIZE INDEX`.
7. **F12/M11 pool split + admission semaphore** (§1.5) — ahead of any
   projection work (codex #22).
8. **F2 slim probe + save-then-retire** (§1.6).
9. **F3 tombstone writer** (§1.3): buffered ids → periodic batch → bounded
   reload. Acceptance: 100-tag kind-5 → ≤1 insert batch + ≤1 reload.
10. **F5/F7/F9/F20 hygiene**: select-columns + matching `scanEvent`,
    `DROP INDEX idx_content`, Debug, REST clamps.

### P2 — ingest economics
11. **F15 AssumeValid + CheckID + single verify** (§1.8).
12. **F14 backfill-aware reconnect `since`** (§1.8): HWM only post-first-EOSE.
13. **F16 `[32]byte` dedup + CheckAndMark/MarkMany**; bounded async durable
    queue with drop-metric (§1.9).
14. **F17/M2 rowid prune** (off-by-one fixed) on its own ticker + auto-vacuum
    decision (§1.9).
15. **F18 fetchOverlap→1–2h + bounded parallelism + conn reuse + REAL periodic
    full sweep** via new `last_full_sweep` column (§1.8).

### P3 — stats correctness
16. **F13 staging + EXCHANGE** (Atomic-engine check, truncate-staging,
    serialized refresh) + `dictHas` in refreshes + follower id-tiebreak +
    zap dedupe (§1.10).

### P4 — migrations & experiments
17. **FINAL-removal eval, THEN projection** (paired — §1.4, codex #9).
18. **Native tags migration** (§1.12) — bundle F11 iff measured.
19. **M9 9735** decision (§1.13).
20. Optional bulk retirement (logical only — §1.13).

## 3. Do NOT do (expanded)

| Tempting fix | Why it's wrong |
|---|---|
| Route crawler through `ReplaceEvent` | F2's cost × firehose rate; can't heal backlog |
| Drop-oldest on batcher overflow | Silent loss of ACKed events |
| `uint64` dedup keys | Collision permanently loses an archive event |
| `WriteTimeout`/`ReadTimeout` | No reason; keep unset (WS is hijacked anyway) |
| Parallel tier scans | 3× CH load per REQ under fan-out |
| Reconstruct tags from accelerator cols | Loses markers/`amount`/non-accelerator tags |
| Reorder PK to `(kind, created_at)` | Breaks `{kinds, authors}` prefix |
| Age-based tombstone prune | Resurrects deleted permanent events |
| **Infer tombstone safety from row absence** | Tombstones have no tier; replay can re-add the event (codex #17) |
| Hard-reject REQs without `since` | Breaks `nak req -k 1`; inject default instead |
| Retire-before-save in ReplaceEvent | Full batcher hides old + drops new = author left with nothing (codex #7) |
| Disconnect-time `since` watermark during backfill | Abandons unfinished history (codex #13) |
| Projection while queries still use `FINAL` | CH can't serve FINAL from projections — wasted materialization (codex #9) |
| Trailing-edge dict-reload debounce | Starves under continuous writes; use periodic coalescing (codex #16) |
| Assume full sweep runs on schedule | `last_fetched` refreshes every tick — it never fires (codex #13) |

## 4. Open questions for the maintainer

1. **M9**: keep zap receipts in `permanent` forever, add a TTL, or migrate to
   `social` (which needs a real data migration — §1.13)?
2. **§1.4**: is a `policy.defaultSinceHours` (≈48h) on the pure global-feed
   shape acceptable?
3. **§1.9**: keep durable `seen_events` (bounded) or warm dedup from CH
   `received_at` at startup and drop the table?
4. **§1.1**: enable addressable kinds via classifier? If yes, collapse key
   needs `tag_d`.
5. Tombstone hide latency: is bounded ~2s reload OK, or must the post-ACK
   query already be clean (→ needs the in-process overlay)?
6. **§1.1 semantics**: "latest matching version" acceptable, or must
   winner-selection precede tag/time predicates?
7. **§1.2**: is the small shutdown-loss window for ACKed client events
   acceptable, or is durable spooling required?
8. COUNT winner semantics: document as approximate, or dedupe?

## 5. Verification approach

- Unit: `LIMIT BY` collapse incl. **same-author mixed replaceable kinds**,
  equal-timestamp cross-tier limits, dup-id collapse; batcher state machine
  with a failing mock conn (backoff, flush-during-backoff, stop-mid-retry,
  partial chunk failure); dedup `[32]byte` + malformed-hex rejection;
  `CheckID` path.
- Integration (`make test-integration`, CH on :9000): multi-version kind-3
  store→REQ; tag-filtered replaceable query (latest-matching semantics);
  tombstone writer batching + bounded reload; staging-table double-swap;
  outage recovery preserving every accepted event; genuine client-visible
  query failure on tier error.
- Load sanity: `nak` REQ flood + crawler ingest with CH down 10 min; pprof
  (127.0.0.1:6060) heap/goroutines flat + recovery.
- `EXPLAIN` before/after: global-feed query, FINAL-vs-LIMIT-BY a/b, projection
  eligibility — all on the deployed CH version (26.9.1 verified).

Artifacts: `docs/perf-review-2026-09.md` (audit), `docs/perf-review-grok.md`,
`docs/perf-review-pi.md`, `docs/perf-plan-codex.md` (validations), this file.
