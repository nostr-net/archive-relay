# Implementation Plan — Nostr Archive Relay (khatru + ClickHouse)

This plan operationalizes `ANALYSIS.md`. It is the engineering roadmap; the analysis doc is the
*why*. Every decision below is grounded there.

---

## 0. Goal & non-goals

**Goal:** a nostr **archive relay** that ingests the full firehose, stores it cheaply at scale
(billions of events) with **tiered per-kind retention**, honors **NIP-09 deletions** via a
tombstone/dictionary layer, and serves standard `REQ`/`COUNT`/negentropy queries — backed by
**ClickHouse** and built on **khatru**.

**Non-goals (for v1):**
- Not a high-fanout real-time chat relay (khatru's broadcast model is sufficient; rely's
  dispatcher strengths are not the bottleneck for an archive).
- No full-text search quality parity with Elasticsearch (ClickHouse n-gram skip indexes are
  "good enough"; defer a search-engine sidecar to a later phase if needed).
- No HA cluster in v1 — single node + S3 cold tier. HA replication (Keeper/ZooKeeper) is a
  documented later step.

---

## 1. Locked design decisions (from ANALYSIS.md)

| # | Decision | Rationale (section) |
|---|---|---|
| D1 | **khatru** as the relay framework (not rely) | negentropy, channel queries, `eventstore.Store` fit (App. A1–A3) |
| D2 | **ClickHouse** as the only storage (no postgres) | OLAP fit, compression, counts (§2) |
| D3 | **Batched inserts** (decouple `StoreEvent` from INSERT) | the one non-negotiable CH rule (§3a) |
| D4 | **`ReplacingMergeTree(version)` + read-time `argMax`/`FINAL`** for replaceable/addressable | keep full version history (§3b) |
| D5 | **Tombstone table + dictionary** for NIP-09/moderation; **`ALTER DELETE` only for legal/GDPR** | instant hide, no hot-path mutations (§3c) |
| D6 | **One MergeTree table per retention class** + unified `UNION ALL` view | per-tier storage policy & observability (§3d) |
| D7 | **TTL** for auto-expiry, **TTL `TO VOLUME`** for NVMe→S3 tiering, **monthly partitions** + `DROP PARTITION` for manual ops | (§3d ③④⑤) |
| D8 | **Scope = nostr social core, aligned to `nostrarchives-api`** — store kinds `0, 1, 3, 6, 7, 16, 9735, 10002`; **drop everything else** including DMs/gift wraps (`4, 13, 1059, 10013, 10050`) and ephemeral (`20000–29999`) | sibling project's implemented scope (`src/db/repository.rs`); gift wraps exist to hide metadata, not worth archiving (§3d) |
| D9 | **Selective archive, not firehose** — NIP-11 must declare the supported kinds; `RejectEvent` drops out-of-scope kinds before they reach ClickHouse | clients need to know it's not general-purpose (§3d) |
| D10 | **`eventstore.Store` interface** as the boundary — build `eventstore/clickhouse` as a new subpackage, contribute upstream | matches existing postgres/elasticsearch pattern |
| D11 | **Stats = on-demand for recent + snapshot tables for all-time/historical** — no materialized views in v1 | snapshots recompute from `FINAL`/`count(DISTINCT id)` so no dedup trap; time-bucket immutability keeps refresh cheap; TTL-surviving (§3e) |

---

## 2. Tech stack & versions

| Component | Choice | Notes |
|---|---|---|
| Language | Go 1.24+ | khatru + go-nostr require recent Go |
| Relay framework | `github.com/fiatjaf/khatru` | latest |
| nostr lib | `github.com/nbd-wtf/go-nostr` | khatru's dependency |
| Storage interface | `github.com/fiatjaf/eventstore` | implement `Store` + `Counter` |
| DB | ClickHouse **≥ 24.0** | lightweight delete GA since 23.3; modern `dictionary`, `ReplacingMergeTree` improvements |
| CH driver | `github.com/ClickHouse/clickhouse-go/v2` (native protocol) | supports `Rows.Next()` streaming → maps to channel |
| Config | YAML (`kind → class` map, retention durations, CH DSN) | retunable without redeploy (D6/D7) |
| Deploy | single binary + ClickHouse (containerized or bare) | S3 bucket for cold tier |
| Observability | prometheus metrics + `system.*` CH tables | ingest rate, batch sizes, query p99, part counts |

---

## 3. Repository layout

```
archive-relay/
├── ANALYSIS.md                  # the why (done)
├── IMPLEMENTATION_PLAN.md       # this file
├── cmd/
│   └── archive-relay/
│       └── main.go              # khatru wiring, config load, lifecycle
├── internal/
│   ├── store/                   # the eventstore.Store impl (the core)
│   │   ├── store.go             # Store struct, Init, Close, driver setup
│   │   ├── schema.sql           # tier tables, view, dictionary, tombstones (embed)
│   │   ├── save.go              # SaveEvent → batcher
│   │   ├── batcher.go           # decoupled batch insert worker (D3)
│   │   ├── query.go             # QueryEvents: Filter → SQL builder, rows→channel
│   │   ├── count.go             # CountEvents
│   │   ├── replace.go           # ReplaceEvent (argMax/Coordinator — see §6)
│   │   ├── delete.go            # DeleteEvent → tombstone insert (D5)
│   │   ├── filter_sql.go        # nostr.Filter → WHERE clauses + params
│   │   ├── classifier.go        # kind → retention class (config-driven) (D6)
│   │   └── store_test.go
│   ├── policy/
│   │   ├── reject.go            # khatru RejectEvent: drop out-of-scope kinds, sig/id validation
│   │   └── backpressure.go      # port rely's ApplyBudget idea to OverwriteFilter (App. steal #1)
│   ├── retention/               # ops: TTL monitoring, DROP PARTITION helpers, mutations
│   │   ├── ops.go
│   │   └── refresh.go          # periodic follower-count (and other replaceable-state) aggregates
│   ├── api/
│   │   └── stats.go            # stats endpoints: on-demand over raw tables (recent) + snapshot tables (all-time)
│   └── config/
│       └── config.go            # load YAML: CH DSN, class map, durations, batch params
├── migrations/                  # schema is embedded, but keep idempotent DDL here for ops
├── deploy/
│   ├── docker-compose.yml       # relay + clickhouse for local dev
│   └── clickhouse-config.xml    # storage policies: hot (NVMe), cold (S3)
└── examples/
    └── README.md
```

---

## 4. Phased plan

Each phase has a **goal**, **deliverables**, a **validation gate** (objective exit criteria),
and **dependencies**.

### Phase 0 — Spike & validation (1 week)

**Goal:** de-risk the three unknowns before committing: ingest throughput, query latency, and
compression at archive scale. Prove the architecture works before building product around it.

**Deliverables:**
- `internal/store/spike/` throwaway code:
  - `ReplacingMergeTree` table with the §4 schema.
  - Batched insert path (buffer → flush every 1s / N events).
  - `QueryEvents` that streams `Rows.Next()` into a channel with `FINAL`/`argMax`.
- A synthetic event generator (1B+ events, realistic kind/tag distribution — copy the
  postgres testdata distribution as a starting point).
- Backfill a real slice: connect to a public archive relay via negentropy and pull
  ~50M events for one hot kind (e.g. kind 1) to get real compression numbers.

**Validation gate (must hit all three):**
- **Ingest:** ≥ 20k events/sec sustained on a single node at batch size 10k. (Target 50k+.)
- **Query p99:** typical `REQ {kinds:[1], authors:[…], since:…, limit:100}` ≤ 200 ms at ≥1B rows.
- **Compression:** ≥ 6× vs equivalent postgres row store on the same data (expect 8–15×).

**Dependencies:** none. **Decision point:** if any gate fails badly, revisit D2 (ClickHouse) or
the ORDER BY key before proceeding.

---

### Phase 1 — Core `eventstore.Store` backend (2 weeks)

**Goal:** a working `eventstore/clickhouse` package implementing `Store` + `Counter`, against a
**single tier table** (no retention classes yet). Wired into a khatru `main.go` that can accept
events and answer REQs.

**Deliverables:**
- `internal/store/store.go` — `Init()` runs `schema.sql` (idempotent), opens
  `clickhouse-go` native connection pool.
- `schema.sql` (single-table version of §4 schema): `events` table, skip indexes, embedded in
  binary via `//go:embed`.
- `save.go` + `batcher.go` — `SaveEvent` enqueues to a buffered channel; a worker goroutine
  flushes via batch `INSERT ... VALUES`. Configurable: flush interval, max batch size, max
  pending. Backpressure: if queue full, `SaveEvent` returns error → khatru rejects (OK:false).
- `query.go` — `QueryEvents(ctx, filter)` builds parameterized SQL, streams rows to channel,
  respects `ctx.Done()` (so CLOSE cancels mid-stream, as khatru expects).
- `filter_sql.go` — port `postgresql/query.go` filter→SQL logic 1:1:
  `ids IN`, `pubkey IN`, `kind IN`, `created_at BETWEEN`, `hasAny(tag_*, [...])`, `LIMIT`
  (capped by a configurable `QueryLimit`, default 1000 for an archive). Plus the tombstone
  predicate (added in Phase 3).
- `count.go` — `CountEvents` = `SELECT count() FROM events WHERE …`.
- `replace.go` — v1 simplification: rely on `ReplacingMergeTree` + `FINAL` and do **nothing
  special** in `ReplaceEvent` (just `SaveEvent`). Full Coordinator-async version deferred to §6.
- `cmd/archive-relay/main.go` — khatru wiring (mirror `examples/basic-postgres`), config load,
  graceful shutdown that flushes the batcher.

**Validation gate:**
- Pass the `eventstore/internal/checks` conformance suite (the repo ships a generic
  Store-interface test — run it against your impl). This is the single most valuable test.
- A swarm of ws clients (use `tests/swarm`-style load) can publish + REQ round-trip correctly,
  including replaceable/addressable dedup on read.
- Batcher flushes on both triggers (time + size) and on shutdown with zero loss in the steady
  state.

**Dependencies:** Phase 0 gates passed.

---

### Phase 2 — Backfill & negentropy, observability (1 week)

**Goal:** make it a *real* archive by populating it from existing relays, and instrument it.

**Deliverables:**
- Verify `khatru/negentropy.go` flows through `QueryEvents` unchanged (it does by design).
  Stress-test a negentropy sync of a large range; add guards so a peer can't trigger unbounded
  scans (cap concurrent negentropy sessions, cap range size).
- Backfill tooling (a `cmd/backfill` or a `--backfill` mode): connect to N seed relays, run
  negentropy for target kind ranges, write into the batcher.
- Metrics (prometheus): events ingested/sec, batch flush size histogram, queue depth, query
  count + latency histogram, CH part count, CH merge queue depth.
- A `status` HTTP endpoint (reusing khatru's `Router()`): counts per kind, per tier, oldest
  event, tombstone count.

**Validation gate:**
- Successfully backfill ≥ 100M events from a public archive via negentropy; verify event counts
  match the source within dedup tolerance.
- Dashboards show ingest/queue/query health; no unbounded growth under sustained load.

**Dependencies:** Phase 1.

---

### Phase 3 — Deletion subsystem: tombstones + dictionary (1 week)

**Goal:** implement D5. Honor NIP-09; provide the moderation/GDPR escape hatches.

**Deliverables:**
- `schema.sql` additions: `tombstones` table + `tombstone_dict` (see §3c).
- `delete.go` — `DeleteEvent` enqueues a tombstone row (batched like events). Returns nil
  immediately (instant hide). Records `id`, `reason`, `deleted_by`, `deleted_at`.
- `filter_sql.go` — every generated WHERE appends `AND NOT dictHas('tombstone_dict', id)`.
  Benchmark the dictionary lookup to confirm it's negligible at query time.
- Wire khatru's `handleDeleteRequest` flow (it already calls `DeleteEvent` after pubkey/owner
  validation) — verify end-to-end: a kind-5 deletion makes the target vanish from subsequent
  REQs within dictionary refresh (`LIFETIME` ≤ 60s; consider `LIFETIME(0)` + explicit
  `SYSTEM RELOAD DICTIONARY` on tombstone flush for near-instant visibility).
- `internal/policy/reject.go` — relay-operator moderation hook: an authenticated admin endpoint
  (NIP-86 or a simple shared-secret HTTP endpoint on `Router()`) to insert tombstones for
  non-NIP-09 removals.
- `internal/retention/ops.go` — the **physical reclamation** path: a periodic job that, if
  configured, runs `ALTER TABLE … DELETE WHERE id IN (SELECT id FROM tombstones WHERE reason IN
  (...))` with `mutations_sync=2` and logs progress from `system.mutations`. Off by default
  (tombstones are cheap to keep); on for legal/GDPR compliance.

**Validation gate:**
- NIP-09 end-to-end: publish event → verify visible → publish kind-5 → verify gone from REQs
  (after dict refresh) → verify still on disk (`SELECT count()` includes it) → optional mutation
  job physically removes it.
- Tombstone overhead on query latency < 5% vs no-tombstone baseline.

**Dependencies:** Phase 1.

---

### Phase 4 — Tiered retention & storage management (1–2 weeks)

**Goal:** implement D6/D7 — the per-kind retention classes and S3 cold tier. The big cost win.

**Deliverables:**
- `internal/store/classifier.go` — kind → class map, **loaded from YAML**, defaulting to the
  nostrarchives-api scope (D8): `0,3,9735,10002 → permanent`; `1 → archive`; `6,7,16 → social`;
  `default → drop`. No DM/gift-wrap routing.
- `schema.sql` — split into the four tier tables (`events_permanent`, `events_archive`,
  `events_social`, `events_transient`) with identical columns + per-tier `TTL` and
  `SETTINGS storage_policy`. Migrate the Phase-1 single table into the tier tables
  (`INSERT INTO … SELECT`).
- `events_all` view (`UNION ALL` over the four). All read queries target the view; verify
  ClickHouse pushes WHERE/LIMIT into each tier.
- `batcher.go` — routes each event to the correct tier table per the classifier.
- `internal/policy/reject.go` — `RejectEvent` returns reject for `drop`-class kinds
  (ephemeral 20000–29999) so they never reach CH.
- `deploy/clickhouse-config.xml` — define `hot` (local NVMe disk) and `cold` (S3 `type='s3'`)
  storage policies and the `default > cold` move policy. Test the S3 tier locally with minio.
- `internal/retention/ops.go` — TTL monitoring (alert if merge queue backs up), and a
  `DROP PARTITION` helper for manual ops.
- **Selective scope gate:** `RejectEvent` returns reject for any kind not mapped to a tier
  (i.e. `drop`, per D8 — everything outside `{0,1,3,6,7,16,9735,10002}`) so it never reaches
  ClickHouse. The NIP-11 info document lists the supported kinds explicitly so clients know
  this is a selective archive, not a general-purpose relay.

**Analytics/counters deliverables (D11 — two timescales):**
- `internal/api/stats.go` — **recent/real-time stats computed on demand** over the raw tier tables
  (§3e queries): per-note engagement via `hasAny(tag_e,…)` + bloom index, last-24h trending,
  today's counts/DAU. 30–60s application cache on the hot endpoints.
- `schema.sql` additions — the **snapshot tables** that survive raw TTL: `stats_note_monthly`
  (per-note-per-month engagement, `SummingMergeTree`), `stats_daily` (network rollups),
  `stats_daily_active` (DAU, `AggregatingMergeTree` + `uniqState`), `author_follower_counts`.
- `internal/retention/refresh.go` — the periodic refresh jobs (all from §3e): daily job
  reprocesses current+previous month for `stats_note_monthly` and last 3 days for
  `stats_daily`/`stats_daily_active` (time-bucket immutability → bounded work, frozen history);
  `author_follower_counts` refreshes every ~5–15 min. All use `count(DISTINCT id)`/`FINAL` so
  duplicate ingests never inflate them — no `seen_events`-style dedup layer needed.
- `internal/store/batcher.go` — make ingest idempotent (skip already-seen ids) as general hygiene;
  it's also the prerequisite if a stat is ever promoted to an incremental MV (rung 3 of §3e).
- **v1 simplifications** (documented): e-tag target = `tag_e[1]` until NIP-10 parsing is added;
  zap amount from the `amount` tag only (bolt11 decode deferred).
- **Escalation only (do NOT build in v1):** incremental materialized views — only if a snapshot's
  daily refresh latency/cost is *measured* insufficient for a specific hot stat. Reference SQL in §3e.

**Validation gate:**
- The four tier tables show distinct sizes in `system.parts`, matching the classifier's
  expectations (e.g. `social` dominated by kind 7).
- Forcing `created_at` back-dated inserts causes rows to vanish on TTL schedule; `DROP PARTITION`
  on a test month removes exactly that month across the right tier.
- S3 cold tier receives aged rows; queries against aged data still return correct results (just
  slower — measure the latency shift, document it).
- **Stats match raw**: for a controlled test set, (a) recent on-demand SQL and (b) snapshot
  tables both return the same numbers as a brute-force `count(DISTINCT id)` over the raw tables;
  duplicate inserts do not inflate either. After back-dating events into a prior month, the next
  `stats_note_monthly` refresh picks them up (straggler window works).
- After a `social`-tier partition is dropped (simulating TTL), `stats_note_monthly` retains the
  prior months' totals (snapshots outlive raw rows) — on-demand recent-window stats correctly
  exclude the dropped data.
- A kind reclassified in YAML (e.g. demote a spammed new kind to `transient`) takes effect on
  next ingest without recompile.

**Dependencies:** Phase 1. (Phase 3's tombstones already cover deletion uniformly across tiers.)

---

### Phase 5 — Backpressure, abuse, correctness hardening (1 week)

**Goal:** survive hostile/abusive clients and the concurrency edge cases khatru's channel model
exposes.

**Deliverables:**
- `internal/policy/backpressure.go` — port rely's `ApplyBudget` idea: a khatru
  `OverwriteFilter` that scales each filter's `Limit` down to a per-client response budget
  (App. "steal #1"). Prevents a greedy REQ from materializing huge result sets.
- `internal/policy/reject.go` — sensible defaults: per-IP connection limits, rate limits on
  EVENT publish, signature/ID validation (khatru's `policies/sane_defaults.go`), max filter
  breadth (IDs/authors/kinds caps like the postgres backend's `QueryIDsLimit` etc.).
- **Race/concurrency hardening:** the batcher, the query streamer, and khatru's listener model
  share channels. Run with `-race` under the swarm load test for an extended period. Close
  every channel exactly once; guarantee `ctx.Done()` aborts in-flight CH row scans.
- Fuzz the filter→SQL builder (`filter_sql_fuzz_test.go`): random `nostr.Filter` values must
  never produce a SQL injection or a panicking query.

**Validation gate:**
- Swarm test: 8000+ clients, mixed publish/REQ/CLOSE/abrupt-disconnect, no deadlock, no race,
  no OOM, queue depths bounded, latency stable.
- Backpressure demonstrably caps a single client's response volume to its budget.

**Dependencies:** Phases 1, 3, 4.

---

### Phase 6 — Operations, backup, docs (1 week)

**Goal:** make it operable by a human on call.

**Deliverables:**
- Backup story: ClickHouse `BACKUP` to S3 on a schedule; document restore. Note that
  ReplacingMergeTree parts are immutable post-merge, so incremental backups are cheap.
- Runbook (`docs/ops.md`): how to read `system.mutations`, `system.parts`,
  `system.merges`; how to retune TTL; how to reclassify a kind; how to do a GDPR wipe
  (`ALTER TABLE events_permanent DELETE WHERE pubkey = ?`, track in `system.mutations`,
  confirm `is_done=1`).
- NIP-11 document accurately listing: supported NIPs (1, 9, 11, 12, 15, 45, 50, 77),
  supported kinds (`0,1,3,6,7,16,9735,10002`), retention policy summary, and a `limitation`
  note that deletions are tombstoned (not necessarily physically erased) unless the reclamation
  job runs.
- Capacity model: expected bytes/month per tier given firehose volume → S3 cost forecast.

**Validation gate:**
- A dry-run disaster recovery: destroy the node, restore from S3 backup, resume serving within
  the documented RTO.
- Another engineer can retune retention and reclassify a kind from the runbook alone.

**Dependencies:** Phases 3, 4, 5.

---

### Phase 7 — Upstream contribution & HA (stretch)

**Goal:** pay it forward and remove the single-node risk.

- Extract `internal/store` into a clean `eventstore/clickhouse` package matching the upstream
  layout (`init.go`/`save.go`/`query.go`/`replace.go`/`delete.go`) and open a PR to
  `fiatjaf/eventstore`. The tiering/tombstone machinery stays in your relay; the backend itself
  is generic.
- HA: replicate ClickHouse via Keeper. Re-evaluate whether the batcher's fire-and-flush model
  needs a WAL for zero-loss ingest across failover.

---

## 5. Testing strategy

| Layer | Tool | What it proves |
|---|---|---|
| **Conformance** | `eventstore/internal/checks` (existing generic suite) | the Store contract is correctly implemented — the highest-value test |
| **Unit** | Go testing | filter→SQL translation, classifier, batcher flush logic |
| **Fuzz** | Go native fuzz | `nostr.Filter` → SQL never injects/panics |
| **Integration** | testcontainers + real ClickHouse | schema init, read-your-batch-after-flush, dedup, tombstone hide |
| **Load / swarm** | khatru ws clients (port rely's swarm idea) | concurrency, backpressure, no races under `-race` |
| **Property** | rapid/gopter on filter algebra | query results ⊇ expected set invariants |

Gate merges on: conformance green + race-free swarm (30 min) + fuzz (1M execs overnight).

---

## 6. Key implementation notes & traps

- **Batcher is the heart.** Get it right first (Phase 1). Design: per-tier channels (so one
  slow tier doesn't block others), worker per tier, flush on `max(age, size)`, drain on
  shutdown via `ctx` + sync.WaitGroup. Persist a WAL only if Phase 0 shows you need zero-loss.
- **`ReplaceEvent`** — v1: no-op (RMT + `argMax` handles it). If query cost of `FINAL`/`argMax`
  hurts, the upgrade path is a **Coordinator table**: a tiny separate table
  (`MergeTree`, ORDER BY `(kind, pubkey, d_tag, created_at DESC)`) holding the "current" pointer
  for replaceable/addressable kinds, written synchronously in `SaveEvent`; reads `LEFT JOIN` it.
  Defer unless measured.
- **`FINAL` vs `argMax`.** Prefer explicit `argMax` in the SELECT over relying on `FINAL` —
  `FINAL` semantics change across CH versions and can be slow. `argMax(columns..., created_at)`
  grouped by `id` is predictable.
- **Dictionary freshness for tombstones.** `dictHas` reads the in-memory dict, refreshed on
  `LIFETIME`. For near-instant NIP-09 visibility, call `SYSTEM RELOAD DICTIONARY tombstone_dict`
  after each tombstone batch flush (debounced), or use a `LAYOUT(CACHE())`. Trade-off: reload
  cost vs visibility latency.
- **`FINAL` + dictionary + tier view:** compose carefully. The tombstone predicate is a simple
  `AND NOT dictHas(...)` added to the WHERE before `argMax`; verify the query plan with
  `EXPLAIN` so the predicate pushes down into each tier table (it should).
- **Negentropy guard.** An archive can return far more than a peer expects. Cap concurrent
  negentropy sessions and the event-count per session in `OverwriteFilter`; log truncations.
- **S3 latency.** Cold-tier queries are slower. If the dominant archive query is "recent
  events", the 90-day NVMe window handles it; only deep-historical queries hit S3. Document the
  expected p99 split.
- **Timezone/`created_at`** — nostr timestamps are unix seconds (UTC). Store as `UInt32`, never
  `DateTime` with tz, to avoid NIP-17 ±2-day-jitter confusion.

---

## 7. Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Phase 0 gates miss (CH wrong tool) | low | high | spike before building; ORDER BY retunable |
| Batcher data loss on crash | med | med | WAL option behind a flag; size flush to bound loss window |
| `FINAL`/`argMax` too slow at scale | med | med | Coordinator table (§6); measure in Phase 0 |
| Tombstone dict grows unbounded | low | low | periodic `ALTER DELETE` reclamation; dict is `HASHED_ARRAY`, tens of M of ids is fine |
| MV counters over-count from dup ingest | low | low | on-demand default avoids MVs entirely; if promoted later, idempotent batcher + `count(DISTINCT id)` |
| S3 cost surprises | med | med | capacity model in Phase 6; per-kind TTL tuning is the main lever |
| Selective scope surprises clients expecting general-purpose relay | med | low (interop) | declare supported kinds loudly in NIP-11; RejectEvent sends a clear `OK:false` reason |
| CH upgrade breaks `FINAL`/dict semantics | low | med | pin version; test in CI before upgrading |
| Single-node failure | med | high (v1) | Phase 6 backups + documented RTO; Phase 7 HA |

---

## 8. Out of scope / future

- Full-text search sidecar (ES/opensearch/bluge) for high-quality NIP-50 — only if CH n-gram
  indexes prove insufficient.
- Matrioshka / outbox-aware ingestion (NIP-65 relay list following).
- Web UI for exploration / stats.
- Multi-tenant relay (one logical archive per pubkey set).
- Streaming subscriptions to the firehose over a dedicated API.

---

## 9. Milestone summary

| Milestone | Phases | Calendar |
|---|---|---|
| **M0 — Proven architecture** | 0 | week 1 |
| **M1 — Working single-tier relay** | 1, 2 | weeks 2–4 |
| **M2 — Deletions work (NIP-09)** | 3 | week 5 |
| **M3 — Tiered retention + S3 cold storage** | 4 | weeks 6–7 |
| **M4 — Production-hardened** | 5, 6 | weeks 8–9 |
| **M5 — Upstream PR + HA (stretch)** | 7 | week 10+ |

Total to a deployable v1 (M4): **~9 weeks** for one engineer who knows Go + some ClickHouse.
