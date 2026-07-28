# Archive Relay on Nostr + ClickHouse + khatru — Feasibility Analysis

## TL;DR / Verdict

**Yes, this makes a lot of sense — and ClickHouse is arguably the *best* fit for an archive
relay specifically.** But it is a meaningfully different shape of project than the existing
`eventstore` backends, because ClickHouse is OLAP/append-optimized and nostr has a small set
of mutation semantics (replaceable/addressable events, deletions) that you must design around.

- **Green light** if your goal is: hold *all* events (billions), cheap, with fast time-range /
  kind / author scans and counts, and you can tolerate ~second-level ingest latency.
- **Yellow light** if you need strict single-event OLTP consistency, instant
  read-your-writes, or heavy replaceable-event churn.
- **Do not** try to treat ClickHouse like postgres. The schema and write path must be designed
  for it, or you'll hate it.

The rest of this doc is grounded in the actual source of `khatru`, `nbd-wtf/go-nostr`, and
`fiatjaf/eventstore` (postgres + elasticsearch backends).

---

## 1. The contract khatru/eventstore actually imposes

khatru is **not** interface-driven on the relay side. `khatru.Relay` exposes **hook slices**:

```go
// from khatru/relay.go
type Relay struct {
    RejectEvent   []func(ctx, *nostr.Event) (reject bool, msg string)
    StoreEvent    []func(ctx, *nostr.Event) error
    ReplaceEvent  []func(ctx, *nostr.Event) error
    DeleteEvent   []func(ctx, *nostr.Event) error
    QueryEvents   []func(ctx, nostr.Filter) (chan *nostr.Event, error)
    CountEvents   []func(ctx, nostr.Filter) (int64, error)
    CountEventsHLL []func(ctx, nostr.Filter, offset int) (int64, *hyperloglog.HyperLogLog, error)
    OnEventSaved  []func(ctx, *nostr.Event)
    // ... + negentropy (NIP-77), NIP-86 mgmt, blossom, routing, etc.
}
```

You wire functions in. Example from `examples/basic-postgres`:

```go
relay.StoreEvent   = append(relay.StoreEvent, db.SaveEvent)
relay.QueryEvents  = append(relay.QueryEvents, db.QueryEvents)
relay.CountEvents  = append(relay.CountEvents, db.CountEvents)
relay.DeleteEvent  = append(relay.DeleteEvent, db.DeleteEvent)
relay.ReplaceEvent = append(relay.ReplaceEvent, db.ReplaceEvent)
```

The storage backends implement the small `eventstore.Store` interface (`eventstore/store.go`):

```go
type Store interface {
    Init() error
    Close()
    QueryEvents(context.Context, nostr.Filter) (chan *nostr.Event, error)
    DeleteEvent(context.Context, *nostr.Event) error
    SaveEvent(context.Context, *nostr.Event) error
    ReplaceEvent(context.Context, *nostr.Event) error
}
type Counter interface {
    CountEvents(context.Context, nostr.Filter) (int64, error)
}
```

A `nostr.Filter` is just:

```go
type Filter struct {
    IDs, Authors []string
    Kinds        []int
    Tags         TagMap      // map[string][]string, keys like "e","p","t"
    Since, Until *Timestamp
    Limit        int
    Search       string      // NIP-50
    LimitZero    bool
}
```

**Implication:** you write a `clickhouse.Store` that implements these ~6 methods. Nothing in
khatru forces a row-store model on you. Good.

**There is no existing ClickHouse backend** in `fiatjaf/eventstore` (backends today: postgres,
mysql, sqlite3, badger, lmdb, elasticsearch, opensearch, bluge, mongo, dynamodb, firestore,
turso, edgedb, strfry). So this is net-new but follows an established pattern.

---

## 2. Why ClickHouse is a great fit for an *archive* relay

An archive relay's workload is the textbook OLAP profile:

| nostr operation | postgres does this by… | ClickHouse does this by… |
|---|---|---|
| `REQ {kinds, since, until, limit}` | btree range scan + sort | partition prune + primary-key range scan (its specialty) |
| `REQ {authors, kinds}` | btree IN scan | primary-key / set-index, vectorized |
| `COUNT` | full count, slow at scale | — this is the fastest engine on earth for it |
| `REQ {"#t":[...]}` tag search | GIN on text[] | `Array(String)` + `hasAny` + bloom skip index |
| NIP-50 content search | tsvector/tsquery | `ngrambf_v1` / `tokenbf_v1` skip index + `multiSearchAny` / `position()` |
| storage cost | row store, weak compression | columnar, 5–20× better compression on nostr data |

Why nostr data compresses amazingly well in a columnar store:
- `pubkey` and `kind` have low cardinality and high repetition → dictionary encoding crushes them.
- `created_at` is a monotonic-ish int32 → delta encoding.
- `content` is often empty or templated (reactions, zaps, client metadata) → generic + ngram.
- tags are highly repetitive strings.
- A realistic expectation is **~5–15× smaller** than the equivalent postgres table, often more.
  At archive scale (billions of events) this is the deciding factor.

Plus you get, for free: materialized views for kind-level rollups, TTL/partitioning by month
for cheap retention control, and `CountEventsHLL` (NIP-45) maps directly to ClickHouse HLL.

---

## 3. The hard parts (and how to handle each)

### 3a. ClickHouse hates many small inserts → must batch

The single biggest gotcha. `StoreEvent` is called per event. If you `INSERT` per call,
ClickHouse will create a "part" per insert and fall over (it warns at >1 insert/sec/table).

**Fix:** decouple the khatru hook from the actual insert.
- In `StoreEvent`, push the event into an in-memory channel.
- A background goroutine batches (e.g. flush every 1s OR every 5k–50k events, whichever first)
  and does a single `INSERT ... VALUES` block.
- Return `nil` from `StoreEvent` once buffered (accept the small risk of losing the last batch
  on crash; mitigate with a WAL/queue if you care).

This is the same shape as the elasticsearch backend, which uses
`esutil.NewBulkIndexer` for exactly this reason. You're in good company.

### 3b. Replaceable & addressable events (kinds 0,3,10000–19999, 30000–39999)

postgres `ReplaceEvent` does: query current → if older, delete it → save new. That's a
read-modify-write, which is awkward in ClickHouse.

**Recommended for an archive: don't delete — keep all versions, dedup on read.**
- Use a `ReplacingMergeTree` with `version` column = `created_at`.
- On `QueryEvents`, query with `FINAL` (or do `argMax`/`GROUP BY` dedup in the SELECT) so only
  the latest version of a replaceable/addressable event is returned.
- This is *strictly better for an archive*: you retain full history of profile changes,
  follow-list changes, etc., which a normal relay throws away. That's a feature, not a bug.

If you truly need hard replace semantics, the escape hatch is a small side-table (postgres/
sqlite/Redis) holding the "current" pointer for replaceable kinds, and ClickHouse as the bulk
archive. But start with `ReplacingMergeTree` + `FINAL`.

### 3c. Deletions — the full picture

ClickHouse has **6 distinct deletion mechanisms**, each for a different workload. The mistake
is picking one for everything. Map them to the *nostr* deletion scenarios instead.

#### The 6 ClickHouse mechanisms (grounded in the official `managing-data/deleting-data` docs)

| Mechanism | Syntax | What it actually does | Cost |
|---|---|---|---|
| **Lightweight delete** | `DELETE FROM t WHERE …` | Marks rows deleted immediately via an internal `_row_exists` column; filtered out of all subsequent SELECTs at once. Physical cleanup happens later in normal merges. GA since v23.3. | Sync by default (`lightweight_deletes_sync`); does a mutation internally to mark rows, so some I/O. Breaks projections. |
| **Delete mutation** | `ALTER TABLE t DELETE WHERE …` | Rewrites entire parts matching WHERE. Async by default (`mutations_sync`). | **Very expensive** — rewrites whole parts even for 1 row. Use only when you must guarantee physical removal (legal/GDPR). No atomicity. |
| **ReplacingMergeTree** | engine choice | Doesn't delete — keeps all versions, dedups on read (`FINAL`/`argMax`). | Zero write cost; read-time dedup cost. |
| **CollapsingMergeTree** | engine choice | A `sign` column: `+1` insert, `−1` cancels. On merge the pair disappears. | Zero-mutation delete *if* inserts are ordered; fiddly. |
| **DROP PARTITION** | `ALTER TABLE t DROP PARTITION p` | Removes a whole partition. | Cheap, instant. Requires good partition key. |
| **TTL** | `TTL created_at + INTERVAL 1 YEAR` | Background auto-expiry by expression; can also tier storage (SSD→HDD→S3). | Zero ops; automatic. |

Plus the **application-level tombstone** pattern — not a CH feature, but the most important one
for nostr.

#### The 6 nostr deletion scenarios → recommended mechanism

| Scenario | Volume | Latency need | Use |
|---|---|---|---|
| **Replaceable/addressable supersede** (kinds 0,3,10000–19999,30000–39999) | moderate | dedup on read | **ReplacingMergeTree + FINAL/argMax** — never delete at all |
| **NIP-09 kind-5 deletion request** (author deletes own event) | low–moderate | disappear from reads fast | **Tombstone table + dictionary filter** (see below); optionally periodic mutation to reclaim space |
| **Moderation / illegal content** (relay op removes) | low | guaranteed gone | Tombstone for instant hide, then `ALTER TABLE … DELETE WHERE id IN …` with `mutations_sync=2` for physical removal (legal) |
| **Retention / expiry** ("keep 12 months") | huge | background | **TTL** on `created_at`, **and/or DROP PARTITION** by month |
| **NIP-40 expiration tag** | varies | background | **TTL expression** on `greatest(created_at, expiration)` — or keep khatru's `expirationManager` and issue lightweight deletes |
| **GDPR / wipe-a-pubkey** | bulk per key | guaranteed | `ALTER TABLE … DELETE WHERE pubkey = ?` (mutation), then verify via `system.mutations` |

#### The tombstone + dictionary pattern (the core of NIP-09 / moderation)

This is the single most useful pattern for an archive. It decouples *"hide from reads"* (must be
instant) from *"reclaim disk"* (can wait).

```sql
-- tiny tombstone table; grows slowly, cheap to keep forever
CREATE TABLE tombstones (
  id          String,
  reason      LowCardinality(String),   -- 'nip09' | 'moderation' | 'expired' | …
  deleted_by  String,                    -- pubkey that requested (NIP-09) or 'relay'
  deleted_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree ORDER BY (id, deleted_at);

-- load tombstones into an in-memory dictionary for O(1) point lookups during SELECTs
CREATE DICTIONARY tombstone_dict (
  id String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'tombstones' DB 'nostr'))
LAYOUT(FLAT())        -- or HASHED() / HASHED_ARRAY() if id space is large/sparse
LIFETIME(MIN 10 MAX 60);
```

Your two khatru hooks then become trivial:

```go
// DeleteEvent: just record a tombstone. Instant, batchable, no mutation.
func (s *Store) DeleteEvent(ctx, evt *nostr.Event) error {
    return s.batcher.InsertTombstone(evt.ID, "nip09", evt.PubKey)
}

// QueryEvents: add one predicate to every generated WHERE.
//   ... AND NOT dictHas('tombstone_dict', id)
```

Why this is the right default:
- **Instant visibility** — the moment the tombstone row lands, reads filter it out. No waiting on
  merges or `FINAL`.
- **No expensive mutations on the hot path** — you avoid the #1 ClickHouse anti-pattern
  (frequent `ALTER DELETE`).
- **Tombstones are tiny** — a 32-hex id + enum + int64 ≈ 50–80 bytes. 100M tombstones ≈ a few GB.
- **Fully reversible / auditable** — you keep a record of *what was deleted and why*, which a
  hard-delete throws away. For an archive this is often a requirement.
- **Physical reclamation is decoupled** — run a nightly/weekly batch `ALTER TABLE … DELETE WHERE
  id IN (SELECT id FROM tombstones WHERE reason='nip09')` job if you actually need the disk back,
  and track it in `system.mutations`. Or never bother and rely on partition drops for retention.

The dictionary lookup (`dictHas`) is the fastest way to filter; ClickHouse evaluates it per row
without a join. For very large tombstone counts use `LAYOUT(HASHED_ARRAY())`.

#### A policy question only you can answer: does the archive honor NIP-09 at all?

Archives split into three camps — pick consciously, it shapes the whole deletion subsystem:

1. **Honor deletions fully** (privacy-respecting): tombstone hides, periodic mutation reclaims.
   What the schema above implements.
2. **Ignore deletions** (censorship-resistant / "read-only history"): record kind-5 events as
   data, never hide anything. Simplest — no tombstone machinery at all, just don't wire
   `DeleteEvent`. Document this loudly in your NIP-11 ` limitation` field.
3. **Hybrid**: honor for a window then hard-delete, or honor for personal data but keep for
   everything else. Most flexible, most code.

If you're unsure, start with **#1** — it's the most interoperable with normal clients, and the
schema above makes it cheap.

#### When to actually reach for the expensive `ALTER DELETE` mutation

Rarely. Reserve it for:
- **Legal/CSAM takedowns** where you must guarantee physical erasure on a deadline — tombstone
  first for instant hide, then mutation, then confirm completion in `system.mutations`.
- **GDPR right-to-erasure** on a whole pubkey.
- **Correcting a bad bulk ingest** (e.g. inserted wrong-kind data).

Never use it for: per-event NIP-09 (tombstone instead), replaceable supersede (ReplacingMergeTree
instead), or time retention (TTL/DROP PARTITION instead).

---

### 3d. Storage management — tiered, per-kind retention

Uniform retention is wrong for nostr. A kind-7 reaction and a kind-30023 long-form article are
not worth the same disk. Treat storage as a tiered, policy-driven system, not one big table.

The design has **six layers**, each doing one job:

```
 ingest ─▶ ① kind classifier ─▶ ② tier table (one per retention class)
                                   │
                                   ├─ ③ monthly PARTITION (TTL efficiency + manual nuke)
                                   ├─ ④ TTL expiry        (auto-delete old rows)
                                   ├─ ⑤ TTL MOVE          (NVMe → S3 storage tier)
                                   └─ shared ⑥ tombstone dictionary (deletion, from §3c)
 reads  ─▶ UNION ALL view over all tiers ─▶ same QueryEvents code as before
```

#### ① Classify kinds into retention classes (explicit, configurable)

**Scope is aligned to the sibling `nostrarchives-api` project** (`src/db/repository.rs`), which
treats the **nostr social core** as in-scope and derives everything else. We adopt its kind
scope; the difference is that nostrarchives-api stores only `{0,1,9735}` as events and derives
`{3,6,7,16,10002}` into counters/graphs/upserts, whereas **we store the whole in-scope set as
raw events** (we have no separate counter/graph subsystem — ClickHouse *is* the store). **DMs
and gift wraps are explicitly not stored** (kinds `4`, `13`, `1059`, `10013`, `10050`).

| class | keep | kinds | rationale |
|---|---|---|---|
| `permanent` | ∞ (100y TTL safety) | `0` metadata, `3` contacts, `9735` zap receipt, `10002` relay list | replaceable metadata + high-value zaps; low volume; `FINAL`/`argMax` dedup |
| `archive` | ~10y | `1` text note | the core content — why the archive exists |
| `social` | ~1y | `6` repost, `16` generic repost, `7` reaction | high volume, declining value (nostrarchives collapses these to counters; we keep raw) |
| `transient` | ~30–90d | _(none in scope yet)_ | reserved for future in-scope kinds |
| `drop` | never store | **everything else** — incl. ephemeral `20000–29999` (NIP-01 forbids), **DMs & gift wraps** (`4`, `13`, `1059`, `10013`, `10050`), and all NIPs outside the social core | out of scope |

This makes the relay a **selective social-core archive**, not a firehose archive. The
NIP-11 `supported_nips`/limitation document must state the supported kinds explicitly so clients
know it is not a general-purpose relay. `drop` is enforced at the khatru `RejectEvent` hook —
out-of-scope rows never reach ClickHouse.

**Why no DMs/gift wraps** (revised): gift wraps (`1059`) and seals (`13`) exist specifically to
hide metadata and serve only `p`-tagged recipients (NIP-17/NIP-59). An archive that indexes them
either undermines that guarantee or must gate every `1059` REQ behind NIP-42 AUTH — neither is
worth it for a social-core archive. Legacy NIP-04 DMs (`4`) and the DM-relay metadata kinds
(`10013`, `10050`) are dropped with them, since they have no value without the messages.

#### ② One MergeTree table per retention class

```sql
-- identical schema across all tiers (required for the UNION view)
CREATE TABLE events_permanent (
  id String, pubkey String, created_at UInt32, kind UInt32,
  content String, sig String, tags_raw String,
  tag_e Array(String), tag_p Array(String), tag_t Array(String), tag_d String,
  received_at DateTime64(3) DEFAULT now64(3),
  version UInt32 MATERIALIZED created_at,
  -- storage policies (defined in config.xml): default=NVMe, cold=S3
  INDEX idx_id      id      TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_t   tag_t   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_content content TYPE ngrambf_v1(3,256,2,0) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime(created_at))
ORDER BY (kind, pubkey, created_at, id)
-- ④ + ⑤: per-tier TTL: age to cold storage, then delete. No DELETE rule = keep forever.
TTL created_at + INTERVAL 90 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 'hot';

CREATE TABLE events_archive    (...) ENGINE = ReplacingMergeTree(version) PARTITION BY toYYYYMM(toDateTime(created_at)) ORDER BY (kind, pubkey, created_at, id)
  TTL created_at + INTERVAL 90 DAY TO VOLUME 'cold', created_at + INTERVAL 10 YEAR DELETE
  SETTINGS storage_policy = 'hot';

CREATE TABLE events_social     (...) ENGINE = ReplacingMergeTree(version) PARTITION BY toYYYYMM(toDateTime(created_at)) ORDER BY (kind, pubkey, created_at, id)
  TTL created_at + INTERVAL 1 YEAR DELETE SETTINGS storage_policy = 'hot';

CREATE TABLE events_transient  (...) ENGINE = ReplacingMergeTree(version) PARTITION BY toYYYYMM(toDateTime(created_at)) ORDER BY (kind, pubkey, created_at, id)
  TTL created_at + INTERVAL 30 DAY DELETE  SETTINGS storage_policy = 'hot';
```

Why one table per class (not a single table with conditional TTL):
- **Different storage policies per tier** — only `permanent`/`archive` need the S3 cold tier;
  `social`/`transient` live and die on hot storage. Cleaner than a single policy.
- **Observable policy** — `system.parts` shows size/rows per tier directly; you see "social is
  40GB, archive is 4TB" without querying.
- **Isolated changes** — bumping reactions from 1y→3y is one `ALTER TABLE events_social MODIFY
  TTL`; it can't accidentally affect articles.
- **Replaceable dedup stays local** — all replaceable kinds live in `permanent`, so `FINAL`/
  `argMax` dedup never spans tiers.

The single-table alternative (`TTL created_at + INTERVAL 10 YEAR IF kind IN (...), created_at +
INTERVAL 1 YEAR IF kind IN (...)`) also works and is simpler for small deploys, but you lose
per-tier storage policies and observability. Pick multi-table for anything real.

#### ③ Monthly partitioning — the TTL efficiency + manual escape hatch

`PARTITION BY toYYYYMM(toDateTime(created_at))` on every tier because:
- **TTL deletes whole parts**, which ClickHouse organizes within partitions. Monthly granularity
  means TTL expiry drops clean monthly chunks, not random scattered rows.
- **`DROP PARTITION` is your manual override** — "nuke all March-2024 social events now" is one
  instant statement. Don't use it for steady-state (that's TTL's job), keep it for ops.
- **Don't go finer than monthly** (the docs warn against high-cardinality partition keys — each
  partition is metadata overhead).

#### ④ ⑤ TTL: automatic expiry + transparent storage tiering

TTL is the workhorse — zero ops, runs in background merges, no external scheduler. Two flavors:
- **`… DELETE`** — row gone after the interval. Use for `social`/`transient`.
- **`… TO VOLUME 'cold'` then later `DELETE`** — move to cheap storage as it ages, delete
  eventually. Use for `archive` (NVMe 90d → S3 → delete 10y) and `permanent` (NVMe 90d → S3,
  no delete).

S3-backed MergeTree (`type='s3'` storage policy) is what makes "keep billions of events forever"
affordable. Recent data on NVMe for fast ingest + recent queries; aged data on S3 at ~$23/TB.

A useful synergy: because `ORDER BY (kind, pubkey, created_at, id)` clusters rows of the same
kind together, TTL evaluation per-kind is granule-efficient — whole granules of expiring kinds
expire together during merges.

#### ⑥ Shared tombstone dictionary (from §3c) — unchanged

Deletion is orthogonal to retention. The single `tombstone_dict` covers all tiers: a tombstoned
id is filtered out regardless of which tier table it lives in. No per-tier tombstone logic.

#### Unified read path — the relay code never sees the tiers

```sql
CREATE VIEW events_all AS
  SELECT * FROM events_permanent
  UNION ALL SELECT * FROM events_archive
  UNION ALL SELECT * FROM events_social
  UNION ALL SELECT * FROM events_transient;
```

Your `QueryEvents` builds SQL against `events_all` exactly as before; ClickHouse pushes `WHERE`/
`LIMIT` into each underlying table. One addition: the tombstone predicate
`AND NOT dictHas('tombstone_dict', id)` (uniform) and `FINAL`/`argMax` for replaceable kinds
(which only exist in `permanent`, so it Just Works).

#### Ingest routing — a 20-line classifier

```go
// classify runs in the RejectEvent hook; 'drop' kinds never reach ClickHouse.
func retentionClass(kind int) string {
    switch kind {
    case 0, 3, 9735, 10002:        // replaceable metadata + zap receipts
        return "permanent"   // 0=profile, 3=contacts, 9735=zap, 10002=relay list
    case 1:                            // text notes
        return "archive"
    case 6, 7, 16:                     // reposts + reactions (nostrarchives derives these as
        return "social"            //   counters; we store them raw since we have no graph tier)
    default:
        return "drop"              // everything else: ephemeral, DMs, gift wraps, out-of-scope NIPs
    }
}
```

`StoreEvent` uses the same map to pick the destination table for the batch insert. Make this a
config file (YAML/`map[int]string`), not a compiled-in switch, so you can retune without
redeploying — e.g. the day a client spams a new kind you can demote it to `transient`.

#### What you get
- **Cost control**: the 90% of firehose volume that's reactions/reposts expires fast or never
  hits cold storage; only valuable content reaches S3.
- **Policy as ops**: change a class's TTL or move a kind between classes without touching relay
  code or re-ingesting.
- **Per-tier visibility**: `system.parts`/`system.tables` tell you exactly what each class costs.
- **Smooth knob**: `permanent` is effectively infinite on S3; `drop` is zero; the middle is a
  slider you set per kind. Start conservative, measure actual query patterns, retune.

---

### 3e. Analytics & counters — on-demand for recent, snapshots for all-time

Stats split cleanly into two timescales, and they want different storage:

- **Recent / right-now stats** (today's counts, last-hour trending, engagement on a live note)
  → **compute on demand** over the raw tier tables. Fast, naturally correct, no infrastructure.
- **All-time / historical stats** (a 3-year-old note's lifetime reactions, a year of daily DAU)
  → **snapshot tables** fed by periodic refresh jobs. These survive raw-data TTL.

You've decided all-time stats matter, so the snapshot layer is a **committed** part of the
 design — not escalation. What you do *not* need is materialized views: snapshots recompute from
`FINAL` reads with `count(DISTINCT id)`, so they inherit on-demand's correctness (no dedup trap)
at the cost of daily/hourly staleness, which is fine for stats.

#### Why compute-on-demand is the right default

1. **It has no dedup trap.** This is the decisive point. The raw tables are
   `ReplacingMergeTree`; a duplicate ingest gets deduped on merge, and `count(DISTINCT id)` or a
   `FINAL`/`GROUP BY id` read is exact *regardless* of duplicate inserts. Materialized views fire
   on insert **before** that dedup, so a re-ingested id double-counts unless you add a separate
   batcher-level dedup layer (the trap nostrarchives papers over with its `seen_events` table).
   On-demand queries inherit correctness from the source of truth for free.
2. **ClickHouse is fast enough at social-core volume.** The scope is only kinds
   `0,1,3,6,7,16,9735,10002`. A trending query scans a day of reactions but touches just the
   `kind`, `created_at`, `tag_e`, `id` columns (columnar — it never reads `content`/`sig`/
   `tags_raw`), so it's sub-second even at firehose scale; with a 30–60s application cache
   (Redis/memo) it's effectively free. Per-note engagement rides the bloom skip index on `tag_e`.
3. **Zero schema/migration burden.** No 5 extra tables, 5 MVs, a refresh job, and a dedup layer
   to build, version, backfill, and monitor. Changing an on-demand query is editing SQL; changing
   an MV is drop+recreate+backfill.
4. **It's naturally correct across all the consistency edge cases** (tombstones, replaceable
   versions, NIP-10 e-tag parsing) because you compute over the same filtered, deduped, FINAL view
   the relay itself serves.

#### The concrete on-demand queries (replace the whole counter layer)

```sql
-- per-note engagement, one round-trip for a whole feed of note ids:
SELECT tag_e[1] AS note_id,
       if(kind=7,'reaction', if(kind IN (6,16),'repost','reply')) AS metric,
       count(DISTINCT id) AS n
FROM events_all
WHERE kind IN (1,6,7,16)
  AND hasAny(tag_e, {note_ids:Array(String)})   -- bloom skip index prunes this
GROUP BY note_id, metric;

-- zap count + sats for a note (kind 9735 in permanent):
SELECT count(DISTINCT id) AS zaps,
       sum(toUInt64OrZero(extract(tags_raw,'"amount","([0-9]+)"')[1])) AS sats
FROM events_permanent
WHERE kind = 9735 AND has(tag_e, {note_id:String});

-- trending notes by reactions in last 24h (cache the result 30–60s):
SELECT tag_e[1] AS note_id, count(DISTINCT id) AS reactions
FROM events_social
WHERE kind = 7 AND created_at >= now() - INTERVAL 1 DAY AND length(tag_e) >= 1
GROUP BY note_id ORDER BY reactions DESC LIMIT 50;

-- daily counts (posts/reactions/reposts/zaps):
SELECT toDate(toDateTime(created_at)) AS day,
       multiIf(kind=1,'posts', kind=7,'reactions', kind IN (6,16),'reposts','zaps') AS metric,
       count(DISTINCT id) AS n
FROM events_all
WHERE kind IN (1,6,7,16,9735) AND created_at >= {since:UInt32}
GROUP BY day, metric;

-- today's DAU:
SELECT uniq(pubkey) AS dau FROM events_all WHERE toDate(toDateTime(created_at)) = today();
```

`count(DISTINCT id)` everywhere is what kills the dedup trap — it's marginally slower than
`count()`, so once you've confirmed ingest is idempotent you can drop to plain `count()`.

#### Snapshot layer for all-time & historical stats (committed)

Compute-on-demand can only count what **still exists**. Once kind-7 reactions age out of the
`social` tier after 1y, "all-time reaction count for a 3-year-old note" is impossible on-demand.
Since you care about stats, persist them — but as **snapshot tables fed by periodic refresh
jobs**, not incremental MVs. The key trick is **time-bucket immutability**: bucket stats by
month (or day); each closed bucket is frozen and never recomputed, so the refresh job does
bounded work (process only the current + previous bucket to catch stragglers) and the tables
grow modestly forever.

**1. Per-note-per-month engagement** — the all-time engagement that outlives raw reactions:
```sql
CREATE TABLE stats_note_monthly (
  note_id String,
  month   Date,                              -- toStartOfMonth(created_at)
  metric  LowCardinality(String),            -- reaction|repost|reply|zap
  count   UInt64,
  sats    UInt64
) ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (note_id, month, metric);

-- daily refresh: reprocess only current + previous month (stragglers); older months frozen
DELETE FROM stats_note_monthly WHERE month >= toStartOfMonth(addMonths(today(), -1));
INSERT INTO stats_note_monthly
SELECT
  tag_e[1] AS note_id,
  toStartOfMonth(toDateTime(created_at)) AS month,
  multiIf(kind=7,'reaction', kind IN (6,16),'repost', kind=1,'reply','zap') AS metric,
  count(DISTINCT id) AS count,
  if(kind=9735, sum(toUInt64OrZero(extract(tags_raw,'"amount","([0-9]+)"')[1])), toUInt64(0)) AS sats
FROM events_all
WHERE kind IN (1,6,7,16,9735) AND length(tag_e) >= 1
  AND created_at >= toUnixTimestamp(toStartOfMonth(addMonths(today(), -1)))
GROUP BY note_id, month, metric;
```
All-time engagement for a note = `SELECT metric, sum(count), sum(sats) FROM stats_note_monthly
WHERE note_id={id} GROUP BY metric`. A note's lifetime reactions survive forever *after* the raw
kind-7 rows are gone — at ~1/1000th the bytes.

**2. Daily network rollups** (posts/reactions/reposts/zaps/sats):
```sql
CREATE TABLE stats_daily (
  day Date, metric LowCardinality(String), value UInt64
) ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day) ORDER BY (day, metric);

-- daily refresh: reprocess last 3 days (stragglers); older days frozen
DELETE FROM stats_daily WHERE day >= today() - 3;
INSERT INTO stats_daily
SELECT toDate(toDateTime(created_at)) AS day,
       multiIf(kind=1,'posts', kind=7,'reactions', kind IN (6,16),'reposts', kind=9735,'zaps','') AS metric,
       count(DISTINCT id) AS value
FROM events_all
WHERE kind IN (1,6,7,16,9735) AND created_at >= toUnixTimestamp(today() - 3)
GROUP BY day, metric HAVING metric != '';
```

**3. Daily active authors (DAU)** — needs `uniq`, so `AggregatingMergeTree`:
```sql
CREATE TABLE stats_daily_active (
  day Date, authors AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree ORDER BY day;

DELETE FROM stats_daily_active WHERE day >= today() - 3;
INSERT INTO stats_daily_active
SELECT toDate(toDateTime(created_at)) AS day, uniqState(pubkey) AS authors
FROM events_all WHERE created_at >= toUnixTimestamp(today() - 3) GROUP BY day;
-- read: SELECT day, uniqMerge(authors) AS dau FROM stats_daily_active WHERE day BETWEEN ... ;
```

**4. Per-author follower counts** — the only one needing *frequent* refresh (read on every
profile render, and derives from replaceable kind-3 so it can't be an MV):
```sql
TRUNCATE TABLE author_follower_counts;
INSERT INTO author_follower_counts
SELECT p_tag AS pubkey, count(DISTINCT author) AS followers
FROM (
  SELECT author, arrayJoin(tag_p) AS p_tag
  FROM (SELECT pubkey AS author, tag_p FROM events_permanent WHERE kind = 3
        ORDER BY created_at DESC LIMIT 1 BY pubkey)   -- newest kind-3 per author
) GROUP BY p_tag;
```
Run every ~5–15 min (exactly what nostrarchives does for `profile_search`).

**Why this beats incremental MVs for your case:** each refresh recomputes from the deduped source
(`count(DISTINCT id)` / `FINAL`), so duplicate ingests never inflate the snapshots — the trap that
forces nostrarchives to maintain a `seen_events` table simply doesn't exist here. The cost is that
a snapshot is up to a day stale, which is irrelevant for historical stats. The DELETE-before-
INSERT on the small stats tables is cheap (they're tiny vs. the raw tiers), so it's not the
hot-path mutation problem — it's a once-daily op on aggregate rows.

**Coverage rule of thumb:** on-demand serves anything inside the raw tiers' TTL windows (recent
engagement, today's DAU, live trending); snapshots serve anything historical or all-time. A query
for "this note's engagement" can read just the snapshot (historical note) or snapshot total + an
on-demand delta since the last snapshot (live note) if you need real-time precision.

#### Escalation ladder (snapshots are now rung 2, not a contingency)

1. **Compute on demand** (+ 30–60s app cache) — serves all recent/real-time stats.
2. **Snapshot tables** (above) — committed; serves all all-time/historical stats and follower
   counts. This is the layer you're building because you care about stats.
3. **Incremental materialized view** (`SummingMergeTree`/`AggregatingMergeTree`) — only if a
   snapshot's *daily* refresh latency/cost is measured insufficient for a specific hot stat.
   Earn it with data first; the dedup trap and migration pain live here.

#### Reference: the incremental-MV SQL (only if you reach rung 3)

Kept here so you don't have to re-derive it if a hot path demands it. **Do not build this in v1.**

```sql
CREATE TABLE agg_note_engagement (
  note_id String, metric LowCardinality(String), count UInt64, sats UInt64
) ENGINE = SummingMergeTree
PARTITION BY cityHash64(note_id) % 16 ORDER BY (note_id, metric);

CREATE MATERIALIZED VIEW mv_engagement_social TO agg_note_engagement AS
SELECT tag_e[1] AS note_id, if(kind=7,'reaction','repost') AS metric,
       count() AS count, toUInt64(0) AS sats
FROM events_social WHERE kind IN (6,7,16) AND length(tag_e) >= 1
GROUP BY note_id, metric;
-- (equivalent MVs for kind-1 replies from events_archive and kind-9735 zaps from events_permanent)
```
If you do build these, the load-bearing prerequisite is **idempotent ingest** (batcher skips
already-seen ids) or counters over-count — same problem nostrarchives solves with `seen_events`.

### 3f. Unique-id dedup (`ON CONFLICT (id) DO NOTHING`)

ClickHouse has no real unique constraint. `ReplacingMergeTree` dedups on the sorting key during
merges, and `FINAL` exposes the deduped view. For an archive, occasional duplicates are
acceptable; if you need exact-once reads, dedup with `argMax`/`GROUP BY id` in the query.
Re-ingest idempotency comes for free with `ReplacingMergeTree`.

### 3g. Tags (`#e`, `#p`, `#t`, `#d`, arbitrary)

postgres uses a generated `tagvalues text[]` + GIN. For ClickHouse, pick by access pattern:

- **General solution:** a child table `event_tags (event_id, key, value, idx)` with
  `ORDER BY (key, value, event_id)` and a `tokenbf_v1` skip index. Queries become a semi-join.
  Scales to arbitrary tag keys, which nostr allows.
- **Fast path for hot single-letter tags** (`e`,`p`,`t`,`d`): also store them as separate
  `Array(String)` columns on the main table (`tags_e`, `tags_p`, …) with `has()`/`hasAny()`
  and a bloom skip index. This avoids the join for the 90% case.
- Store the full original tags JSON/Array too, so you never lose data and can reconstruct.

### 3h. Read-your-writes / consistency

ClickHouse is eventually consistent between write and select (parts merge async). For a normal
relay REQ right after publishing, this is a non-issue because the publishing client already has
the event. For a *consumer* querying, sub-second lag is typical and fine for an archive. Use
`FINAL` / `SYSTEM SYNC ...` only where you must.

---

## 4. Proposed schema (sketch)

> This is the **single-table** template. For production, the schema below is replicated
> once per retention class as described in §3d — identical columns, different TTL/storage policy.

```sql
CREATE TABLE events (
  id           String,            -- 32-hex event id
  pubkey       String,
  created_at   UInt32,            -- nostr timestamp (unix seconds)
  kind         UInt32,
  content      String,
  sig          String,
  tags_raw     String,            -- original JSON, lossless
  -- hot-path denormalized tag columns:
  tag_e        Array(String),
  tag_p        Array(String),
  tag_t        Array(String),
  tag_d        String,            -- only ever single value; helps addressable
  received_at  DateTime64(3) DEFAULT now64(3),  -- ingest time, useful for ops
  -- replace/dedup metadata:
  version      UInt32 MATERIALIZED created_at
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime(created_at))
ORDER BY (kind, pubkey, created_at, id)        -- matches the dominant filter shape
SETTINGS index_granularity = 8192;

-- bloom skip indexes for fast IN/has on big columns
ALTER TABLE events ADD INDEX idx_id        id        TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE events ADD INDEX idx_tag_e     tag_e     TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE events ADD INDEX idx_tag_p     tag_p     TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE events ADD INDEX idx_tag_t     tag_t     TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE events ADD INDEX idx_content   content   TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4; -- NIP-50-ish

-- arbitrary tags table for everything else (#r, #a, custom…)
CREATE TABLE event_tags (
  event_id   String,
  key        LowCardinality(String),
  value      String,
  created_at UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(created_at))
ORDER BY (key, value, event_id);
```

Rationale for `ORDER BY (kind, pubkey, created_at, id)`:
- Nostr REQs overwhelmingly filter `kinds` + `authors` + a time window with `LIMIT`.
- Putting kind first also makes `COUNT`/aggregate queries (very common on archives) cheap.
- If your access pattern skews harder to "global firehose by time", consider
  `ORDER BY (created_at, kind, pubkey, id)` instead. Decide from your expected query mix.

Filter → SQL translation is a near-1:1 port of `postgresql/query.go`:
`ids IN`, `pubkey IN`, `kind IN`, `created_at BETWEEN`, `LIMIT N` (default cap, e.g. 1000,
archive relays often raise this), and `hasAny(tag_t, [...])` for tags.

---

## 5. Proposed architecture

```
                ┌──────────────────────────────────────────────┐
   clients ──ws──▶  khatru Relay (Go)                           │
                │   hooks: RejectEvent, QueryEvents, Count…     │
                │      │                                        │
                │      ▼  StoreEvent                            │
                │   in-mem batcher (flush 1s / 10k evts) ──INSERT─┐
                │      │                                        │ │
                │      ▼  QueryEvents                           │ │
                │   clickhouse-go  ──SELECT (FINAL/argMax)──▶  ClickHouse
                │      │                                        │
                │      ▼  DeleteEvent ──▶ tombstone table       │
                └──────────────────────────────────────────────┘
                              │
   backfill/sync ◀── NIP-77 negentropy (uses QueryEvents)
```

- **Ingest**: khatru `StoreEvent` → batched bulk insert.
- **Reads**: `QueryEvents` streams `Rows.Next()` into the channel (same shape as postgres
  `query.go`). Use `FINAL` on replaceable kinds or always, depending on perf.
- **Counts**: trivial `SELECT count()`.
- **Backfill**: NIP-77 negentropy works through `QueryEvents` unchanged — but guard huge
  ranges; an archive can return more than a client expects.
- **Ops**: monthly partitions give you `DROP PARTITION` for cheap retention expiry.
- **Optional**: a tiny postgres/sqlite sidecar for `ReplaceEvent` current-pointer if you want
  strict replace semantics without `FINAL`.

---

## 6. Risks / when to reconsider

- **You need a general-purpose, low-latency relay** (read-your-own-writes, strict replace,
  heavy point reads by id): stay on postgres or strfry. ClickHouse is the wrong tool.
- **You won't batch inserts**: you will have a bad time. This is non-negotiable in CH.
- **You want best-in-class full-text search**: ClickHouse is "ok", not great. Pair with the
  existing `eventstore/elasticsearch` or `opensearch` backend, or the `bluge` one, for NIP-50
  if search quality matters more than storage cost.
- **Operational maturity**: CH clusters (replication/ZooKeeper|Keeper) add ops surface vs a
  single-binary sqlite/lmdb relay. A single CH node is easy; HA is not. An archive can often
  run on one beefy node + S3-backed MergeTree, which keeps this simple.

---

## 7. Recommendation & concrete next steps

1. **Prototype the `eventstore.Store` impl** for ClickHouse as a new subpackage (e.g.
   `eventstore/clickhouse/`), mirroring the postgres backend's file layout (`init.go`,
   `save.go`, `query.go`, `replace.go`, `delete.go`). Wire into a khatru `main.go` like
   `examples/basic-postgres`.
2. **Start with `ReplacingMergeTree` + `FINAL`**, batched inserts, and the hot-path tag
   columns. Skip the side-table until you measure replaceable latency.
3. **Backfill from an existing archive** (e.g. via NIP-77 against a public archive relay) to
   get realistic compression and query numbers.
4. **Benchmark three things** before committing:
   - ingest throughput at batch sizes 1k / 10k / 50k (target: tens of thousands/sec on one node)
   - p99 latency of a typical `REQ {kinds:[1], authors:[...], limit:100}` at 1B+ rows
   - compression ratio vs the same data in postgres
5. If those three look good, this is clearly the right architecture for an archive relay and
   you'd be filling a real gap (no CH backend exists upstream today — it would be a
   contribution candidate).

---

## 8. Sources read

- `khatru/relay.go` — hook-based `Relay` struct (the integration surface).
- `khatru/get-started.go`, `khatru/examples/basic-postgres/main.go` — wiring pattern.
- `khatru/negentropy.go` — NIP-77 flows through `QueryEvents`.
- `nbd-wtf/go-nostr/relay.go`, `filter.go` — `Filter` shape, client side.
- `eventstore/store.go` — the `Store` + `Counter` interface to implement.
- `eventstore/postgresql/{init,query,save,replace}.go` — schema, filter→SQL, replace logic.
- `eventstore/elasticsearch/elasticsearch.go` — the precedent for bulk-batched backends.
- No ClickHouse backend exists in `eventstore` today (confirmed by tree listing).

---

# Appendix: rely (github.com/pippellia-btc/rely) vs khatru for this project

[rely](https://github.com/pippellia-btc/rely) is a newer, independently-written Go relay
framework. It's well-engineered and in several ways nicer than khatru — but **for an archive
relay on ClickHouse specifically, khatru remains the better choice.** Here's the detailed call.

## What rely is good at (genuinely)

- **Cleaner hook DX.** `On.Event` returns an `EventResult` with `Success()` / `Fail()` /
  `NoBroadcast()` / `.WithReply()` — more expressive than khatru's `error`. `On` hooks are single
  functions (not slices), simpler mental model.
- **Better broadcast engine.** The `dispatcher` uses inverted indexes by id/author/tag/kind
  plus a time-window index (`twindow`) for matching live events to subscriptions — explicitly
  strfry-inspired, lock-free on hot paths. This is the strongest part of rely and beats
  khatru's simpler listener model for high-fanout live broadcasting.
- **Principled backpressure.** Each client has a bounded response queue (default
  `responseLimit = 1000`). Before each REQ, `ApplyBudget` scales filter limits down so total
  results can't exceed remaining queue capacity. This protects against greedy/abusive clients
  far more robustly than khatru's rate-limit policies. ("Secure by Design" in the README.)
- **Modern Go.** `slog`, generics, `cmp.Ordered`, `xsync`. Tidy, ~6.4k LOC, single focused
  author.

## Why it's the wrong pick for an archive + ClickHouse

Four concrete blockers, ordered by severity:

### A1. No negentropy / NIP-77

Default `SupportedNIPs = [1, 11, 42]`. A grep of the whole repo finds **zero** negentropy code.
This is the decisive gap: **archive relays sync and backfill via NIP-77 negentropy.** Without it
you're stuck doing `REQ`-based backfill (slower, and further constrained by A2). khatru ships
negentropy out of the box (`khatru/negentropy.go`) and routes it through `QueryEvents`.

### A2. `On.Req` returns a slice, not a channel — and it's capped by queue budget

```go
// rely/hooks.go
Req func(ctx, c Client, id string, filters nostr.Filters) ([]nostr.Event, error)
```

vs khatru/eventstore:
```go
QueryEvents(ctx, nostr.Filter) (chan *nostr.Event, error)  // streams lazily
```

Two consequences:
- **Materialization.** rely requires building the full result set in memory before returning.
  khatru streams. For ClickHouse, `Rows.Next()` maps directly onto a channel; onto a slice it
  means buffering everything (bounded, but still).
- **Hard ceiling per REQ.** `processor.Process` calls `ApplyBudget(remainingCapacity, filters...)`
  *before* your hook runs, rewriting `filter.Limit` down to the client's free queue slots (≤1000).
  So a single REQ can never return more than ~1000 events regardless of what your DB can serve.
  An interactive client works around this by paginating (`until`), but for archive backfills
  this is friction by design. rely is optimized for *interactive* relays; archives want large,
  streamed reads.

### A3. No `eventstore.Store` reuse without an adapter

rely's `On.Req` takes `nostr.Filters` (plural) and returns `[]nostr.Event`. `eventstore.Store`
takes one `Filter` and returns a channel. So you can't drop in the postgres backend (or your
future ClickHouse one) directly — you must write an adapter that loops filters and collects into
a slice. khatru consumes `eventstore.Store` natively (it's the same author's ecosystem). Since
the whole ClickHouse plan centers on building an `eventstore.Store` impl, khatru is the natural
host.

### A4. Younger / narrower

Single contributor, fewer NIPs wired (no blossom, no NIP-86 management API, no routing/
sub-relay muxing, NIP-45 only opt-in via hook). khatru is by fiatjaf (core nostr tooling author),
battle-tested across many deployed relays, and tracks upstream NIPs faster. For a system you'll
operate at archive scale, that maturity matters.

## Summary table

| dimension | khatru | rely | winner for archive+CH |
|---|---|---|---|
| NIP-77 negentropy (backfill/sync) | yes | **no** | khatru |
| Query result streaming | channel | slice (capped) | khatru |
| `eventstore.Store` fit | native | needs adapter | khatru |
| Live broadcast fan-out perf | ok | excellent (inverted idx) | rely |
| Anti-abuse / backpressure | policies | queue-budget (principled) | rely |
| Hook DX | slices of funcs | typed results, single fns | rely |
| NIP coverage (blossom/86/routing) | yes | minimal | khatru |
| Maturity / deployment footprint | large | small | khatru |

## Verdict

- **Use khatru** for the archive relay. The negentropy gap alone is dispositive for an archive,
  and the channel + `eventstore.Store` model is exactly the shape your ClickHouse backend needs.
- **Steal rely's best ideas.** Two are worth borrowing or layering onto khatru:
  1. **Queue-budget backpressure** — implement a `RejectFilter`/`OverwriteFilter` in khatru that
     caps `filter.Limit` to the client's outstanding capacity, like rely's `ApplyBudget`. khatru
     gives you the hooks (`OverwriteFilter`, `policies/ratelimits.go`) to do this.
  2. **Time-windowed subscription matching** — rely's `twindow` idea (only match live events whose
     `created_at` is within `now ± radius`) is a cheap, big win for broadcast CPUs. Consider it if
     your archive also serves live firehose subscriptions.
- **Consider rely instead** only if your relay is primarily *interactive/real-time* (chat, live
  feeds, DVMs) rather than archival — i.e. if you don't need negentropy, bounded REQ results are a
  feature not a bug, and you value broadcast throughput. The README's "Used by" list
  (ContextVM, Zapstore, Vertex, NextBlock) are exactly that shape of project.

## Sources read (rely)

- `rely/README.md` — claims (DX, perf, backpressure, architecture).
- `rely/hooks.go` — `On.Req func(...) ([]nostr.Event, error)`, `On.Event` → `EventResult`,
  default NIPs.
- `rely/processor.go` — `ApplyBudget(remainingCapacity, filters...)` runs *before* `On.Req`;
  results streamed to client then EOSE.
- `rely/client.go` — `RemainingCapacity() = cap(responses) - len(responses)`.
- `rely/options.go` — default `responseLimit = 1000`, default `SupportedNIPs = [1, 11, 42]`.
- `rely/dispatcher.go`, `rely/twindow/twindow.go` — inverted-index + time-window matching.
- `rely/examples/{basic,count}/main.go` — DB wiring shape (you supply `On.Req`).
- Repo-wide grep: no `negentropy`/`nip77` references anywhere.

## Sources read (storage management)

- `nips/01.md` — kind taxonomy (regular / replaceable / ephemeral / addressable ranges), DM kind, ephemeral MUST-NOT-store rule — basis for the retention-class table in §3d.
- `../archives/nostrarchives-api/src/db/repository.rs` (`insert_event` kind routing) — ground-truth
  for the implemented stored/derived kind policy that §3d ① aligns to: stores `{0,1,9735}` as
  events, derives `{3,6,7,16,10002}`, drops the rest.
- `../archives/nostrarchives-api/CLAUDE.md` — architecture overview confirming the kind-routing
  policy and the WoT-gating model.
- `../archives/nostrarchives-api/docs/NIP_REFERENCE.md` — broader aspirational NIP coverage (used
  only for context; the implemented `repository.rs` scope is what §3d follows).

## Sources read (ClickHouse deletion)

- `clickhouse-docs/docs/managing-data/deleting-data/{index,delete_mutations}.md` — the 4-method overview; explicit guidance that RMT/CollapsingMergeTree avoid mutation writes.
- `clickhouse-docs/docs/guides/starter_guides/mutations.md` — `ALTER ... DELETE` vs lightweight `DELETE FROM`, `mutations_sync`.
- `clickhouse-docs/docs/faq/operations/delete-old-data.md` — TTL vs DELETE FROM vs ALTER DELETE vs DROP PARTITION mapping; GA note (lightweight delete GA v23.3).
