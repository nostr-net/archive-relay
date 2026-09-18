# Perf-plan P4 evaluation results & maintainer decisions

Date: 2026-09-18. Orchestrated implementation of `docs/perf-plan-2026-09.md`.
All measurements against the local ClickHouse 26.9.1 instance (300k-row synthetic
tier table, same column/ORDER BY shape as production).

## Item 17 — FINAL-removal eval, THEN projection (§1.4)

### FINAL removal: DONE in code

`QueryEvents` now reads with `ORDER BY created_at DESC, id ASC LIMIT 1 BY kind,
if(kind IN (0,3,10002), pubkey, id) LIMIT n` — no `FINAL`. Rationale beyond
cost, verified empirically:

- `FINAL` on `ReplacingMergeTree(version)` with ORDER BY `(kind, pubkey,
  created_at, id)` only collapses **exact key duplicates** (re-ingests). It
  NEVER collapsed different *versions* of a replaceable event (different
  `created_at` = different sort key) — the tombstone-retire path was carrying
  that load. `LIMIT 1 BY` provides the real "latest per (kind,pubkey)"
  semantics in one pass.
- Empirical check of the codex-corrected group key on live CH 26.9.1:
  same-author `{kind 0 + kind 3}` fixture returns BOTH newest versions with
  the corrected key; the naive key (no `kind`) drops kind 3. Exact-id dupes
  collapse. Multi-version kind-3 → exactly the newest.
- Micro-benchmark (300k rows, 1000-row read, 5 runs): FINAL ≈ 7–9 ms,
  LIMIT 1 BY ≈ 25–37 ms. FINAL is *faster* at this small scale/part count;
  the plan's cost claim against FINAL scales with part count and merge debt.
  The switch is justified by **semantics** (version collapse without the
  retire-before-read dependency), not by this micro-benchmark. `CountEvents`
  keeps FINAL (exact-dup collapse for approximate NIP-45 counts, §1.10).

### Projection `p_feed`: BLOCKED — do not materialize (verified)

`ALTER TABLE events_<tier> ADD PROJECTION …` on `ReplacingMergeTree` fails on
CH 26.9.1 unless `deduplicate_merge_projection_mode` is set to `drop` or
`rebuild`:

```
Code: 344. ADD PROJECTION is not supported in ReplacingMergeTree with
deduplicate_merge_projection_mode = throw. Please set … to 'drop' or 'rebuild'.
```

`drop` disables dedup for parts that have projections — it weakens the exact
duplicate collapse the store relies on for re-ingest idempotence. Decision:
**deferred indefinitely**; revisit only if global-feed p95 demands it AND
dedup idempotence is moved entirely to the seen_events layer first.

## Item 18 — Native tags migration (§1.12): IMPLEMENTED (wave 2)

`tags Array(Array(String))` column added (dual-write with `tags_raw` retained
for rollback); read path reconstructs `nostr.Tags` without JSON; backfill
mutation emitted at startup; stats' `extract(tags_raw, …)` keeps working.
Consumer checklist + rollback notes: `docs/tags-migration-notes.md`.

## Item 19 — M9 / 9735 tier move (§1.13): DEFERRED (data migration required)

Moving 9735 `permanent → social` requires relocating existing rows AND
atomically switching read routing; changing `classify()` alone strands
permanent-tier rows for kind-9735 queries. Recommended default: keep 9735 in
`permanent` forever (receipts are small, kind-9735 queries are rare) or add a
`permanent` TTL — either is a maintainer call (open question §4.1). No code
change shipped.

## Item 20 — Bulk retirement: NOT DONE (by design, §1.13)

Tombstones hide but do not reclaim disk. Logical-only bulk retirement is only
worth running if dict RAM / read overhead justify it AFTER F1 collapse landed.
Deferred; re-measure before scheduling.

## §4 Open questions — defaults adopted (maintainer may override)

1. **9735**: keep in permanent, no TTL (see item 19 above).
2. **defaultSinceHours**: 48h on the pure global-feed shape (config knob,
   `policy.defaultSinceHours`, 0 disables). Shipped default ON.
3. **seen_events**: kept, bounded (rowid-cap prune + drop-with-metric async
   queue). Warm-from-CH remains possible later; not shipped.
4. **Addressable kinds (30000–39999)**: not enabled; classifier unchanged.
   If enabled later: collapse key needs `tag_d` for those kinds +
   `isReplaceableKind` extension (one place each, already commented).
5. **Tombstone hide latency**: bounded ~2s reload accepted; in-process overlay
   NOT implemented (sub-2s window documented). Say the word if post-ACK
   queries must be instantly clean.
6. **"Latest matching version"** semantics accepted (documented on
   QueryEvents).
7. **Shutdown loss window** for ACKed client events: accepted, documented in
   batcher (§1.2). Durable spooling not implemented.
8. **COUNT winner semantics**: documented as approximate (NIP-45), no dedupe.
   Followers/zap-sats DO dedupe winners (id tiebreak / LIMIT 1 BY id).
