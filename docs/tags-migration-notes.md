# Native tags migration notes (P4-18 / §1.12)

Dual-write of `tags Array(Array(String))` alongside `tags_raw String`. The
read path (`QueryEvents` / `scanEvent`) reconstructs `nostr.Tags` from the
native column with no per-row JSON unmarshal (F6).

## Rollback

`tags_raw` is still inserted on every row. Rolling the Go read path back to
`SELECT tags_raw` + `json.Unmarshal` is safe. The INSERT list omits
`tag_e/tag_p/tag_t/tag_d` because those columns are `DEFAULT`-derived from
`tags`; an older INSERT list that still supplies `tag_*` (and omits `tags`)
is also accepted: `tags` itself has `DEFAULT JSONExtract(tags_raw, …)`.

Do **not** drop `tags_raw` until the stats consumer below is migrated.

## Dropping `tags_raw` later

`internal/stats/stats.go` `RefreshNoteMonthly` extracts zap amounts with:

```sql
toUInt64OrZero(extract(tags_raw, '"amount","([0-9]+)"'))
```

That query must be rewritten against the native `tags` column (for example
`arrayFirst(x -> length(x) >= 2 AND x[1] = 'amount', tags)[2]`) before
`tags_raw` can be dropped. Arbitrary-tag filters in `buildFilterSQL` also
still substring-scan `tags_raw`; move those to `arrayExists` on `tags` in
the same change.

## NIP-10 `reply_to`

Unchanged. Direct-parent resolution stays a Go-computed, inserted column.
Tag accelerators (`tag_e/p/t/d`) do not cover reply-marker logic.

## `events_all`

The view is `SELECT *` UNION ALL of the three tiers. Every tier receives the
same `ADD COLUMN` / `MODIFY COLUMN` ALTERs, then the view is `CREATE OR
REPLACE`d so column lists match. `tag_*` are `DEFAULT` (not `MATERIALIZED`)
so they remain visible through `SELECT *` for stats.

## DEFAULT vs MATERIALIZED (CH 26.9.1)

`MATERIALIZED` derivation of `tag_e/p/t/d` from `tags` works, including
`ALTER TABLE MODIFY COLUMN … MATERIALIZED` on existing tables with bloom
indexes. It is **not** used: ClickHouse omits `MATERIALIZED` columns from
`SELECT *`, which would hide `tag_e`/`tag_p` from `events_all` and break
`RefreshNoteMonthly` / `RefreshFollowers`. `DEFAULT` derivation is stored,
appears in `SELECT *`, and keeps the bloom indexes.

F11 (`FixedString` ids) is not bundled.
