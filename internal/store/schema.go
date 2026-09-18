package store

import (
	"context"
	"fmt"
	"strings"
)

// tierColumns is the canonical column list for every tier table, in INSERT order.
// tags_raw is dual-written for rollback; tag_e/p/t/d and received_at have DEFAULT
// expressions and are omitted; version is MATERIALIZED.
const tierColumns = `id, pubkey, created_at, kind, content, sig, tags_raw, tags, reply_to`

// DEFAULT expressions for native tags and the tag_* accelerators. tag_* are
// DEFAULT rather than MATERIALIZED: on CH 26.9.1 MATERIALIZED columns are
// omitted from SELECT *, which would strip accelerators from events_all and
// break stats (tag_e / tag_p). DEFAULT columns are stored, appear in SELECT *,
// and still accept explicit INSERT (rollback of the Go INSERT list).
const (
	tagsDefaultExpr = `JSONExtract(tags_raw, 'Array(Array(String))')`
	tagEDefaultExpr = `arrayMap(x -> x[2], arrayFilter(x -> length(x) >= 2 AND x[1] = 'e', tags))`
	tagPDefaultExpr = `arrayMap(x -> x[2], arrayFilter(x -> length(x) >= 2 AND x[1] = 'p', tags))`
	tagTDefaultExpr = `arrayMap(x -> x[2], arrayFilter(x -> length(x) >= 2 AND x[1] = 't', tags))`
	tagDDefaultExpr = `arrayFirst(x -> length(x) >= 2 AND x[1] = 'd', tags)[2]`
)

func tierColumnsType() string {
	return `
  id           String,
  pubkey       String,
  created_at   UInt32,
  kind         UInt32,
  content      String,
  sig          String,
  tags_raw     String,
  tags         Array(Array(String)) DEFAULT ` + tagsDefaultExpr + `,
  tag_e        Array(String) DEFAULT ` + tagEDefaultExpr + `,
  tag_p        Array(String) DEFAULT ` + tagPDefaultExpr + `,
  tag_t        Array(String) DEFAULT ` + tagTDefaultExpr + `,
  tag_d        String DEFAULT ` + tagDDefaultExpr + `,
  reply_to     String,
  received_at  DateTime64(3) DEFAULT now64(3),
  version      UInt32 MATERIALIZED created_at,
  INDEX idx_id      id      TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_e   tag_e   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_p   tag_p   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_t   tag_t   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_pubkey pubkey TYPE bloom_filter(0.01) GRANULARITY 4
`
}

// tierDDL builds a CREATE TABLE statement for one tier. ttlDelete of "" means
// keep forever. The interval is expressed as e.g. "10 YEAR" / "1 YEAR" / "30 DAY"
// and is added to toDateTime(created_at) (created_at is UInt32, so a raw
// created_at + INTERVAL would type-error). The NVMe→S3 cold-tier move
// (TTL ... TO VOLUME 'cold') is an ops-layer concern configured in
// clickhouse-config.xml; it is intentionally NOT emitted here so the DDL works
// against any default single-node install.
func tierDDL(name, ttlDelete string) string {
	ttl := ""
	if ttlDelete != "" {
		ttl = "\n  TTL toDateTime(created_at) + INTERVAL " + ttlDelete + " DELETE"
	}
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS events_%[1]s (%[2]s
) ENGINE = ReplacingMergeTree(version)
  PARTITION BY toYYYYMM(toDateTime(created_at))
  ORDER BY (kind, pubkey, created_at, id)%[3]s
  SETTINGS index_granularity = 8192;
`, name, tierColumnsType(), ttl)
}

// eventsViewDDL builds the UNION ALL view over all tiers (used by stats and
// ad-hoc queries; the relay read path queries tiers directly with
// LIMIT 1 BY collapse — FINAL survives only in CountEvents).
func eventsViewDDL() string {
	return `
CREATE OR REPLACE VIEW events_all AS
  SELECT * FROM events_permanent
  UNION ALL SELECT * FROM events_archive
  UNION ALL SELECT * FROM events_social;
`
}

const tombstonesDDL = `
CREATE TABLE IF NOT EXISTS tombstones (
  id         String,
  reason     LowCardinality(String),
  deleted_by String,
  deleted_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY (id, deleted_at);

CREATE DICTIONARY IF NOT EXISTS tombstone_dict (
  id String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'tombstones'))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 60);
`

// snapshotsDDL creates the stats tables. Refresh jobs populate them; they survive
// raw-data TTL.
const snapshotsDDL = `
CREATE TABLE IF NOT EXISTS stats_note_monthly (
  note_id String,
  month   Date,
  metric  LowCardinality(String),
  count   UInt64,
  sats    UInt64
) ENGINE = SummingMergeTree
  PARTITION BY toYYYYMM(month)
  ORDER BY (note_id, month, metric);

CREATE TABLE IF NOT EXISTS stats_daily (
  day    Date,
  metric LowCardinality(String),
  value  UInt64
) ENGINE = SummingMergeTree
  PARTITION BY toYYYYMM(day)
  ORDER BY (day, metric);

CREATE TABLE IF NOT EXISTS stats_daily_active (
  day     Date,
  authors AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree ORDER BY day;

CREATE TABLE IF NOT EXISTS author_follower_counts (
  pubkey    String,
  followers UInt64
) ENGINE = MergeTree ORDER BY pubkey;

-- Staging twin for the followers swap: refresh builds into staging, then
-- EXCHANGE TABLES swaps atomically (P3 F13). Truncated before each build.
CREATE TABLE IF NOT EXISTS author_follower_counts_staging (
  pubkey    String,
  followers UInt64
) ENGINE = MergeTree ORDER BY pubkey;
`

// execParts splits a multi-statement DDL blob and Execs each piece.
func (s *Store) execParts(ctx context.Context, q string) error {
	for _, part := range strings.Split(q, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if err := s.wch.Exec(ctx, part); err != nil {
			return fmt.Errorf("schema init failed on %q: %w", truncate(part, 80), err)
		}
	}
	return nil
}

// initSchema runs all DDL idempotently.
func (s *Store) initSchema(ctx context.Context) error {
	ttl := map[string]string{
		TierPermanent: s.cfg.Retention.Permanent,
		TierArchive:   s.cfg.Retention.Archive,
		TierSocial:    s.cfg.Retention.Social,
	}
	for _, t := range activeTiers {
		if err := s.execParts(ctx, tierDDL(t, ttl[t])); err != nil {
			return err
		}
	}
	if err := s.execParts(ctx, tombstonesDDL); err != nil {
		return err
	}
	if err := s.execParts(ctx, snapshotsDDL); err != nil {
		return err
	}
	for _, tier := range activeTiers {
		table := "events_" + tier
		// Was idx_pubkey just created (fresh/migrated install)? Only then queue
		// a MATERIALIZE mutation — running it every startup would re-queue a
		// mutation on all parts each boot (grok review #7).
		var haveIdx uint64
		if err := s.wch.QueryRow(ctx,
			"SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = ? AND name = 'idx_pubkey'",
			table).Scan(&haveIdx); err != nil {
			return fmt.Errorf("index introspection failed for %q: %w", table, err)
		}
		for _, ddl := range []string{
			"ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS tags Array(Array(String)) DEFAULT " + tagsDefaultExpr + " AFTER tags_raw",
			"ALTER TABLE " + table + " MODIFY COLUMN tags Array(Array(String)) DEFAULT " + tagsDefaultExpr,
			"ALTER TABLE " + table + " MODIFY COLUMN tag_e Array(String) DEFAULT " + tagEDefaultExpr,
			"ALTER TABLE " + table + " MODIFY COLUMN tag_p Array(String) DEFAULT " + tagPDefaultExpr,
			"ALTER TABLE " + table + " MODIFY COLUMN tag_t Array(String) DEFAULT " + tagTDefaultExpr,
			"ALTER TABLE " + table + " MODIFY COLUMN tag_d String DEFAULT " + tagDDefaultExpr,
			"ALTER TABLE " + table + " ADD INDEX IF NOT EXISTS idx_pubkey pubkey TYPE bloom_filter(0.01) GRANULARITY 4",
			"ALTER TABLE " + table + " DROP INDEX IF EXISTS idx_content",
		} {
			if err := s.wch.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("schema column/index migration failed on %q: %w", truncate(ddl, 80), err)
			}
		}
		if haveIdx == 0 {
			if err := s.wch.Exec(ctx, "ALTER TABLE "+table+" MATERIALIZE INDEX idx_pubkey"); err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "already exists") {
					s.log.Info("pubkey index materialization already exists", "table", table, "err", err)
				} else {
					s.log.Warn("pubkey index materialization failed; will retry at next startup", "table", table, "err", err)
				}
			}
		}
		// Mutation is queued (mutations_sync=0); do not wait for is_done.
		backfill := "ALTER TABLE " + table + " UPDATE tags = JSONExtract(tags_raw, 'Array(Array(String))') WHERE empty(tags)"
		if err := s.wch.Exec(ctx, backfill); err != nil {
			s.log.Warn("tags backfill mutation failed; will retry at next startup", "table", table, "err", err)
		} else {
			s.log.Info("tags backfill mutation submitted", "table", table)
		}
	}
	// Recreate the view after every tier has the same columns so SELECT * unions.
	if err := s.execParts(ctx, eventsViewDDL()); err != nil {
		return err
	}
	var found uint8
	if err := s.wch.QueryRow(ctx, "SELECT dictHas('tombstone_dict', '0000000000000000000000000000000000000000000000000000000000000000')").Scan(&found); err != nil {
		return fmt.Errorf("tombstone dictionary startup sanity probe failed: %w", err)
	}

	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
