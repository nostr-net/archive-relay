package store

import (
	"context"
	"fmt"
	"strings"
)

// tierColumns is the canonical column list for every tier table, in INSERT order.
// (received_at has a DEFAULT; version is MATERIALIZED — neither is inserted.)
const tierColumns = `id, pubkey, created_at, kind, content, sig, tags_raw, tag_e, tag_p, tag_t, tag_d, reply_to`

const tierColumnsType = `
  id           String,
  pubkey       String,
  created_at   UInt32,
  kind         UInt32,
  content      String,
  sig          String,
  tags_raw     String,
  tag_e        Array(String),
  tag_p        Array(String),
  tag_t        Array(String),
  tag_d        String,
  reply_to     String,
  received_at  DateTime64(3) DEFAULT now64(3),
  version      UInt32 MATERIALIZED created_at,
  INDEX idx_id      id      TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_e   tag_e   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_p   tag_p   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_tag_t   tag_t   TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_content content TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
`

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
`, name, tierColumnsType, ttl)
}

// eventsViewDDL builds the UNION ALL view over all tiers (used by stats and
// ad-hoc queries; the relay read path queries tiers directly with FINAL).
func eventsViewDDL() string {
	return `
CREATE OR REPLACE VIEW events_all AS
  SELECT * FROM events_permanent
  UNION ALL SELECT * FROM events_archive
  UNION ALL SELECT * FROM events_social
  UNION ALL SELECT * FROM events_transient;
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
) ENGINE = ReplacingMergeTree ORDER BY pubkey;
`

// initSchema runs all DDL idempotently.
func (s *Store) initSchema(ctx context.Context) error {
	ttl := map[string]string{
		TierPermanent: s.cfg.Retention.Permanent,
		TierArchive:   s.cfg.Retention.Archive,
		TierSocial:    s.cfg.Retention.Social,
		TierTransient: s.cfg.Retention.Transient,
	}
	stmts := []string{}
	for _, t := range activeTiers {
		stmts = append(stmts, tierDDL(t, ttl[t]))
	}
	stmts = append(stmts, eventsViewDDL(), tombstonesDDL, snapshotsDDL)
	for _, q := range stmts {
		// split on ';' — clickhouse-go executes one statement per Exec
		for _, part := range strings.Split(q, ";") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if err := s.ch.Exec(ctx, part); err != nil {
				return fmt.Errorf("schema init failed on %q: %w", truncate(part, 80), err)
			}
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
