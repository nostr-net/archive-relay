// Package control is the embedded SQLite control plane for the relay: the
// small mutable, high-frequency-update state that ClickHouse is bad at
// (crawler queue/progress, scheduled events, auth challenges). It is a
// library-backed .db file, NOT a separate server — part of the one binary.
package control

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGo)
)

const schema = `
CREATE TABLE IF NOT EXISTS crawl_state (
  pubkey        TEXT PRIMARY KEY,
  last_fetched  INTEGER NOT NULL DEFAULT 0,
  tier          INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_crawl_state_tier ON crawl_state(tier, last_fetched);

CREATE TABLE IF NOT EXISTS scheduled_events (
  id         TEXT PRIMARY KEY,          -- the future-dated event id
  event_json TEXT NOT NULL,             -- full serialized event
  publish_at INTEGER NOT NULL,          -- created_at, when to publish
  taken_by   TEXT                       -- worker lease; NULL = available
);
CREATE INDEX IF NOT EXISTS idx_scheduled_due ON scheduled_events(publish_at, taken_by);

CREATE TABLE IF NOT EXISTS auth_challenges (
  challenge TEXT PRIMARY KEY,
  pubkey    TEXT,
  created_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS seen_events (
  id TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL DEFAULT 0
);

-- Customers granted relay access. Mutated via NIP-86 management RPC (and later
-- the freedompay webhook). Static always-allowed keys live in config instead.
CREATE TABLE IF NOT EXISTS allowed_pubkeys (
  pubkey     TEXT PRIMARY KEY,
  note       TEXT,
  created_at INTEGER NOT NULL DEFAULT 0
);
`

// DB wraps the SQLite connection used by crawler, scheduler, and auth.
type DB struct {
	conn *sql.DB
	log  *slog.Logger
}

// Open opens (creating if absent) the control DB and ensures the schema.
func Open(path string, log *slog.Logger) (*DB, error) {
	conn, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; a small pool is plenty and lets query vs write overlap.
	conn.SetMaxOpenConns(4)
	if _, err := conn.Exec(schema); err != nil {
		return nil, fmt.Errorf("control schema init: %w", err)
	}
	log.Info("control db ready", "path", path)
	return &DB{conn: conn, log: log}, nil
}

func (d *DB) Close() error { return d.conn.Close() }

// Conn exposes the underlying *sql.DB for subsystems that need direct access
// (crawler queue with FOR UPDATE SKIP LOCKED semantics, scheduler leases).
func (d *DB) Conn() *sql.DB { return d.conn }

// MarkSeen records an event id as ingested, for idempotent batcher dedup.
// Returns true if it was newly inserted (i.e. not seen before).
func (d *DB) MarkSeen(ctx context.Context, id string, createdAt int64) (bool, error) {
	res, err := d.conn.ExecContext(ctx,
		"INSERT OR IGNORE INTO seen_events(id, created_at) VALUES(?, ?)", id, createdAt)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// PruneSeen deletes seen_events rows older than maxAge. seen_events is only
// useful for backfill dedup (days/weeks), not archival — without pruning it
// grows ~one row per ingested event forever, which at firehose scale is a real
// disk problem. Call periodically (e.g. hourly, maxAge ~7 days).
func (d *DB) PruneSeen(ctx context.Context, maxAge time.Duration) (int64, error) {
	cutoff := time.Now().Add(-maxAge).Unix()
	res, err := d.conn.ExecContext(ctx, "DELETE FROM seen_events WHERE created_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// AllowedPubkey is one row of the dynamic allow-list.
type AllowedPubkey struct {
	Pubkey    string
	Note      string
	CreatedAt int64
}

// AllowPubkey grants access to a pubkey (INSERT OR REPLACE so it's idempotent).
func (d *DB) AllowPubkey(ctx context.Context, pubkey, note string) error {
	_, err := d.conn.ExecContext(ctx,
		"INSERT OR REPLACE INTO allowed_pubkeys(pubkey, note, created_at) VALUES(?, ?, ?)",
		pubkey, note, time.Now().Unix())
	return err
}

// RevokePubkey removes a pubkey's access. No-op (no error) if not present.
func (d *DB) RevokePubkey(ctx context.Context, pubkey string) error {
	_, err := d.conn.ExecContext(ctx, "DELETE FROM allowed_pubkeys WHERE pubkey = ?", pubkey)
	return err
}

// ListAllowed returns every dynamic allow-list row, oldest first.
func (d *DB) ListAllowed(ctx context.Context) ([]AllowedPubkey, error) {
	rows, err := d.conn.QueryContext(ctx,
		"SELECT pubkey, note, created_at FROM allowed_pubkeys ORDER BY created_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AllowedPubkey
	for rows.Next() {
		var a AllowedPubkey
		if err := rows.Scan(&a.Pubkey, &a.Note, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// LoadAllowedSet returns the set of allowed pubkeys for an in-memory cache.
func (d *DB) LoadAllowedSet(ctx context.Context) (map[string]struct{}, error) {
	rows, err := d.conn.QueryContext(ctx, "SELECT pubkey FROM allowed_pubkeys")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		out[pk] = struct{}{}
	}
	return out, rows.Err()
}

// MarkFetched records that a priority-crawl pubkey was just fetched (used by
// the per-pubkey crawler for last_fetched visibility). tier is preserved.
func (d *DB) MarkFetched(ctx context.Context, pubkey string) error {
	now := time.Now().Unix()
	_, err := d.conn.ExecContext(ctx, `
INSERT INTO crawl_state(pubkey, last_fetched, tier, updated_at)
VALUES(?, ?, COALESCE((SELECT tier FROM crawl_state WHERE pubkey = ?), 0), ?)
ON CONFLICT(pubkey) DO UPDATE SET last_fetched = excluded.last_fetched, updated_at = excluded.updated_at`,
		pubkey, now, pubkey, now)
	return err
}
