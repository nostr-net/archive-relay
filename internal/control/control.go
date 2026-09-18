// Package control is the embedded SQLite control plane for the relay: the
// small mutable, high-frequency-update state that ClickHouse is bad at
// (crawler progress, scheduled events, dedup, allow-list). It is a
// library-backed .db file, NOT a separate server — part of the one binary.
package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGo)
)

const schema = `
CREATE TABLE IF NOT EXISTS crawl_state (
  pubkey          TEXT PRIMARY KEY,
  last_fetched    INTEGER NOT NULL DEFAULT 0,
  last_full_sweep INTEGER NOT NULL DEFAULT 0,
  updated_at      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scheduled_events (
  id         TEXT PRIMARY KEY,          -- the future-dated event id
  event_json TEXT NOT NULL,             -- full serialized event
  publish_at INTEGER NOT NULL           -- created_at, when to publish
);
CREATE INDEX IF NOT EXISTS idx_scheduled_due ON scheduled_events(publish_at);

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
//
// auto_vacuum=INCREMENTAL is set via DSN so it applies before CREATE TABLE on a
// fresh file (SQLite ignores a mid-life auto_vacuum change unless VACUUM is
// run). Existing databases keep their original auto_vacuum mode until an
// operator VACUUMs them; we still emit the pragma so new files get it.
func Open(path string, log *slog.Logger) (*DB, error) {
	conn, err := sql.Open("sqlite", path+"?_pragma=auto_vacuum(INCREMENTAL)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; a small pool is plenty and lets query vs write overlap.
	conn.SetMaxOpenConns(4)
	if _, err := conn.Exec(schema); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("control schema init: %w", err)
	}
	db := &DB{conn: conn, log: log}
	if err := db.migrateCrawlState(context.Background()); err != nil {
		_ = conn.Close()
		return nil, err
	}
	log.Info("control db ready", "path", path)
	return db, nil
}

// migrateCrawlState adds last_full_sweep if an older crawl_state table lacks
// it. Idempotent: Open twice (or a DB created with the current schema) is a
// no-op after the column exists.
func (d *DB) migrateCrawlState(ctx context.Context) error {
	rows, err := d.conn.QueryContext(ctx, "PRAGMA table_info(crawl_state)")
	if err != nil {
		return fmt.Errorf("crawl_state table_info: %w", err)
	}
	defer rows.Close()
	has := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, colType string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "last_full_sweep" {
			has = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if has {
		return nil
	}
	_, err = d.conn.ExecContext(ctx,
		"ALTER TABLE crawl_state ADD COLUMN last_full_sweep INTEGER NOT NULL DEFAULT 0")
	if err != nil {
		return fmt.Errorf("add last_full_sweep: %w", err)
	}
	return nil
}

func (d *DB) Close() error { return d.conn.Close() }

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

// SeenRef is one (id, created_at) pair for a batched MarkSeen.
type SeenRef struct {
	ID        string
	CreatedAt int64
}

// MarkSeenBatch records many seen ids in a single transaction. Used by the
// post-flush hook so a whole ClickHouse batch costs one commit, not one
// autocommit per event.
func (d *DB) MarkSeenBatch(ctx context.Context, items []SeenRef) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx,
		"INSERT OR IGNORE INTO seen_events(id, created_at) VALUES(?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, it := range items {
		if _, err := stmt.ExecContext(ctx, it.ID, it.CreatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadRecentSeen returns up to limit most-recently-recorded seen ids (insert
// order via rowid), used to warm the in-memory dedup cache after a restart so
// recent events aren't re-ingested.
func (d *DB) LoadRecentSeen(ctx context.Context, limit int) ([]string, error) {
	rows, err := d.conn.QueryContext(ctx,
		"SELECT id FROM seen_events ORDER BY rowid DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
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

// pruneSeenChunk is the max seen_events rows deleted per transaction so a
// cap-prune cannot stall the SQLite pool (busy_timeout 5s) with a single
// multi-million-row DELETE. Tests may lower it to exercise the chunk loop.
var pruneSeenChunk = 50_000

// PruneSeenByRowid deletes the oldest seen_events rows so at most cap newest
// rows remain. Deletes run in chunks of pruneSeenChunk, each in its own
// transaction, then a bounded `PRAGMA incremental_vacuum(1000)` reclaims a
// limited number of free pages. cap <= 0 is a no-op.
func (d *DB) PruneSeenByRowid(ctx context.Context, cap int) (int64, error) {
	if cap <= 0 {
		return 0, nil
	}
	var count int64
	if err := d.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM seen_events").Scan(&count); err != nil {
		return 0, err
	}
	var deleted int64
	for count > int64(cap) {
		n := int64(pruneSeenChunk)
		if extra := count - int64(cap); n > extra {
			n = extra
		}
		tx, err := d.conn.BeginTx(ctx, nil)
		if err != nil {
			return deleted, err
		}
		res, err := tx.ExecContext(ctx, `
DELETE FROM seen_events WHERE rowid IN (
  SELECT rowid FROM seen_events ORDER BY rowid ASC LIMIT ?
)`, n)
		if err != nil {
			_ = tx.Rollback()
			return deleted, err
		}
		got, _ := res.RowsAffected()
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return deleted, err
		}
		if got == 0 {
			break
		}
		deleted += got
		count -= got
	}
	// Bounded reclaim: 1000 pages per prune, not an unbounded vacuum of the
	// whole free-list (which can stall MarkSeenBatch behind busy_timeout).
	_, _ = d.conn.ExecContext(ctx, "PRAGMA incremental_vacuum(1000)")
	return deleted, nil
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

// LastFetched returns the last successful priority-crawl time for a pubkey
// (0 if the pubkey was never crawled).
func (d *DB) LastFetched(ctx context.Context, pubkey string) (int64, error) {
	var ts int64
	err := d.conn.QueryRowContext(ctx,
		"SELECT last_fetched FROM crawl_state WHERE pubkey = ?", pubkey).Scan(&ts)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return ts, err
}

// MarkFetched records that a priority-crawl pubkey was just fetched (used by
// the per-pubkey crawler for last_fetched visibility and incremental fetches).
func (d *DB) MarkFetched(ctx context.Context, pubkey string) error {
	now := time.Now().Unix()
	_, err := d.conn.ExecContext(ctx, `
INSERT INTO crawl_state(pubkey, last_fetched, updated_at) VALUES(?, ?, ?)
ON CONFLICT(pubkey) DO UPDATE SET last_fetched = excluded.last_fetched, updated_at = excluded.updated_at`,
		pubkey, now, now)
	return err
}

// LastSwept returns the last successful full-history sweep time for a pubkey
// (0 if the pubkey was never fully swept).
func (d *DB) LastSwept(ctx context.Context, pubkey string) (int64, error) {
	var ts int64
	err := d.conn.QueryRowContext(ctx,
		"SELECT last_full_sweep FROM crawl_state WHERE pubkey = ?", pubkey).Scan(&ts)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return ts, err
}

// MarkSwept records that a priority-crawl pubkey just completed a full-history
// sweep. Does not clobber last_fetched.
func (d *DB) MarkSwept(ctx context.Context, pubkey string) error {
	now := time.Now().Unix()
	_, err := d.conn.ExecContext(ctx, `
INSERT INTO crawl_state(pubkey, last_full_sweep, updated_at) VALUES(?, ?, ?)
ON CONFLICT(pubkey) DO UPDATE SET last_full_sweep = excluded.last_full_sweep, updated_at = excluded.updated_at`,
		pubkey, now, now)
	return err
}

// --- scheduled (future-dated) events ---

// SaveScheduled parks (or replaces) a future-dated event row.
func (d *DB) SaveScheduled(ctx context.Context, id, eventJSON string, publishAt int64) error {
	_, err := d.conn.ExecContext(ctx,
		"INSERT OR REPLACE INTO scheduled_events(id, event_json, publish_at) VALUES(?, ?, ?)",
		id, eventJSON, publishAt)
	return err
}

// ScheduledEvent is one parked future-dated event.
type ScheduledEvent struct {
	ID        string
	EventJSON string
}

// LoadDueScheduled returns up to limit parked events whose publish time has
// passed, oldest first.
func (d *DB) LoadDueScheduled(ctx context.Context, now int64, limit int) ([]ScheduledEvent, error) {
	rows, err := d.conn.QueryContext(ctx,
		"SELECT id, event_json FROM scheduled_events WHERE publish_at <= ? ORDER BY publish_at LIMIT ?",
		now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScheduledEvent
	for rows.Next() {
		var e ScheduledEvent
		if err := rows.Scan(&e.ID, &e.EventJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DeleteScheduled removes a parked event (after it was published or proven
// unloadable).
func (d *DB) DeleteScheduled(ctx context.Context, id string) error {
	_, err := d.conn.ExecContext(ctx, "DELETE FROM scheduled_events WHERE id = ?", id)
	return err
}
