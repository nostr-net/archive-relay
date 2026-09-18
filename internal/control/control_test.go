package control

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := Open(filepath.Join(t.TempDir(), "control.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMarkSeenIsIdempotent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	new, err := db.MarkSeen(ctx, "id-1", 123)
	if err != nil {
		t.Fatal(err)
	}
	if !new {
		t.Error("first MarkSeen should report newly-seen")
	}

	new2, err := db.MarkSeen(ctx, "id-1", 123)
	if err != nil {
		t.Fatal(err)
	}
	if new2 {
		t.Error("second MarkSeen for the same id should report already-seen")
	}
}

func TestPruneSeenOnlyAffectsOldRows(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour).Unix()
	now := time.Now().Unix()
	if _, err := db.MarkSeen(ctx, "old", old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkSeen(ctx, "fresh", now); err != nil {
		t.Fatal(err)
	}

	n, err := db.PruneSeen(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("PruneSeen deleted %d rows, want 1", n)
	}

	// the fresh row survives and is still reported as already-seen
	again, err := db.MarkSeen(ctx, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Error("fresh row should still be present (reported as seen)")
	}
}

func TestAllowedPubkeysCRUD(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := db.AllowPubkey(ctx, "alice", "customer #1"); err != nil {
		t.Fatal(err)
	}
	if err := db.AllowPubkey(ctx, "bob", ""); err != nil {
		t.Fatal(err)
	}

	set, err := db.LoadAllowedSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != 2 {
		t.Fatalf("LoadAllowedSet = %+v, want 2 keys", set)
	}
	for _, k := range []string{"alice", "bob"} {
		if _, ok := set[k]; !ok {
			t.Errorf("LoadAllowedSet missing %q: %+v", k, set)
		}
	}

	rows, err := db.ListAllowed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("ListAllowed returned %d rows, want 2", len(rows))
	}
	// AllowPubkey is idempotent (INSERT OR REPLACE): re-allowing alice updates, not duplicates.
	if err := db.AllowPubkey(ctx, "alice", "customer #1 (paid)"); err != nil {
		t.Fatal(err)
	}
	rows, _ = db.ListAllowed(ctx)
	if len(rows) != 2 {
		t.Errorf("after re-allow, %d rows, want 2", len(rows))
	}

	if err := db.RevokePubkey(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	set, _ = db.LoadAllowedSet(ctx)
	if _, ok := set["bob"]; ok || len(set) != 1 {
		t.Errorf("after revoke, set = %+v, want only alice", set)
	}

	// revoking a missing key is a no-op (no error)
	if err := db.RevokePubkey(ctx, "nobody"); err != nil {
		t.Errorf("revoking missing key should be a no-op, got %v", err)
	}
}

func TestMarkFetchedUpserts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := db.MarkFetched(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	var last1 int64
	if err := db.conn.QueryRowContext(ctx,
		"SELECT last_fetched FROM crawl_state WHERE pubkey = ?", "alice").Scan(&last1); err != nil {
		t.Fatal(err)
	}
	if last1 == 0 {
		t.Error("MarkFetched should set last_fetched")
	}
	if err := db.MarkFetched(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	// last_fetched is Unix-second granularity, so an immediate re-mark may tie;
	// it must never go backwards.
	var last2 int64
	if err := db.conn.QueryRowContext(ctx,
		"SELECT last_fetched FROM crawl_state WHERE pubkey = ?", "alice").Scan(&last2); err != nil {
		t.Fatal(err)
	}
	if last2 < last1 {
		t.Errorf("last_fetched went backwards: %d -> %d", last1, last2)
	}
}

func TestLastFullSweepMigrationIdempotent(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	path := filepath.Join(t.TempDir(), "control.db")

	db1, err := Open(path, log)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db1.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('crawl_state') WHERE name = 'last_full_sweep'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("fresh Open missing last_full_sweep column (count=%d)", n)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, log)
	if err != nil {
		t.Fatalf("second Open (idempotent migrate) failed: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	if err := db2.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('crawl_state') WHERE name = 'last_full_sweep'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("second Open dropped or duplicated last_full_sweep (count=%d)", n)
	}
}

func TestLastFullSweepMigratesOldSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
CREATE TABLE crawl_state (
  pubkey        TEXT PRIMARY KEY,
  last_fetched  INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT 0
)`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := Open(path, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var n int
	if err := db.conn.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('crawl_state') WHERE name = 'last_full_sweep'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("migrate did not add last_full_sweep (count=%d)", n)
	}
}

func TestMarkSweptLastSweptRoundtrip(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	got, err := db.LastSwept(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Errorf("LastSwept of unknown pubkey = %d, want 0", got)
	}

	if err := db.MarkFetched(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	fetched, err := db.LastFetched(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if fetched == 0 {
		t.Fatal("MarkFetched should set last_fetched")
	}

	if err := db.MarkSwept(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	swept, err := db.LastSwept(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if swept == 0 {
		t.Error("MarkSwept should set last_full_sweep")
	}

	fetched2, err := db.LastFetched(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if fetched2 != fetched {
		t.Errorf("MarkSwept clobbered last_fetched: %d -> %d", fetched, fetched2)
	}

	if err := db.MarkSwept(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	bobFetched, err := db.LastFetched(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if bobFetched != 0 {
		t.Errorf("MarkSwept-only row last_fetched = %d, want 0", bobFetched)
	}
}

func TestPruneSeenByRowidExactCap(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const cap = 3
	for i := 0; i < cap+3; i++ {
		if _, err := db.MarkSeen(ctx, fmt.Sprintf("id-%d", i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := db.PruneSeenByRowid(ctx, cap)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("deleted %d, want 3", n)
	}
	ids, err := db.LoadRecentSeen(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != cap {
		t.Errorf("remaining %d, want exactly cap=%d", len(ids), cap)
	}
}

func TestPruneSeenByRowidChunked(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const (
		rows  = 120
		cap   = 10
		chunk = 50
	)
	old := pruneSeenChunk
	pruneSeenChunk = chunk
	t.Cleanup(func() { pruneSeenChunk = old })

	for i := 0; i < rows; i++ {
		if _, err := db.MarkSeen(ctx, fmt.Sprintf("id-%d", i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := db.PruneSeenByRowid(ctx, cap)
	if err != nil {
		t.Fatal(err)
	}
	if n != rows-cap {
		t.Errorf("deleted %d, want %d", n, rows-cap)
	}
	ids, err := db.LoadRecentSeen(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != cap {
		t.Errorf("remaining %d, want exactly cap=%d", len(ids), cap)
	}
}
