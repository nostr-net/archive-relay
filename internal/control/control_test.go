package control

import (
	"context"
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
