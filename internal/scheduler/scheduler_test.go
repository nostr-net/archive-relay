package scheduler

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// openDB creates an in-memory SQLite with just the scheduled_events table.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE scheduled_events (
		id         TEXT PRIMARY KEY,
		event_json TEXT NOT NULL,
		publish_at INTEGER NOT NULL,
		taken_by   TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func newSched(t *testing.T, db *sql.DB, publish PublishFunc, buffer time.Duration) *Scheduler {
	t.Helper()
	return New(db, publish, buffer, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestShouldDeferBoundary(t *testing.T) {
	s := New(nil, nil, 60*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Now()

	// 5 min out is beyond the 60s buffer -> defer
	far := nostr.Timestamp(now.Add(5 * time.Minute).Unix())
	if !s.ShouldDefer(&nostr.Event{CreatedAt: far}) {
		t.Error("event well beyond the buffer should be deferred")
	}
	// exactly now -> not deferred
	if s.ShouldDefer(&nostr.Event{CreatedAt: nostr.Timestamp(now.Unix())}) {
		t.Error("event at the current time should not be deferred")
	}
	// within the buffer -> not deferred
	near := nostr.Timestamp(now.Add(10 * time.Second).Unix())
	if s.ShouldDefer(&nostr.Event{CreatedAt: near}) {
		t.Error("event within the buffer should not be deferred")
	}
}

func TestDeferAndPublishDuePublishesOnlyPastEvents(t *testing.T) {
	db := openDB(t)
	var published []*nostr.Event
	s := newSched(t, db, func(_ context.Context, e *nostr.Event) error {
		published = append(published, e)
		return nil
	}, 10*time.Second)

	ctx := context.Background()
	past := &nostr.Event{ID: "past", PubKey: "pk", CreatedAt: nostr.Timestamp(time.Now().Add(-time.Minute).Unix()), Kind: 1, Content: "x"}
	future := &nostr.Event{ID: "future", PubKey: "pk", CreatedAt: nostr.Timestamp(time.Now().Add(time.Hour).Unix()), Kind: 1, Content: "x"}

	if err := s.Defer(ctx, past); err != nil {
		t.Fatalf("Defer past: %v", err)
	}
	if err := s.Defer(ctx, future); err != nil {
		t.Fatalf("Defer future: %v", err)
	}

	s.publishDue(ctx)

	if len(published) != 1 {
		t.Fatalf("expected exactly 1 published event, got %d", len(published))
	}
	if published[0].ID != "past" {
		t.Errorf("published event id = %q, want past", published[0].ID)
	}

	// the due event is removed from the table; the future one stays
	var remaining int
	if err := db.QueryRow("SELECT COUNT(*) FROM scheduled_events").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Errorf("remaining scheduled events = %d, want 1 (the future event)", remaining)
	}
}

func TestDeferIsIdempotent(t *testing.T) {
	db := openDB(t)
	s := newSched(t, db, func(context.Context, *nostr.Event) error { return nil }, time.Second)
	ctx := context.Background()
	evt := &nostr.Event{ID: "dup", PubKey: "pk", CreatedAt: nostr.Timestamp(time.Now().Add(time.Hour).Unix()), Kind: 1}

	if err := s.Defer(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if err := s.Defer(ctx, evt); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM scheduled_events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("after two Defers of the same id, row count = %d, want 1", n)
	}
}
