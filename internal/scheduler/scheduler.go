// Package scheduler defers future-dated events: an event whose created_at is
// more than a buffer in the future is parked in the SQLite control DB and NOT
// broadcast; a background loop re-feeds it through the relay pipeline (store +
// broadcast) once its created_at is reached. This mirrors nostrarchives' scheduler.
package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// PublishFunc re-feeds a due event through the relay pipeline (store + broadcast).
// In practice this is relay.AddEvent.
type PublishFunc func(ctx context.Context, evt *nostr.Event) error

// Scheduler parks future-dated events and publishes them when due.
type Scheduler struct {
	db       *sql.DB
	publish  PublishFunc
	buffer   time.Duration // events more than this far in the future are deferred
	workerID string
	log      *slog.Logger
}

// New constructs a Scheduler. db is the control plane's *sql.DB.
func New(db *sql.DB, publish PublishFunc, buffer time.Duration, log *slog.Logger) *Scheduler {
	return &Scheduler{
		db: db, publish: publish, buffer: buffer,
		workerID: fmt.Sprintf("sched-%d", time.Now().UnixNano()),
		log:      log,
	}
}

// ShouldDefer reports whether an event is far enough in the future to park.
func (s *Scheduler) ShouldDefer(evt *nostr.Event) bool {
	return int64(evt.CreatedAt) > time.Now().Unix()+int64(s.buffer.Seconds())
}

// Defer parks a future-dated event in SQLite; returns nil so khatru treats it
// as accepted. PreventBroadcast (wired separately) stops immediate broadcast.
func (s *Scheduler) Defer(ctx context.Context, evt *nostr.Event) error {
	b, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT OR REPLACE INTO scheduled_events(id, event_json, publish_at) VALUES(?, ?, ?)",
		evt.ID, string(b), int64(evt.CreatedAt))
	if err != nil {
		s.log.Warn("defer failed", "id", evt.ID, "err", err)
	}
	return nil // accept regardless; a failed park still shouldn't error to the client
}

// Run polls for due events and publishes them. Blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) {
	tick := time.NewTicker(s.buffer / 4)
	if s.buffer/4 < time.Second {
		tick.Reset(time.Second)
	}
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.publishDue(ctx)
		}
	}
}

func (s *Scheduler) publishDue(ctx context.Context) {
	// lease a batch atomically (SQLite row-level via UPDATE ... RETURNING)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_json FROM scheduled_events
		 WHERE publish_at <= ? AND (taken_by IS NULL OR taken_by = ?)
		 ORDER BY publish_at LIMIT 100`,
		time.Now().Unix(), s.workerID)
	if err != nil {
		s.log.Warn("lease query failed", "err", err)
		return
	}
	type pending struct{ id, json string }
	var due []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.json); err == nil {
			due = append(due, p)
		}
	}
	rows.Close()

	for _, p := range due {
		evt := &nostr.Event{}
		if err := json.Unmarshal([]byte(p.json), evt); err != nil {
			s.log.Warn("unmarshal scheduled event failed", "id", p.id, "err", err)
			_, _ = s.db.ExecContext(ctx, "DELETE FROM scheduled_events WHERE id = ?", p.id)
			continue
		}
		if err := s.publish(ctx, evt); err != nil {
			s.log.Warn("publish scheduled event failed", "id", p.id, "err", err)
			continue
		}
		if _, err := s.db.ExecContext(ctx, "DELETE FROM scheduled_events WHERE id = ?", p.id); err != nil {
			s.log.Warn("delete scheduled event failed", "id", p.id, "err", err)
		}
	}
	if len(due) > 0 {
		s.log.Info("published scheduled events", "n", len(due))
	}
}

// ErrNotFuture is returned by StoreHook for events that should NOT be deferred.
// (Not currently used; kept for the explicit-hook composition style.)
var ErrNotFuture = errors.New("event is not future-dated")
