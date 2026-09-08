// Package scheduler defers future-dated events: an event whose created_at is
// more than a buffer in the future is parked in the SQLite control DB and NOT
// broadcast; a background loop re-feeds it through the relay pipeline (store +
// broadcast) once its created_at is reached. This mirrors nostrarchives' scheduler.
package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/control"
)

// PublishFunc re-feeds a due event through the relay pipeline (store + broadcast).
// In practice this is relay.AddEvent.
type PublishFunc func(ctx context.Context, evt *nostr.Event) error

// Scheduler parks future-dated events and publishes them when due.
type Scheduler struct {
	db      *control.DB
	publish PublishFunc
	buffer  time.Duration // events more than this far in the future are deferred
	log     *slog.Logger
}

// New constructs a Scheduler. db is the control plane.
func New(db *control.DB, publish PublishFunc, buffer time.Duration, log *slog.Logger) *Scheduler {
	return &Scheduler{db: db, publish: publish, buffer: buffer, log: log}
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
	if err := s.db.SaveScheduled(ctx, evt.ID, string(b), int64(evt.CreatedAt)); err != nil {
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

// publishDue loads due events, re-feeds each through the publish pipeline, and
// deletes it only after a successful publish (failed ones retry next tick).
func (s *Scheduler) publishDue(ctx context.Context) {
	due, err := s.db.LoadDueScheduled(ctx, time.Now().Unix(), 100)
	if err != nil {
		s.log.Warn("load due scheduled events failed", "err", err)
		return
	}
	for _, p := range due {
		evt := &nostr.Event{}
		if err := json.Unmarshal([]byte(p.EventJSON), evt); err != nil {
			s.log.Warn("unmarshal scheduled event failed", "id", p.ID, "err", err)
			_ = s.db.DeleteScheduled(ctx, p.ID)
			continue
		}
		if err := s.publish(ctx, evt); err != nil {
			s.log.Warn("publish scheduled event failed", "id", p.ID, "err", err)
			continue
		}
		if err := s.db.DeleteScheduled(ctx, p.ID); err != nil {
			s.log.Warn("delete scheduled event failed", "id", p.ID, "err", err)
		}
	}
	if len(due) > 0 {
		s.log.Info("published scheduled events", "n", len(due))
	}
}
