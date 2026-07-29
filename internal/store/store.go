// Package store is the ClickHouse-backed eventstore.Store implementation for
// the archive relay. It owns the tier tables, the tombstone dictionary,
// per-tier batched inserts, and replaceable dedup via ReplacingMergeTree
// with FINAL on read.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/config"
)

// Store implements eventstore.Store (Init/Close/QueryEvents/SaveEvent/
// DeleteEvent/ReplaceEvent) plus Counter (CountEvents) over ClickHouse.
type Store struct {
	ch    driver.Conn
	cfg   *config.Config
	log   *slog.Logger
	tiers map[string]*batcher // keyed by tier name
}

// New constructs an unopened Store. Call Init() to connect + create schema.
func New(cfg *config.Config, log *slog.Logger) *Store {
	return &Store{cfg: cfg, log: log}
}

// Init connects to ClickHouse, creates the schema, and starts the batchers.
func (s *Store) Init() error {
	opts := &clickhouse.Options{
		Addr: []string{s.cfg.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: s.cfg.ClickHouse.Database,
			Username: s.cfg.ClickHouse.Username,
			Password: s.cfg.ClickHouse.Password,
		},
		DialTimeout: 5 * time.Second,
		Settings:    clickhouse.Settings{"max_execution_time": 120},
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping %s: %w", s.cfg.ClickHouse.Addr, err)
	}
	s.ch = conn

	if err := s.initSchema(ctx); err != nil {
		return err
	}

	// start one batcher per active tier (transient is idle until kinds map to it)
	s.tiers = make(map[string]*batcher, len(activeTiers))
	for _, t := range activeTiers {
		b := newBatcher(conn, t, s.cfg.Batch.MaxSize, s.cfg.Batch.MaxAge, s.log.With("tier", t))
		b.start()
		s.tiers[t] = b
	}
	s.log.Info("store initialized", "tiers", activeTiers)
	return nil
}

// FlushAll synchronously flushes every tier's batch buffer into ClickHouse.
// Used by tests, the scheduler, and a future graceful-SIGTERM drain.
func (s *Store) FlushAll() {
	for _, b := range s.tiers {
		b.FlushAll()
	}
}

// CH exposes the underlying ClickHouse connection for subsystems (stats refresh
// jobs, the API) that run their own queries. Read-only callers only.
func (s *Store) CH() driver.Conn { return s.ch }

// SetOnFlushed registers a callback fired after a batch is durably written to
// ClickHouse on ANY tier. Used by the crawler to record durable dedup state
// only after the data is safe — never mark an event "seen" before it is stored.
func (s *Store) SetOnFlushed(fn func(events []*nostr.Event)) {
	for _, b := range s.tiers {
		b.OnFlushed = fn
	}
}

// Close flushes all batchers and closes the connection. Safe to call once.
func (s *Store) Close() {
	for _, b := range s.tiers {
		b.shutdown()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
}

// SaveEvent routes the event to its tier's batcher. Out-of-scope (drop) kinds
// are rejected here as a defense-in-depth; the policy layer normally drops
// them earlier at RejectEvent.
func (s *Store) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	tier := tierForEvent(evt, s.cfg.Classifier)
	if tier == TierDrop {
		return errors.New("event kind out of scope")
	}
	b, ok := s.tiers[tier]
	if !ok {
		return fmt.Errorf("no batcher for tier %q", tier)
	}
	return b.enqueue(evt)
}

// DeleteEvent records a tombstone (instant hide via the dictionary) and reloads
// the dictionary so the hide is visible to subsequent reads immediately.
// Physical reclamation, if ever wanted, is a separate periodic ALTER DELETE job.
func (s *Store) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return s.retireIDs(ctx, []string{evt.ID}, "nip09", evt.PubKey)
}

// retireIDs tombstones a batch of event ids with a single dictionary reload, so
// the hides are visible to subsequent reads immediately. Used by DeleteEvent
// (NIP-09) and ReplaceEvent (superseded versions). reason is a short
// LowCardinality label ("nip09" / "replaced"); deletedBy is the acting pubkey
// ("" when unknown, e.g. internal retirement). An insert error aborts the batch;
// the caller decides whether to surface or log it.
func (s *Store) retireIDs(ctx context.Context, ids []string, reason, deletedBy string) error {
	if len(ids) == 0 {
		return nil
	}
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, id := range ids {
		if err := s.ch.Exec(ctx2,
			"INSERT INTO tombstones (id, reason, deleted_by) VALUES (?, ?, ?)",
			id, reason, deletedBy); err != nil {
			return err
		}
	}
	// make the hides visible now (auto-refresh would also do this within LIFETIME)
	_ = s.ch.Exec(ctx2, "SYSTEM RELOAD DICTIONARY tombstone_dict")
	return nil
}

// ReplaceEvent implements nostr replaceable/addressable semantics: only the
// latest version per (pubkey, kind) — tie-broken by lowest id (NIP-01) — should
// be served. Because the tier ORDER BY includes created_at (for query
// performance), ReplacingMergeTree alone does NOT collapse different versions,
// so we retire older versions via the tombstone path and save the new one.
// Non-replaceable kinds fall through to a plain save.
func (s *Store) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	if !isReplaceableKind(evt.Kind) {
		return s.SaveEvent(ctx, evt)
	}
	// fetch current versions for this (pubkey, kind) — already tombstone-filtered
	ch, err := s.QueryEvents(ctx, nostr.Filter{
		Authors: []string{evt.PubKey}, Kinds: []int{evt.Kind}, Limit: 100,
	})
	if err != nil {
		return fmt.Errorf("replace query: %w", err)
	}
	shouldStore := true
	var retire []string
	for prev := range ch {
		// prev is "older" (should be retired) if it has an earlier timestamp,
		// or the same timestamp but a higher id (NIP-01 keeps the lowest id).
		prevOlder := prev.CreatedAt < evt.CreatedAt ||
			(prev.CreatedAt == evt.CreatedAt && prev.ID > evt.ID)
		if prevOlder {
			retire = append(retire, prev.ID)
		} else {
			shouldStore = false // an equal-or-newer version exists; discard incoming
		}
	}
	// retire all superseded versions in one batch with a single dictionary reload
	if len(retire) > 0 {
		if err := s.retireIDs(ctx, retire, "replaced", evt.PubKey); err != nil {
			s.log.Warn("retire superseded versions failed", "n", len(retire), "err", err)
		}
	}
	if shouldStore {
		return s.SaveEvent(ctx, evt)
	}
	return nil
}

// QueryEvents streams events matching the filter, querying each relevant tier
// with FINAL (so replaceable/addressable dedup is correct) and merging in Go.
// The tombstone predicate is added by buildFilterSQL.
func (s *Store) QueryEvents(ctx context.Context, f nostr.Filter) (chan *nostr.Event, error) {
	where, args, tail := buildFilterSQL(f)
	tiers := tiersForFilter(f, s.cfg.Classifier)

	// Merge across tiers in Go: collect, sort by created_at desc, respect Limit.
	limit := f.Limit
	if limit < 1 || limit > defaultQueryLimit {
		limit = defaultQueryLimit
	}

	out := make(chan *nostr.Event, 64)
	go func() {
		defer close(out)
		var collected []*nostr.Event
		for _, t := range tiers {
			if ctx.Err() != nil {
				return
			}
			q := fmt.Sprintf("SELECT %s FROM events_%s FINAL WHERE %s%s",
				tierColumns, t, where, tail)
			rows, err := s.ch.Query(ctx, q, args...)
			if err != nil {
				s.log.Error("query failed", "tier", t, "err", err)
				continue
			}
			n := 0
			for rows.Next() {
				e, err := scanEvent(rows)
				if err != nil {
					s.log.Error("scan event failed", "tier", t, "err", err)
					_ = rows.Close()
					return
				}
				collected = append(collected, e)
				n++
			}
			_ = rows.Close()
			s.log.Info("tier scan", "tier", t, "rows", n)
		}
		// stable-ish ordering across tiers + global limit
		sortDesc(collected)
		if len(collected) > limit {
			collected = collected[:limit]
		}
		for _, e := range collected {
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// CountEvents returns the count of matching live (non-tombstoned) events,
// deduped via FINAL per tier.
func (s *Store) CountEvents(ctx context.Context, f nostr.Filter) (int64, error) {
	where, args, _ := buildFilterSQL(f)
	tiers := tiersForFilter(f, s.cfg.Classifier)
	var total int64
	for _, t := range tiers {
		q := fmt.Sprintf("SELECT count() FROM events_%s FINAL WHERE %s", t, where)
		var n uint64
		if err := s.ch.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

// tiersForFilter returns the tiers that could contain the filter's kinds.
// Empty Kinds (match-all) → every active tier. Honors the same override map as
// ingest so query routing matches write routing.
func tiersForFilter(f nostr.Filter, override map[int]string) []string {
	if len(f.Kinds) == 0 {
		return activeTiers
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, k := range f.Kinds {
		t := classifyWithOverride(k, override)
		if t == TierDrop {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func sortDesc(ev []*nostr.Event) {
	// stable: equal-timestamp events keep their tier-query order, which already
	// sorted by id, so the cross-tier merge is deterministic.
	sort.SliceStable(ev, func(i, j int) bool {
		return ev[i].CreatedAt > ev[j].CreatedAt
	})
}

// scanEvent reads a tier-table row (in tierColumns order) into a nostr.Event.
func scanEvent(rows driver.Rows) (*nostr.Event, error) {
	var (
		id, pubkey, content, sig, tagsRaw, tagD, replyTo string
		tagE, tagP, tagT                                 []string
		createdAt, kind                                  uint32
	)
	if err := rows.Scan(&id, &pubkey, &createdAt, &kind, &content, &sig, &tagsRaw, &tagE, &tagP, &tagT, &tagD, &replyTo); err != nil {
		return nil, err
	}
	tags := nostr.Tags{}
	_ = json.Unmarshal([]byte(tagsRaw), &tags) // tags_raw is authoritative; arrays are accelerators
	return &nostr.Event{
		ID:        id,
		PubKey:    pubkey,
		CreatedAt: nostr.Timestamp(createdAt),
		Kind:      int(kind),
		Content:   content,
		Sig:       sig,
		Tags:      tags,
	}, nil
}
