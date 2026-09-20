// Package store is the ClickHouse-backed eventstore.Store implementation for
// the archive relay. It owns the tier tables, the tombstone dictionary,
// per-tier batched inserts, and replaceable dedup via LIMIT 1 BY on read
// (ReplacingMergeTree FINAL is retained only on CountEvents — §1.10).
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/config"
)

// readColumns is the 7-column SELECT list for QueryEvents. scanEvent must
// match this order and count (codex #10 / §1.7). Native `tags` replaces
// tags_raw so the read path does not JSON-decode per row (F6).
const readColumns = "id, pubkey, created_at, kind, content, sig, tags"

// readAdmissionCap is the in-flight bound for QueryEvents / CountEvents /
// ReplaceEvent probes. It matches the intended read-pool MaxOpenConns (16).
const readAdmissionCap = 16

// ErrReadBusy is returned when the read-admission semaphore and its waiter
// queue are both full. khatru surfaces this as a NOTICE.
var ErrReadBusy = errors.New("read admission full")

// Store implements eventstore.Store (Init/Close/QueryEvents/SaveEvent/
// DeleteEvent/ReplaceEvent) plus Counter (CountEvents) over ClickHouse.
type Store struct {
	wch   driver.Conn // write pool: batchers, tombstones, DDL
	sch   driver.Conn // stats pool: snapshot refresh jobs + API reads
	cfg   *config.Config
	log   *slog.Logger
	tiers map[string]*batcher // keyed by tier name
	tw    *tombstoneWriter    // owns all tombstone I/O (§1.3)

	// readSem is the shared query admission semaphore (cap 16 = read pool
	// bound). QueryEvents, CountEvents, and ReplaceEvent's slim probe all
	// share it. ReplaceEvent is invoked from khatru's SaveEvent path, not
	// from inside QueryEvents, so sharing cannot deadlock. khatru internal
	// calls (NIP-09 delete lookups) bypass admission — a hide must never be
	// silently dropped because client REQs saturated the semaphore.
	readSem  chan struct{}
	readWait chan struct{} // bounded waiters (same cap); overflow → ErrReadBusy

	ch driver.Conn // read pool: QueryEvents / CountEvents / probe
}

// Pool sizes per plan §1.5 (F12/M11): write ~4 / read ~16 / stats ~2.
const (
	poolWriteConns = 4
	poolReadConns  = 16
	poolStatsConns = 2
)

// New constructs an unopened Store. Call Init() to connect + create schema.
func New(cfg *config.Config, log *slog.Logger) *Store {
	return &Store{
		cfg:      cfg,
		log:      log,
		readSem:  make(chan struct{}, readAdmissionCap),
		readWait: make(chan struct{}, readAdmissionCap),
	}
}

// Init connects to ClickHouse (three pools: write/read/stats — §1.5), creates
// the schema, and starts the batchers.
func (s *Store) Init() error {
	if err := s.ensureDatabase(); err != nil {
		return err
	}
	wch, err := s.openConn(poolWriteConns)
	if err != nil {
		return err
	}
	s.wch = wch

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.initSchema(ctx); err != nil {
		return err
	}

	// tombstone writer: single goroutine owning all tombstone inserts +
	// bounded dictionary reloads (§1.3).
	s.tw = newTombstoneWriter(wch, s.log.With("worker", "tombstone"))
	s.tw.start()

	// start one batcher per active tier
	s.tiers = make(map[string]*batcher, len(activeTiers))
	for _, t := range activeTiers {
		b := newBatcher(wch, t, s.cfg.Batch.MaxSize, s.cfg.Batch.MaxAge, s.log.With("tier", t))
		b.start()
		s.tiers[t] = b
	}

	// read + stats pools; failure aborts Init (Close cleans up what opened).
	s.ch, err = s.openConn(poolReadConns)
	if err != nil {
		return err
	}
	s.sch, err = s.openConn(poolStatsConns)
	if err != nil {
		return err
	}
	s.log.Info("store initialized", "tiers", activeTiers,
		"pools", fmt.Sprintf("write=%d read=%d stats=%d", poolWriteConns, poolReadConns, poolStatsConns))
	return nil
}

// identifierRe matches a plain ClickHouse identifier (letters, digits,
// underscore; not starting with a digit) — the only names allowed in the
// bootstrap CREATE DATABASE DDL.
var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ensureDatabase creates the configured database if this is a first boot
// (the pools below authenticate against it, so it must exist first), using
// ENGINE=Atomic — required by the followers EXCHANGE refresh. An existing
// non-Atomic database is NOT converted (IF NOT EXISTS is a no-op); that case
// still fails loudly later at the stats engine check. Connects to the
// always-present `default` database for the bootstrap DDL.
func (s *Store) ensureDatabase() error {
	if s.cfg.ClickHouse.Database == "" || s.cfg.ClickHouse.Database == "default" {
		return nil
	}
	// the name is embedded in DDL — accept plain identifiers only
	if !identifierRe.MatchString(s.cfg.ClickHouse.Database) {
		return fmt.Errorf("clickhouse.database %q is not a valid identifier", s.cfg.ClickHouse.Database)
	}
	opts := &clickhouse.Options{
		Addr: []string{s.cfg.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: s.cfg.ClickHouse.Username,
			Password: s.cfg.ClickHouse.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open (bootstrap): %w", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Exec(ctx,
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ENGINE = Atomic", s.cfg.ClickHouse.Database)); err != nil {
		return fmt.Errorf("create database %s: %w", s.cfg.ClickHouse.Database, err)
	}
	return nil
}

// openConn opens one bounded pool connection.
func (s *Store) openConn(maxOpen int) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: []string{s.cfg.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: s.cfg.ClickHouse.Database,
			Username: s.cfg.ClickHouse.Username,
			Password: s.cfg.ClickHouse.Password,
		},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: maxOpen,
		MaxIdleConns: maxOpen,
		Settings:     clickhouse.Settings{"max_execution_time": 120},
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := conn.Ping(pctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping %s: %w", s.cfg.ClickHouse.Addr, err)
	}
	return conn, nil
}

// FlushAll synchronously flushes every tier's batch buffer into ClickHouse.
// Returns the first tier's flush error (or nil). Used by tests and graceful
// shutdown.
func (s *Store) FlushAll() error {
	var firstErr error
	for _, b := range s.tiers {
		if err := b.FlushAll(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CH exposes the stats-pool connection for subsystems (stats refresh jobs,
// the API) that run their own queries, so heavy snapshot scans never starve
// the read pool. Read-only callers only.
func (s *Store) CH() driver.Conn { return s.sch }

// Ping checks ClickHouse liveness on the read pool — the right pool for
// probes (16 conns; the 2-conn stats pool can be busy for 120s with a FINAL
// refresh job and flap a health check).
func (s *Store) Ping(ctx context.Context) error {
	if s.ch == nil {
		return errors.New("store not initialized")
	}
	return s.ch.Ping(ctx)
}

// SetOnFlushed registers a callback fired after a batch is durably written to
// ClickHouse on ANY tier. Used by the crawler to record durable dedup state
// only after the data is safe — never mark an event "seen" before it is stored.
// Safe to call at any time (the callback is stored atomically).
func (s *Store) SetOnFlushed(fn func(events []*nostr.Event)) {
	for _, b := range s.tiers {
		b.onFlushed.Store(&fn)
	}
}

// Close flushes all batchers, drains the tombstone writer, and closes every
// pool connection. Safe to call once.
func (s *Store) Close() {
	for _, b := range s.tiers {
		b.shutdown()
	}
	if s.tw != nil {
		s.tw.stop()
	}
	for _, c := range []driver.Conn{s.ch, s.sch, s.wch} {
		if c != nil {
			_ = c.Close()
		}
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

// DeleteEvent hands the id to the tombstone writer: a coalesced insert
// flushes within ~250ms and the dictionary reloads on a bounded (~2s)
// cadence, so the hide becomes visible to subsequent reads within that
// window (bounded-latency eventual visibility — no in-process overlay;
// docs/perf-p4-decisions.md §4.5). Physical reclamation, if ever wanted,
// is a separate periodic ALTER DELETE job.
func (s *Store) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return s.retireIDs(ctx, []string{evt.ID}, "nip09", evt.PubKey)
}

// retireIDs tombstones a batch of event ids by handing them to the tombstone
// writer (single coalesced insert + bounded dictionary reload — §1.3). Used by
// DeleteEvent (NIP-09) and ReplaceEvent (superseded versions). reason is a
// short LowCardinality label ("nip09" / "replaced"); deletedBy is the acting
// pubkey ("" when unknown, e.g. internal retirement).
func (s *Store) retireIDs(ctx context.Context, ids []string, reason, deletedBy string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.tw.retire(ids, reason, deletedBy)
}

// ReplaceEvent implements nostr replaceable/addressable semantics: only the
// latest version per (pubkey, kind) — tie-broken by lowest id (NIP-01) — should
// be served. Because the tier ORDER BY includes created_at (for query
// performance), ReplacingMergeTree alone does NOT collapse different versions,
// so we save the new version first, then retire superseded ids via the
// tombstone path. A stored-but-unretired overlap is healed by QueryEvents'
// LIMIT 1 BY collapse; a retired-but-unstored gap is not (codex #7).
// Non-replaceable kinds fall through to a plain save.
func (s *Store) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	if !isReplaceableKind(evt.Kind) {
		return s.SaveEvent(ctx, evt)
	}

	// A previous version may still be in a batcher buffer (published seconds
	// ago, not yet in ClickHouse) — flush so the probe sees it; otherwise a
	// superseded version can survive unretired (read collapse still heals the
	// common case; equal-timestamp edges need the probe complete).
	if err := s.FlushAll(); err != nil {
		s.log.Warn("flush before replace probe failed", "err", err)
	}

	release, err := s.acquireRead(ctx)
	if err != nil {
		return fmt.Errorf("replace probe: %w", err)
	}
	prev, err := s.probeReplaceable(ctx, evt)
	release()
	if err != nil {
		return fmt.Errorf("replace query: %w", err)
	}

	shouldStore := true
	var retire []string
	for _, p := range prev {
		prevTS := nostr.Timestamp(p.createdAt)
		// prev is "older" (should be retired) if it has an earlier timestamp,
		// or the same timestamp but a higher id (NIP-01 keeps the lowest id).
		prevOlder := prevTS < evt.CreatedAt ||
			(prevTS == evt.CreatedAt && p.id > evt.ID)
		if prevOlder {
			retire = append(retire, p.id)
		} else {
			shouldStore = false // an equal-or-newer version exists; discard incoming
		}
	}

	// Save first, then retire. If the save fails we must not hide the old
	// versions — that would leave the author with nothing served. Because
	// SaveEvent only ENQUEUES (the new row isn't queryable until the batcher
	// flushes, up to maxAge/maxSize later) while the tombstone can hide the
	// old version within ~250ms–2s, an enqueue-then-retire leaves a window
	// where a REQ returns NOTHING (grok review #1: retire-before-STORED is
	// the §1.6 gap). Gate the retire on a synchronous flush of that tier so
	// the new version is durably queryable first.
	if shouldStore {
		if err := s.SaveEvent(ctx, evt); err != nil {
			return err
		}
		b, ok := s.tiers[tierForEvent(evt, s.cfg.Classifier)]
		if !ok {
			// unreachable today (SaveEvent uses the same map), but a missing
			// batcher must abort the retire — enqueue ≠ stored (grok r2 #12)
			return fmt.Errorf("no batcher for tier %q", tierForEvent(evt, s.cfg.Classifier))
		}
		if err := b.FlushAll(); err != nil {
			return fmt.Errorf("flush new version before retire: %w", err)
		}
	}
	if len(retire) > 0 {
		if err := s.retireIDs(ctx, retire, "replaced", evt.PubKey); err != nil {
			s.log.Warn("retire superseded versions failed", "n", len(retire), "err", err)
		}
	}
	return nil
}

type replaceableRow struct {
	id        string
	createdAt uint32
}

// probeReplaceable runs the two-column slim probe (§1.6) against every tier
// that can hold evt's kind. No FINAL — LIMIT 50 of live (non-tombstoned) rows.
func (s *Store) probeReplaceable(ctx context.Context, evt *nostr.Event) ([]replaceableRow, error) {
	f := nostr.Filter{Authors: []string{evt.PubKey}, Kinds: []int{evt.Kind}}
	tiers := tiersForFilter(f, s.cfg.Classifier)
	var out []replaceableRow
	for _, t := range tiers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := fmt.Sprintf("SELECT id, created_at FROM events_%s WHERE pubkey=? AND kind=? AND NOT dictHas('tombstone_dict', id) ORDER BY created_at DESC, id ASC LIMIT 50", t)
		rows, err := s.ch.Query(ctx, q, evt.PubKey, uint32(evt.Kind))
		if err != nil {
			return nil, fmt.Errorf("events_%s: %w", t, err)
		}
		for rows.Next() {
			var row replaceableRow
			if err := rows.Scan(&row.id, &row.createdAt); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan events_%s: %w", t, err)
			}
			out = append(out, row)
		}
		qErr := rows.Err()
		_ = rows.Close()
		if qErr != nil {
			return nil, fmt.Errorf("rows events_%s: %w", t, qErr)
		}
	}
	return out, nil
}

// QueryEvents streams events matching the filter. Per-tier SQL uses
// LIMIT 1 BY to collapse replaceable versions and exact-id duplicates — no
// FINAL (this is the FINAL removal, P4-17 prerequisite).
//
// Semantic: "latest matching version". Collapse runs AFTER WHERE, so a
// tag/`until`-filtered query can legitimately return an older version when
// the newest does not match. This is consistent with most relays; winner-
// then-filter would require a subquery.
//
// Tier queries run eagerly and any SQL/scan error is returned synchronously
// (khatru then sends NOTICE) before the result channel is created. The
// collected rows are then streamed through a buffered channel.
func (s *Store) QueryEvents(ctx context.Context, f nostr.Filter) (chan *nostr.Event, error) {
	// NIP-09 lookups (khatru internal calls) query by id for an event the
	// client may have published SECONDS ago — which can still be sitting in
	// a tier batcher, invisible to ClickHouse. A missed lookup means the
	// tombstone is never written and the "deleted" event is served forever.
	// Flush first so deletes of just-published events land (internal calls
	// are rare — deletes + expirations — so the forced flush is cheap).
	if khatru.IsInternalCall(ctx) {
		if err := s.FlushAll(); err != nil {
			s.log.Warn("flush before internal lookup failed", "err", err)
		}
	}
	release, err := s.acquireRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	collected, err := s.collectEvents(ctx, f)
	if err != nil {
		return nil, err
	}

	out := make(chan *nostr.Event, 64)
	go func() {
		defer close(out)
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

func (s *Store) collectEvents(ctx context.Context, f nostr.Filter) ([]*nostr.Event, error) {
	where, args, tail := buildFilterSQL(f)
	tiers := tiersForFilter(f, s.cfg.Classifier)

	limit := f.Limit
	if limit < 1 || limit > defaultQueryLimit {
		limit = defaultQueryLimit
	}

	var collected []*nostr.Event
	for _, t := range tiers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q := fmt.Sprintf("SELECT %s FROM events_%s WHERE %s%s",
			readColumns, t, where, tail)
		rows, err := s.ch.Query(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("query events_%s: %w", t, err)
		}
		n := 0
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan events_%s: %w", t, err)
			}
			collected = append(collected, e)
			n++
		}
		qErr := rows.Err()
		_ = rows.Close()
		if qErr != nil {
			return nil, fmt.Errorf("rows events_%s: %w", t, qErr)
		}
		s.log.Debug("tier scan", "tier", t, "rows", n)
	}
	// Deterministic merge across tiers + global limit (codex #4).
	sortDesc(collected)
	if len(collected) > limit {
		collected = collected[:limit]
	}
	return collected, nil
}

// CountEvents returns the count of matching live (non-tombstoned) events.
// Unlike QueryEvents, COUNT still uses FINAL and counts every stored version
// (no LIMIT 1 BY winner collapse) — a documented divergence (NIP-45 counts
// are approximate; see perf-plan §1.10).
func (s *Store) CountEvents(ctx context.Context, f nostr.Filter) (int64, error) {
	release, err := s.acquireRead(ctx)
	if err != nil {
		return 0, err
	}
	defer release()

	where, args, _ := buildFilterSQL(f)
	tiers := tiersForFilter(f, s.cfg.Classifier)
	var total int64
	for _, t := range tiers {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		q := fmt.Sprintf("SELECT count() FROM events_%s FINAL WHERE %s", t, where)
		var n uint64
		if err := s.ch.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

// acquireRead takes one admission token and returns the matching release
// func. If all tokens are held, the caller joins a bounded waiter queue
// (cap 16) that is also ctx-cancellable. Overflow returns ErrReadBusy
// immediately so khatru can NOTICE rather than accumulating unbounded
// blocked REQs (codex #11). khatru internal calls (NIP-09 delete lookups)
// bypass admission entirely — a delete lookup failing on ErrReadBusy would
// silently skip tombstoning that id; correctness of hides outranks the bound.
func (s *Store) acquireRead(ctx context.Context) (func(), error) {
	nop := func() {}
	if khatru.IsInternalCall(ctx) {
		return nop, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	select {
	case s.readSem <- struct{}{}:
		return func() { <-s.readSem }, nil
	default:
	}
	select {
	case s.readWait <- struct{}{}:
		defer func() { <-s.readWait }()
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, ErrReadBusy
	}
	select {
	case s.readSem <- struct{}{}:
		return func() { <-s.readSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	// (created_at DESC, id ASC) matches the per-tier ORDER BY so a cross-tier
	// global LIMIT is deterministic (codex #4).
	sort.Slice(ev, func(i, j int) bool {
		if ev[i].CreatedAt != ev[j].CreatedAt {
			return ev[i].CreatedAt > ev[j].CreatedAt
		}
		return ev[i].ID < ev[j].ID
	})
}

// scanEvent reads a QueryEvents row (readColumns order: 7 destinations).
func scanEvent(rows driver.Rows) (*nostr.Event, error) {
	var (
		id, pubkey, content, sig string
		createdAt, kind          uint32
		tags                     [][]string
	)
	if err := rows.Scan(&id, &pubkey, &createdAt, &kind, &content, &sig, &tags); err != nil {
		return nil, err
	}
	return &nostr.Event{
		ID:        id,
		PubKey:    pubkey,
		CreatedAt: nostr.Timestamp(createdAt),
		Kind:      int(kind),
		Content:   content,
		Sig:       sig,
		Tags:      nostrTags(tags),
	}, nil
}
