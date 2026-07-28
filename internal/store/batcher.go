package store

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

// ErrBatchFull is returned by SaveEvent when a tier's buffer is full, so khatru
// rejects the event with OK:false and the client retries / backs off. This is
// the load-shedding backpressure valve (ANALYSIS.md §3a).
var ErrBatchFull = errors.New("batch buffer full; event rejected (retry)")

// batcher decouples SaveEvent from the actual ClickHouse INSERT (§3a). It keeps
// a bounded in-memory channel per tier; a single worker goroutine owns the
// buffer and is the ONLY goroutine that touches it. FlushAll requests a flush
// through flushReq and waits for the worker to service it, so there is no race
// between concurrent flushes and appends.
type batcher struct {
	conn    driver.Conn
	table   string
	maxSize int
	maxAge  time.Duration
	log     *slog.Logger

	// OnFlushed, if set, is invoked from the worker after a batch is durably
	// written to ClickHouse. Used to record durable dedup state ONLY after the
	// data is safe — so a crash never leaves "seen but not stored" holes.
	OnFlushed func(events []*nostr.Event)

	in       chan *nostr.Event
	flushReq chan chan error // FlushAll sends a reply chan; worker drains+flushes, then replies

	wg   sync.WaitGroup
	stop chan struct{}
}

func newBatcher(conn driver.Conn, table string, maxSize int, maxAge time.Duration, log *slog.Logger) *batcher {
	cap := maxSize * 2
	if cap < 1024 {
		cap = 1024
	}
	return &batcher{
		conn:     conn,
		table:    table,
		maxSize:  maxSize,
		maxAge:   maxAge,
		log:      log,
		in:       make(chan *nostr.Event, cap),
		flushReq: make(chan chan error, 16),
		stop:     make(chan struct{}),
	}
}

func (b *batcher) start() {
	b.wg.Add(1)
	go b.run()
}

// enqueue pushes an event toward the worker; returns errBatchFull if the
// channel is saturated (load-shed).
func (b *batcher) enqueue(evt *nostr.Event) error {
	select {
	case b.in <- evt:
		return nil
	default:
		return ErrBatchFull
	}
}

func (b *batcher) run() {
	defer b.wg.Done()
	buf := make([]*nostr.Event, 0, b.maxSize)
	tick := time.NewTicker(b.maxAge)
	defer tick.Stop()

	// drain pulls everything currently in `in` into buf.
	drain := func() {
		for {
			select {
			case evt := <-b.in:
				buf = append(buf, evt)
			default:
				return
			}
		}
	}
	// flush resets buf and sends the batch; on error, re-appends to buf.
	flush := func() {
		drain()
		if len(buf) == 0 {
			return
		}
		batch := buf
		buf = make([]*nostr.Event, 0, b.maxSize)
		if err := b.flush(batch); err != nil {
			b.log.Error("batch flush failed", "table", b.table, "n", len(batch), "err", err)
			// preserve data for the next attempt
			buf = append(batch, buf...)
			return
		}
		if b.OnFlushed != nil {
			b.OnFlushed(batch)
		}
	}

	for {
		select {
		case <-b.stop:
			flush()
			return
		case evt := <-b.in:
			buf = append(buf, evt)
			if len(buf) >= b.maxSize {
				flush()
			}
		case <-tick.C:
			flush()
		case reply := <-b.flushReq:
			flush()
			reply <- nil
		}
	}
}

// FlushAll synchronously flushes this tier's buffer (draining the channel
// first). Safe to call concurrently with enqueue.
func (b *batcher) FlushAll() {
	reply := make(chan error, 1)
	b.flushReq <- reply
	select {
	case <-reply:
	case <-time.After(30 * time.Second):
		b.log.Error("FlushAll timed out", "table", b.table)
	}
}

func (b *batcher) shutdown() {
	close(b.stop)
	b.wg.Wait()
}

// flush sends a batch via PrepareBatch. ClickHouse creates one part per INSERT,
// so this is the granularity that matters.
func (b *batcher) flush(events []*nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stmt := "INSERT INTO events_" + b.table + " (" + tierColumns + ")"
	batch, err := b.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		return err
	}
	for _, evt := range events {
		if err := batch.Append(rowFromEvent(evt)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

// rowFromEvent extracts a tier-table row (matching tierColumns order) from an event.
func rowFromEvent(evt *nostr.Event) []any {
	var tagE, tagP, tagT []string
	var tagD string
	for _, t := range evt.Tags {
		if len(t) < 2 {
			continue
		}
		switch t[0] {
		case "e":
			tagE = append(tagE, t[1])
		case "p":
			tagP = append(tagP, t[1])
		case "t":
			tagT = append(tagT, t[1])
		case "d":
			tagD = t[1] // first d-tag wins; addressable kinds have one
		}
	}
	tagsJSON, _ := json.Marshal(evt.Tags)
	return []any{
		evt.ID,
		evt.PubKey,
		uint32(evt.CreatedAt),
		uint32(evt.Kind),
		evt.Content,
		evt.Sig,
		string(tagsJSON),
		tagE,
		tagP,
		tagT,
		tagD,
		replyTarget(evt.Tags), // NIP-10-resolved direct parent, or "" if not a reply
	}
}

// replyTarget resolves the direct parent of a reply per NIP-10:
//   - if an e-tag has marker "reply", that id is the parent;
//   - else the LAST positional (unmarked) e-tag is the parent (legacy clients);
//   - else the event is not a reply (returns "").
//
// `root`-only and `mention` tags do NOT make this event a reply-to-X. This is
// what fixes the v1 over-count where every e-tagged note was counted as a reply.
func replyTarget(tags nostr.Tags) string {
	var lastPositional string
	for _, t := range tags {
		if len(t) < 2 || t[0] != "e" {
			continue
		}
		marker := ""
		if len(t) >= 4 {
			marker = t[3]
		}
		switch marker {
		case "reply":
			return t[1]
		case "", "root":
			// positional (empty) or root: track as candidate; root is the thread
			// root, acceptable as a fallback parent only if no explicit reply.
			if marker == "" {
				lastPositional = t[1]
			}
		}
	}
	return lastPositional
}
