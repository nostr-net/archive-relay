package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

// ErrBatchFull is returned by SaveEvent when a tier's buffer is full, so khatru
// rejects the event with OK:false and the client retries / backs off. This is
// the load-shedding backpressure valve.
var ErrBatchFull = errors.New("batch buffer full; event rejected (retry)")

// ErrBatcherStopped is returned when a flush is requested after shutdown.
var ErrBatcherStopped = errors.New("batcher stopped")

// batcher decouples SaveEvent from the actual ClickHouse INSERT. It keeps
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

	// onFlushed, if set, is invoked from the worker after a batch is durably
	// written to ClickHouse. Used to record durable dedup state ONLY after the
	// data is safe — so a crash never leaves "seen but not stored" holes.
	// atomic.Pointer so SetOnFlushed is race-free even after workers start.
	onFlushed atomic.Pointer[func([]*nostr.Event)]

	in       chan *nostr.Event
	flushReq chan chan error // FlushAll sends a reply chan; worker drains+flushes, then replies

	wg   sync.WaitGroup
	stop chan struct{}
	// gate makes enqueue and closing stop mutually exclusive. in is never closed.
	gate     sync.RWMutex
	stopOnce sync.Once
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

// enqueue pushes an event toward the worker, or load-sheds if full or stopped.
func (b *batcher) enqueue(evt *nostr.Event) error {
	b.gate.RLock()
	defer b.gate.RUnlock()
	select {
	case <-b.stop:
		return ErrBatchFull
	default:
	}
	select {
	case b.in <- evt:
		return nil
	default:
		return ErrBatchFull
	}
}

func (b *batcher) run() {
	defer b.wg.Done()
	type workerState uint8
	const (
		normal workerState = iota
		retry
		stopped
	)
	state := normal
	buf := make([]*nostr.Event, 0, b.maxSize)
	var pending []*nostr.Event
	var replies []chan error
	// A flush request covers a finite snapshot, so concurrent producers cannot
	// keep it open forever. Queued input stays untouched until pending is empty.
	queued := 0
	fails := 0
	tick := time.NewTicker(b.maxAge)
	defer tick.Stop()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var retryC <-chan time.Time
	arm := func(delay time.Duration) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
		retryC = timer.C
	}
	replyAll := func(err error) {
		for _, reply := range replies {
			reply <- err
		}
		replies = nil
	}
	// stage is only called with no pending batch. Snapshot draining bounds
	// retained memory even when producers continuously refill the channel.
	stage := func(n int) {
		pending = buf
		buf = nil
		for i := 0; i < n; i++ {
			pending = append(pending, <-b.in)
		}
		if len(pending) > 0 {
			state = retry
		}
	}
	sendOne := func(ctx context.Context) error {
		n := min(len(pending), b.maxSize)
		chunk := pending[:n:n]
		if err := b.flushContext(ctx, chunk); err != nil {
			return err
		}
		if fn := b.onFlushed.Load(); fn != nil {
			(*fn)(chunk)
		}
		pending = pending[n:]
		if len(pending) == 0 {
			pending = nil
		}
		return nil
	}
	attempt := func() {
		if len(pending) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := sendOne(ctx)
			cancel()
			if err != nil {
				b.log.Error("batch flush failed", "table", b.table, "n", min(len(pending), b.maxSize), "err", err)
				replyAll(err)
				queued = 0
				// fails counts prior consecutive failures: 1s, 2s, 4s, ... 30s.
				arm(min(30*time.Second, time.Second<<min(fails, 5)))
				fails++
				return
			}
			fails = 0
		}
		if len(pending) == 0 && queued > 0 {
			stage(queued)
			queued = 0
		}
		if len(pending) > 0 {
			// Yield to stop and flush requests between successful chunks.
			arm(0)
			return
		}
		state = normal
		retryC = nil
		timer.Stop()
		replyAll(nil)
	}
	for state != stopped {
		var input <-chan *nostr.Event
		var ticks <-chan time.Time
		if state == normal {
			input = b.in
			ticks = tick.C
		}
		select {
		case <-b.stop:
			state = stopped
			// One deadline bounds the entire final flush, including all chunks.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			var err error
			for {
				if len(pending) == 0 {
					stage(len(b.in))
					state = stopped
				}
				if len(pending) == 0 {
					break
				}
				if err = sendOne(ctx); err != nil {
					b.log.Error("final batch flush failed; events lost", "table", b.table, "events_lost", len(pending)+len(buf)+len(b.in), "err", err)
					break
				}
			}
			cancel()
			replyAll(err)
		case evt := <-input:
			buf = append(buf, evt)
			if len(buf) >= b.maxSize {
				stage(len(b.in))
				attempt()
			}
		case <-ticks:
			stage(len(b.in))
			attempt()
		case reply := <-b.flushReq:
			replies = append(replies, reply)
			queued = len(b.in)
			if len(pending) == 0 {
				stage(queued)
				queued = 0
			}
			attempt()
		case <-retryC:
			attempt()
		}
	}
}

// FlushAll flushes the events buffered at the request, returning the actual
// send error. Concurrent enqueues after that snapshot await a later flush.
func (b *batcher) FlushAll() error {
	select {
	case <-b.stop:
		return ErrBatcherStopped
	default:
	}
	reply := make(chan error, 1)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-b.stop:
		return ErrBatcherStopped
	case b.flushReq <- reply:
	case <-timer.C:
		return fmt.Errorf("FlushAll request timed out (table %s)", b.table)
	}
	select {
	case err := <-reply:
		return err
	case <-b.stop:
		return ErrBatcherStopped
	case <-timer.C:
		return fmt.Errorf("FlushAll timed out (table %s)", b.table)
	}
}

// shutdown rejects new enqueues, then waits for a final bounded flush. Callers
// should cancel producers first. An outage can lose already-ACKed client events;
// crawler events can be re-ingested, but closing this loss window requires durable
// spooling or post-persistence ACKs. Failed final flushes log the lost count.
func (b *batcher) shutdown() {
	b.stopOnce.Do(func() {
		b.gate.Lock()
		close(b.stop)
		b.gate.Unlock()
	})
	b.wg.Wait()
}

// flush sends a batch via PrepareBatch. ClickHouse creates one part per INSERT,
// so this is the granularity that matters.
func (b *batcher) flush(events []*nostr.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return b.flushContext(ctx, events)
}

func (b *batcher) flushContext(ctx context.Context, events []*nostr.Event) error {
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
