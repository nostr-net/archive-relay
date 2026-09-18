package store

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// errTombstoneWriterStopped is returned by retire after shutdown begins.
var errTombstoneWriterStopped = errors.New("tombstone writer stopped")

// §1.3 (perf-plan-2026-09): a single tombstone writer goroutine owns ALL
// tombstone I/O. retireIDs / DeleteEvent / ReplaceEvent push ids into a
// buffered channel; the writer coalesces them into one PrepareBatch insert
// (flush at ~250ms or 1000 ids) and marks the tombstone dictionary dirty; a
// reload worker refreshes tombstone_dict at a bounded rate (≤1 per ~2s) while
// dirty — periodic, NOT trailing-edge debounce (which starves under continuous
// writes). Deleting must become invisible within ~2s, which this guarantees.
type tombstoneWriter struct {
	conn driver.Conn
	log  *slog.Logger

	in     chan tombstoneReq
	stopCh chan struct{}
	done   chan struct{}

	// writer-goroutine-only state
	lastReload time.Time
}

type tombstoneReq struct {
	id        string
	reason    string
	deletedBy string
}

// tombstoneFlushEvery bounds how long an id can sit unwritten; 1000 ids or
// this tick, whichever first, produces at most one insert part.
const tombstoneFlushEvery = 250 * time.Millisecond

// tombstoneBatchMax is the coalescing threshold for one insert.
const tombstoneBatchMax = 1000

// tombstoneReloadEvery is the bounded dictionary-reload rate while dirty.
const tombstoneReloadEvery = 2 * time.Second

func newTombstoneWriter(conn driver.Conn, log *slog.Logger) *tombstoneWriter {
	return &tombstoneWriter{
		conn:   conn,
		log:    log,
		in:     make(chan tombstoneReq, 4096),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// start launches the writer + reload workers.
func (w *tombstoneWriter) start() {
	go w.run()
}

// retire pushes ids toward the writer. Blocks if the buffer is full (hides
// must not be lost); returns an error only when the writer is gone.
func (w *tombstoneWriter) retire(ids []string, reason, deletedBy string) error {
	for _, id := range ids {
		select {
		case w.in <- tombstoneReq{id: id, reason: reason, deletedBy: deletedBy}:
		case <-w.stopCh:
			return errTombstoneWriterStopped
		}
	}
	return nil
}

func (w *tombstoneWriter) run() {
	defer close(w.done)
	ctx := context.Background()
	buf := make([]tombstoneReq, 0, tombstoneBatchMax)
	tick := time.NewTicker(tombstoneFlushEvery)
	defer tick.Stop()
	dirty := false

	for {
		select {
		case req := <-w.in:
			buf = append(buf, req)
			if len(buf) >= tombstoneBatchMax {
				dirty = w.flush(ctx, buf) || dirty
				buf = buf[:0]
			}
		case <-tick.C:
			if len(buf) > 0 {
				dirty = w.flush(ctx, buf) || dirty
				buf = buf[:0]
			}
		case <-w.stopCh:
			// drain what's already queued, flush once, reload once, exit.
			for {
				select {
				case req := <-w.in:
					buf = append(buf, req)
					if len(buf) >= tombstoneBatchMax {
						dirty = w.flush(ctx, buf) || dirty
						buf = buf[:0]
					}
				default:
					if len(buf) > 0 {
						dirty = w.flush(ctx, buf) || dirty
					}
					if dirty {
						w.reload(ctx)
					}
					return
				}
			}
		}
		// bounded reload while dirty (periodic coalescing)
		if dirty && time.Since(w.lastReload) >= tombstoneReloadEvery {
			w.reload(ctx)
			dirty = false
		}
	}
}

// flush inserts a coalesced batch; returns true on success (dict now dirty).
func (w *tombstoneWriter) flush(ctx context.Context, reqs []tombstoneReq) bool {
	if len(reqs) == 0 {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	batch, err := w.conn.PrepareBatch(cctx, "INSERT INTO tombstones (id, reason, deleted_by)")
	if err != nil {
		w.log.Error("tombstone prepare failed", "n", len(reqs), "err", err)
		return false
	}
	for _, r := range reqs {
		if err := batch.Append(r.id, r.reason, r.deletedBy); err != nil {
			w.log.Error("tombstone append failed", "err", err)
			return false
		}
	}
	if err := batch.Send(); err != nil {
		w.log.Error("tombstone insert failed", "n", len(reqs), "err", err)
		return false
	}
	return true
}

func (w *tombstoneWriter) reload(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := w.conn.Exec(cctx, "SYSTEM RELOAD DICTIONARY tombstone_dict"); err != nil {
		w.log.Warn("tombstone dict reload failed", "err", err)
		return
	}
	// SYSTEM RELOAD can report success without applying; tests verify a
	// known id actually hides. Reload errors are logged; the writer stays
	// dirty and the periodic reload retries.
	w.lastReload = time.Now()
}

// stop drains, flushes, reloads once, and waits for the writer to exit.
func (w *tombstoneWriter) stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	<-w.done
}
