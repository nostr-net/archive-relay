package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// errTombstoneWriterStopped is returned by retire after shutdown begins.
var errTombstoneWriterStopped = errors.New("tombstone writer stopped")
var errTombstoneWriterFull = errors.New("tombstone writer enqueue timed out")

// §1.3 (perf-plan-2026-09): a single tombstone writer goroutine owns ALL
// tombstone I/O. retireIDs / DeleteEvent / ReplaceEvent push ids into a
// buffered channel; the writer coalesces them into one PrepareBatch insert
// (flush at ~250ms or 1000 ids) and marks the tombstone dictionary dirty; a
// writer refreshes tombstone_dict at a bounded rate (≤1 per ~2s) while
// dirty — periodic, NOT trailing-edge debounce (which starves under continuous
// writes). Visibility is eventual; database failures may delay it.
type tombstoneWriter struct {
	conn driver.Conn
	log  *slog.Logger

	in        chan tombstoneReq
	stopCh    chan struct{}
	done      chan struct{}
	producers sync.RWMutex
	stopOnce  sync.Once

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

// start launches the writer.
func (w *tombstoneWriter) start() {
	go w.run()
}

// retire bounds the entire enqueue call to five seconds. An error may follow
// a partially accepted request; callers may safely retry tombstone ids.
func (w *tombstoneWriter) retire(ids []string, reason, deletedBy string) error {
	w.producers.RLock()
	defer w.producers.RUnlock()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-w.stopCh:
		return errTombstoneWriterStopped
	default:
	}
	for _, id := range ids {
		select {
		case <-w.stopCh:
			return errTombstoneWriterStopped
		default:
		}
		select {
		case w.in <- tombstoneReq{id: id, reason: reason, deletedBy: deletedBy}:
		case <-w.stopCh:
			return errTombstoneWriterStopped
		case <-timer.C:
			return errTombstoneWriterFull
		}
	}
	return nil
}

const tombstoneRetainedMax = 3 * tombstoneBatchMax

func (w *tombstoneWriter) run() {
	defer close(w.done)
	ctx := context.Background()
	buf := make([]tombstoneReq, 0, tombstoneRetainedMax)
	tick := time.NewTicker(tombstoneFlushEvery)
	defer tick.Stop()
	dirty := false
	probeID := ""
	retrying := false
	dropped := 0
	reportDrops := func() {
		if dropped > 0 {
			w.log.Error("tombstones dropped: deleted events may reappear", "n", dropped)
			dropped = 0
		}
	}
	appendReq := func(req tombstoneReq) {
		if len(buf) == tombstoneRetainedMax {
			dropped++
			return
		}
		buf = append(buf, req)
	}
	flush := func() {
		reportDrops()
		for len(buf) > 0 {
			n := min(len(buf), tombstoneBatchMax)
			if !w.flush(ctx, buf[:n]) {
				retrying = true
				return
			}
			probeID = buf[n-1].id
			dirty = true
			copy(buf, buf[n:])
			clear(buf[len(buf)-n:])
			buf = buf[:len(buf)-n]
		}
		retrying = false
	}
	for {
		select {
		case req := <-w.in:
			appendReq(req)
			if len(buf) >= tombstoneBatchMax && !retrying {
				flush()
			}
		case <-tick.C:
			flush()
		case <-w.stopCh:
			// Join enqueue calls before draining, so no accepted id arrives after
			// the channel has been observed empty.
			w.producers.Lock()
			w.producers.Unlock()
			shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			ctx = shutdownCtx
			for {
				select {
				case req := <-w.in:
					appendReq(req)
					if len(buf) >= tombstoneBatchMax && !retrying {
						flush()
					}
				default:
					flush()
					if len(buf) > 0 {
						w.log.Error("tombstones lost at shutdown: deleted events may reappear", "n", len(buf))
					}
					if dirty {
						w.reload(ctx, probeID)
					}
					return
				}
			}
		}
		if dirty && time.Since(w.lastReload) >= tombstoneReloadEvery {
			dirty = !w.reload(ctx, probeID)
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
	defer batch.Abort()
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

func (w *tombstoneWriter) reload(ctx context.Context, probeID string) bool {
	// Bound failed attempts too: a broken dictionary must not cause a reload
	// on every incoming id. Measure from completion to bound slow attempts.
	defer func() { w.lastReload = time.Now() }()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := w.conn.Exec(cctx, "SYSTEM RELOAD DICTIONARY tombstone_dict"); err != nil {
		w.log.Warn("tombstone dict reload failed", "err", err)
		return false
	}
	var found uint8
	if err := w.conn.QueryRow(cctx, "SELECT dictHas('tombstone_dict', ?)", probeID).Scan(&found); err != nil {
		w.log.Warn("tombstone dict reload probe failed", "err", err)
		return false
	}
	if found == 0 {
		w.log.Warn("tombstone dict reload did not apply; retrying")
		return false
	}
	return true
}

// stop drains, flushes, reloads once, and waits for the writer to exit.
func (w *tombstoneWriter) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.done
}
