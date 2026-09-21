package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

// feedRetention is how long events_feed keeps rows. It must comfortably cover
// policy.defaultSinceHours (default 48h); if an operator configures a longer
// default window than this, the feed path disengages (falls back to tier
// tables) — see feedEligible.
const feedRetention = 7 * 24 * time.Hour

// feedFlushEvery / feedBatchMax coalesce tier-batch flushes into feed-table
// INSERTs: one part per interval at load, none at idle. CH guidance is ≤1
// insert/sec/table with sizeable batches — 5s / 10k fits with margin.
const (
	feedFlushEvery = 5 * time.Second
	feedBatchMax   = 10000
	feedQueueCap   = 20000 // bounded; overflow drops (feed is an optimization)
)

// feedDDL creates the recent-feed table: a time-ordered mirror of recent
// events serving the pure global-feed REQ shape. ORDER BY (created_at, id)
// lets the reverse-read early-terminate (LIMIT n touches ~n rows, not the
// whole window) — the tier tables' PK (kind, pubkey, ...) shreds time across
// pubkeys and forces a full window scan per REQ (read-perf-2026-09: 148ms vs
// 8ms). Plain MergeTree is enough: read-side collapse + Go-side id-dedupe
// handle duplicates; ReplacingMergeTree dedup semantics are not needed.
func feedDDL() string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS events_feed (%[1]s
) ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(toDateTime(created_at))
  ORDER BY (created_at, id)
  TTL toDateTime(created_at) + INTERVAL %[2]d DAY DELETE
  SETTINGS index_granularity = 8192;
`, tierColumnsTypeNoIndexes(), int(feedRetention.Hours()/24))
}

// tierColumnsTypeNoIndexes is tierColumnsType without the tier-only bloom
// indexes (the feed table is scanned by time, not by pubkey/tags).
func tierColumnsTypeNoIndexes() string {
	return `
  id           String,
  pubkey       String,
  created_at   UInt32,
  kind         UInt32,
  content      String,
  sig          String,
  tags_raw     String,
  tags         Array(Array(String)),
  tag_e        Array(String) DEFAULT ` + tagEDefaultExpr + `,
  tag_p        Array(String) DEFAULT ` + tagPDefaultExpr + `,
  tag_t        Array(String) DEFAULT ` + tagTDefaultExpr + `,
  tag_d        String DEFAULT ` + tagDDefaultExpr + `,
  reply_to     String,
  received_at  DateTime64(3) DEFAULT now64(3)`
}

// feedWriter coalesces events from every tier batcher's successful flush into
// events_feed (single goroutine, tombstone-writer pattern).
type feedWriter struct {
	conn   driver.Conn
	log    *slog.Logger
	in     chan *nostr.Event
	stopCh chan struct{}
	done   chan struct{}
	once   sync.Once
	drops  uint64 // approximate; diagnostics only
	dropsM sync.Mutex
}

func newFeedWriter(conn driver.Conn, log *slog.Logger) *feedWriter {
	return &feedWriter{
		conn: conn, log: log,
		in:     make(chan *nostr.Event, feedQueueCap),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (w *feedWriter) start() { go w.run() }

// offer pushes an event non-blocking; a full queue drops (feed is an
// optimization — tier tables remain the source of truth).
func (w *feedWriter) offer(evt *nostr.Event) {
	select {
	case w.in <- evt:
	default:
		w.dropsM.Lock()
		w.drops++
		w.dropsM.Unlock()
	}
}

// Drops returns the approximate count of dropped events (diagnostics).
func (w *feedWriter) Drops() uint64 {
	w.dropsM.Lock()
	defer w.dropsM.Unlock()
	return w.drops
}

func (w *feedWriter) run() {
	defer close(w.done)
	ctx := context.Background()
	buf := make([]*nostr.Event, 0, feedBatchMax)
	tick := time.NewTicker(feedFlushEvery)
	defer tick.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		w.dropsM.Lock()
		dropped := w.drops
		w.drops = 0
		w.dropsM.Unlock()
		if dropped > 0 {
			// ponnytail: aggregate-per-flush logging; per-event would spam at
			// overload. Wire Drops() into /v1/health if feed holes ever matter.
			w.log.Warn("feed writer dropped events (feed will have holes until they age out of the window)", "n", dropped)
		}
		if err := w.insert(ctx, buf); err != nil {
			// Feed is best-effort: log and move on (next flush re-offers via
			// nothing — these rows are lost from the feed until TTL window
			// makes it moot; tier tables serve any shape that needs them).
			w.log.Warn("feed insert failed", "n", len(buf), "err", err)
		}
		buf = buf[:0]
	}

	for {
		select {
		case evt := <-w.in:
			buf = append(buf, evt)
			if len(buf) >= feedBatchMax {
				flush()
			}
		case <-tick.C:
			flush()
		case <-w.stopCh:
			for {
				select {
				case evt := <-w.in:
					buf = append(buf, evt)
					if len(buf) >= feedBatchMax {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (w *feedWriter) insert(ctx context.Context, events []*nostr.Event) error {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	batch, err := w.conn.PrepareBatch(cctx, "INSERT INTO events_feed ("+tierColumns+")")
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

func (w *feedWriter) stop() {
	w.once.Do(func() { close(w.stopCh) })
	<-w.done
}

// feedEligible reports whether a filter may be served from events_feed:
// the pure global-feed shape (no ids/authors/tags/until), a bounded Since
// inside the feed TTL window, and only in-scope kinds. This mirrors the
// relay-layer default-since injection shape; REST/ COUNT/ internal calls
// that arrive without Since are NOT feed-eligible (they scan tiers).
func feedEligible(f nostr.Filter, override map[int]string) bool {
	if len(f.IDs) > 0 || len(f.Authors) > 0 || len(f.Tags) > 0 || f.Until != nil {
		return false
	}
	if f.Since == nil {
		return false
	}
	oldest := time.Now().Add(-feedRetention + 2*time.Hour) // 2h margin
	if f.Since.Time().Before(oldest) {
		return false
	}
	for _, k := range f.Kinds {
		if classifyWithOverride(k, override) == TierDrop {
			return false
		}
	}
	return true
}
