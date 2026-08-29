package crawler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/store"
)

// ingest is the shared per-event ingestion step used by the firehose Crawler and
// the PriorityCrawler: in-memory dedup hot path, scope check, signature check,
// then SaveEvent into the tier batchers. Durable dedup (seen_events) is recorded
// post-flush by Dedup.OnFlushed. Returns true if the event was newly enqueued.
func ingest(ctx context.Context, s *store.Store, d *Dedup, log *slog.Logger, ev *nostr.Event) bool {
	if d.Seen(ev.ID) {
		return false
	}
	if store.TierForKind(ev.Kind) == store.TierDrop {
		return false
	}
	if ok, _ := ev.CheckSignature(); !ok {
		return false
	}
	// Optimistic: mark in-memory seen now to dedupe within the current buffer
	// window; the durable record happens post-flush. If SaveEvent fails below we
	// back the mark out on batch-full so a later re-pull retries.
	d.Mark(ev.ID)
	if err := s.SaveEvent(ctx, ev); err != nil {
		if errors.Is(err, store.ErrBatchFull) {
			d.Unmark(ev.ID)
			log.Warn("batch full; event dropped (will re-ingest next sync)", "id", ev.ID)
		} else {
			log.Warn("save failed", "id", ev.ID, "err", err)
		}
		return false
	}
	return true
}
