package crawler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/store"
)

// eventSaver is the store seam ingest needs: *store.Store satisfies it, and
// tests can inject a fake without ClickHouse.
type eventSaver interface {
	SaveEvent(ctx context.Context, ev *nostr.Event) error
}

// ingest is the shared per-event ingestion step used by the firehose Crawler and
// the PriorityCrawler: in-memory dedup hot path, scope check, id↔body check,
// signature check, then SaveEvent into the tier batchers. Durable dedup
// (seen_events) is recorded post-flush by Dedup.OnFlushed. Returns true if the
// event was newly enqueued.
func ingest(ctx context.Context, s eventSaver, d *Dedup, log *slog.Logger, ev *nostr.Event) bool {
	return ingestStep(ctx, s, d, log, ev, verifyEvent)
}

// verifyEvent is the production CheckID + CheckSignature pair. CheckID runs
// first: CheckSignature ignores evt.ID, so a validly-signed event with a
// mismatched id field would poison ID-based dedup/storage. AssumeValid on the
// upstream relay skips the library's subscription-loop verify, so this remains
// the single signature check.
func verifyEvent(ev *nostr.Event) (idOK, sigOK bool) {
	if !ev.CheckID() {
		return false, false
	}
	ok, _ := ev.CheckSignature()
	return true, ok
}

func ingestStep(ctx context.Context, s eventSaver, d *Dedup, log *slog.Logger, ev *nostr.Event, verify func(*nostr.Event) (idOK, sigOK bool)) bool {
	if d.Seen(ev.ID) {
		return false
	}
	if store.TierForKind(ev.Kind) == store.TierDrop {
		return false
	}
	idOK, sigOK := verify(ev)
	if !idOK {
		d.droppedBadID.Add(1)
		if log != nil {
			log.Debug("dropping event: id does not match body", "id", ev.ID)
		}
		return false
	}
	if !sigOK {
		return false
	}
	// CheckAndMark under one lock closes the Seen-then-Mark race between
	// concurrent ingest goroutines. Optimistic: mark in-memory seen now to
	// dedupe within the current buffer window; the durable record happens
	// post-flush. If SaveEvent fails below we back the mark out on batch-full
	// so a later re-pull retries.
	if !d.CheckAndMark(ev.ID) {
		return false
	}
	if err := s.SaveEvent(ctx, ev); err != nil {
		if errors.Is(err, store.ErrBatchFull) {
			d.Unmark(ev.ID)
			if log != nil {
				log.Warn("batch full; event dropped (will re-ingest next sync)", "id", ev.ID)
			}
		} else if log != nil {
			log.Warn("save failed", "id", ev.ID, "err", err)
		}
		return false
	}
	return true
}
