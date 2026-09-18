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
// the PriorityCrawler: CheckAndMark (single-lock winner), scope check, id↔body
// check, signature check, then SaveEvent into the tier batchers. Losers of
// CheckAndMark skip verify; a failed verify Unmarks so a later re-pull retries.
// Durable dedup (seen_events) is recorded post-flush by Dedup.OnFlushed.
// Returns true if the event was newly enqueued.
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
	// CheckAndMark first so only the single-lock winner pays verify. Invalid-hex
	// ids return unseen=true without poisoning the map; CheckID then drops them.
	if !d.CheckAndMark(ev.ID) {
		return false
	}
	if store.TierForKind(ev.Kind) == store.TierDrop {
		d.Unmark(ev.ID)
		return false
	}
	idOK, sigOK := verify(ev)
	if !idOK {
		d.Unmark(ev.ID)
		d.droppedBadID.Add(1)
		if log != nil {
			log.Debug("dropping event: id does not match body", "id", ev.ID)
		}
		return false
	}
	if !sigOK {
		d.Unmark(ev.ID)
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
