You are the third independent reviewer of a performance-improvement plan for the
Go codebase at /home/user/archive-relay (a Nostr archive relay: khatru +
ClickHouse + embedded SQLite + crawlers).

Read these files, in this order:
1. docs/perf-plan-2026-09.md — THE PLAN UNDER REVIEW (your target)
2. docs/perf-review-2026-09.md — the original audit (F1-F21)
3. docs/perf-review-grok.md and docs/perf-review-pi.md — two prior validations
4. The source files the plan touches (internal/store/*, internal/crawler/*,
   internal/control/*, internal/stats/*, internal/api/*, cmd/archive-relay/main.go)

Evaluate the PLAN, not just the findings:

1. CORRECTNESS: For each solution in §2, verify it would actually work against
   the real code. Check the SQL (ClickHouse LIMIT 1 BY semantics, EXCHANGE
   TABLES, projections, dictionary debounce mechanics), the Go concurrency
   reasoning (batcher pending/backoff, async OnFlushed, semaphores), and the
   khatru/go-nostr integration claims (AssumeValid, LIMIT 1 BY key choices).
   Flag anything that is wrong, won't compile/apply, or has a subtle failure
   mode. In particular scrutinize:
   - `LIMIT 1 BY if(kind IN (0,3,10002), pubkey, id)` — is this valid
     ClickHouse? Does it produce the claimed semantics (newest version per
     pubkey for replaceable kinds, exact-dup collapse for regular kinds)?
     Does LIMIT BY interact correctly with ORDER BY ... LIMIT n per tier and
     the cross-tier merge in QueryEvents? Any edge cases (e.g., Limit < number
     of authors, id ASC tiebreak ordering)?
   - The batcher redesign (§2.2): does "don't drain while pending" actually
     preserve all events, or can it deadlock/stall FlushAll/shutdown?
   - `EXCHANGE TABLES` for author_follower_counts: correct syntax/semantics
     for a MergeTree table?
   - Default-since injection (§2.4): any filter shape it mishandles (e.g.,
     filter with Until but no Since? ids + kinds? #tag-only filters)?
   - The rowid-based seen_events prune: is the SQL correct?
   - AssumeValid implications: what exactly is skipped, and does the remaining
     CheckSignature in ingest cover the gap claimed?
2. GAPS: anything the plan missed or sequenced wrong (dependencies between
   items, migration hazards, things that should be verified before/after).
3. RISKS: where the plan's own "do NOT do" list is incomplete, or where a
   chosen solution has an unacknowledged downside.
4. PRIORITIES: does the P0-P4 ordering make sense? What would you reorder?

Output format: numbered findings, each with severity (BLOCKER / MAJOR / MINOR /
NIT), the plan section it applies to, evidence (file:line), and the recommended
change. End with a one-paragraph overall verdict on whether the plan is sound.
Be skeptical and concrete. Do NOT modify any files.
