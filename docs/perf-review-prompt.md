You are reviewing a performance audit of the Go codebase at /home/user/archive-relay
(a Nostr archive relay: khatru + ClickHouse + embedded SQLite + crawlers).

The audit findings are in docs/perf-review-2026-09.md — read it first, then read
the referenced source files to verify each claim against the actual code.

Your job, in order:

1. VALIDATE each finding F1-F21. For each, verdict: CORRECT / PARTIALLY CORRECT /
   INCORRECT / OVERSTATED, with a one-line reason. Check the real code — e.g.,
   does the crawler really bypass ReplaceEvent? Does the batcher retry path
   really grow unboundedly? Is the content index really unused? Cite code.
2. CHALLENGE the proposed fix directions. Where the suggested fix is wrong,
   risky, or suboptimal, propose the better one. Especially: tombstone
   dictionary reloads, tags_raw JSON decode, dedup memory, batcher backpressure,
   replaceable-version handling on ingest, and the global-feed query pattern.
3. BRAINSTORM the best solution for the top ~8 issues — concrete, implementable,
   minimal-blast-radius. Flag any finding where the "fix" would hurt correctness
   or operational simplicity.
4. ADD anything the audit missed that matters for performance at firehose scale
   (thousands of events/sec ingest, large ClickHouse tables, many concurrent
   REQ subscribers).
5. Give a final prioritized ordering (what to do first and why).

Be skeptical and technical. If a finding is wrong, say so plainly. Keep the
response structured and cite file:line where relevant. Do NOT modify any files —
this is read-only review.
