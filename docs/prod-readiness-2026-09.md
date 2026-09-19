# Production-readiness evaluation — archive-relay

Date: 2026-09-19. Evidence: live failure-injection tests against a running
instance (relay.nostr.net feed, ClickHouse 26.9.1 local), the full test suite,
and an adversarial ops review (grok). This document is the verdict record.

## Verdict: NOT production-ready as-is — close.

The relay **engine** is production-grade. The **operations surface** is not.
Four specific blockers separate "works" from "can be paged on".

## 1. Live evidence (all passed)

| Test | Procedure | Result |
|---|---|---|
| CH outage under load | SIGSTOP clickhouse-server 60s, 25k-event publish flood (8 conns), SIGCONT | First valve = per-IP limiter (600 OK, 24,400 clean OK:false at 372 ev/s); batcher held pending with 1s→30s backoff; **RSS +3MB flat**; after recovery **600/600 accepted events stored, zero loss** |
| REQ load | 16 concurrent clients, 30s (3.4M REQs) | **113,400 REQ/s sustained**, RSS flat at 36MB, zero goroutine leak |
| Parts pressure | after outage + flood | **7 active parts** across 3 tier tables — batching works as designed |
| Silent WS stall | library source audit + live socket | go-nostr pings every 29s, closes after 3 fails → our reconnect loop handles dead TCP (~90–120s detection) |
| SIGTERM shutdown | kill -TERM mid-flood | clean exit, final flush, no hang |
| Restart idempotency | restart with warm dedup | `ingested=0 skipped=125` — no mass re-ingest |
| NIP-09 / replaceable lifecycle | live publish→delete→REQ, publish v1→v2→REQ | hidden ≤500ms with `nip09` tombstone; only newest version served |

Plus: 5 code-review rounds (grok) with all findings fixed; unit + race +
integration suites green.

## 2. Ops readiness scores (grok adversarial review)

| Dimension | Score | One-line summary |
|---|---|---|
| Observability | **2/5** | slog only; no /metrics; dedup counters exist but unexposed; no ingest heartbeat |
| Deployment | **2/5** | good image (distroless nonroot); no probes contract, no relay compose/k8s, shutdown unbounded |
| Failure modes | **3/5** | batcher/dedup excellent; scheduler fails OPEN on disk-full; hung-but-pingable subscription invisible |
| Security | **3/5** | NIP-98 + limiter wired; XFF spoofable, NIP-86 body unbounded (khatru), CORS * |
| Data safety | **3/5** | ingest crash-safety good; tombstone loss windows bounded+logged; tags backfill mutation re-queued every boot; TTL changes are no-ops on existing tables |
| Docs/runbook | **2/5** | quickstart only; no first-boot DB contract, no backup order, example maxSize drifts from code default |

## 3. BLOCKERS (must fix before real traffic)

1. **Liveness vs readiness are one endpoint.** `/v1/health` 503s on CH ping
   failure and is documented as a liveness probe — a k8s liveness using it
   kills the pod on any CH blip and **discards the in-memory retry buffer**
   (ACKed events). Split: liveness = process-up (no CH), readiness = CH ping.
   Also stop probing on the 2-conn stats pool that 120s FINAL jobs occupy.
2. **Shutdown exceeds a 30s grace period.** `srv.Shutdown(context.Background())`
   is unbounded; client WS are hijacked (never waited); worst case
   3×30s sequential batcher flushes + 20s tombstone + 10s writer → SIGKILL
   mid-flush loses ACKed events. Need a total shutdown deadline + parallel
   tier flush budget + documented terminationGracePeriodSeconds.
3. **Ingest stall is undetectable.** After EOSE a hung (but pingable)
   subscription looks identical to a quiet one — health stays 200. No
   /metrics; `DroppedWrites`/`DroppedBadID` counters are unexposed. Need
   per-source last-event-age + firehose idle timeout + a scrape endpoint.
4. **First boot does not start.** The process pings the configured database
   before creating it (fresh deploy exits); follower refresh needs the DB
   `ENGINE = Atomic`. Need `CREATE DATABASE IF NOT EXISTS ... ENGINE = Atomic`
   at init (or a documented bootstrap step).

## 4. HIGH (first week of production)

Prometheus endpoint for existing counters; NIP-86 body cap (khatru reads
unbounded); stop trusting XFF unless configured behind a proxy; CH password
via env; gate the every-boot `UPDATE tags WHERE empty(tags)` mutation storm;
`retention.*` changes silently no-op on existing tables (no MODIFY TTL);
scheduler fail-open ACKs-and-drops future-dated events on SQLite errors;
config.example maxSize 10000 vs code default 5000.

## 5. MEDIUM/LATER (tracked)

JSON logs + level knob; limiter/pprof/sources as YAML; tombstone table prune;
seen_events vacuum sizing at firehose rate (20M cap ≈ 1.1h at 5k ev/s);
backup runbook (CH snapshot then control.db*, WAL files); CI image build +
govulncheck + CH version pin parity; config validation (maxSize≥1, TTL
strings); writeErr leaking CH internals; classifier override ≠ crawler scope.

## 6. Recommended deployment shape (when blockers are fixed)

- k8s: liveness=/live (process), readiness=/ready (CH ping), terminationGracePeriodSeconds ≥ 90
- CH: version-pinned to match prod (26.9.1 validated), max_server_memory_usage set
- Sizing baseline observed: relay RSS 36MB at 113k REQ/s + firehose; dedup maps
  ~100–200MB at memCap 2M ×2 generations; batcher buffers ≈ maxSize×2 events/tier
- Reverse proxy terminates TLS; XFF trusted only there

Full ops review transcript: session logs (grok prod-review, 2026-09-19).
