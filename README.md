# archive-relay

[![CI](https://github.com/nostr-net/archive-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/nostr-net/archive-relay/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nostr-net/archive-relay.svg)](https://pkg.go.dev/github.com/nostr-net/archive-relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A selective **social-core Nostr archive relay**, built as a single Go binary on
[`khatru`](https://github.com/fiatjaf/khatru) + [ClickHouse](https://clickhouse.com/)
(+ embedded SQLite for the control plane). It ingests from upstream relays,
stores events with tiered per-kind retention, honors NIP-09 deletions, and
serves REQ/COUNT/negentropy plus a small stats REST API.

> What does "selective social-core" mean? It stores only kinds `0, 1, 3, 6, 7,
> 16, 9735, 10002` — profiles, notes, contacts, reposts, reactions, zaps, relay
> lists. DMs, gift wraps, and ephemeral kinds are explicitly out of scope by
> design (see [`ANALYSIS.md`](./ANALYSIS.md) §3d).

## Status

Working and end-to-end tested against real ClickHouse and the real
`relay.nostr.net`. Features:

- **Relay protocol** (khatru): REQ, COUNT, EVENT, NIP-09 deletion, NIP-50 search
  (n-gram bloom), NIP-77 negentropy (flows through `QueryEvents`).
- **Tiered storage**: per-kind retention (`permanent` ∞, `archive` 10y, `social`
  1y) as separate `ReplacingMergeTree` tables with monthly partitions and TTL.
- **Deletion**: tombstone table + dictionary (instant hide), no hot-path
  mutations; physical reclamation via a separate optional job.
- **Crawler**: multi-source ingest with idempotent, **crash-safe** dedup
  (`seen_events` is recorded only after events are durable in ClickHouse).
- **Stats**: snapshot refresh jobs (per-note engagement, daily rollups, DAU,
  follower counts) that survive raw-data TTL.
- **Scheduler**: future-dated events parked in SQLite, published when due.
- **WoT read filter** (off by default), per-IP **rate limiting**, embedded
  SQLite control plane, a real `/v1/health` (pings ClickHouse).

`gofmt` clean, `go vet ./...` clean, all tests green.

## Quickstart

```bash
# 1. ClickHouse (single node)
docker compose -f deploy/docker-compose.yml up -d

# 2. Build + run (crawls damus/nos.lol/primal/nostr.net, serves :3334)
make build
cp config.example.yaml config.yaml   # edit if needed
./archive-relay --config config.yaml
```

Then talk to it over the websocket relay, the REST API, or ClickHouse directly:

```bash
# nostr relay (ws)
nak event -k 1 "hello archive" -r ws://localhost:3334
nak req  -k 1 -r ws://localhost:3334

# REST stats
curl localhost:3334/v1/health
curl 'localhost:3334/v1/stats/daily?days=7'
curl localhost:3334/v1/note/<event-id>          # engagement
curl localhost:3334/v1/pubkey/<pubkey>          # follower count
```

## Configuration

YAML, with sensible production-tuned defaults. See
[`config.example.yaml`](./config.example.yaml). Notable knobs:

| key | default | what |
|---|---|---|
| `relay.addr` | `:3334` | ws + http listen address |
| `clickhouse.*` | localhost:9000 | native protocol; **set a password for production** |
| `batch.maxSize` / `maxAge` | `5000` / `5s` | coalesce inserts (avoid the ClickHouse small-parts anti-pattern) |
| `retention.*` | `archive 10 YEAR`, `social 1 YEAR` | per-tier TTL; empty = forever |
| `classifier` | — | override the kind→tier map without recompiling |

The crawler sources are a `-sources` flag (comma-separated), defaulting to four
open relays. Run with `-h` for all flags.

## Testing

```bash
make test-unit          # fast, no external deps
make test-integration   # needs ClickHouse on localhost:9000
go test -tags=integration ./internal/crawler/   # hits live relay.nostr.net
```

CI ([`.github/workflows/ci.yml`](./.github/workflows/ci.yml)) runs unit tests on
every push and integration tests against a ClickHouse service container.

## Architecture

One Go binary, three storage roles chosen by fitness:

```
khatru (nostr protocol)  ─┐
REST + crawler + scheduler ├──▶ ClickHouse: events, tombstones, stats
WoT read filter, policies │    SQLite:     crawler dedup, scheduled events, auth
```

No Postgres, no Redis — SQLite and the relay are libraries in the one binary.
Full rationale (khatru vs alternatives, tiered retention, deletion strategy,
on-demand vs materialized stats, the crash-safe ingest ordering) is in
[`ANALYSIS.md`](./ANALYSIS.md); the build roadmap in
[`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md).

```
cmd/archive-relay/   entrypoint — wires store+crawler+stats+scheduler+api+limits
internal/
  store/             ClickHouse backend (schema, batcher, query, NIP-10 replies)
  crawler/           upstream ingest (crash-safe dedup, reconnect/backoff)
  stats/             snapshot refresh jobs + query helpers
  api/               REST endpoints
  scheduler/         future-dated event deferral
  policy/            scope gate, filter caps, WoT read filter, rate limiter
  control/           embedded SQLite control plane
  relay/             khatru wiring
  e2e/               full websocket end-to-end test
deploy/              docker-compose for ClickHouse
```

## Honest limitations

- **Compression is only validated up to ~tens of thousands of rows.** The
  expected 6–15× vs postgres needs a million-row run (e.g. via a negentropy
  backfill) to confirm. See `ANALYSIS.md` §2.
- **Single-node ClickHouse.** HA replication (Keeper) is not configured; for
  production use backups or a replicated cluster.
- **NIP-42 AUTH is not enforced.** The relay accepts open reads/writes; rely on
  the rate limiter + a reverse proxy + a private ClickHouse until you wire AUTH.
- **Negentropy is served, not pulled.** Deep historical backfill from other
  archives is the next planned capability.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md). Keep changes focused and tested,
and respect the social-core scope. By contributing you agree to abide by the
[Code of Conduct](./CODE_OF_CONDUCT.md).

## License

MIT — see [`LICENSE`](./LICENSE).
