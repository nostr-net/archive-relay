# archive-relay

[![CI](https://github.com/nostr-net/archive-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/nostr-net/archive-relay/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nostr-net/archive-relay.svg)](https://pkg.go.dev/github.com/nostr-net/archive-relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A **selective social-core Nostr archive relay** — one Go binary on
[`khatru`](https://github.com/fiatjaf/khatru) + [ClickHouse](https://clickhouse.com/)
(+ embedded SQLite for the control plane). Stores only kinds
`0, 1, 3, 6, 7, 16, 9735, 10002` (profiles, notes, contacts, reposts, reactions,
zaps, relay lists). DMs, gift wraps, and ephemeral kinds are out of scope by
design ([`ANALYSIS.md`](./ANALYSIS.md) §3d).

## Quickstart

```bash
docker compose -f deploy/docker-compose.yml up -d   # ClickHouse
make build
cp config.example.yaml config.yaml
./archive-relay --config config.yaml                # :3334, crawls 4 relays
```

```bash
# nostr relay
nak event -k 1 "hello" -r ws://localhost:3334
nak req  -k 1 -r ws://localhost:3334

# REST
curl localhost:3334/v1/health
curl 'localhost:3334/v1/stats/daily?days=7'
curl localhost:3334/v1/note/<event-id>      # engagement
curl localhost:3334/v1/pubkey/<pubkey>      # followers
```

## Config

YAML, production-tuned defaults — see [`config.example.yaml`](./config.example.yaml).
Key knobs: `batch.maxSize`/`maxAge` (insert coalescing), per-tier `retention.*`
TTL, and an optional `classifier` map to override kind→tier without recompiling.
Crawler sources are the `-sources` flag.

## Test

```bash
make test-unit          # no deps
make test-integration   # needs ClickHouse on localhost:9000
```

CI runs both, integration against a ClickHouse service container.

## Architecture

```
khatru + REST + crawler + scheduler ──▶ ClickHouse (events, tombstones, stats)
                                      ──▶ SQLite     (dedup, scheduled events)
```

One binary, no Postgres, no Redis. Design rationale in
[`ANALYSIS.md`](./ANALYSIS.md); roadmap in [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md).

## Limitations

- Compression validated only to ~tens of thousands of rows (needs a
  million-row negentropy backfill to confirm the expected 6–15× vs postgres).
- Single-node ClickHouse (no replication configured).
- NIP-42 AUTH not enforced — open reads/writes; rely on the rate limiter +
  reverse proxy + a private ClickHouse until wired.
- Negentropy is served, not pulled (deep backfill is planned).

## License

MIT — see [`LICENSE`](./LICENSE).
