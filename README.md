# archive-relay

A **selective social-core Nostr archive relay** — one Go binary on
[`khatru`](https://github.com/fiatjaf/khatru) + [ClickHouse](https://clickhouse.com/)
(+ embedded SQLite for the control plane). Stores only kinds
`0, 1, 3, 6, 7, 16, 9735, 10002` (profiles, notes, contacts, reposts, reactions,
zaps, relay lists). DMs, gift wraps, and ephemeral kinds are out of scope by design.

## Quickstart

```bash
docker compose -f deploy/docker-compose.yml up -d   # ClickHouse (dev)
make build
cp config.example.yaml config.yaml
./archive-relay --config config.yaml                # :3334, crawls 4 relays
```

The first boot creates its ClickHouse database (ENGINE=Atomic) automatically.

## Production (systemd on an LXC container / KVM VM)

No orchestrator needed:

```bash
# 1. ClickHouse: install natively (or point clickhouse.addr at any server),
#    set a password, and put the same credentials in config.yaml.
# 2. Relay binary + config:
install -m 0755 archive-relay /usr/local/bin/
install -d -o archiver -g archiver /var/lib/archive-relay /etc/archive-relay
install -m 0600 -o archiver config.yaml /etc/archive-relay/config.yaml
# 3. Service (hardened unit with the shutdown-budget comment):
install -m 0644 deploy/archive-relay.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now archive-relay
journalctl -u archive-relay -f
```

`TimeoutStopSec=150` in the unit covers the worst-case graceful flush
(3×30s batchers + 20s tombstones + 10s dedup writer). Monitoring is
journald + curl — no metrics server needed:

```bash
curl -s localhost:3334/v1/health
# → ingest.last_event_age_s (alert if climbing past ~30m on a busy relay),
#   ingest.dropped_durable_writes / dropped_bad_id (want 0 or flat),
#   "firehose heartbeat" log lines every 5min per source.
```

Backups: snapshot ClickHouse first, then copy `control.db` + `control.db-wal`
+ `control.db-shm` AFTER a quiet period (or stop the service first). A stale
`seen_events` is safe (re-ingest dedupes); a stale `allowed_pubkeys`/
`scheduled_events` is not — back up SQLite regularly.

Plain HTTP/WS by design — front it with a TLS proxy (Caddy/nginx/Traefik) for
`wss://`, and give ClickHouse a password before exposing it.

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
TTL, `policy.*` REQ-breadth limits, and an optional `classifier` map to override
kind→tier without recompiling. Crawler sources are the `-sources` flag.

Two optional subsystems:

- **`crawler.*`** — a per-pubkey *priority* crawl: `crawler.priorityPubkeys` are
  fetched from `crawler.relays` every `crawler.interval` (incremental, with a
  periodic full-history sweep) so their in-scope events are never missed.
- **`auth.*`** — `auth.enabled` gates publish+read behind NIP-42 AUTH plus a
  pubkey allow-list (static `auth.allowPubkeys` ∪ the `allowed_pubkeys` table,
  managed by `auth.adminPubkeys` over the NIP-86 RPC). Requires
  `relay.serviceURL`. When enabled, the `/v1/*` REST API also requires a NIP-98
  signed request from an allow-listed pubkey (`/v1/health` stays open).

## Test

```bash
make test-unit          # no external deps
make test-integration   # needs ClickHouse on localhost:9000
```

Ops profiling (stdlib `pprof`) binds to `127.0.0.1:6060` and is never exposed
on the public relay port.

CI runs both, integration against a ClickHouse service container.

## Architecture

```
khatru + REST + crawler + scheduler ──▶ ClickHouse (events, tombstones, stats)
                                      ──▶ SQLite     (dedup, scheduled events)
```


## License

MIT — see [`LICENSE`](./LICENSE).
