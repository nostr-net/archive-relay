# Changelog

All notable changes to archive-relay are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/) and uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Selective social-core Nostr archive relay (kinds 0,1,3,6,7,16,9735,10002).
- Tiered ClickHouse storage (`permanent`/`archive`/`social`/`transient`) with
  per-tier TTL and monthly partitions.
- Tombstone + dictionary deletion (NIP-09) with instant hide, no hot-path
  mutations.
- Multi-source crawler with reconnect/backoff and crash-safe dedup
  (`seen_events` recorded only after a durable flush).
- Stats snapshot jobs: per-note engagement (NIP-10-aware replies), daily
  rollups, DAU, follower counts.
- Future-dated event scheduler (SQLite-backed).
- WoT read-time filter (default disabled), per-IP rate limiter.
- REST API (`/v1/health`, `/v1/stats/{daily,dau}`, `/v1/note/{id}`,
  `/v1/pubkey/{pk}`, `/v1/events`) with a real health check that pings CH.
- End-to-end + integration tests (real ClickHouse, real relay.nostr.net).

### Notes
- Compression at million-row scale is not yet validated (see `ANALYSIS.md` §2).
- Negentropy is served but not pulled (deep backfill is planned).
