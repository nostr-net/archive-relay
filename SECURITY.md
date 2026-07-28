# Security Policy

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security problems.

Instead, report vulnerabilities privately by opening a
[GitHub Security Advisory](https://github.com/nostr-net/archive-relay/security/advisories/new)
("Security" → "Report a vulnerability"), or email a maintainer directly.

We aim to acknowledge reports within 72 hours and to ship a fix or mitigation
within 30 days for confirmed issues affecting a release.

## Scope

This policy covers the `archive-relay` relay binary and its direct dependencies
as pinned in `go.mod`. It does **not** cover:

- Your ClickHouse deployment, your network, or your relay configuration (those
  are your responsibility).
- Vulnerabilities in dependencies reported via `go vuln check` without a
  working exploit path against this project.

## Hardening notes for operators

archive-relay is designed to run internet-facing. Before exposing it publicly:

- Put it behind a reverse proxy that terminates TLS and sets a sane
  `X-Forwarded-For` (the rate limiter honors the first public hop).
- Run ClickHouse on a private interface only (never expose :9000/:8123).
- Set `clickhouse.password` and a non-default user in `config.yaml`.
- Consider enabling NIP-42 AUTH gating for publish if you can't tolerate open
  writes.
- Back up ClickHouse and `control.db` on a schedule.
