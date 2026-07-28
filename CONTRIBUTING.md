# Contributing to archive-relay

Thanks for your interest in improving archive-relay! This is a small project and
the fastest path to merged code is keeping changes focused and tested.

## Setup

```bash
go install              # Go 1.24+
# ClickHouse (for running integration tests):
docker compose -f deploy/docker-compose.yml up -d
```

## Development loop

```bash
make fmt vet            # format + lint (run before every commit)
make test-unit          # fast, no external deps
make test-integration   # needs ClickHouse on localhost:9000
go build -o archive-relay ./cmd/archive-relay
```

The crawler test (`internal/crawler`) and e2e test (`internal/e2e`) hit the live
network (`relay.nostr.net`) — run them by hand, not in every loop:

```bash
go test -tags=integration -count=1 -timeout 120s ./internal/crawler/ ./internal/e2e/
```

## Before opening a PR

1. `make fmt vet` is clean.
2. New behavior comes with a test. Pure-logic code → unit test; storage/query
   behavior → an integration test under `//go:build integration`.
3. Don't change the on-disk schema in a way that breaks existing deployments
   without a migration note in the PR description.
4. Keep commits focused; one logical change per PR.

## Scope

archive-relay intentionally stores only the nostr **social core**
(kinds `0, 1, 3, 6, 7, 16, 9735, 10002`). DMs, gift wraps, and ephemeral kinds
are explicitly out of scope by design — see `ANALYSIS.md` §3d. If your change
adds a new kind, justify the scope decision in the PR.

## Reporting bugs

Open an issue with: the commit/SHA, ClickHouse version, `archive-relay` config
(redact secrets), and the exact `relay.log` lines around the problem.
