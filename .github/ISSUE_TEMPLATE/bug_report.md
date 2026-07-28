---
name: Bug report
about: Something is broken
---

**Describe the bug**
A clear description of what's wrong.

**To reproduce**
Steps, the `archive-relay` config (redact secrets), and the exact `relay.log` lines.

**Expected vs actual**

**Environment**
- archive-relay commit/SHA:
- ClickHouse version:
- Go version (if building from source):
- OS:

**Scope check (if about stored kinds)**
archive-relay intentionally stores only kinds `0,1,3,6,7,16,9735,10002` (the
nostr social core). DMs/gift wraps/ephemeral are dropped by design. Is your bug
about an out-of-scope kind?
