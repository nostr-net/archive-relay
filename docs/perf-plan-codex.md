I would revise the plan before implementation. I checked the source and pinned Go dependencies, consulted ClickHouse documentation, and reproduced the SQLite pruning issue in memory. No files were modified. ClickHouse was unavailable locally, so SQL conclusions below are based on source and documented semantics, not integration execution.

1. **BLOCKER — §2.1: the proposed grouping key merges different replaceable kinds.**

   `LIMIT 1 BY if(kind IN (0,3,10002), pubkey, id)` is valid ClickHouse syntax: both branches return `String`. But an author’s profile, contacts, and relay list share the same key, so `{kinds:[0,3,10002]}` returns only one of those three events. They all normally occupy the permanent tier. Evidence: [classifier.go:31](/home/user/archive-relay/internal/store/classifier.go:31), [plan:68](/home/user/archive-relay/docs/perf-plan-2026-09.md:68).

   **Change:** use `LIMIT 1 BY kind, if(kind IN (0,3,10002), pubkey, id)`. Put it after `ORDER BY created_at DESC, id ASC` and before the final `LIMIT n`. This separates replaceable kinds and prevents the pubkey/regular-ID namespaces from colliding. ClickHouse explicitly supports expressions and multiple grouping expressions in [LIMIT BY](https://clickhouse.com/docs/reference/statements/select/limit-by).

2. **BLOCKER — §§2.3, 2.10: the plan assumes a working tombstone dictionary, but the supplied DDL uses the wrong layout.**

   The dictionary declares `id String` with `LAYOUT(HASHED())`. Simple `HASHED` dictionaries require a `UInt64` key; arbitrary string keys require a complex-key layout. Every event query depends on this dictionary. Evidence: [schema.go:75](/home/user/archive-relay/internal/store/schema.go:75), [filter_sql.go:83](/home/user/archive-relay/internal/store/filter_sql.go:83). ClickHouse documents the distinction in its [dictionary guidance](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/dictionary/best-practices.md).

   **Change:** make dictionary creation/loading a P0 prerequisite: use `COMPLEX_KEY_HASHED()`, the corresponding tuple-key lookup form, and a migration for existing installations. `CREATE DICTIONARY IF NOT EXISTS` will not repair an existing definition. Verify actual membership after reload, including in a non-default database.

3. **MAJOR — §2.1: collapse happens after filtering, so it does not universally hide superseded versions.**

   `WHERE` already contains IDs, tags, time bounds, and tombstone exclusion before the proposed `LIMIT BY`. Suppose the newest contacts event no longer contains `#p=X`: filtering for `#p=X` first can return an older contacts event. Likewise, `Until` can expose an earlier version, and deleting the latest crawled version exposes the previous untombstoned version. Evidence: [filter_sql.go:27](/home/user/archive-relay/internal/store/filter_sql.go:27), [store.go:217](/home/user/archive-relay/internal/store/store.go:217).

   **Change:** define whether the relay serves the *latest matching version* or filters only the *current version*. The latter requires winner selection before predicates that can exclude the winner, plus an explicit deletion policy. Add tests for tags removed by a newer version, old-ID lookup, historical `Until`, and deletion of the newest version. The proposed clause alone does not establish the stronger semantics.

4. **MAJOR — §2.1: the cross-tier merge does not preserve the proposed total ordering.**

   SQL already uses ascending `id` by default, but Go sorts only by timestamp. Equal-time events retain tier traversal order; changing the order of filter kinds can therefore change which events survive the global limit. Evidence: [filter_sql.go:95](/home/user/archive-relay/internal/store/filter_sql.go:95), [store.go:278](/home/user/archive-relay/internal/store/store.go:278), [store.go:294](/home/user/archive-relay/internal/store/store.go:294).

   **Change:** compare `(created_at DESC, id ASC)` in Go too. Per-tier top-N followed by global top-N is otherwise sufficient when groups occupy exactly one tier. `Limit < number of authors` legitimately returns only N winners—it does not promise one result for every author. Require a migration invariant if classifier changes can leave the same event/group in multiple tiers; the current merge performs no cross-tier deduplication.

5. **MAJOR — §2.2: “do not drain” must disable ordinary input consumption and preserve control-channel progress.**

   Avoiding the `drain()` helper alone leaves `case evt := <-b.in` active, so the worker still consumes arrivals. Conversely, blocking inside a retry loop or sleeping through backoff can prevent servicing `flushReq` and shutdown. `FlushAll` currently has an unbounded request send, discards the reply error, and both public layers return no error. Evidence: [batcher.go:115](/home/user/archive-relay/internal/store/batcher.go:115), [batcher.go:136](/home/user/archive-relay/internal/store/batcher.go:136), [store.go:78](/home/user/archive-relay/internal/store/store.go:78).

   **Change:** specify a worker state machine: disable the input select case while pending, use a retry timer, and keep stop/flush requests selectable. Bound draining before creating a batch; retain every unsent chunk after a partial success. Make `FlushAll(ctx) error` propagate through both layers and define whether it flushes a snapshot of accepted work or waits for producers to stop. Test flush during backoff, cancellation, partial chunk failure, and flush after shutdown.

6. **MAJOR — §2.2: logged shutdown loss is not guaranteed recoverable.**

   “Dedup never marked them” is false for the in-memory cache: crawler events are marked before enqueue. More importantly, an acknowledged client event may exist nowhere else. Logging its loss cannot make a crawler recover it. Producers are not joined before deferred `Store.Close`, and `enqueue` has no closed-state check, allowing acceptance into an already-stopped batcher. Evidence: [ingest.go:27](/home/user/archive-relay/internal/crawler/ingest.go:27), [batcher.go:70](/home/user/archive-relay/internal/store/batcher.go:70), [main.go:155](/home/user/archive-relay/cmd/archive-relay/main.go:155).

   **Change:** stop acceptance, terminate/join producers and active relay handlers, then drain batchers under an explicit deadline. Choose and document the durability contract: durable spooling or acknowledgement after persistence is needed to guarantee preservation across shutdown outages. At minimum, distinguish recoverable crawler replay from potentially permanent loss of client events.

7. **MAJOR — §2.6: slimming the probe preserves a dangerous retire-before-save sequence.**

   `ReplaceEvent` tombstones old versions before attempting to enqueue the replacement. Once the repaired batcher starts reliably returning `ErrBatchFull`, a replacement can hide the existing event and then be rejected. Enqueue success is also not persistence. Evidence: [store.go:184](/home/user/archive-relay/internal/store/store.go:184).

   **Change:** coordinate retirement with successful durable storage, or defer retirement and rely on a correctly defined read-side winner mechanism. The two-column `LIMIT 50` probe is valid and sufficient to detect the latest committed winner, but it does not make replacement atomic or serialize concurrent replacements. Test a full batcher, failed replacement flush, and competing publishes.

8. **MAJOR — §2.4: default-since injection breaks historical pagination and targeted tag queries.**

   `{until: one_year_ago, kinds:[1]}` receives `since=now-48h`, producing an impossible interval. Tag-only queries for old threads, reactions, or mentions are silently restricted too. IDs plus kinds are correctly exempt under the stated condition. Evidence: [plan:119](/home/user/archive-relay/docs/perf-plan-2026-09.md:119), [filter_sql.go:43](/home/user/archive-relay/internal/store/filter_sql.go:43), [filter_sql.go:73](/home/user/archive-relay/internal/store/filter_sql.go:73).

   **Change:** limit the default to an explicitly defined global-feed filter shape. Exempt `Until` and targeted tags, or deliberately anchor a historical window to `Until`. Apply policy at a request boundary capable of notifying clients; `buildFilterSQL(f)` has neither client context nor configuration. Remember that changing this helper also changes COUNT. REST currently exposes no `since`/`until` escape hatch: [api.go:125](/home/user/archive-relay/internal/api/api.go:125).

9. **MAJOR — §§2.4, 2.13, P1/P4: the projection experiment is sequenced behind an incompatible query modifier.**

   Production queries retain `FINAL`, while removing it is deferred to P4. ClickHouse documents that projections are not supported for SELECTs with `FINAL`. Thus P1 may materialize an expensive full-copy projection merely to discover that the real query cannot use it. Evidence: [store.go:217](/home/user/archive-relay/internal/store/store.go:217), [plan:125](/home/user/archive-relay/docs/perf-plan-2026-09.md:125), [ClickHouse projection restriction](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree#projections).

   **Change:** test projection eligibility and corrected explicit deduplication together, on the deployed ClickHouse version, before full materialization. Include `dictHas`, `LIMIT BY`, and the actual ordering in EXPLAIN. Budget storage, merge work, and insert overhead for `SELECT *`; materialize representative partitions first. Use the complete `ALTER TABLE ... MATERIALIZE PROJECTION p_feed` statement.

10. **MAJOR — §§2.5, 2.7: closing the result channel does not report a query failure.**

    `QueryEvents` returns `(out,nil)` before executing SQL. khatru interprets channel closure as completion; REST returns HTTP 200. An error log therefore still produces successful-looking empty/partial results. Iteration errors from `rows.Err()` are also never checked. Evidence: [store.go:210](/home/user/archive-relay/internal/store/store.go:210), [store.go:235](/home/user/archive-relay/internal/store/store.go:235), [api.go:137](/home/user/archive-relay/internal/api/api.go:137), [khatru responding.go:40](/home/user/go/pkg/mod/github.com/fiatjaf/khatru@v0.19.1/responding.go:40).

    **Change:** finish the bounded collection and check all errors before returning the channel, or introduce an explicit error-aware adapter. Returning an actual error lets khatru send NOTICE, although khatru still completes its EOSE accounting; strict failed-subscription behavior needs additional integration. Update `scanEvent` alongside the seven-column SELECT—it currently scans twelve destinations.

11. **MAJOR — §2.5: a semaphore needs an admission policy, not an unbounded waiting population.**

    “Queues cheaply in Go” and “overflow → fast error” describe different designs. Acquiring inside every newly spawned goroutine can still accumulate unbounded waiters. The proposed direct replacement probe and API stats helpers can bypass the QueryEvents/CountEvents semaphore. Evidence: [store.go:210](/home/user/archive-relay/internal/store/store.go:210), [stats.go:151](/home/user/archive-relay/internal/stats/stats.go:151), [plan:137](/home/user/archive-relay/docs/perf-plan-2026-09.md:137).

    **Change:** acquire synchronously before spawning work; use nonblocking admission or a bounded, cancellable queue. Release when database work finishes, not when a slow client finishes draining results. Explicitly route and bound replacement probes, health, and stats API reads. Pool separation limits connection contention, not total ClickHouse CPU/memory consumption.

12. **MAJOR — §§2.8, 2.9: single signature verification is equivalent to today’s signature checks, but does not validate event IDs.**

    `AssumeValid` skips only the subscription’s `CheckSignature`; subscription filter matching remains active. The remaining ingest check covers exactly that skipped signature verification. However, `CheckSignature` explicitly ignores `evt.ID` and hashes the body. A valid signed event with an altered supplied ID can therefore enter storage and poison ID-based deduplication. This is an existing gap, not a new consequence of `AssumeValid`. Evidence: [go-nostr relay.go:276](/home/user/go/pkg/mod/github.com/nbd-wtf/go-nostr@v0.52.3/relay.go:276), [signature.go:14](/home/user/go/pkg/mod/github.com/nbd-wtf/go-nostr@v0.52.3/signature.go:14), [ingest.go:18](/home/user/archive-relay/internal/crawler/ingest.go:18).

    **Change:** validate ID/body consistency for unseen events before marking or saving. Preserve the cheap Seen fast path, then validate, verify, and atomically check-and-mark. Do not move marking ahead of validation without ownership-safe rollback. Reject malformed IDs before conversion to `[32]byte`. The compilable configuration is `relay := nostr.NewRelay(...); relay.AssumeValid = true` before `Connect`; this version has no named `AssumeValid` constructor argument.

13. **MAJOR — §2.8: disconnect time is not a completeness watermark, and the promised daily sweep does not exist.**

    A disconnect during initial backfill would switch subsequent requests to recent history and abandon older unseen events. `ErrBatchFull` drops are merely unmarked; no general retry queue or firehose reconciliation guarantees another delivery. Priority crawling covers only configured authors. Its “full sweep” runs only when `last_fetched` is stale for 24 hours; successful ten-minute ticks continually reset that timestamp. Evidence: [crawler.go:104](/home/user/archive-relay/internal/crawler/crawler.go:104), [ingest.go:31](/home/user/archive-relay/internal/crawler/ingest.go:31), [priority.go:99](/home/user/archive-relay/internal/crawler/priority.go:99), [control.go:220](/home/user/archive-relay/internal/control/control.go:220).

    **Change:** distinguish incomplete backfill, live progress, and failed-ingest intervals. Do not advance past an unresolved gap merely because a connection ended or EOSE arrived. Persist a separate successful-full-sweep timestamp before shrinking overlap. Connection reuse must also move relay ownership outside the per-fetch timeout context and explicitly close each subscription.

14. **MAJOR — §2.9: async `OnFlushed` can recreate the memory/backpressure problem.**

    A bounded channel eventually blocks the flush worker; an unbounded queue or goroutine-per-batch can grow indefinitely when SQLite falls behind. Passing whole events retains potentially large content and tag arrays. Main has no lifecycle ordering for this new worker. Evidence: [batcher.go:110](/home/user/archive-relay/internal/store/batcher.go:110), [dedup.go:98](/home/user/archive-relay/internal/crawler/dedup.go:98), [main.go:53](/home/user/archive-relay/cmd/archive-relay/main.go:53).

    **Change:** update in-memory dedup synchronously after persistence, enqueue compact ID/timestamp records into a bounded queue, and define overflow behavior. Since durable dedup is an optimization, dropping its records with a metric is defensible; dropping stored-event work is not. Drain or deliberately discard this queue before closing SQLite.

15. **MAJOR — §2.9: the rowid prune is off by one, and “O(deleted)” is incorrect.**

    With descending `OFFSET cap`, the selected row is the `(cap+1)`th newest. Deleting rows strictly below it retains `cap+1` rows. I reproduced this: ten rows with cap four leave five. The cutoff subquery must also walk the offset; its query plan is a table scan. Evidence: [plan:188](/home/user/archive-relay/docs/perf-plan-2026-09.md:188), [control.go:32](/home/user/archive-relay/internal/control/control.go:32).

    **Change:** use `rowid <= (... OFFSET cap)` for a positive cap, handle cap zero explicitly, and describe cutoff work as O(cap). Bound deletion transaction sizes. Keeping the current hourly schedule permits roughly **18 million additional rows between prunes at 5,000 events/s**, so a four-million target is not a four-million bound. Schedule pruning independently of firehose sources; currently no sources means no prune loop. Deleting rows also does not automatically shrink an already-large database file.

16. **MAJOR — §2.3: the debounce proposal does not guarantee two-second visibility or one reload per deletion request.**

    A trailing-edge debounce can starve under continuous writes. Clearing dirty after a reload can erase a concurrent dirty mark. A reload taking longer than two seconds exceeds the stated latency, and `SYSTEM RELOAD DICTIONARY` can report success despite an update failure. Evidence: [store.go:150](/home/user/archive-relay/internal/store/store.go:150), [ClickHouse reload semantics](https://clickhouse.com/docs/reference/statements/system#reload-dictionary).

    khatru also invokes `DeleteEvent` separately for each deletion tag, so batching IDs inside `retireIDs` still creates singleton inserts for that path. A slow 100-tag request may span multiple reload periods. Evidence: [khatru deleting.go:14](/home/user/go/pkg/mod/github.com/fiatjaf/khatru@v0.19.1/deleting.go:14), [store.go:126](/home/user/archive-relay/internal/store/store.go:126).

    **Change:** specify periodic coalescing, dirty generations, retry/backoff, dictionary-health checks, and shutdown handling. If immediate post-ACK invisibility is required, retain recent tombstones in an overlay until confirmed visible. Treat cross-call insert batching as separate work, and replace “exactly one reload” acceptance with a bounded-rate/visibility requirement.

17. **MAJOR — §§2.3, 2.13: tombstoning is not disk compaction, and the pruning predicate is unsafe as written.**

    Tombstones hide rows; permanent-tier data remains physically present. The code explicitly documents physical reclamation as separate work. Therefore the deferred “compaction” job adds storage and dictionary RAM without bounding event-table growth. Evidence: [store.go:123](/home/user/archive-relay/internal/store/store.go:123), [plan:227](/home/user/archive-relay/docs/perf-plan-2026-09.md:227).

    Also, `id NOT IN events_<tier>` cannot safely classify global tombstones: a permanent event is absent from the archive tier too, and tombstones record no tier. Even absence from all tiers does not prevent an upstream replay from reinserting a deleted event.

    **Change:** distinguish logical retirement from physical reclamation and design the latter explicitly. Keep tombstone pruning deferred until original retention provenance and replay behavior establish safety. Add “do not infer safe deletion of a tombstone from current row absence” to §4.

18. **MAJOR — §§2.1, 2.10: query collapse and tombstone filtering do not finish COUNT/stats correctness.**

    COUNT still counts every replaceable version. Followers already perform their own `LIMIT 1 BY pubkey`; changing QueryEvents will not reduce that scan, and the follower query lacks the lowest-ID tiebreak. Historical aggregate buckets are frozen, so adding `dictHas` only to future refreshes cannot remove older deleted contributions. Zap amounts use `sum(...)` over undeduplicated `events_all`, despite counts using distinct IDs. Evidence: [store.go:261](/home/user/archive-relay/internal/store/store.go:261), [stats.go:80](/home/user/archive-relay/internal/stats/stats.go:80), [stats.go:86](/home/user/archive-relay/internal/stats/stats.go:86), [stats.go:97](/home/user/archive-relay/internal/stats/stats.go:97).

    **Change:** define shared winner semantics for REQ, COUNT, and followers; count deduplicated winners without applying the response limit. Add the follower tiebreak. Define repair/deletion behavior for frozen snapshots and deduplicate zap events before summing amounts.

19. **MINOR — §2.10: `EXCHANGE TABLES` is correct, but its prerequisites and refresh lifecycle are missing.**

    The syntax is correct for these MergeTree tables. The relevant restriction is the **database engine**: Atomic or Shared, not the table engine. Evidence: [schema.go:110](/home/user/archive-relay/internal/store/schema.go:110), [ClickHouse EXCHANGE documentation](https://clickhouse.com/docs/reference/statements/exchange).

    **Change:** create staging with the same schema, verify database-engine support, truncate staging before every build, and exchange only after a successful insert. After exchange, staging contains the old live snapshot; appending to it on the next run would corrupt results. Serialize refreshes and test failure before exchange plus two consecutive successful rebuilds.

20. **MINOR — §§2.4, 2.7, 2.12, 2.13: migrations need explicit existing-data and consumer coverage.**

    Adding a bloom-index definition does not immediately index historical parts; materialization is needed for predictable coverage. Update both migration DDL and fresh-install DDL. Evidence: [schema.go:28](/home/user/archive-relay/internal/store/schema.go:28), [ClickHouse index materialization](https://clickhouse.com/docs/reference/statements/alter/skipping-index).

    Native tags also require updating INSERT columns, `scanEvent`, the union view, and stats’ `tags_raw` amount extraction. Preserve NIP-10 `reply_to` handling; moving accelerator extraction into ClickHouse does not eliminate that separate logic. Evidence: [batcher.go:202](/home/user/archive-relay/internal/store/batcher.go:202), [stats.go:97](/home/user/archive-relay/internal/stats/stats.go:97).

    **Change:** add a migration consumer checklist and rollback strategy. Moving kind 9735 between tiers requires relocating existing rows and switching query routing coherently; changing classifier configuration alone makes old rows inaccessible to kind-specific queries. A permanent-tier TTL would also affect profiles, contacts, and relay lists.

21. **MINOR — §§2.11, 4: the timeout/config changes are reasonable, but two claims are too absolute.**

    `ReadHeaderTimeout` and `IdleTimeout` are sensible. However, Read/WriteTimeout do not categorically kill all WebSockets: hijacked connections are outside ordinary HTTP lifecycle management, and inherited deadlines depend on the upgrade implementation. Keeping those fields unset is reasonable; the blanket explanation is misleading.

    Increasing `maxAge` also does not guarantee fewer parts at sustained high throughput: reaching `maxSize` still triggers a flush, and monthly partitioning can produce multiple parts per batch. Evidence: [batcher.go:122](/home/user/archive-relay/internal/store/batcher.go:122), [schema.go:50](/home/user/archive-relay/internal/store/schema.go:50).

    **Change:** qualify both claims and measure insert rows/parts. The metadata health-count replacement is reasonable; use an explicit table list and label it as physical rows, including duplicates/tombstoned rows.

22. **MAJOR — §§3, 6: reorder dependencies and strengthen acceptance criteria.**

    Keep bounded batcher recovery first, but include shutdown and replacement-failure behavior. Put dictionary validity, observable query errors, and correct mixed-kind collapse in P0. Move admission control and bounded SQLite maintenance ahead of expensive projection materialization. Couple the projection experiment with the FINAL evaluation. Complete retry-gap/full-sweep tracking before narrowing crawler history.

    Evidence: [plan:239](/home/user/archive-relay/docs/perf-plan-2026-09.md:239), [plan:261](/home/user/archive-relay/docs/perf-plan-2026-09.md:261). Current SQL unit tests check string fragments rather than execution semantics: [store_test.go:36](/home/user/archive-relay/internal/store/store_test.go:36).

    **Change:** require integration tests for same-author mixed replaceable kinds, equal-time cross-tier limits, stale-version filtering/deletion, genuine client-visible query failure, repeated staging swaps, and outage recovery preserving every accepted event. Add boundedness tests for async dedup and semaphore waiters. `[32]byte` memory savings, column pruning, index removal, logging, and the deferred native-tags migration remain sensible work once those contracts are explicit.

Overall, the plan’s direction is sound—bounded ingestion, fewer redundant verifications, narrower reads, isolated workloads, and deferred schema changes—but it is **not implementation-ready**. The grouping key is wrong, several integration claims overstate guarantees, and the proposed sequencing can convert backpressure or reconnects into permanent archive gaps. Correct the blockers, specify the failure-state contracts, and expand acceptance tests before treating P0–P2 as small mechanical changes.