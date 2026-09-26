# FIX-132 controlled cutover runbook

Candidate only. No commands here have been run against your server. **First read FIX-132-ownership-correction.md. Ownership is acquired before ingestion; restart/uncertainty can still pause candle collection.** Keep LIVE disabled until the checks below pass. Do not reset an existing LIVE checkpoint as a restart procedure.

## 1. Schema and collector preparation

Back up both schemas. Verify MySQL versions, permissions, available space and migration duration on a representative copy. Trader Flyway V89 runs even when shared mode is OFF; its ALTER of the potentially large market_price_event table needs a maintenance/online-DDL assessment. Do not modify old Flyway files.

Apply collector `md/sql/FIX-132-source-tables.sql` to **crypto_ai_v2** using its schema owner's migration process. It creates additive stream tables, including the observation lane. Collector does not take over another application's migration history. Existing market_data_candle_event claim fields remain untouched.

Deploy collector with MARKET_DATA_STREAM_ENABLED=false first and verify ownership/reconnect behavior. Arrange exactly one active candle writer before enabling its feed. Every writer must use the enabled publication protocol; it cannot fence old binaries or feed-disabled writers. Only after the isolated ownership tests and deployment checks pass, enable MARKET_DATA_STREAM_ENABLED=true under the agreed validation window and measure source transaction time, event volume, disk growth and existing-consumer latency. Validate timeout/late-handshake/drain cases and historical repair behavior on MySQL. No feed-retention deletion is provided: agree storage/retention that protects every consumer and the required Replay evidence before activation.

## 2. Trader observation

Give a dedicated user SELECT only on crypto_ai_v2.candle, market_data_stream_event and market_data_stream_cursor. Do not give it source claim/update privileges. JDBC readOnly is not a substitute for grants. Keep Trader's normal local datasource unchanged.

Set SHARED_MARKET_MODE=OBSERVE, SHARED_MARKET_JDBC_URL to the source schema with UTC session semantics, and SHARED_MARKET_USERNAME / SHARED_MARKET_PASSWORD through your secret configuration. OBSERVE keeps the existing candle readers and Trader ingestion; feed events have no business effects. Inspect persisted diagnostics and verify source sequences and classifications. This observation period is not a full business-effect load test.

Use a production-sized MySQL staging copy to validate ordered delivery, fence/rollback behavior, crash reconciliation, protection-before-analysis, OLD/NEW Replay windows and UI coverage. FIX-125 remains an explicit unresolved wallet coordination risk; do not infer its closure from the new consumer tests. Test existing collector consumers and source writer-version rules too.

Measure actual configured symbol/interval update rates, burst rates, publication overhead, backlog growth, p95/p99/worst protection and close-analysis delay, and freshness exclusions. Initial 2 price workers, 3 read connections and 100-row discovery batches are not throughput guarantees. Accept limits based on those measurements, not an assumed events/sec calculation. Confirm required history per enabled symbol/interval, including 1d if configured. Startup's 300-row check is a minimum, not proof of full Replay history.

## 3. First cutover only

Stop Trader cleanly and wait for old transactions and workers to finish. Reconcile old-path wallet outcomes independently. Never equate its identifiers with collector feed identifiers. Preserve observations/audit evidence before editing state.

For each enabled symbol, capture the collector's committed `market_data_stream_cursor.last_sequence` and an explicit UTC cutover time. Use a consistent source read and record both values together in the operator's cutover evidence. Persist these as cutover_sequence, cutover_at and discovered_sequence in crypto_ai.shared_market_consumer_state with status READY and no inherited last-price/observation authority. This is an initial cutover operation, not an unconditional restart UPDATE.

If OBSERVE already populated state, inspect all delivery rows first. Proceed only after confirming none ran LIVE effects or has started protection/analysis. Preserve them, classify their outcomes as pre-cutover historical/observed and ensure no pending work at or below the selected boundary remains dispatchable. Do not blindly delete/requeue/reset a previously LIVE consumer. Previously LIVE/uncertain work requires event-by-event reconciliation and a reviewed maintenance script tailored to actual rows. The package intentionally does not provide an unconditional destructive reset command.

Events at or below the sequence boundary are not eligible for live execution. Events above it must also pass original observation time, source classification, observation order and freshness checks. A repair published later remains historical.

Before the optional old-table rename, inspect live views, triggers, foreign keys, scripts and external consumers referencing crypto_ai.candle; inspect whether candle_bck already exists. Confirm the complete read inventory and migration/rollback rehearsal. With Trader stopped and only after these checks, the requested operation is:

```sql
RENAME TABLE crypto_ai.candle TO crypto_ai.candle_bck;
```

It has NOT been executed. Do not overwrite an existing backup table. Shared mode removes Candle from JPA entity validation and disables local candle writers, but source inspection cannot certify external database dependencies.

Set SHARED_MARKET_MODE=LIVE and SHARED_MARKET_ACTIVATION_APPROVED=true only after accepting the gates above. Start Trader. Startup validates explicit READY boundaries, source sequence and minimum history. Confirm FIX-132 source configuration, no Trader kline ingestion/reload, applied prices, ordered closes, source provenance and persisted outcomes in Proven. Preserve the existing temporary scheduled-analysis recovery setting during the agreed diagnostic window; re-enable it through the separately agreed operational procedure, not silently as part of this migration.

## 4. Failure/recovery and rollback

Do not automatically retry REVIEW_REQUIRED protection or started analysis. Reconcile source event, local phase, actual wallet outcome and any surviving worker first. Stop the old worker before any approved manual reassignment. Observer failures are recorded separately and do not stop later price protection. Sequence gaps require source/retention investigation; never skip the cursor merely to clear an alert.

For rollback, stop/drain Trader, preserve all new evidence/state, and verify no uncertain effects. Restore the old candle name only if both table identities and dependencies are correct. The backup candle table becomes stale while LIVE runs: repair and validate its history before allowing old-path trading. Then change mode to OFF. Do not drop the new tables or price columns just to roll back code. A rollback to an older binary needs its own migration compatibility review.

No wall-clock latency guarantee, exactly-once external wallet guarantee, or complete historical snapshot guarantee is made by this delivery.
