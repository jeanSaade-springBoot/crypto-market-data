# FIX-133 rollout — collector first

1. Review the source diff. Run the collector's normal Jenkins/Maven build with tests enabled. Do not replace the deployed collector with an unbuilt source archive.
2. In MySQL Workbench select crypto_ai_v2 and execute md/sql/FIX-133-websocket-version.sql. It only creates the new sidecar table. Existing production FIX-132 tables must already exist; do not rerun unrelated Trader migrations or edit Flyway history.
3. Confirm `SELECT DATABASE(); SHOW CREATE TABLE market_data_stream_ws_version;` in that schema. Never seed the new table from mixed-provenance legacy versions or delete existing lanes.
4. Deploy the built collector using the existing single-owner restart procedure. Retain its current publication setting; this package does not modify application.yml. A restart can interrupt ingestion while ownership is reacquired and recovery runs.
5. Verify ownership OWNED, WS establishment and [FIX-133][ORDERING_CONFIGURATION] with publicationEnabled=true. Readiness alone does not prove every symbol/interval has current closes.
6. Check newly generated closed events and candle coverage during normal traffic and recovery overlap. Restrict assessment to the deployment window; earlier OUT_OF_ORDER rows are intentionally unchanged. Review existing latency/lock metrics for added overhead.
7. Only after collector verification, finish Trader's separate cutover procedure: latest tested build, old-path drain/reconciliation, isolated authenticated/authorized approval workflow and LIVE startup validation. Keep the primary datasource on crypto_ai; only shared source targets crypto_ai_v2. Do not rename crypto_ai.candle during this collector change.

Example read-only check (replace the UTC deployment boundary):

```sql
SET time_zone = '+00:00';
SET @fix133_started = '2026-09-28 00:00:00';
SELECT id, symbol, interval_code, candle_open_time, symbol_sequence,
       source, classification, observed_at, received_at, created_at
FROM crypto_ai_v2.market_data_stream_event
WHERE created_at >= @fix133_started AND closed = 1
ORDER BY id DESC LIMIT 300;
```

This sample is not a full missing-candle audit. A non-LIVE close can still be correctly historical/stale/duplicate; inspect its lane and observation order before concluding the fix failed.

For isolated MySQL verification (NOT the production schema), create a local `fix133_test_*` database, configure FIX133_TEST_MYSQL_URL/USER/PASSWORD and run:

```
mvn -B -ntp -Pfix132-mysql -Dtest=StreamPublisherProvenanceMySqlIT test
```

The scenario fixture DROPS its test tables. Its standalone/MySQL entry point refuses non-loopback URLs or schemas not starting fix133_test. Normal `mvn clean test` also runs the same assertions on H2.

Rollback: the new table can remain if the old collector is restored; older code ignores it. Rolling back restores the original classification defect. Do not start both versions concurrently or reset consumer checkpoints.
