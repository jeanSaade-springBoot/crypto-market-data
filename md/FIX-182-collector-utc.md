# FIX-182 — Collector UTC session alignment

Applies to crypto-market-data, the producer for crypto-ai-next / crypto_ai_v2.

## Evidence and cause

Events 305682–305687 stored observation/candle times about two hours before
database-generated creation times. The consumer read the stored times faithfully
and deferred them as stale within about six seconds of creation. The supplied
collector specifies serverTimezone=UTC but never sets its MySQL session timezone.
That configuration allows UTC JDBC timestamp serialization into a local +02
session, producing the observed shift. Production connection overrides were not
available for inspection; fresh-row verification remains required.

## Change

CollectorUtcConfiguration configures the Hikari datasource before initialization:
connectionTimeZone=UTC, forceConnectionTimeZoneToSession=true, preserveInstants=true,
and connectionInitSql=SET SESSION time_zone = '+00:00'. This replaces any external
connection-init SQL; the supplied project has none. Retain this statement if adding
other connection initialization requirements later.

No trading thresholds, consumer freshness allowance, event processing flags,
credentials, or historical rows are changed. Applies to both REST and WebSocket
candle/event writes through the collector datasource. No fix-registry.js exists
in this collector; this document records the release.

## Deployment and verification

Merge this patch into the crypto-market-data project (folder containing pom.xml).
Run `mvn test package`, deploy its JAR, and restart the collector so all connections
are new. No crypto-ai-next deployment is included or required by this patch.
Verify startup logs contain FIX-182, then inspect newly created events with a UTC
SQL session. Their observed_at and created_at should reflect actual ingestion
latency, not a fixed two-hour difference. Confirm DURABLE_CANDLE_EVENT_LIVE in next
and fresh trade_signal rows / dashboard activity. Existing wrongly timestamped
rows are not repaired or re-enabled by this change. Historical replay data needs
a separate scoped review; SAMPLE_PASSED alone does not prove exchange alignment.

Unit tests cover datasource policy and unrelated beans. Local environment has no
Maven; these tests and a MySQL round-trip were not executed here. Java syntax was
checked separately. Deployment success is not claimed.
