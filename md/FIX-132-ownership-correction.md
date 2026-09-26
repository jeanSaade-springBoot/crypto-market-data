# FIX-132 — collector ownership correction

Implemented September 26, 2026 against the delivered FIX-132 durable-feed candidate. Collector only; Trader and crypto-ai-next code were not edited. Publication remains `${MARKET_DATA_STREAM_ENABLED:false}`. The five source tables are unchanged.

## Why this correction exists

The original publisher acquired/refreshed ownership inside each candle transaction. On a restart, the new process could receive work while another token's lease remained valid; those writes were rejected. The interruption was the remaining lease lifetime, not necessarily 30 seconds from startup, and could persist longer if another writer renewed or retained the row lock. A failing publication still rolls back the entire candle transaction: atomicity is intentionally retained. This correction prevents admitting new work while ownership is known to be unavailable; it does not promise uninterrupted reception or recover missing intraminute observations.

## Implemented classes and invariants

- OwnershipGate: one monitor atomically guards state, admission and in-flight accounting. It never holds the monitor across JDBC/network I/O. Permits capture immutable token/epoch; closing admission does not erase existing permits. Epoch mismatch rejects late old-generation work.
- OwnershipLeaseStore: a private Hikari pool (maximum 1, minimum idle 0), constructed only when feed enabled. It is not a DataSource bean. Existing Boot datasource, JdbcTemplate and transaction manager remain the candle path (collector default maximum 6, not Trader's separate read-pool maximum 3).
- OwnershipCoordinator: serial fixed-delay attempts, one second after each completion. Thirty-second lease, renewal after five seconds since the last successful renewal. Only coordinator acquire/renew/release changes the lease. Same process token survives uncertainty recovery; each drained reopening advances the local epoch. No runtime property toggling. The existing feed-enabled startup schema validation remains: missing tables or failure of that initial validation stops startup; later acquisition/renewal database failures use the waiting/uncertain lifecycle.
- StreamPublisher: pure transactional owner-row fence. It locks, verifies captured token and valid database-time expiry, and never takes/extends ownership. Expiry is evaluated in a statement AFTER acquiring the lock to avoid using an earlier statement-start timestamp. Lock remains held through candle/event commit, so a successor cannot supersede that transaction mid-effect.
- BinanceWebSocketHandler / CollectorConnectionOwner: generation captures ownership epoch; callbacks require both connection admission and ownership admission. Token permit lasts until the proxied CandleStore call has returned after commit/rollback. Late establishment and old-epoch messages cannot write.
- AdmittedCandleWriter: nontransactional REST boundary outside the transactional store; holds the permit through proxy completion. Database/transaction failures suspend admission.
- RecoveryCoordinator: startup/reconnect/scheduled recovery share one non-overlapping pass. Duplicate invocations are not queued. The pass includes REST network work in drain accounting, and each candle write is admitted again against that pass's epoch. Recovery qualification rules are unchanged; scheduling is deliberately coordinated even with feed OFF.

The WebSocket manager and ownership clock are separate. A failure closes the ownership gate immediately; transport close/revocation is requested by the manager's next management pass. A delayed management pass cannot authorize writes because callbacks still check the closed gate. Reopening requires both gate in-flight count zero and the old connection generation drained/transport closed. A stuck REST request, transaction or transport close can therefore postpone reconnection indefinitely.

## State contract

| State | New ingestion | Readiness contribution |
| --- | --- | --- |
| FEED_DISABLED | Existing candle/feed semantics | UP; feature does not override other checks |
| WAITING_FOR_OWNERSHIP | No WebSocket connect or REST recovery pass | OUT_OF_SERVICE |
| OWNED | Admitted by matching epoch | UP only after connection established/admission enabled |
| OWNERSHIP_UNCERTAIN | Closed immediately on first ambiguous/failed renewal or database fence failure | OUT_OF_SERVICE |
| OWNERSHIP_LOST | Closed on confirmed token/expiry rejection | OUT_OF_SERVICE |
| STOPPING | No new work; drain existing work | OUT_OF_SERVICE |
| STOPPED | No new work; drained and own lease absence/release confirmed | OUT_OF_SERVICE |

Uncertain/lost -> drain and close old generation -> WAITING_FOR_OWNERSHIP -> successful database acquisition -> OWNED with a new epoch -> new connection. There is no heartbeat-only reopening. Token identity is never used as a substitute for epoch admission.

Graceful stop closes admission first, stops/closes transport and waits up to 30 seconds for confirmed drain/release attempts. It releases only its own token. An unconfirmed drain/release leaves STOPPING and logs DRAIN_UNCONFIRMED; it never claims STOPPED or forcibly removes someone else's lease. A crash executes no reliable state transition; the successor relies on database expiry and row-lock release. Individual database/network operations can extend elapsed shutdown time; 30 seconds is not a hard kill deadline.

## Timeout and connection cleanup

Ownership pool borrow: 2 seconds; Connector/J connectTimeout: 2000ms; socketTimeout: 3000ms; dedicated MySQL session innodb_lock_wait_timeout: 1 second; UTC session. These apply only to coordinator connections. Normal candle-pool settings remain unchanged. Configure no contradictory timeout overrides in the JDBC URL.

On SQL failure, roll back the whole ownership transaction. A known server lock timeout/deadlock with successful rollback and reset can reuse the connection. Other ambiguity, failed rollback or failed reset evicts through Hikari's physical eviction operation. Do not close/reset the proxy again after eviction; the real-MySQL fault-injection test caught that unsafe cleanup sequence and it was corrected. Commit ambiguity never authorizes admission merely because the database may have committed; a subsequent acquisition must establish ownership.

The global id=1 row still serializes all feed-enabled persistence transactions across symbols. The dedicated coordinator pool prevents borrowing a candle-pool slot, but DOES NOT remove database lock contention. Long writes can make renewal fail conservatively and pause ingestion. No lock-strategy optimization is bundled into this patch.

## Health and metrics

`/actuator/health/readiness` includes readinessState and collectorIngestion. Details show state, reason and inFlight, never token/credentials. `/actuator/health/liveness` includes livenessState only, so ownership/database waiting does not create a database-driven restart loop. OFF contributes UP rather than asserting all infrastructure is healthy. Existing aggregate health retains its own contributors.

Logs: `[FIX-132][OWNERSHIP_CONFIGURATION]`, `[OWNERSHIP_STATE]`, `[WAITING_FOR_OWNERSHIP]`, `[OWNERSHIP_ATTEMPT_FAILED]`, `[LEASE_RELEASE_UNCONFIRMED]`, `[DRAIN_UNCONFIRMED]`, `[RECOVERY_FAILED]`; successful attempt timings also available at DEBUG as `[OWNERSHIP_ATTEMPT]`.

Micrometer timers (nanosecond recording; Actuator renders timer units):
- fix132.ownership.pool_acquire / sql / rollback / cleanup / attempt, tagged acquire/renew/release.
- fix132.persistence.fence, tagged websocket/rest: whole owner-row lock and verification sequence, including round trips and SQL execution, NOT pure InnoDB lock wait.
- fix132.persistence.after_fence: through transaction completion, including remaining SQL and commit/rollback.
- fix132.collector.transaction: method entry through transaction completion, tagged source/outcome; excludes proxy begin/pool borrow.
- fix132.candle_pool.active / pending, and fix132.ingestion.inflight gauges.

Boot/Hikari's regular pool metrics supply candle connection-acquisition timing when enabled. No invented per-message pool-borrow metric is claimed. Source publication received_at remains publisher-method entry, and created_at precedes commit. This patch does not relabel either as exact network arrival/commit time.

## Verification and remaining gates

Actual results: 36 unit/regression tests and 9 real MySQL integration tests; zero failures, errors or skips. See FIX-132-test-results.txt for Maven outputs. Deterministic tests use forced barriers/latches, fake coordinator time and explicit state transitions. H2 checks candle+feed atomicity and the pure fence. Real MySQL/InnoDB tests cover ownership SQL lock timeout, rollback/reuse, committed expiry/takeover, token-conditional release, coordinator handover, renewal failure/drain, physical eviction, session settings and datasource isolation. Rollback/reset failures are injected at JDBC over a real Hikari/MySQL connection, and disconnected-connection replacement uses an actual KILL CONNECTION in the isolated schema.

No real Binance reconnect/load soak, production-size contention benchmark, live consumer-impact measurement or Trader cutover was performed. Before enabling on the server, verify feed-OFF startup and readiness, then complete the original FIX-132 measured activation gates. Do not exercise multiple writers against live collector tables. Keep Trader unchanged and do not rename crypto_ai.candle during this collector-only rollout.
