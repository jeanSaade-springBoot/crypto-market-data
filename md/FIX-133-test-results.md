# FIX-133 verification evidence

Executed here on 2026-09-28:

- Java 21 compilation of ALL uploaded collector production Java sources with the patched StreamPublisher: passed, using runtime dependencies extracted from the retained prior collector Spring Boot JAR (Spring 7.0.8). This is direct javac compilation, not a Maven package build.
- Standalone Fix133Scenarios against isolated local MySQL 8.0.44: 11 scenarios passed. Exact output: test-evidence/FIX-133-mysql.log. Each scenario uses real CandleStore, StreamPublisher, JDBC transactions and a fresh fixture; no mocked SQL or publisher.
- Schema creation and rollback behavior were therefore exercised on MySQL. Production is MySQL 8.4; a Jenkins/isolated 8.4 run remains required before final rollout acceptance.

Scenarios: REST then live close; live close then REST then duplicate; forming WS then REST then live close; stale WS after REST; existing contaminated canonical version with no WS sidecar; conservative equal-time legacy-lane rejection; no closed-candle reopening; older lane stays historical; publication failure rolls back all sidecars/candle; feed OFF; existing legacy consumer claim/processed fields preserved.

Not executed successfully here:

- Full Maven build/H2/JUnit suite: offline Maven cannot resolve org.springframework.boot:spring-boot-starter-parent:4.1.0. Included new JUnit wrappers must run in Jenkins. No claim that the full test suite passed.
- A subsequent attempt to run the unchanged baseline as a red regression comparison could not start its fresh MySQL fixture (InnoDB missing redo file / connection refused). Its infrastructure failure is retained in test-evidence/FIX-133-baseline-regression.log. It is not a failing FIX-133 assertion and no red-run result is claimed.
- No production DB connection, deployment, live throughput measurement, new concurrent interleaving test or Trader end-to-end execution was performed.

Compatibility was checked against uploaded crypto-ai(1).zip SharedMarketSource and SharedEventPolicy. Their source/schema/classification contract remains unchanged; a source-only compatibility check is not production cutover acceptance.
