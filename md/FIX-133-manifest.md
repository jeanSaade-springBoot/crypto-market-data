# FIX-133 change inventory

Baseline archive SHA-256: `6d78aefe110550516cd2b73fbbd01d8485e9b5d9b8acfbe62045310853936a72`

Modified baseline files:
- src/main/java/com/crypto/marketdata/StreamPublisher.java
- src/test/java/com/crypto/marketdata/StreamPublisherTest.java
- src/test/java/com/crypto/marketdata/OwnershipMySqlIT.java

All other baseline files, including application.yml, pom.xml, CandleStore and FIX-228 code, are byte-identical. Added files are FIX-133 tests/docs/schema, the restored FIX-132 test schema, and verification evidence. No built production JAR is supplied.
