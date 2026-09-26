# FIX-132 isolated MySQL verification

Never point these tests at production or the existing collector tables. The integration suite deliberately mutates its test ownership row and kills one of its own test connections. It refuses URLs other than loopback with a schema name starting fix132_test. Use a separate local MySQL instance or port-forward to an ISOLATED TEST instance, never the production server.

Create a dedicated empty schema, e.g. fix132_test_ownership, and a user with privileges on that schema. The tests create their test tables. No Binance connections are made. Source publication/admission is enabled explicitly in the relevant test cases while the shipped application setting stays OFF.

Windows CMD:

```cmd
set "FIX132_TEST_MYSQL_URL=jdbc:mysql://127.0.0.1:33316/fix132_test_ownership?allowPublicKeyRetrieval=true&useSSL=false"
set "FIX132_TEST_MYSQL_USER=your_test_user"
set "FIX132_TEST_MYSQL_PASSWORD=your_test_password"
mvn -Pfix132-mysql test
```

Use the TLS parameters appropriate to your isolated test instance. The above URL is for a local disposable instance only. Ordinary unit tests remain `mvn test`; integration tests are selected by the explicit profile. A missing/unsafe URL fails the profile rather than silently skipping it.

The supplied results were run on isolated MySQL Community Server 8.0.44 / InnoDB, Connector/J 9.7.0 and HikariCP 7.0.2. This environment required a Mockito startup javaagent for unit tests; the recorded command is included in the results document. Re-run against your deployment MySQL version before activation. A passing integration suite does not measure live feed throughput or other consumers' latency.
