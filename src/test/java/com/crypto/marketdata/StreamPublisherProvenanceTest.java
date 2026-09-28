package com.crypto.marketdata;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.util.UUID;
import java.util.stream.Stream;

class StreamPublisherProvenanceTest {
    @TestFactory Stream<DynamicTest> provenanceAndCompatibility() {
        return Fix133Scenarios.CASES.stream().map(name -> DynamicTest.dynamicTest(name, () -> {
            var ds=new DriverManagerDataSource("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1","sa","");
            new Fix133Scenarios(ds).check(name);
        }));
    }
}
