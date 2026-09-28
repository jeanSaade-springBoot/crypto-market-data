package com.crypto.marketdata;

import org.junit.jupiter.api.Test;

/** Explicit -Pfix132-mysql profile only; never use the application datasource. */
class StreamPublisherProvenanceMySqlIT {
    @Test void realMySqlProvenanceAndCompatibility() throws Exception {
        Fix133Scenarios.main(new String[0]);
    }
}
