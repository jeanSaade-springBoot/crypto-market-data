package com.crypto.marketdata;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CollectorUtcConfigurationTest {
    @Test
    void enforcesUtcEvenWhenExternalPropertiesSpecifyLocalTime() {
        try (HikariDataSource source = new HikariDataSource()) {
            source.addDataSourceProperty("connectionTimeZone", "LOCAL");
            source.addDataSourceProperty("forceConnectionTimeZoneToSession", "false");
            source.setMaximumPoolSize(6);
            Object result = CollectorUtcConfiguration.collectorUtcDataSourcePolicy()
                    .postProcessBeforeInitialization(source, "dataSource");
            assertSame(source, result);
            assertEquals("UTC", source.getDataSourceProperties().getProperty("connectionTimeZone"));
            assertEquals("true", source.getDataSourceProperties().getProperty("forceConnectionTimeZoneToSession"));
            assertEquals("true", source.getDataSourceProperties().getProperty("preserveInstants"));
            assertEquals("SET SESSION time_zone = '+00:00'", source.getConnectionInitSql());
            assertEquals(6, source.getMaximumPoolSize());
        }
    }

    @Test
    void leavesOtherBeansAlone() {
        Object bean = new Object();
        assertSame(bean, CollectorUtcConfiguration.collectorUtcDataSourcePolicy()
                .postProcessBeforeInitialization(bean, "other"));
    }
}
