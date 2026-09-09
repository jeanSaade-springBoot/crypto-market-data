package com.crypto.marketdata;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** FIX-182: align JDBC instant serialization and every collector MySQL session. */
@Configuration(proxyBeanMethods = false)
public class CollectorUtcConfiguration {
    @Bean
    static BeanPostProcessor collectorUtcDataSourcePolicy() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) {
                if (bean instanceof HikariDataSource dataSource) {
                    // serverTimezone alone describes the connection; it does not SET
                    // the database session. A UTC driver with a +02 session shifts
                    // supplied TIMESTAMP values two hours behind database NOW().
                    dataSource.addDataSourceProperty("connectionTimeZone", "UTC");
                    dataSource.addDataSourceProperty("forceConnectionTimeZoneToSession", "true");
                    dataSource.addDataSourceProperty("preserveInstants", "true");
                    dataSource.setConnectionInitSql("SET SESSION time_zone = '+00:00'");
                    LoggerFactory.getLogger(CollectorUtcConfiguration.class).info(
                            "FIX-182 collector UTC policy configured: datasource={}, driver=UTC, session=+00:00",
                            beanName);
                }
                return bean;
            }
        };
    }
}
