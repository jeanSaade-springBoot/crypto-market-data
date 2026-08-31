package com.crypto.marketdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(MarketDataProperties.class)
public class CryptoMarketDataApplication {
    public static void main(String[] args) {
        SpringApplication.run(CryptoMarketDataApplication.class, args);
    }
}
