package com.crypto.marketdata;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class CoinConfigurationReader {
    private final JdbcTemplate jdbcTemplate;

    public CoinConfigurationReader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> enabledSymbols() {
        return jdbcTemplate.queryForList("""
                SELECT symbol
                FROM coin_configuration
                WHERE enabled = 1
                ORDER BY symbol ASC
                """, String.class);
    }
}
