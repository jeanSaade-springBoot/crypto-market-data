package com.crypto.marketdata;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class BinanceRestClient {
    private final RestClient client;

    public BinanceRestClient(RestClient.Builder builder, MarketDataProperties properties) {
        this.client = builder.baseUrl(properties.getRestBaseUrl()).build();
    }

    public List<BinanceKline> latest(String symbol, String interval, int limit) {
        return fetch(symbol, interval, null, limit);
    }

    public List<BinanceKline> from(String symbol, String interval, Instant startTime, int limit) {
        return fetch(symbol, interval, startTime, limit);
    }

    private List<BinanceKline> fetch(String symbol, String interval, Instant startTime, int limit) {
        List<List<Object>> response = client.get()
                .uri(uriBuilder -> {
                    var builder = uriBuilder.path("/api/v3/klines")
                            .queryParam("symbol", symbol)
                            .queryParam("interval", interval)
                            .queryParam("limit", Math.max(1, Math.min(1000, limit)));
                    if (startTime != null) {
                        builder.queryParam("startTime", startTime.toEpochMilli());
                    }
                    return builder.build();
                })
                .retrieve()
                .body(new ParameterizedTypeReference<>() {});

        if (response == null || response.isEmpty()) {
            return List.of();
        }
        List<BinanceKline> result = new ArrayList<>(response.size());
        for (List<Object> row : response) {
            if (row == null || row.size() < 11) {
                continue;
            }
            result.add(new BinanceKline(
                    Instant.ofEpochMilli(longValue(row.get(0))),
                    decimal(row.get(1)), decimal(row.get(2)), decimal(row.get(3)), decimal(row.get(4)),
                    decimal(row.get(5)),
                    Instant.ofEpochMilli(longValue(row.get(6))),
                    decimal(row.get(7)), longValue(row.get(8)), decimal(row.get(9)), decimal(row.get(10))
            ));
        }
        return result;
    }

    private BigDecimal decimal(Object value) { return new BigDecimal(String.valueOf(value)); }
    private long longValue(Object value) {
        return value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
    }
}
