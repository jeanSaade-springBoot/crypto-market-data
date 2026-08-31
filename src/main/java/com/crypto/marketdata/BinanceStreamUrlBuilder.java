package com.crypto.marketdata;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Component
public class BinanceStreamUrlBuilder {
    private final MarketDataProperties properties;

    public BinanceStreamUrlBuilder(MarketDataProperties properties) {
        this.properties = properties;
    }

    public String build(List<String> symbols) {
        if (symbols == null || symbols.isEmpty()) {
            throw new IllegalStateException("No enabled Binance symbols configured");
        }
        String streams = symbols.stream()
                .flatMap(symbol -> properties.getIntervals().stream()
                        .map(interval -> symbol.trim().toLowerCase(Locale.ROOT) + "@kline_" + interval.trim()))
                .collect(Collectors.joining("/"));
        return properties.getWebsocketBaseUrl() + "/stream?streams=" + streams;
    }
}
