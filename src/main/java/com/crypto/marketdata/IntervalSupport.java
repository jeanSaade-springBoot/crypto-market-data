package com.crypto.marketdata;

import java.time.Duration;

final class IntervalSupport {
    private IntervalSupport() {}

    static Duration duration(String interval) {
        return switch (interval) {
            case "1m" -> Duration.ofMinutes(1);
            case "5m" -> Duration.ofMinutes(5);
            case "15m" -> Duration.ofMinutes(15);
            case "1h" -> Duration.ofHours(1);
            case "4h" -> Duration.ofHours(4);
            case "1d" -> Duration.ofDays(1);
            default -> throw new IllegalArgumentException("Unsupported interval: " + interval);
        };
    }
}
