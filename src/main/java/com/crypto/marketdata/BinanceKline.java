package com.crypto.marketdata;

import java.math.BigDecimal;
import java.time.Instant;

public record BinanceKline(
        Instant openTime,
        BigDecimal openPrice,
        BigDecimal highPrice,
        BigDecimal lowPrice,
        BigDecimal closePrice,
        BigDecimal volume,
        Instant closeTime,
        BigDecimal quoteAssetVolume,
        long numberOfTrades,
        BigDecimal takerBuyBaseVolume,
        BigDecimal takerBuyQuoteVolume
) {}
