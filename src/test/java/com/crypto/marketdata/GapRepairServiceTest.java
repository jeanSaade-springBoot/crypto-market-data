package com.crypto.marketdata;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GapRepairServiceTest {

    @Test
    void fix228RepairsAnInternalGapEvenWhenNewerCandlesExist() {
        CandleStore store = mock(CandleStore.class);
        BinanceRestClient client = mock(BinanceRestClient.class);
        MarketDataProperties properties = new MarketDataProperties();
        properties.setGapLookbackHours(24);
        properties.setMaxInternalGapsPerPass(25);
        GapRepairService service = new GapRepairService(store, client, properties);

        Instant observedAt = Instant.parse("2026-09-23T11:30:00Z");
        Instant missing = Instant.parse("2026-09-23T11:12:00Z");
        CandleStore.CandleGap gap = new CandleStore.CandleGap(missing, missing.plusSeconds(60));
        BinanceKline repaired = candle(missing);
        when(store.findInternalGaps(eq("BTCUSDT"), eq("1m"), any(), eq(60L), eq(25)))
                .thenReturn(List.of(gap));
        when(client.from("BTCUSDT", "1m", missing, 1000)).thenReturn(List.of(repaired));

        service.repairInternalGaps("BTCUSDT", "1m", observedAt);

        verify(store).persistRest("BTCUSDT", "1m", repaired,
                "REST_INTERNAL_GAP_REPAIR", observedAt);
    }

    @Test
    void fix228DoesNotCallRestWhenSqlFindsNoInternalGap() {
        CandleStore store = mock(CandleStore.class);
        BinanceRestClient client = mock(BinanceRestClient.class);
        MarketDataProperties properties = new MarketDataProperties();
        GapRepairService service = new GapRepairService(store, client, properties);
        when(store.findInternalGaps(anyString(), anyString(), any(), anyLong(), anyInt()))
                .thenReturn(List.of());

        service.repairInternalGaps("BTCUSDT", "1m", Instant.parse("2026-09-23T11:30:00Z"));

        verifyNoInteractions(client);
    }

    private BinanceKline candle(Instant openTime) {
        BigDecimal one = BigDecimal.ONE;
        return new BinanceKline(openTime, one, one, one, one, one,
                openTime.plusSeconds(59), one, 1L, one, one);
    }
}
