package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.TextMessage;

import static org.mockito.Mockito.*;

class BinanceWebSocketHandlerTest {

    @Test
    void fix228ClosedCandlePersistenceHasNoRestRepairDependency() throws Exception {
        CandleStore store = mock(CandleStore.class);
        when(store.persistWebsocket(any())).thenReturn(true);
        BinanceWebSocketHandler handler = new BinanceWebSocketHandler(new ObjectMapper(), store);
        String payload = """
                {"data":{"E":1790151900000,"k":{"s":"BTCUSDT","i":"1m",
                "t":1790151840000,"T":1790151899999,"x":true}}}
                """;

        handler.handleTextMessage(null, new TextMessage(payload));

        verify(store).persistWebsocket(any());
    }
}
