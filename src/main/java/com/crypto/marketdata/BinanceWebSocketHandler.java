package com.crypto.marketdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinanceWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketHandler.class);

    private final ObjectMapper objectMapper;
    private final CandleStore candleStore;
    private final GapRepairService gapRepairService;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private volatile WebSocketSession session;

    public BinanceWebSocketHandler(ObjectMapper objectMapper, CandleStore candleStore, GapRepairService gapRepairService) {
        this.objectMapper = objectMapper;
        this.candleStore = candleStore;
        this.gapRepairService = gapRepairService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        this.session = session;
        connected.set(true);
        log.info("FIX-139 connected to Binance candle websocket: session={}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            JsonNode root = objectMapper.readTree(message.getPayload());
            JsonNode data = root.has("data") ? root.path("data") : root;
            JsonNode k = data.path("k");
            if (k.path("x").asBoolean(false)) {
                String symbol = k.path("s").asText();
                String interval = k.path("i").asText();
                Instant openTime = Instant.ofEpochMilli(k.path("t").asLong());
                gapRepairService.repairBefore(symbol, interval, openTime);
            }
            if (candleStore.persistWebsocket(root)) {
                log.info("FIX-139 durable candle closed: symbol={}, interval={}, openTime={}",
                        k.path("s").asText(), k.path("i").asText(), Instant.ofEpochMilli(k.path("t").asLong()));
            }
        } catch (Exception exception) {
            log.error("FIX-139 failed to persist Binance kline", exception);
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        connected.set(false);
        log.error("FIX-139 Binance websocket transport error", exception);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        connected.set(false);
        log.warn("FIX-139 Binance websocket disconnected: {}", status);
    }

    public boolean isConnected() { return connected.get(); }

    public void close() {
        connected.set(false);
        WebSocketSession current = session;
        if (current != null && current.isOpen()) {
            try { current.close(CloseStatus.NORMAL); }
            catch (Exception exception) { log.warn("Unable to close Binance websocket cleanly", exception); }
        }
    }
}
