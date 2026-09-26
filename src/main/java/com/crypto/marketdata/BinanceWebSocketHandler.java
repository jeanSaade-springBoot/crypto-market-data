package com.crypto.marketdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** FIX-132 collector prerequisite: a revoked connection can never admit another write.
 * Admission and revocation share one monitor, but database work never holds it.
 * An already admitted persistence call must return (including Spring commit/rollback)
 * before the manager is allowed to create a replacement generation.
 */
public class BinanceWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketHandler.class);
    private final ObjectMapper objectMapper;
    private final CandleStore candleStore;
    private final long generation;
    private WebSocketSession session;
    private boolean revoked;
    private int inFlight;
    private OwnershipGate gate;private long ownershipEpoch;
    void ownershipGate(OwnershipGate value,long epoch){gate=value;ownershipEpoch=epoch;}

    public BinanceWebSocketHandler(ObjectMapper objectMapper, CandleStore candleStore) {
        this(objectMapper, candleStore, 0);
    }

    BinanceWebSocketHandler(ObjectMapper objectMapper, CandleStore candleStore, long generation) {
        this.objectMapper = objectMapper;
        this.candleStore = candleStore;
        this.generation = generation;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession established) {
        boolean reject;
        boolean newlyEstablished;
        synchronized (this) {
            // A timeout/shutdown may have won before the provider reports success.
            // A duplicate callback for our own session is harmless; a distinct extra
            // session is closed without taking ownership away from the real session.
            reject = (gate!=null&&(!gate.admitting()||gate.epoch()!=ownershipEpoch)) || revoked || (session != null && session != established);
            newlyEstablished = !reject && session == null;
            if (!reject) session = established;
        }
        if (reject) {
            log.warn("[FIX-132][WS-LIFECYCLE][REJECTED_ESTABLISHMENT] generation={}, session={}",
                    generation, established.getId());
            closeSession(established);
        } else if (newlyEstablished) {
            log.info("[FIX-132][WS-LIFECYCLE][ESTABLISHED] generation={}, session={}",
                    generation, established.getId());
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession source, TextMessage message) {
        synchronized (this) {
            if (revoked || session == null || session != source) return;
            inFlight++;
        }
        long started = System.nanoTime();
        try (var permit=gate==null?null:gate.admit(ownershipEpoch)) {
            if(gate!=null&&permit==null)return;
            JsonNode root = objectMapper.readTree(message.getPayload());
            JsonNode data = root.has("data") ? root.path("data") : root;
            JsonNode k = data.path("k");
            // FIX-228 retained: REST repair never runs on the callback thread.
            if (candleStore.persistWebsocket(root)) {
                log.info("FIX-139 durable candle closed: symbol={}, interval={}, openTime={}",
                        k.path("s").asText(), k.path("i").asText(),
                        java.time.Instant.ofEpochMilli(k.path("t").asLong()));
            }
        } catch (Exception exception) {
            if(gate!=null&&(exception instanceof org.springframework.dao.DataAccessException || exception instanceof org.springframework.transaction.TransactionException))
                gate.uncertain("WebSocket persistence database failure");
            log.error("[FIX-132][CANDLE_PERSIST_FAILED] generation={}; candle may be missing", generation, exception);
        } finally {
            synchronized (this) {
                inFlight--;
                notifyAll();
            }
            long elapsed = (System.nanoTime() - started) / 1_000_000;
            if (elapsed >= 1_000) {
                log.warn("[FIX-132][CALLBACK_SLOW] generation={}, persistenceReturnMs={}", generation, elapsed);
            }
        }
    }

    @Override
    public void handleTransportError(WebSocketSession source, Throwable exception) {
        if (!owns(source)) return;
        log.error("[FIX-132][WS-LIFECYCLE][TRANSPORT_ERROR] generation={}", generation, exception);
        close();
    }

    @Override
    public void afterConnectionClosed(WebSocketSession source, CloseStatus status) {
        synchronized (this) {
            if (session != source) return; // late callback from an extra session is not authoritative
            revoked = true;
        }
        log.warn("[FIX-132][WS-LIFECYCLE][CLOSED] generation={}, status={}", generation, status);
    }

    private synchronized boolean owns(WebSocketSession source) { return session != null && session == source; }
    public synchronized boolean isConnected() { return !revoked && session != null && session.isOpen(); }
    synchronized boolean isDrained() { return revoked && inFlight == 0; }
    synchronized boolean isTransportClosed() { return session == null || !session.isOpen(); }
    synchronized int inFlight() { return inFlight; }

    public void close() {
        WebSocketSession current;
        synchronized (this) {
            revoked = true;
            current = session;
        }
        // Network close may call us back; never hold the admission monitor across it.
        if (current != null) closeSession(current);
    }

    private void closeSession(WebSocketSession value) {
        try {
            if (value.isOpen()) value.close(CloseStatus.NORMAL);
        } catch (Exception exception) {
            // Transport closure and processing revocation are separate. Even a failed
            // socket close cannot re-enable this generation's database writes.
            log.warn("[FIX-132][WS-LIFECYCLE][CLOSE_FAILED] generation={}, session={}",
                    generation, value.getId(), exception);
        }
    }
}
