package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.WebSocketSession;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** FIX-132: owns even PENDING attempts, so timeout never leaves an untracked writer.
 * Calls to connect are serialized by the manager's one scheduler. stop is independent
 * and may revoke an attempt while the scheduler is waiting for its handshake.
 */
final class CollectorConnectionOwner {
    interface Connector {
        CompletableFuture<WebSocketSession> connect(BinanceWebSocketHandler handler, URI uri);
    }
    enum Outcome { CONNECTED, DRAINING, FAILED, STOPPED }
    private static final Logger log = LoggerFactory.getLogger(CollectorConnectionOwner.class);
    private final ObjectMapper mapper;
    private final CandleStore store;
    private final Connector connector;
    private final Duration timeout;
    private BinanceWebSocketHandler current;
    private CompletableFuture<WebSocketSession> attempt;
    private boolean connecting;
    private boolean stopped;
    private long generation;

    CollectorConnectionOwner(ObjectMapper mapper, CandleStore store, Connector connector, Duration timeout) {
        this.mapper = mapper;
        this.store = store;
        this.connector = connector;
        this.timeout = timeout;
    }

    Outcome connect(URI uri) {
        BinanceWebSocketHandler previous;
        synchronized (this) {
            if (stopped) return Outcome.STOPPED;
            if (connecting) return Outcome.DRAINING;
            connecting = true;
            previous = current;
        }
        // Close outside the owner monitor: close callbacks may be synchronous.
        if (previous != null) previous.close();
        BinanceWebSocketHandler next;
        synchronized (this) {
            if (stopped) { connecting = false; return Outcome.STOPPED; }
            if (previous != null && (!previous.isDrained() || !previous.isTransportClosed())) {
                connecting = false;
                log.warn("[FIX-132][WS-LIFECYCLE][DRAIN_OR_CLOSE_PENDING] generation={}, inFlight={}; replacement deferred",
                        generation, previous.inFlight());
                return Outcome.DRAINING;
            }
            next = new BinanceWebSocketHandler(mapper, store, ++generation);
            current = next; // register ownership BEFORE the provider can establish
        }
        CompletableFuture<WebSocketSession> future = null;
        try {
            log.info("[FIX-132][WS-LIFECYCLE][CONNECTING] generation={}", generation);
            future = connector.connect(next, uri);
            // A provider may finish after timeout/cancellation. Handler revocation is
            // authoritative; this callback also closes a returned session if the provider
            // omitted its establishment notification or completed in a different order.
            future.thenAccept(next::afterConnectionEstablished);
            synchronized (this) {
                attempt = future;
                if (stopped) future.cancel(true);
            }
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            synchronized (this) {
                return !stopped && next.isConnected() ? Outcome.CONNECTED : Outcome.FAILED;
            }
        } catch (Exception failure) {
            next.close(); // revoke FIRST; cancellation alone cannot close a late session
            if (future != null) future.cancel(true);
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            log.error("[FIX-132][WS-LIFECYCLE][CONNECT_FAILED] generation={}", generation, failure);
            return Outcome.FAILED;
        } finally {
            synchronized (this) { attempt = null; connecting = false; }
        }
    }

    synchronized boolean isConnected() { return !stopped && current != null && current.isConnected(); }

    void stop() {
        BinanceWebSocketHandler old;
        CompletableFuture<WebSocketSession> pending;
        synchronized (this) {
            stopped = true;
            old = current;
            pending = attempt;
        }
        if (old != null) old.close();
        if (pending != null) pending.cancel(true);
        log.info("[FIX-132][WS-LIFECYCLE][STOPPED] lastGeneration={}", generation);
    }
}
