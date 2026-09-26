package com.crypto.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(10)
class CollectorConnectionOwnerTest {
    private static final URI URI_VALUE = URI.create("wss://example.invalid/ws");
    private static final TextMessage MESSAGE = new TextMessage("{\"k\":{\"s\":\"BTCUSDT\",\"i\":\"1m\",\"t\":1}}");

    private WebSocketSession session(String id) {
        WebSocketSession session = mock(WebSocketSession.class);
        java.util.concurrent.atomic.AtomicBoolean open = new java.util.concurrent.atomic.AtomicBoolean(true);
        when(session.isOpen()).thenAnswer(call -> open.get());
        try { doAnswer(call -> { open.set(false); return null; }).when(session).close(any()); }
        catch (java.io.IOException e) { throw new IllegalStateException(e); }
        when(session.getId()).thenReturn(id);
        return session;
    }

    @Test void waitingOwnershipPreventsHandshakeAndOldEpochCannotResume() throws Exception {
        var gate=new OwnershipGate(true);var store=mock(CandleStore.class);
        var handlers=new ArrayList<BinanceWebSocketHandler>();var sessions=new ArrayList<WebSocketSession>();
        var owner=new CollectorConnectionOwner(new ObjectMapper(),store,(handler,uri)->{
            handlers.add(handler);var socket=session("epoch-"+handlers.size());sessions.add(socket);
            handler.afterConnectionEstablished(socket);return CompletableFuture.completedFuture(socket);
        },Duration.ofSeconds(15));owner.ownershipGate(gate);
        assertEquals(CollectorConnectionOwner.Outcome.DRAINING,owner.connect(URI_VALUE));assertTrue(handlers.isEmpty());
        gate.acquired("token");assertEquals(CollectorConnectionOwner.Outcome.CONNECTED,owner.connect(URI_VALUE));
        handlers.getFirst().handleTextMessage(sessions.getFirst(),MESSAGE);verify(store,times(1)).persistWebsocket(any());
        gate.uncertain("renew failed");handlers.getFirst().handleTextMessage(sessions.getFirst(),MESSAGE);verify(store,times(1)).persistWebsocket(any());
        owner.pause();assertTrue(owner.quiescent());gate.waiting();gate.acquired("token");
        assertEquals(CollectorConnectionOwner.Outcome.CONNECTED,owner.connect(URI_VALUE));
        handlers.getFirst().handleTextMessage(sessions.getFirst(),MESSAGE);verify(store,times(1)).persistWebsocket(any());
        handlers.get(1).handleTextMessage(sessions.get(1),MESSAGE);verify(store,times(2)).persistWebsocket(any());owner.stop();
    }

    @Test
    void timedOutAttemptRejectsLateEstablishmentAndCannotWrite() throws Exception {
        CandleStore store = mock(CandleStore.class);
        AtomicReference<BinanceWebSocketHandler> captured = new AtomicReference<>();
        // Force the precise timeout interleaving, without relying on clock timing.
        CompletableFuture<WebSocketSession> timeout = new CompletableFuture<>() {
            @Override public WebSocketSession get(long value, TimeUnit unit) throws TimeoutException {
                throw new TimeoutException("controlled provider timeout");
            }
        };
        CollectorConnectionOwner owner = new CollectorConnectionOwner(new ObjectMapper(), store,
                (handler, uri) -> { captured.set(handler); return timeout; }, Duration.ofSeconds(15));
        assertEquals(CollectorConnectionOwner.Outcome.FAILED, owner.connect(URI_VALUE));
        WebSocketSession late = session("late");
        captured.get().afterConnectionEstablished(late);
        captured.get().handleTextMessage(late, MESSAGE);
        verify(late).close(CloseStatus.NORMAL);
        verifyNoInteractions(store);
        assertFalse(owner.isConnected());
    }

    @Test
    void replacementWaitsForAdmittedPersistenceToReturn() throws Exception {
        CandleStore store = mock(CandleStore.class);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(call -> { entered.countDown(); assertTrue(release.await(5, TimeUnit.SECONDS)); return false; })
                .when(store).persistWebsocket(any());
        ArrayList<BinanceWebSocketHandler> handlers = new ArrayList<>();
        ArrayList<WebSocketSession> sessions = new ArrayList<>();
        CollectorConnectionOwner owner = new CollectorConnectionOwner(new ObjectMapper(), store, (handler, uri) -> {
            handlers.add(handler);
            WebSocketSession session = session("generation-" + handlers.size());
            sessions.add(session);
            handler.afterConnectionEstablished(session);
            return CompletableFuture.completedFuture(session);
        }, Duration.ofSeconds(15));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            assertEquals(CollectorConnectionOwner.Outcome.CONNECTED, owner.connect(URI_VALUE));
            Future<?> admitted = worker.submit(() -> handlers.getFirst().handleTextMessage(sessions.getFirst(), MESSAGE));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(CollectorConnectionOwner.Outcome.DRAINING, owner.connect(URI_VALUE));
            assertEquals(1, handlers.size(), "No replacement handshake while predecessor is writing");
            handlers.getFirst().handleTextMessage(sessions.getFirst(), MESSAGE);
            verify(store, times(1)).persistWebsocket(any());
            release.countDown();
            admitted.get(5, TimeUnit.SECONDS);
            assertEquals(CollectorConnectionOwner.Outcome.CONNECTED, owner.connect(URI_VALUE));
            assertEquals(2, handlers.size());
            handlers.getFirst().handleTextMessage(sessions.getFirst(), MESSAGE);
            verify(store, times(1)).persistWebsocket(any());
        } finally { release.countDown(); owner.stop(); worker.shutdownNow(); }
    }

    @Test
    void distinctDuplicateSessionIsClosedWithoutRevokingTheOwner() throws Exception {
        CandleStore store = mock(CandleStore.class);
        BinanceWebSocketHandler handler = new BinanceWebSocketHandler(new ObjectMapper(), store, 7);
        WebSocketSession first = session("first"), extra = session("extra");
        handler.afterConnectionEstablished(first);
        handler.afterConnectionEstablished(extra);
        handler.afterConnectionClosed(extra, CloseStatus.NORMAL);
        handler.handleTransportError(extra, new IllegalStateException("extra failed"));
        handler.handleTextMessage(extra, MESSAGE);
        handler.handleTextMessage(first, MESSAGE);
        verify(extra).close(CloseStatus.NORMAL);
        verify(first, never()).close(any());
        verify(store).persistWebsocket(any());
        assertTrue(handler.isConnected());
    }

    @Test
    void shutdownDuringPendingHandshakeRejectsLateSuccess() throws Exception {
        CandleStore store = mock(CandleStore.class);
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<BinanceWebSocketHandler> captured = new AtomicReference<>();
        CompletableFuture<WebSocketSession> future = new CompletableFuture<>();
        CollectorConnectionOwner owner = new CollectorConnectionOwner(new ObjectMapper(), store,
                (handler, uri) -> { captured.set(handler); started.countDown(); return future; }, Duration.ofSeconds(15));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> connect = worker.submit(() -> owner.connect(URI_VALUE));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            owner.stop();
            connect.get(5, TimeUnit.SECONDS);
            WebSocketSession late = session("shutdown-late");
            captured.get().afterConnectionEstablished(late);
            captured.get().handleTextMessage(late, MESSAGE);
            verify(late).close(CloseStatus.NORMAL);
            verifyNoInteractions(store);
            assertEquals(CollectorConnectionOwner.Outcome.STOPPED, owner.connect(URI_VALUE));
        } finally { owner.stop(); worker.shutdownNow(); }
    }

    @Test
    void revokeBeforeAdmissionRejectsWriteAndHealthyEmptyIsNotDrained() {
        CandleStore store = mock(CandleStore.class);
        BinanceWebSocketHandler handler = new BinanceWebSocketHandler(new ObjectMapper(), store);
        WebSocketSession session = session("owner");
        handler.afterConnectionEstablished(session);
        assertFalse(handler.isDrained());
        handler.close();
        assertTrue(handler.isDrained());
        handler.handleTextMessage(session, MESSAGE);
        verifyNoInteractions(store);
    }

    @Test
    void failedTransportCloseDoesNotRestoreWriteAuthority() throws Exception {
        CandleStore store = mock(CandleStore.class);
        BinanceWebSocketHandler handler = new BinanceWebSocketHandler(new ObjectMapper(), store);
        WebSocketSession session = session("close-failure");
        doThrow(new java.io.IOException("forced close failure")).when(session).close(any());
        handler.afterConnectionEstablished(session);
        handler.close();
        handler.handleTextMessage(session, MESSAGE);
        assertTrue(handler.isDrained());
        assertFalse(handler.isConnected());
        verifyNoInteractions(store);
    }
    @Test
    void failedCloseRetainsOwnerAndPreventsAnotherHandshake() throws Exception {
        CandleStore store = mock(CandleStore.class);
        WebSocketSession session = session("cannot-close");
        doThrow(new java.io.IOException("forced failure")).when(session).close(any());
        java.util.concurrent.atomic.AtomicInteger handshakes = new java.util.concurrent.atomic.AtomicInteger();
        CollectorConnectionOwner owner = new CollectorConnectionOwner(new ObjectMapper(), store, (handler, uri) -> {
            handshakes.incrementAndGet();
            handler.afterConnectionEstablished(session);
            return CompletableFuture.completedFuture(session);
        }, Duration.ofSeconds(15));
        try {
            assertEquals(CollectorConnectionOwner.Outcome.CONNECTED, owner.connect(URI_VALUE));
            assertEquals(CollectorConnectionOwner.Outcome.DRAINING, owner.connect(URI_VALUE));
            assertEquals(1, handshakes.get());
        } finally { owner.stop(); }
    }

    @Test
    void shutdownDuringAdmittedPersistenceNeverStartsReplacement() throws Exception {
        CandleStore store = mock(CandleStore.class);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(call -> { entered.countDown(); assertTrue(release.await(5, TimeUnit.SECONDS)); return false; })
                .when(store).persistWebsocket(any());
        WebSocketSession session = session("draining");
        AtomicReference<BinanceWebSocketHandler> captured = new AtomicReference<>();
        CollectorConnectionOwner owner = new CollectorConnectionOwner(new ObjectMapper(), store, (handler, uri) -> {
            captured.set(handler);
            handler.afterConnectionEstablished(session);
            return CompletableFuture.completedFuture(session);
        }, Duration.ofSeconds(15));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            owner.connect(URI_VALUE);
            Future<?> admitted = worker.submit(() -> captured.get().handleTextMessage(session, MESSAGE));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            owner.stop();
            assertFalse(captured.get().isDrained());
            assertEquals(CollectorConnectionOwner.Outcome.STOPPED, owner.connect(URI_VALUE));
            captured.get().handleTextMessage(session, MESSAGE);
            release.countDown();
            admitted.get(5, TimeUnit.SECONDS);
            assertTrue(captured.get().isDrained());
            verify(store, times(1)).persistWebsocket(any());
        } finally { release.countDown(); owner.stop(); worker.shutdownNow(); }
    }

}
