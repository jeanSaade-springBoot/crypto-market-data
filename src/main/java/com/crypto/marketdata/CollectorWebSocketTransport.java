package com.crypto.marketdata;

import org.apache.tomcat.websocket.WsWebSocketContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/** FIX-132: one explicitly owned provider/container and bounded handshake executor.
 * Tomcat is the WebSocket provider supplied by this project's Boot starter. Pinning
 * that existing provider here makes its destroy operation explicit on shutdown;
 * it is not evidence that a particular provider resource leaked in Production.
 */
final class CollectorWebSocketTransport implements CollectorConnectionOwner.Connector, AutoCloseable {
    private final StandardWebSocketClient client;
    private final ThreadPoolTaskExecutor executor;
    private final Runnable destroyContainer;
    private boolean closed;

    CollectorWebSocketTransport() {
        this(new WsWebSocketContainer(), new ThreadPoolTaskExecutor());
    }

    private CollectorWebSocketTransport(WsWebSocketContainer container, ThreadPoolTaskExecutor executor) {
        this(new StandardWebSocketClient(container), executor, container::destroy);
    }

    CollectorWebSocketTransport(StandardWebSocketClient client, ThreadPoolTaskExecutor executor, Runnable destroyContainer) {
        this.client = client;
        this.executor = executor;
        this.destroyContainer = destroyContainer;
        // A provider stalled beyond timeout cannot create unbounded handshake threads.
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("fix132-collector-connect-");
        executor.initialize();
        client.setTaskExecutor(executor);
    }

    @Override
    public synchronized CompletableFuture<WebSocketSession> connect(BinanceWebSocketHandler handler, URI uri) {
        if (closed) throw new IllegalStateException("FIX-132 collector transport is stopped");
        return client.execute(handler, null, uri);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
        }
        try { executor.shutdown(); }
        finally { destroyContainer.run(); }
    }
}
