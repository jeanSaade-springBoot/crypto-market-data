package com.crypto.marketdata;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CollectorWebSocketTransportTest {
    @Test
    void reusesClientAndDestroysResourcesExactlyOnce() {
        StandardWebSocketClient client = mock(StandardWebSocketClient.class);
        ThreadPoolTaskExecutor executor = mock(ThreadPoolTaskExecutor.class);
        Runnable destroy = mock(Runnable.class);
        BinanceWebSocketHandler handler = mock(BinanceWebSocketHandler.class);
        URI uri = URI.create("wss://example.invalid/ws");
        when(client.execute(handler, null, uri)).thenReturn(new CompletableFuture<>());
        CollectorWebSocketTransport transport = new CollectorWebSocketTransport(client, executor, destroy);
        transport.connect(handler, uri);
        transport.connect(handler, uri);
        verify(client, times(2)).execute(handler, null, uri);
        verify(executor).setMaxPoolSize(1);
        verify(executor).setQueueCapacity(1);
        transport.close();
        transport.close();
        verify(executor).shutdown();
        verify(destroy).run();
        assertThrows(IllegalStateException.class, () -> transport.connect(handler, uri));
        verify(client, times(2)).execute(handler, null, uri);
    }

    @Test
    void providerCleanupStillRunsWhenExecutorShutdownFails() {
        ThreadPoolTaskExecutor executor = mock(ThreadPoolTaskExecutor.class);
        Runnable destroy = mock(Runnable.class);
        doThrow(new IllegalStateException("forced shutdown failure")).when(executor).shutdown();
        CollectorWebSocketTransport transport = new CollectorWebSocketTransport(
                mock(StandardWebSocketClient.class), executor, destroy);
        assertThrows(IllegalStateException.class, transport::close);
        verify(destroy).run();
    }
}
