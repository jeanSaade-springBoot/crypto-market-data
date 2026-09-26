package com.crypto.marketdata;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import io.micrometer.core.instrument.MeterRegistry;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class CollectorIngestionHealthTest {
    @Test void ownershipAloneIsNotReadyAndUncertaintyIsVisible(){
        var gate=new OwnershipGate(true);var manager=mock(BinanceWebSocketManager.class);
        var health=new CollectorIngestionHealth(gate,manager,new StaticListableBeanFactory().getBeanProvider(MeterRegistry.class));
        assertEquals("OUT_OF_SERVICE",health.health().getStatus().getCode());gate.acquired("token");
        assertEquals("OUT_OF_SERVICE",health.health().getStatus().getCode());
        when(manager.ingestionReady()).thenReturn(true);assertEquals("UP",health.health().getStatus().getCode());
        gate.uncertain("test");when(manager.ingestionReady()).thenReturn(false);
        assertEquals("OUT_OF_SERVICE",health.health().getStatus().getCode());assertEquals("OWNERSHIP_UNCERTAIN",health.health().getDetails().get("state"));
        assertFalse(health.health().getDetails().containsKey("token"));
    }
    @Test void disabledContributionDoesNotRequireOwnership(){
        var health=new CollectorIngestionHealth(new OwnershipGate(false),mock(BinanceWebSocketManager.class),new StaticListableBeanFactory().getBeanProvider(MeterRegistry.class));
        assertEquals("UP",health.health().getStatus().getCode());
    }
}
