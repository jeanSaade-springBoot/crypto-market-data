package com.crypto.marketdata;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class RecoveryCoordinatorTest {
    @Test void waitingPreventsRestAndConcurrentPassIsNotQueued()throws Exception {
        var gate=new OwnershipGate(true);var repair=mock(GapRepairService.class);var properties=mock(MarketDataProperties.class);
        when(properties.getIntervals()).thenReturn(List.of("1m"));var coordinator=new RecoveryCoordinator(gate,repair,properties);
        coordinator.reconcile(List.of("BTCUSDT"));verifyNoInteractions(repair);gate.acquired("owner");
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        doAnswer(inv->{entered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));return null;}).when(repair).reconcile("BTCUSDT","1m");
        var executor=Executors.newSingleThreadExecutor();try{
            var future=executor.submit(()->coordinator.reconcile(List.of("BTCUSDT","ETHUSDT")));assertTrue(entered.await(5,TimeUnit.SECONDS));
            coordinator.reconcile(List.of("BTCUSDT"));verify(repair,times(1)).reconcile("BTCUSDT","1m");gate.uncertain("test");
            assertEquals(1,gate.inFlight());release.countDown();future.get(5,TimeUnit.SECONDS);assertEquals(0,gate.inFlight());verify(repair,never()).reconcile("ETHUSDT","1m");
        }finally{release.countDown();executor.shutdownNow();}
    }
}
