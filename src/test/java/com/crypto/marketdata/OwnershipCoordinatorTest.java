package com.crypto.marketdata;
import org.junit.jupiter.api.*;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class OwnershipCoordinatorTest {
    OwnershipGate gate;OwnershipLeaseStore leases;OwnershipCoordinator coordinator;AtomicLong time;
    @BeforeEach void setup(){gate=new OwnershipGate(true);leases=mock(OwnershipLeaseStore.class);time=new AtomicLong();coordinator=new OwnershipCoordinator(gate,leases,time::get);}
    @Test void validPredecessorAndDatabaseFailureKeepAdmissionClosed() throws Exception {
        when(leases.acquire(anyString())).thenReturn(OwnershipLeaseStore.Result.BUSY).thenThrow(new SQLException("unavailable"));
        coordinator.tick();assertNull(gate.admit());coordinator.tick();assertNull(gate.admit());assertEquals(OwnershipGate.State.WAITING_FOR_OWNERSHIP,gate.state());
    }
    void acquire()throws Exception{when(leases.acquire(anyString())).thenReturn(OwnershipLeaseStore.Result.ACQUIRED);coordinator.tick();}
    @Test void firstRenewalFailureClosesAdmissionAndDrainPrecedesRecovery()throws Exception {
        acquire();var permit=gate.admit();long epoch=permit.epoch();
        when(leases.renew(anyString())).thenThrow(new SQLException("lost reply"));time.set(TimeUnit.SECONDS.toNanos(6));coordinator.tick();
        assertEquals(OwnershipGate.State.OWNERSHIP_UNCERTAIN,gate.state());assertNull(gate.admit());assertNotNull(permit.token());
        coordinator.tick();verify(leases,times(1)).acquire(anyString());
        permit.close();coordinator.transportDrained(()->false);coordinator.tick();verify(leases,times(1)).acquire(anyString());
        coordinator.transportDrained(()->true);coordinator.tick();assertEquals(OwnershipGate.State.OWNED,gate.state());assertTrue(gate.epoch()>epoch);assertNull(gate.admit(epoch));
    }
    @Test void lostLeaseMustDrainAndPassThroughWaiting()throws Exception{
        acquire();var permit=gate.admit();when(leases.renew(anyString())).thenReturn(OwnershipLeaseStore.Result.LOST);time.set(TimeUnit.SECONDS.toNanos(6));coordinator.tick();
        assertEquals(OwnershipGate.State.OWNERSHIP_LOST,gate.state());coordinator.tick();verify(leases,times(1)).acquire(anyString());permit.close();
        when(leases.acquire(anyString())).thenAnswer(inv->{assertEquals(OwnershipGate.State.WAITING_FOR_OWNERSHIP,gate.state());return OwnershipLeaseStore.Result.BUSY;});
        coordinator.tick();assertEquals(OwnershipGate.State.WAITING_FOR_OWNERSHIP,gate.state());
    }
    @Test void shutdownCannotReleaseBeforeAdmittedWorkReturns()throws Exception {
        acquire();var permit=gate.admit();coordinator.beginStop();assertFalse(coordinator.releaseIfDrained());verify(leases,never()).release(anyString());
        permit.close();assertTrue(coordinator.releaseIfDrained());verify(leases).release(anyString());assertEquals(OwnershipGate.State.STOPPED,gate.state());
    }
    @Test void shutdownDuringBlockedAcquireNeverReopensAdmission()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        when(leases.acquire(anyString())).thenAnswer(inv->{entered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));return OwnershipLeaseStore.Result.ACQUIRED;});
        var executor=Executors.newSingleThreadExecutor();try{
            var future=executor.submit(coordinator::tick);assertTrue(entered.await(5,TimeUnit.SECONDS));coordinator.beginStop();release.countDown();future.get(5,TimeUnit.SECONDS);
            assertNull(gate.admit());assertTrue(coordinator.releaseIfDrained());verify(leases).release(anyString());
        }finally{release.countDown();executor.shutdownNow();}
    }
    @Test void disabledNeverCallsOwnershipDatabase(){var disabled=new OwnershipCoordinator(new OwnershipGate(false),leases,time::get);disabled.tick();verifyNoInteractions(leases);}
}
