package com.crypto.marketdata;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.transaction.*;
import org.springframework.transaction.interceptor.*;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import java.util.concurrent.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class AdmittedCandleWriterTest {
    @Test void permitIncludesProxyCommitAndOldRestWorkCannotCrossEpoch()throws Exception{
        var gate=new OwnershipGate(true);gate.acquired("owner");
        var entered=new CountDownLatch(1);var finish=new CountDownLatch(1);
        var tm=new PlatformTransactionManager(){
            public TransactionStatus getTransaction(TransactionDefinition d){return new SimpleTransactionStatus();}
            public void rollback(TransactionStatus s){}
            public void commit(TransactionStatus s){entered.countDown();try{assertTrue(finish.await(5,TimeUnit.SECONDS));}catch(InterruptedException e){throw new RuntimeException(e);}}
        };
        var target=new CandleStore(null){@Override @org.springframework.transaction.annotation.Transactional public void persistRest(String a,String b,BinanceKline c,String d,java.time.Instant e){assertNotNull(gate.currentPermit());}};
        var factory=new ProxyFactory(target);factory.setProxyTargetClass(true);factory.addAdvice(new TransactionInterceptor(tm,new AnnotationTransactionAttributeSource()));
        var writer=new AdmittedCandleWriter(gate,(CandleStore)factory.getProxy());var executor=Executors.newSingleThreadExecutor();
        try{
            var future=executor.submit(()->writer.persistRest("BTCUSDT","1m",null,"REST",null));
            assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(1,gate.inFlight());gate.uncertain("renew failed");
            assertThrows(IngestionPaused.class,()->writer.persistRest("BTCUSDT","1m",null,"REST",null));
            finish.countDown();future.get(5,TimeUnit.SECONDS);assertEquals(0,gate.inFlight());
        }finally{finish.countDown();executor.shutdownNow();}
    }
}
