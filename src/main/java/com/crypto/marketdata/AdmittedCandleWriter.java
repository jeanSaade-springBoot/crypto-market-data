package com.crypto.marketdata;

import org.springframework.stereotype.Component;
import java.time.Instant;

/** FIX-132 nontransactional outer boundary. Permit lasts through the proxied
 * CandleStore commit/rollback, unlike a finally block inside an @Transactional method. */
@Component
public class AdmittedCandleWriter {
    private final OwnershipGate gate;private final CandleStore store;
    public AdmittedCandleWriter(OwnershipGate gate,CandleStore store){this.gate=gate;this.store=store;}
    public void persistRest(String symbol,String interval,BinanceKline candle,String source,Instant observed){
        var outer=gate.currentPermit();long expected=outer==null?gate.epoch():outer.epoch();
        try(var permit=gate.admit(expected)){
            if(permit==null)throw new IngestionPaused();
            try{store.persistRest(symbol,interval,candle,source,observed);}
            catch(org.springframework.dao.DataAccessException | org.springframework.transaction.TransactionException failure){
                gate.uncertain("REST persistence database failure");throw failure;
            }
        }
    }
}
