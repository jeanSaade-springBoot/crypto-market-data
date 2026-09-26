package com.crypto.marketdata;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.slf4j.LoggerFactory;

/** FIX-132 ownership correction. This monitor ONLY guards admission/state, never JDBC/network.
 * A permit captures an immutable epoch/token; closing admission cannot erase an admitted
 * transaction's fence identity. Release must happen AFTER the Spring transaction returns. */
@Component
public final class OwnershipGate {
    public enum State { FEED_DISABLED, WAITING_FOR_OWNERSHIP, OWNED, OWNERSHIP_UNCERTAIN, OWNERSHIP_LOST, STOPPING, STOPPED }
    private final boolean enabled;
    private State state;
    private String token;
    private long epoch;
    private int inFlight;
    private String reason="startup";
    private final ThreadLocal<Permit> current=new ThreadLocal<>();
    @org.springframework.beans.factory.annotation.Autowired
    public OwnershipGate(Environment env) { this(env.getProperty("market-data.stream.enabled",Boolean.class,false)); }
    OwnershipGate(boolean enabled) { this.enabled=enabled;state=enabled?State.WAITING_FOR_OWNERSHIP:State.FEED_DISABLED; }
    public boolean enabled(){return enabled;}
    public synchronized State state(){return state;}
    public synchronized long epoch(){return epoch;}
    public synchronized int inFlight(){return inFlight;}
    public synchronized String reason(){return reason;}
    public synchronized boolean admitting(){return state==State.OWNED || state==State.FEED_DISABLED;}
    synchronized void acquired(String value) {
        if(state!=State.WAITING_FOR_OWNERSHIP || inFlight!=0) throw new IllegalStateException("Acquisition requires drained waiting state");
        token=value;epoch++;transition(State.OWNED,"lease acquired");
    }
    public synchronized void uncertain(String detail){if(state==State.OWNED)transition(State.OWNERSHIP_UNCERTAIN,detail);}
    public synchronized void lost(String detail){if(state==State.OWNED || state==State.OWNERSHIP_UNCERTAIN)transition(State.OWNERSHIP_LOST,detail);}
    synchronized void waitingReason(String detail){if(state==State.WAITING_FOR_OWNERSHIP)reason=detail;}
    synchronized void waiting(){if(inFlight!=0)throw new IllegalStateException("Old work not drained");if(state!=State.STOPPING&&state!=State.STOPPED)transition(State.WAITING_FOR_OWNERSHIP,"old generation drained");}
    synchronized void stopping(){if(state==State.STOPPED||state==State.STOPPING)return;transition(State.STOPPING,"shutdown; release not yet confirmed");}
    synchronized void stopped(){transition(State.STOPPED,"drained; release/no ownership confirmed");}
    private void transition(State next,String detail){
        State old=state;state=next;reason=detail;
        LoggerFactory.getLogger(OwnershipGate.class).info("[FIX-132][OWNERSHIP_STATE] {} -> {}, epoch={}, inFlight={}, reason={}",old,next,epoch,inFlight,detail);
        notifyAll();
    }
    public Permit admit(long expectedEpoch){
        synchronized(this){
            if(!admitting() || (enabled&&expectedEpoch!=epoch))return null;
            inFlight++;return new Permit(epoch,token,current.get());
        }
    }
    public Permit admit(){return admit(epoch());}
    public Permit currentPermit(){return current.get();}
    public final class Permit implements AutoCloseable {
        private final long epoch; private final String token; private final Permit previous; private boolean closed;
        private Permit(long epoch,String token,Permit previous){this.epoch=epoch;this.token=token;this.previous=previous;current.set(this);}
        public long epoch(){return epoch;} public String token(){return token;}
        public boolean stillCurrent(){synchronized(OwnershipGate.this){return admitting()&&(!enabled||epoch==OwnershipGate.this.epoch);}}
        @Override public void close(){synchronized(OwnershipGate.this){if(!closed){closed=true;inFlight--;current.set(previous);OwnershipGate.this.notifyAll();}}}
    }
}
