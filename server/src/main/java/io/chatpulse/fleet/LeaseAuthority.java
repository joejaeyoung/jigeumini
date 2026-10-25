package io.chatpulse.fleet;

import io.chatpulse.pipeline.PublishingAuthority;
import io.chatpulse.poc.SourceFailure;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** DB 요청 시작의 단조 시각을 기준으로 TTL을 줄인다. 응답 지연이 권한 수명을 늘리지 않는다. */
public final class LeaseAuthority implements PublishingAuthority {
    private record Token(long epoch,long deadline) { }
    private final Map<String,Token> tokens=new HashMap<>();private final LongSupplier nanoTime;
    private boolean revoked;
    public LeaseAuthority(){this(System::nanoTime);}
    LeaseAuthority(LongSupplier nanoTime){this.nanoTime=nanoTime;}
    public synchronized void update(CollectorLeases.Grant grant,long requestStarted) {
        if(revoked)throw lost();
        if(tokens.values().stream().anyMatch(t->t.deadline()-nanoTime.getAsLong()<=0)){revoke();throw lost();}
        var next=new HashMap<String,Token>();
        for(var lease:grant.leases()) {
            long remaining=Duration.between(grant.databaseNow(),lease.expiresAt()).minusSeconds(2).toNanos();
            long deadline=requestStarted+Math.max(0,remaining);
            if(deadline-nanoTime.getAsLong()<=0){revoke();throw lost();}
            var old=tokens.get(lease.channelId());if(old!=null&&old.epoch()!=lease.epoch()){revoke();throw lost();}
            next.put(lease.channelId(),new Token(lease.epoch(),deadline));
        }
        if(!next.keySet().containsAll(tokens.keySet())){revoke();throw lost();}
        tokens.clear();tokens.putAll(next);
    }
    public synchronized void revoke(){revoked=true;tokens.clear();}
    @Override public synchronized Permit acquire(String channel) {
        var token=tokens.get(channel);if(revoked||token==null||token.deadline()-nanoTime.getAsLong()<=0)throw lost();
        long epoch=token.epoch();return ()->check(channel,epoch);
    }
    private synchronized void check(String channel,long epoch) {
        var current=tokens.get(channel);
        if(revoked||current==null||current.epoch()!=epoch||current.deadline()-nanoTime.getAsLong()<=0)throw lost();
    }
    public synchronized void checkAll(){if(revoked||tokens.values().stream().anyMatch(t->t.deadline()-nanoTime.getAsLong()<=0))throw lost();}
    private static SourceFailure lost(){return new SourceFailure("COLLECTION_LEASE_LOST",true);}
}
