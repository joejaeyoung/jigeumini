package io.chatpulse.fleet;

import io.chatpulse.poc.*;
import java.net.URI;
import java.time.Duration;

/** 수집기 한 인스턴스의 HTTP 호출 시작 간격·총량. 원천의 실제 공식 quota를 뜻하지 않는다. */
public final class BudgetedChatSource implements ChatSource {
    private final ChatSource source;private final LeaseAuthority authority;private final int maxRequests;
    private final long interval;private long next;private int requests;
    public BudgetedChatSource(ChatSource source,LeaseAuthority authority,int maxRequests,Duration interval) {
        if(maxRequests<1||maxRequests>10000||interval.compareTo(Duration.ofMillis(100))<0||interval.compareTo(Duration.ofSeconds(10))>0)throw new IllegalArgumentException("FLEET_SOURCE_BUDGET");
        this.source=source;this.authority=authority;this.maxRequests=maxRequests;this.interval=interval.toNanos();next=System.nanoTime();
    }
    private synchronized void reserve() {
        authority.checkAll();if(requests>=maxRequests)throw new SourceFailure("SOURCE_BUDGET_EXHAUSTED",true);
        while(next-System.nanoTime()>0) {
            try{Thread.sleep(Math.min(100,Math.max(1,(next-System.nanoTime())/1_000_000)));}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new SourceFailure("SOURCE_BUDGET_INTERRUPTED",true);}
            authority.checkAll();
        }
        requests++;next=System.nanoTime()+interval;
    }
    public io.chatpulse.pipeline.PublishingAuthority.Permit authorize(String channel){return authority.acquire(channel);}
    public void reserveFor(String channel){authority.acquire(channel).check();reserve();authority.acquire(channel).check();}
    @Override public String resolveRoom(String channel){authority.acquire(channel).check();reserve();return source.resolveRoom(channel);}
    @Override public String readToken(String room){reserve();return source.readToken(room);}
    @Override public URI socketUri(String room){authority.checkAll();return source.socketUri(room);}
    public synchronized int requests(){return requests;}
}
