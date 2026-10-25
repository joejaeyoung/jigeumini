package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/** 실험용 bounded async 후보. 운영 발행기를 대체하지 않는다. */
final class ExperimentPublisher implements AutoCloseable {
    private final KafkaTemplate<String,byte[]> kafka;
    private final String topic;
    private final boolean sharded;
    private final Semaphore slots;
    private final int capacity;
    private final EventCodec codec=new EventCodec();
    private final ConcurrentHashMap<Long,ChatEvent> unacked=new ConcurrentHashMap<>();
    private final AtomicLong accepted=new AtomicLong(),acknowledged=new AtomicLong(),rejected=new AtomicLong(),failedCallbacks=new AtomicLong();
    private final AtomicInteger highWater=new AtomicInteger();
    private volatile boolean failed,closing;
    ExperimentPublisher(KafkaTemplate<String,byte[]> kafka,String topic,int capacity,boolean sharded) {
        if(capacity<1||capacity>1000)throw new IllegalArgumentException("EXPERIMENT_CAPACITY");
        this.kafka=kafka;this.topic=topic;this.capacity=capacity;this.sharded=sharded;slots=new Semaphore(capacity);
    }
    synchronized void accept(List<ChatEvent> events,long sequence) {
        if(closing||failed||!slots.tryAcquire(events.size())) {rejected.addAndGet(events.size());failed=true;throw new IllegalStateException("EXPERIMENT_REJECTED");}
        List<byte[]> payloads;
        try { payloads=events.stream().map(codec::encode).toList(); }
        catch(RuntimeException error) {slots.release(events.size());rejected.addAndGet(events.size());failed=true;throw new IllegalStateException("EXPERIMENT_INVALID");}
        long first=accepted.getAndAdd(events.size());for(int i=0;i<events.size();i++)unacked.put(first+i,events.get(i));
        highWater.accumulateAndGet(capacity-slots.availablePermits(),Math::max);
        for(int i=0;i<events.size();i++) {
            var event=events.get(i);long token=first+i;String key=event.channelId()+(sharded?"/"+((sequence+i)%3):"");
            try {
                kafka.send(new ProducerRecord<>(topic,null,event.receivedAt().toEpochMilli(),key,payloads.get(i))).whenComplete((result,error)->{
                    if(error==null) {acknowledged.incrementAndGet();unacked.remove(token);slots.release();}
                    else {failedCallbacks.incrementAndGet();failed=true;}
                });
            } catch(RuntimeException error) {failedCallbacks.incrementAndGet();failed=true;}
        }
    }
    boolean failed() { return failed; }
    int pending() { return capacity-slots.availablePermits(); }
    double oldestAgeMillis() { Instant oldest=unacked.values().stream().map(ChatEvent::receivedAt).min(Comparator.naturalOrder()).orElse(null);return oldest==null?0:Math.max(0,Duration.between(oldest,Instant.now()).toNanos()/1_000_000d); }
    Map<String,Object> stats() { return Map.of("accepted",accepted.get(),"acknowledged",acknowledged.get(),"rejected",rejected.get(),"unconfirmed",accepted.get()-acknowledged.get(),"failedCallbacks",failedCallbacks.get(),"highWater",highWater.get(),"capacity",capacity,"complete",!failed&&accepted.get()==acknowledged.get()); }
    @Override public void close() {
        closing=true;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while(pending()>0&&!failed&&System.nanoTime()<deadline)try {Thread.sleep(10);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;}
    }
}
