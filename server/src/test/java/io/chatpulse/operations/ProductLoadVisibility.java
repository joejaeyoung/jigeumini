package io.chatpulse.operations;

import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;

/** 수신 시각부터 독립 DB 질의에서 commit된 ledger가 처음 보인 시각까지의 표본 상한. */
final class ProductLoadVisibility implements AutoCloseable {
    private record Pending(UUID id,Instant receivedAt,String phase) { }
    private static final class Phase {
        final LongAdder sampled=new LongAdder(),timedOut=new LongAdder(),queryErrors=new LongAdder(),notQueued=new LongAdder();
        final ProductLoadLatency latency=new ProductLoadLatency();
        Map<String,Object> snapshot(){return Map.of("sampled",sampled.sum(),"notVisibleWithin10Seconds",timedOut.sum(),"queryErrors",queryErrors.sum(),"notQueued",notQueued.sum(),"latency",latency.snapshot());}
    }
    private final JdbcTemplate jdbc;
    private final RuntimeEnvironment config;
    private final Supplier<String> phase;
    private final AtomicLong nextSample=new AtomicLong();
    private final Queue<Pending> pending=new ArrayBlockingQueue<>(256);
    private final Map<String,Phase> phases=new ConcurrentHashMap<>();
    private final ScheduledExecutorService sampler=Executors.newSingleThreadScheduledExecutor();
    ProductLoadVisibility(JdbcTemplate jdbc,RuntimeEnvironment config,Supplier<String> phase) {
        this.jdbc=jdbc;this.config=config;this.phase=phase;
        sampler.scheduleWithFixedDelay(this::poll,200,200,TimeUnit.MILLISECONDS);
    }
    void accepted(ChatEvent event) {
        long now=System.nanoTime(),next=nextSample.get();
        if(now<next || !nextSample.compareAndSet(next,now+100_000_000L))return;
        String name=phase.get();phases.computeIfAbsent(name,ignored->new Phase()).sampled.increment();
        if(!pending.offer(new Pending(event.eventId(),event.receivedAt(),name)))phases.get(name).notQueued.increment();
    }
    private void poll() {
        var values=new ArrayList<>(pending);if(values.isEmpty())return;
        var args=new ArrayList<Object>();args.add(config.projection());args.add(config.runId());values.forEach(v->args.add(v.id()));
        try {
            String parameters=String.join(",",Collections.nCopies(values.size(),"?"));
            var visible=new HashSet<>(jdbc.queryForList("SELECT event_id FROM processed_event WHERE projection=? AND run_id=? AND event_id IN ("+parameters+")",UUID.class,args.toArray()));
            Instant now=Instant.now();
            for(var value:values) {
                double millis=Duration.between(value.receivedAt(),now).toNanos()/1e6;
                var group=phases.get(value.phase());
                if(visible.contains(value.id())){group.latency.add(Math.max(0,millis));pending.remove(value);}
                else if(millis>=10000){group.timedOut.increment();pending.remove(value);}
            }
        }catch(RuntimeException failure){values.stream().map(Pending::phase).distinct().forEach(name->phases.get(name).queryErrors.increment());}
        // DB가 계속 실패해도 큐는 256개, 응답을 받을 때 적용하는 표본 기한은 10초다.
        Instant cutoff=Instant.now().minusSeconds(10);
        for(var value:values)if(value.receivedAt().isBefore(cutoff)&&pending.remove(value))phases.get(value.phase()).timedOut.increment();
    }
    Map<String,Object> snapshot() {
        return Map.of("metric","RECEIVED_TO_COMMITTED_RANKING_LEDGER_VISIBLE_UPPER_BOUND","maximumSamplesPerSecond",10,"pollDelayMillis",200,"pendingSamples",pending.size(),
                "phases",phases.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().snapshot())));
    }
    @Override public void close() throws InterruptedException {
        long deadline=System.nanoTime()+12_000_000_000L;
        while(!pending.isEmpty()&&System.nanoTime()<deadline)Thread.sleep(100);
        sampler.shutdown();if(!sampler.awaitTermination(10,TimeUnit.SECONDS))sampler.shutdownNow();
    }
}
