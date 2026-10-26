package io.chatpulse.operations;

import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import org.springframework.jdbc.core.JdbcTemplate;

/** 수신 시각부터 독립 DB 질의에서 commit된 ledger가 처음 보인 시각까지의 표본 상한. */
final class ProductLoadVisibility implements AutoCloseable {
    record Pending(UUID id,Instant receivedAt,String phase,int partition,String channel) { }
    static final class SampleWindow {
        private final SplittableRandom random=new SplittableRandom(ProductLoadFixture.SEED);
        private long bucket=Long.MIN_VALUE,seen;
        private Pending selected;
        synchronized Pending offer(Pending candidate,long nanos) {
            long next=nanos/100_000_000L;Pending completed=null;
            if(bucket!=next){completed=selected;selected=null;seen=0;bucket=next;}
            if(random.nextLong(++seen)==0)selected=candidate;
            return completed;
        }
        synchronized Pending expired(long nanos) {
            if(bucket==nanos/100_000_000L)return null;
            Pending completed=selected;selected=null;seen=0;return completed;
        }
    }
    private static final class Phase {
        final LongAdder sampled=new LongAdder(),timedOut=new LongAdder(),queryErrors=new LongAdder(),notQueued=new LongAdder();
        final ProductLoadLatency latency=new ProductLoadLatency();
        final Map<Integer,LongAdder> partitions=new ConcurrentHashMap<>();
        final Map<Integer,ProductLoadLatency> partitionLatency=new ConcurrentHashMap<>();
        final Set<String> channels=ConcurrentHashMap.newKeySet();
        final Set<Integer> inputPartitions=ConcurrentHashMap.newKeySet();
        Map<String,Object> snapshot(){return Map.of("sampled",sampled.sum(),"notVisibleWithin10Seconds",timedOut.sum(),"queryErrors",queryErrors.sum(),"notQueued",notQueued.sum(),"latency",latency.snapshot(),
                "sampledChannels",channels.size(),"sampledByPartition",partitions.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().sum())),
                "latencyByPartition",partitionLatency.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().snapshot())),"inputPartitions",inputPartitions);}
    }
    private final JdbcTemplate jdbc;
    private final RuntimeEnvironment config;
    private final Function<ChatEvent,String> phase;
    private final SampleWindow window=new SampleWindow();
    private final Queue<Pending> pending=new ArrayBlockingQueue<>(256);
    private final Map<String,Phase> phases=new ConcurrentHashMap<>();
    private final ScheduledExecutorService sampler=Executors.newSingleThreadScheduledExecutor();
    ProductLoadVisibility(JdbcTemplate jdbc,RuntimeEnvironment config,Function<ChatEvent,String> phase) {
        this.jdbc=jdbc;this.config=config;this.phase=phase;
        sampler.scheduleWithFixedDelay(this::poll,200,200,TimeUnit.MILLISECONDS);
    }
    void accepted(ChatEvent event) {
        int partition=org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(event.channelId().getBytes(java.nio.charset.StandardCharsets.UTF_8)))%3;
        String name=phase.apply(event);phases.computeIfAbsent(name,ignored->new Phase()).inputPartitions.add(partition);
        queue(window.offer(new Pending(event.eventId(),event.receivedAt(),name,partition,event.channelId()),System.nanoTime()));
    }
    private void queue(Pending value) {
        if(value==null)return;
        var group=phases.computeIfAbsent(value.phase(),ignored->new Phase());group.sampled.increment();group.channels.add(value.channel());
        group.partitions.computeIfAbsent(value.partition(),ignored->new LongAdder()).increment();
        if(!pending.offer(value))group.notQueued.increment();
    }
    private void poll() {
        queue(window.expired(System.nanoTime()));
        var values=new ArrayList<>(pending);if(values.isEmpty())return;
        var args=new ArrayList<Object>();args.add(config.projection());args.add(config.runId());values.forEach(v->args.add(v.id()));
        try {
            String parameters=String.join(",",Collections.nCopies(values.size(),"?"));
            var visible=new HashSet<>(jdbc.queryForList("SELECT event_id FROM processed_event WHERE projection=? AND run_id=? AND event_id IN ("+parameters+")",UUID.class,args.toArray()));
            Instant now=Instant.now();
            for(var value:values) {
                double millis=Duration.between(value.receivedAt(),now).toNanos()/1e6;
                var group=phases.get(value.phase());
                if(visible.contains(value.id())){group.latency.add(Math.max(0,millis));group.partitionLatency.computeIfAbsent(value.partition(),ignored->new ProductLoadLatency()).add(Math.max(0,millis));pending.remove(value);}
                else if(millis>=10000){group.timedOut.increment();pending.remove(value);}
            }
        }catch(RuntimeException failure){values.stream().map(Pending::phase).distinct().forEach(name->phases.get(name).queryErrors.increment());}
        // DB가 계속 실패해도 큐는 256개, 응답을 받을 때 적용하는 표본 기한은 10초다.
        Instant cutoff=Instant.now().minusSeconds(10);
        for(var value:values)if(value.receivedAt().isBefore(cutoff)&&pending.remove(value))phases.get(value.phase()).timedOut.increment();
    }
    Map<String,Object> snapshot() {
        return Map.of("metric","RECEIVED_TO_COMMITTED_RANKING_LEDGER_VISIBLE_UPPER_BOUND","selection","ONE_UNIFORM_RESERVOIR_SAMPLE_PER_100MS_WINDOW","maximumSamplesPerSecond",10,"pollDelayMillis",200,"pendingSamples",pending.size(),
                "phases",phases.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().snapshot())));
    }
    @Override public void close() throws InterruptedException {
        queue(window.expired(Long.MAX_VALUE));
        long deadline=System.nanoTime()+12_000_000_000L;
        while(!pending.isEmpty()&&System.nanoTime()<deadline)Thread.sleep(100);
        sampler.shutdown();if(!sampler.awaitTermination(10,TimeUnit.SECONDS))sampler.shutdownNow();
    }
}
