package io.chatpulse.operations;

import io.chatpulse.pipeline.BufferedKafkaPublisher;
import io.chatpulse.poc.*;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.operations.ProductLoadFixture.check;

/** 실제 ChannelProbe/HTTP/프로토콜/발행 경로. 원천 생성 프로세스와 자원 계측을 분리한다. */
final class ProductLoadWebSockets implements AutoCloseable {
    private final ProductLoadPlan plan;
    private final ProductLoadFixture fixture;
    private final BufferedKafkaPublisher publisher;
    private final CollectionObserver observer;
    private final ProductLoadVisibility visibility;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final ExecutorService collectors;
    private final List<ProbeStats> stats=new ArrayList<>();
    private final List<Future<?>> tasks=new ArrayList<>();
    private final AtomicBoolean stop=new AtomicBoolean();
    private final AtomicLong received=new AtomicLong();
    private final ConcurrentMap<String,LongAdder> receivedByChannel=new ConcurrentHashMap<>();
    private final JsonMapper json=JsonMapper.builder().build();
    private double connectionSeconds;
    ProductLoadWebSockets(ProductLoadPlan plan,ProductLoadFixture fixture,BufferedKafkaPublisher publisher,CollectionObserver observer,ProductLoadVisibility visibility) {
        this.plan=plan;this.fixture=fixture;this.publisher=publisher;this.observer=observer;this.visibility=visibility;
        collectors=Executors.newFixedThreadPool(plan.channels());
    }
    void connect() throws Exception {
        long begin=System.nanoTime(),deadline=begin+90_000_000_000L;
        boolean ready=false;
        while(System.nanoTime()<deadline) {
            try{sourceSnapshot();ready=true;break;}catch(Exception ignored){Thread.sleep(200);}
        }
        check(ready,"SOURCE_NOT_READY");
        var source=ProductLoadHttpSource.local(URI.create("http://synthetic-source:18086"),URI.create("ws://synthetic-source:18085"));
        EventSink sink=new EventSink(){
            @Override public void checkHealthy(){publisher.checkHealthy();}
            @Override public void accept(List<ChatEvent> events){
                var frame=new ArrayList<ChatEvent>();
                for(var event:events){frame.add(event);if(received.incrementAndGet()%100==0)frame.add(event);}
                publisher.accept(frame);
                for(var event:events){fixture.accepted(event);visibility.accepted(event);receivedByChannel.computeIfAbsent(event.channelId(),ignored->new LongAdder()).increment();}
            }
        };
        for(String channel:fixture.channels) {
            var state=new ProbeStats(channel);stats.add(state);
            tasks.add(collectors.submit(()->new ChannelProbe(source,sink,observer).run(channel,Duration.ofSeconds(plan.seconds()+300),Duration.ZERO,state,stop)));
            Thread.sleep(20); // 연결 시작률 50개/s. 무제한 동시 handshake와 구분한다.
        }
        while(System.nanoTime()<deadline) {
            if(stats.stream().allMatch(s->((Number)s.snapshot(false).get("connections")).intValue()==1)) {connectionSeconds=(System.nanoTime()-begin)/1e9;return;}
            check(tasks.stream().noneMatch(Future::isDone),"COLLECTOR_EARLY_EXIT");Thread.sleep(100);
        }
        throw new IllegalStateException("COLLECTOR_CONNECT_TIMEOUT");
    }
    long startSource() throws Exception {
        long at=System.currentTimeMillis()+2000;
        var response=http.send(HttpRequest.newBuilder(URI.create("http://synthetic-source:18086/start")).timeout(Duration.ofSeconds(3)).POST(HttpRequest.BodyPublishers.ofString(Long.toString(at))).build(),HttpResponse.BodyHandlers.ofString());
        check(response.statusCode()==200,"SOURCE_START_REJECTED");return at;
    }
    @SuppressWarnings("unchecked") Map<String,Object> sourceSnapshot() throws Exception {
        var response=http.send(HttpRequest.newBuilder(URI.create("http://synthetic-source:18086/stats")).timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.ofString());
        check(response.statusCode()==200,"SOURCE_STATUS_FAILED");return json.readValue(response.body(),Map.class);
    }
    Map<String,Object> reconcileSource() throws Exception {
        long deadline=System.nanoTime()+15_000_000_000L;Map<String,Object> source=sourceSnapshot();
        while(System.nanoTime()<deadline&&(!Boolean.TRUE.equals(source.get("finished"))||fixture.uniqueCount()!=plan.uniqueEvents())) {publisher.checkHealthy();Thread.sleep(100);source=sourceSnapshot();}
        check("NONE".equals(source.get("failure"))&&Boolean.TRUE.equals(source.get("finished")),"SOURCE_NOT_COMPLETE");
        check(((Number)source.get("sent")).longValue()==plan.uniqueEvents()&&fixture.uniqueCount()==plan.uniqueEvents(),"SOURCE_RECEIVE_COUNT");
        var expected=(Map<?,?>)source.get("sentByChannel");
        check(expected.size()==receivedByChannel.size()&&expected.entrySet().stream().allMatch(e->receivedByChannel.containsKey(e.getKey())&&receivedByChannel.get(e.getKey()).sum()==((Number)e.getValue()).longValue()),"SOURCE_CHANNEL_COUNTS");
        check(((Number)source.get("protocolErrors")).longValue()==0&&((Number)source.get("duplicateConnections")).longValue()==0,"SOURCE_PROTOCOL_ERRORS");
        return source;
    }
    Map<String,Object> snapshot() {
        var reasons=new TreeMap<String,Long>();long pongs=0,frames=0,bytes=0;int connected=0;
        for(var state:stats){var value=state.snapshot(false);if(((Number)value.get("connections")).longValue()==1)connected++;
            pongs+=((Number)value.get("serverPongs")).longValue();frames+=((Number)value.get("frames")).longValue();bytes+=((Number)value.get("receivedFrameBytes")).longValue();
            ((Map<?,?>)value.get("reasons")).forEach((k,v)->reasons.merge(k.toString(),((Number)v).longValue(),Long::sum));}
        return Map.of("requestedChannels",plan.channels(),"connectedOnce",connected,"connectionSeconds",connectionSeconds,"connectionStartsPerSecond",50,"received",received.get(),"serverPongs",pongs,"frames",frames,"frameBytes",bytes,"reasons",reasons);
    }
    @Override public void close() throws InterruptedException {
        stop.set(true);collectors.shutdown();if(!collectors.awaitTermination(20,TimeUnit.SECONDS)){collectors.shutdownNow();collectors.awaitTermination(10,TimeUnit.SECONDS);}
        http.shutdownNow();http.awaitTermination(Duration.ofSeconds(3));
    }
}
