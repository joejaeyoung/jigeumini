package io.chatpulse.operations;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

/** 응답 속도와 독립된 도착률. 실행하지 못한 요청도 계수해 느려진 입력을 성공으로 숨기지 않는다. */
final class ProductLoadApiTraffic implements AutoCloseable {
    private static final class Phase {
        final LongAdder offered=new LongAdder(),issued=new LongAdder(),ok=new LongAdder(),failed=new LongAdder(),dropped=new LongAdder();
        final ProductLoadLatency latency=new ProductLoadLatency(),scheduleDelay=new ProductLoadLatency();
        final Map<String,LongAdder> statuses=new ConcurrentHashMap<>();
        Map<String,Object> snapshot(){return Map.of("offered",offered.sum(),"issued",issued.sum(),"successful",ok.sum(),"failed",failed.sum(),"notIssuedConcurrentLimit",dropped.sum(),"latency",latency.snapshot(),"scheduleDelay",scheduleDelay.snapshot(),"statuses",statuses.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().sum())));}
    }
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor();
    private final Map<String,Phase> phases=new ConcurrentHashMap<>();
    private final Semaphore permits;
    private final List<String> channels;
    private final Supplier<String> phase;
    private final int rate,maximum;
    private final String base;
    private final AtomicInteger peak=new AtomicInteger();
    private final Set<CompletableFuture<?>> inFlight=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    private long ordinal,startNanos;
    ProductLoadApiTraffic(ProductLoadPlan plan,List<String> channels,Supplier<String> phase) {
        this(plan,channels,phase,"http://web:8080");
    }
    ProductLoadApiTraffic(ProductLoadPlan plan,List<String> channels,Supplier<String> phase,String base) {
        rate=plan.apiRequestsPerSecond();maximum=plan.apiMaxInFlight();permits=new Semaphore(maximum);
        this.channels=channels;this.phase=phase;this.base=base;
    }
    void start() {
        startNanos=System.nanoTime();
        if(rate>0)scheduler.scheduleAtFixedRate(this::issue,0,1_000_000_000L/rate,TimeUnit.NANOSECONDS);
    }
    private void issue() {
        long index=ordinal++;var sample=phases.computeIfAbsent(phase.get(),ignored->new Phase());sample.offered.increment();
        sample.scheduleDelay.add(Math.max(0,(System.nanoTime()-(startNanos+index*(1_000_000_000L/rate)))/1e6));
        if(!permits.tryAcquire()){sample.dropped.increment();return;}
        sample.issued.increment();peak.accumulateAndGet(maximum-permits.availablePermits(),Math::max);
        String channel=channels.get((int)(index/7%channels.size()));
        String path=switch((int)(index%7)) {
            case 0 -> "/api/v1/rankings?window=60&limit=20";
            case 1 -> "/api/v1/spikes";
            case 2 -> "/api/v1/categories";
            case 3 -> "/api/v1/channels/"+channel+"/timeline";
            case 4 -> "/api/v1/channels/"+channel+"/reports";
            case 5 -> "/api/v1/notifications?channels="+channel;
            default -> "/api/channels?ids="+channel;
        };
        long begin=System.nanoTime();
        try {
            var completion=http.sendAsync(HttpRequest.newBuilder(URI.create(base+path)).timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.discarding())
                    .handle((response,error)->{
                        sample.latency.add((System.nanoTime()-begin)/1e6);
                        String status=error==null?Integer.toString(response.statusCode()):"TRANSPORT_ERROR";
                        sample.statuses.computeIfAbsent(status,ignored->new LongAdder()).increment();
                        if(error==null&&response.statusCode()==200)sample.ok.increment();else sample.failed.increment();
                        permits.release();
                        return null;
                    });
            inFlight.add(completion);completion.whenComplete((ignored,error)->inFlight.remove(completion));
        }catch(RuntimeException failure){sample.failed.increment();sample.statuses.computeIfAbsent("ISSUE_ERROR",ignored->new LongAdder()).increment();permits.release();}
    }
    Map<String,Object> snapshot() {
        return Map.of("targetRequestsPerSecond",rate,"maxInFlight",maximum,"peakInFlight",peak.get(),"phases",phases.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().snapshot())));
    }
    @Override public void close() throws InterruptedException {
        if(!closed.compareAndSet(false,true))return;
        scheduler.shutdown();if(!scheduler.awaitTermination(5,TimeUnit.SECONDS))scheduler.shutdownNow();
        try {CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).get(5,TimeUnit.SECONDS);}
        catch(ExecutionException|TimeoutException failure){throw new IllegalStateException("API_COMPLETION_TIMEOUT",failure);}
        finally {
        http.shutdown();if(!http.awaitTermination(Duration.ofSeconds(5))){http.shutdownNow();http.awaitTermination(Duration.ofSeconds(5));}
        }
    }
}
