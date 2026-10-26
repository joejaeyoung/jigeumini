package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.kafka.core.KafkaTemplate;

/** 한 프로세스가 소유한 채널·발행기·임대 갱신의 수명을 관리한다. 상실 후 같은 프로세스에서 재획득하지 않는다. */
public final class CollectorWorker implements AutoCloseable {
    public record Limits(int capacity,Duration leaseTtl,Duration duration,int httpRequests,Duration httpInterval,
                         BufferedKafkaPublisher.Limits publisher) {
        public Limits {
            if(capacity<1||capacity>20||leaseTtl.compareTo(Duration.ofSeconds(6))<0||leaseTtl.compareTo(Duration.ofSeconds(60))>0
                    ||duration.compareTo(Duration.ofSeconds(1))<0||duration.compareTo(Duration.ofMinutes(30))>0||publisher==null)
                throw new IllegalArgumentException("FLEET_LIMITS");
        }
        public static Limits defaults(int capacity,Duration duration){return new Limits(capacity,Duration.ofSeconds(10),duration,500,Duration.ofMillis(500),new BufferedKafkaPublisher.Limits(1000,500,64));}
    }
    private record Active(CollectorLeases.Lease lease,ProbeStats stats,Future<?> task) { }
    private final UUID owner=UUID.randomUUID();private final CollectorLeases leases;private final SourceMode mode;private final Limits limits;
    private final Clock clock;
    private final LeaseAuthority authority=new LeaseAuthority();private final BudgetedChatSource source;
    private final BufferedKafkaPublisher publisher;private final CollectionObserver observer;
    private final AtomicBoolean stop=new AtomicBoolean();private final Set<String> connected=ConcurrentHashMap.newKeySet();
    private final Map<String,Active> active=new LinkedHashMap<>();private final Object lock=new Object();
    private final ExecutorService connections=Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeat=Executors.newSingleThreadScheduledExecutor();
    private int consecutiveContentions,leaseContentionRetries;
    private boolean closing,closed,persisted;private volatile String failureCode="NONE";private Map<String,Object> finalReport;private Instant startedAt;
    public CollectorWorker(CollectorLeases leases,SourceMode mode,Limits limits,ChatSource source,KafkaTemplate<String,byte[]> kafka,String topic,
                           CollectionObserver observer) {
        this(leases,mode,limits,source,kafka,topic,ignored->observer);
    }
    public CollectorWorker(CollectorLeases leases,SourceMode mode,Limits limits,ChatSource source,KafkaTemplate<String,byte[]> kafka,String topic,
                           java.util.function.Function<BufferedKafkaPublisher,CollectionObserver> observerFactory) {
        this(leases,mode,limits,source,kafka,topic,(publisher,budget)->observerFactory.apply(publisher));
    }
    public CollectorWorker(CollectorLeases leases,SourceMode mode,Limits limits,ChatSource source,KafkaTemplate<String,byte[]> kafka,String topic,
                           java.util.function.BiFunction<BufferedKafkaPublisher,BudgetedChatSource,CollectionObserver> observerFactory) {
        this(leases,mode,limits,source,kafka,topic,observerFactory,Clock.systemUTC());
    }
    // 벽시계는 수신·보고 시각에만 사용하며 소유권 기한은 DB 시각과 nanoTime으로 판정한다.
    public CollectorWorker(CollectorLeases leases,SourceMode mode,Limits limits,ChatSource source,KafkaTemplate<String,byte[]> kafka,String topic,
                           java.util.function.BiFunction<BufferedKafkaPublisher,BudgetedChatSource,CollectionObserver> observerFactory,Clock clock) {
        this.clock=Objects.requireNonNull(clock);
        this.leases=leases;this.mode=mode;this.limits=limits;
        if(mode==SourceMode.OFFICIAL_AUTHORIZED||mode==SourceMode.UNOFFICIAL_POC&&limits.capacity()>3)throw new IllegalArgumentException("FLEET_SOURCE_MODE");
        // producer 생성/metadata 대기는 수신 큐를 차지하거나 임대 시간을 소비하기 전에 끝낸다.
        // 이후 broker 장애에는 기존 유한 버퍼와 실패 종료 계약을 그대로 적용한다.
        awaitKafkaMetadata(kafka,topic);
        this.source=new BudgetedChatSource(source,authority,limits.httpRequests(),limits.httpInterval());
        publisher=new BufferedKafkaPublisher(kafka,topic,new EventCodec(),limits.publisher(),authority);
        try {this.observer=observerFactory.apply(publisher,this.source);}
        catch(RuntimeException|Error failure){publisher.close();throw failure;}
    }
    /** 임대·원천 수신 전에만 metadata timeout을 최대5회 시도한다. 실행 중 발행 한도는 바꾸지 않는다. */
    static void awaitKafkaMetadata(KafkaTemplate<String,byte[]> kafka,String topic) {
        for(int attempt=1;attempt<=5;attempt++) {
            try {
                if(kafka.partitionsFor(topic).isEmpty())throw new IllegalStateException("FLEET_KAFKA_NOT_READY");
                return;
            } catch(org.apache.kafka.common.errors.TimeoutException pending) {
                if(attempt==5)throw new IllegalStateException("FLEET_KAFKA_NOT_READY",pending);
            } catch(RuntimeException failure){throw new IllegalStateException("FLEET_KAFKA_NOT_READY",failure);}
        }
    }
    public UUID owner(){return owner;}
    public Map<String,Object> run() {return run(ignored->{ });}
    public Map<String,Object> run(java.util.function.Consumer<Map<String,Object>> persist) {
        startedAt=clock.instant();
        var hook=new Thread(()->{close();persistResult(persist);},"collector-graceful-stop");Runtime.getRuntime().addShutdownHook(hook);
        long deadline=System.nanoTime()+limits.duration().toNanos();
        try {
            tick();heartbeat.scheduleWithFixedDelay(this::safeTick,1,1,TimeUnit.SECONDS);
            while(!stop.get()&&System.nanoTime()<deadline) {
                authority.checkAll();publisher.checkHealthy();Thread.sleep(100);
                synchronized(lock){if(!stop.get()&&System.nanoTime()<deadline&&active.values().stream().anyMatch(a->a.task().isDone())){failureCode="CHANNEL_ENDED";stop.set(true);}}
            }
        } catch(InterruptedException interrupted){Thread.currentThread().interrupt();fail("WORKER_INTERRUPTED");}
        catch(Exception failure){fail("COLLECTION_LEASE_OR_DELIVERY_LOST");}
        finally {
            close();
            try {persistResult(persist);}
            finally {try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){}}
        }
        return finalReport;
    }
    // SIGTERM hook은 main thread의 파일 쓰기 완료까지 기다린다. CAS만으로 중복을 막으면
    // main이 쓰기 권한만 얻고 hook이 먼저 반환하여 JVM이 파일 저장 전에 종료될 수 있다.
    private synchronized void persistResult(java.util.function.Consumer<Map<String,Object>> persist) {
        if(!persisted){persist.accept(finalReport);persisted=true;}
    }
    private void safeTick(){try{tick();}catch(Exception failure){fail("COLLECTION_LEASE_LOST");}}
    private void tick() {
        synchronized(lock) {
            if(closed)return;authority.checkAll();long began=System.nanoTime();
            var held=held();CollectorLeases.Grant grant;
            try {grant=leases.tick(owner,limits.capacity(),limits.leaseTtl(),mode,held,Set.copyOf(connected),!closing);}
            catch(CollectorLeases.PlanContended contention) {
                // 이미 보유한 epoch의 실패는 재획득하지 않는다. 미배정 상태만 다음 1초 tick을 기다린다.
                if(!held.isEmpty()||closing||stop.get()||++consecutiveContentions>5)throw contention;
                leaseContentionRetries++;return;
            }
            consecutiveContentions=0;
            authority.update(grant,began);
            if(closing)return;
            for(var lease:grant.leases())if(!active.containsKey(lease.channelId())) {
                var stats=new ProbeStats(lease.channelId(),clock);String channel=lease.channelId();
                EventSink guarded=new EventSink(){
                    public void accept(List<ChatEvent> events){authority.acquire(channel).check();publisher.accept(events);}
                    public void checkHealthy(){if(stop.get())throw new SourceFailure("COLLECTOR_STOPPING",true);authority.acquire(channel).check();publisher.checkHealthy();}
                };
                CollectionObserver tracking=CollectionObserver.combine(List.of(observer,new CollectionObserver(){
                    public void connected(String id,UUID session,Instant at){connected.add(id);}
                    public void stopped(String id){connected.remove(id);}
                    public void state(String id,String state){if(!state.equals("CONNECTED"))connected.remove(id);}
                    public void ended(UUID session,Instant at,String reason){connected.remove(channel);}
                }));
                var task=connections.submit(()->new ChannelProbe(source,guarded,tracking,clock).run(channel,limits.duration(),Duration.ZERO,stats,stop));
                active.put(channel,new Active(lease,stats,task));
            }
        }
    }
    private Map<String,Long> held(){var held=new LinkedHashMap<String,Long>();active.forEach((c,a)->held.put(c,a.lease().epoch()));return held;}
    private void fail(String code){if(failureCode.equals("NONE"))failureCode=code;authority.revoke();stop.set(true);}
    public void requestStop(){stop.set(true);}
    @Override public void close() {
        synchronized(this) {
            if(closed)return;
            synchronized(lock){closing=true;stop.set(true);}
            connections.shutdown();
            try{if(!connections.awaitTermination(15,TimeUnit.SECONDS)){connections.shutdownNow();fail("SOURCE_STOP_TIMEOUT");}}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();connections.shutdownNow();fail("WORKER_INTERRUPTED");}
            // 갱신은 수신 종료와 최대10초 drain 동안 계속된다. 만료/DB 실패는 permit을 즉시 취소한다.
            var delivery=publisher.finish();
            heartbeat.shutdown();try{if(!heartbeat.awaitTermination(12,TimeUnit.SECONDS))heartbeat.shutdownNow();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
            boolean released=false;Map<String,Long> held;synchronized(lock){held=held();closed=true;}
            authority.revoke();try{leases.release(owner,held,!failureCode.equals("NONE"));released=true;}catch(Exception failure){if(failureCode.equals("NONE"))failureCode="LEASE_RELEASE_FAILED";}
            var result=new LinkedHashMap<String,Object>();result.put("workerId",owner);result.put("sourceMode",mode);result.put("failureCode",failureCode);
            result.put("leaseContentionRetries",leaseContentionRetries);
            result.put("delivery",delivery);result.put("released",released);result.put("httpRequests",source.requests());result.put("completedAt",clock.instant());result.put("startedAt",startedAt==null?clock.instant():startedAt);
            result.put("leases",active.values().stream().map(a->Map.of("channelId",a.lease().channelId(),"epoch",a.lease().epoch(),"policyVersion",a.lease().policyVersion(),"stats",a.stats().snapshot(false))).toList());
            result.put("complete",failureCode.equals("NONE")&&Boolean.TRUE.equals(delivery.get("complete"))&&released);finalReport=Map.copyOf(result);
        }
    }
}
