package io.chatpulse.operations;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.metadata.*;
import io.chatpulse.observations.BroadcastObserved;
import io.chatpulse.pipeline.*;
import io.chatpulse.quality.CollectionTelemetry;
import java.nio.file.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.operations.ProductLoadFixture.*;

/** 격리된 실제 제품 이미지에 합성 입력·원천 생존 관측을 공급하는 별도 JVM. */
public final class ProductLoadProbe {
    private final Map<String,Object> result=Collections.synchronizedMap(new LinkedHashMap<>());
    private final List<Map<String,Object>> samples=new ArrayList<>();
    private final java.util.concurrent.atomic.AtomicReference<String> samplerFailure=new java.util.concurrent.atomic.AtomicReference<>();
    private final Path output;
    private volatile String phase="INITIALIZE";
    private long startNanos;
    private ProductLoadProbe(Path output) {this.output=output;}
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        if(args.length==1&&args[0].equals("clock")){System.out.println(System.currentTimeMillis());return;}
        var probe=new ProductLoadProbe(Path.of(args[1]));
        System.exit(probe.run(ProductLoadPlan.read(args[0]))?0:2);
    }
    private boolean run(ProductLoadPlan plan) throws Exception {
        result.put("plan",plan);result.put("seed",SEED);result.put("channels",plan.channels());result.put("startedAt",Instant.now());result.put("samples",samples);
        result.put("inputContract",Map.of("duplicateEvery",100,"observationIntervalSeconds",30,"observationTtlSeconds",120,"outboxIntervalSeconds",30,
                "outboxSource","SYNTHETIC_SQL_SEED","sourceSessions",plan.websocket()?"PRODUCT_CHANNEL_PROBE":"SYNTHETIC_COLLECTION_TELEMETRY","eventTime","FIXED_BASE_PLUS_SCHEDULE","sampleIntervalSeconds",5));
        var config=RuntimeEnvironment.from(System.getenv());var fixture=new ProductLoadFixture(plan.channels());
        var factory=KafkaSupport.producerFactory(config.bootstrap());var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
        var publisher=new BufferedKafkaPublisher(kafka,config.topic(),new EventCodec(),new BufferedKafkaPublisher.Limits(1000,500,64));
        var scheduler=Executors.newScheduledThreadPool(2);
        var finalMetrics=new LinkedHashMap<String,java.util.function.Supplier<Map<String,Object>>>();
        var inputEpoch=new java.util.concurrent.atomic.AtomicLong();
        java.util.function.Supplier<String> measuredPhase=()->startNanos==0?"INITIALIZE":plan.phaseAtMillis((System.nanoTime()-startNanos)/1_000_000L);
        try(var ds=PostgresSupport.open(config.jdbcUrl(),config.user(),config.password(),false)) {
            var jdbc=new JdbcTemplate(ds);var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
            try(var telemetry=new CollectionTelemetry(jdbc,tx,publisher);var measurements=new ProductLoadMeasurements(config,jdbc,publisher,fixture.channels.get(0));
                    var visibility=new ProductLoadVisibility(jdbc,config,event->plan.phaseAtMillis(event.eventTime().toEpochMilli()+2000-inputEpoch.get()));var apiTraffic=new ProductLoadApiTraffic(plan,fixture.channels,measuredPhase);
                    var sockets=plan.websocket()?new ProductLoadWebSockets(plan,fixture,publisher,telemetry,visibility):null) {
                finalMetrics.put("apiTraffic",apiTraffic::snapshot);finalMetrics.put("commitVisibility",visibility::snapshot);
                if(sockets!=null)finalMetrics.put("collector",sockets::snapshot);
                telemetry.start();long startEpoch=System.currentTimeMillis();
                if(sockets!=null){sockets.connect();result.put("collectorStartup",sockets.snapshot());startEpoch=sockets.startSource();}
                inputEpoch.set(startEpoch);Instant base=Instant.ofEpochMilli(startEpoch).minusSeconds(2).truncatedTo(ChronoUnit.MICROS);result.put("eventBase",base);result.put("channelOrder",fixture.channels);
                if(sockets==null)for(String channel:fixture.channels) {telemetry.start(channel);telemetry.connected(channel,id("session/"+channel),base);}
                startNanos=System.nanoTime()+(startEpoch-System.currentTimeMillis())*1_000_000L;result.put("inputStartedAtEpochMillis",startEpoch);pace(startNanos);apiTraffic.start();
                scheduler.scheduleWithFixedDelay(()->{
                    try {
                        var sample=measurements.sample(measuredPhase.get(),elapsed());
                        Files.writeString(output.resolveSibling("samples.jsonl"),JsonMapper.builder().build().writeValueAsString(sample)+"\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                        synchronized(this){samples.add(sample);checkpoint();}
                    } catch(Exception failure) {samplerFailure.compareAndSet(null,failure.getClass().getSimpleName());}
                },0,5,TimeUnit.SECONDS);
                scheduler.scheduleWithFixedDelay(()->{
                    try {
                        var sample=measurements.offsetSample(measuredPhase.get(),elapsed());
                        Files.writeString(output.resolveSibling("offsets.jsonl"),JsonMapper.builder().build().writeValueAsString(sample)+"\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                        checkpoint();
                    } catch(Exception failure) {samplerFailure.compareAndSet(null,failure.getClass().getSimpleName());}
                },0,1,TimeUnit.SECONDS);
                long ordinal=0,physical=0;int elapsedSeconds=0;double maximumPacingDelay=0;
                var stages=new ArrayList<Map<String,Object>>();result.put("stages",stages);
                for(var stage:plan.stages()) {
                    phase=stage.name();long stageStart=System.nanoTime();long first=ordinal;
                    for(int second=0;second<stage.seconds();second++,elapsedSeconds++) {
                        pace(startNanos+elapsedSeconds*1_000_000_000L);
                        if(elapsedSeconds%30==0)seedSideInputs(kafka,jdbc,tx,config,fixture,base.plusSeconds(elapsedSeconds),elapsedSeconds/30);
                        if(sockets==null)for(String channel:fixture.channels)telemetry.alive(id("session/"+channel),Instant.now());
                        for(int tick=0;tick<10;tick++) {
                            long due=startNanos+elapsedSeconds*1_000_000_000L+tick*100_000_000L;pace(due);
                            maximumPacingDelay=Math.max(maximumPacingDelay,(System.nanoTime()-due)/1e6);
                            check(elapsed()<plan.seconds()+60,"GENERATOR_TOO_SLOW");publisher.checkHealthy();
                            if(sockets==null) {
                                var frame=new ArrayList<io.chatpulse.poc.ChatEvent>();var unique=new ArrayList<io.chatpulse.poc.ChatEvent>();
                                for(int i=0;i<stage.eventsPerSecond()/10;i++) {
                                    var event=fixture.event(ordinal++,stage.hot(),base.plusSeconds(elapsedSeconds).plusMillis(tick*100L));
                                    unique.add(event);frame.add(event);if(ordinal%100==0)frame.add(event);
                                }
                                publisher.accept(frame);physical+=frame.size();unique.forEach(event->{fixture.accepted(event);visibility.accepted(event);});
                            } else {
                                ordinal+=stage.eventsPerSecond()/10;
                                physical=ordinal+ordinal/100;
                            }
                        }
                    }
                    pace(startNanos+elapsedSeconds*1_000_000_000L);
                    synchronized(this){stages.add(Map.of("name",stage.name(),"uniqueEvents",ordinal-first,"actualSeconds",(System.nanoTime()-stageStart)/1e9,"plannedSeconds",stage.seconds()));checkpoint();}
                }
                if(sockets!=null){result.put("syntheticSource",sockets.reconcileSource());var collector=sockets.snapshot();result.put("collector",collector);
                    check(((Number)collector.get("connectedOnce")).intValue()==plan.channels()&&((Map<?,?>)collector.get("reasons")).isEmpty(),"COLLECTOR_HEALTH");}
                result.put("inputSeconds",elapsed());result.put("maximumPacingDelayMillis",maximumPacingDelay);
                result.put("expectedUnique",fixture.events.size());result.put("expectedPhysical",physical);result.put("expectedObservations",fixture.observations.size());result.put("expectedOutbox",fixture.spikes.size());
                check(ordinal==plan.uniqueEvents(),"INPUT_COUNT");
                var delivery=publisher.finish();result.put("publisherFinal",delivery);check(Boolean.TRUE.equals(delivery.get("complete")),"PUBLISH_INCOMPLETE");
                check(((Number)delivery.get("acknowledged")).longValue()==physical,"ACK_COUNT");
                phase="DRAIN";long drainStart=System.nanoTime();long deadline=drainStart+180_000_000_000L;
                boolean caughtUp=false;
                while(System.nanoTime()<deadline) {
                    var counts=measurements.counts();
                    caughtUp=counts.get(config.projection())==ordinal&&counts.get(config.projection()+"-trend")==ordinal
                            &&counts.get("category_event")==ordinal&&counts.get("report_event")==ordinal
                            &&counts.get("broadcast_observation")==fixture.observations.size()&&counts.get("notification_event")==fixture.spikes.size()&&counts.get("pendingOutbox")==0;
                    if(caughtUp&&measurements.partitions(false).stream().allMatch(p->p.get("lag") instanceof Number lag&&lag.longValue()==0))break;
                    caughtUp=false;Thread.sleep(1000);
                }
                result.put("drainSeconds",(System.nanoTime()-drainStart)/1e9);check(caughtUp,"DRAIN_TIMEOUT");
                phase="VERIFY";apiTraffic.close();visibility.close();stopSampler(scheduler);
                // 입력과 drain이 끝난 뒤 연결 버퍼를 해제해 최종 기대/실제 대조의 메모리와 겹치지 않게 한다.
                if(sockets!=null)sockets.close();
                result.put("apiTraffic",apiTraffic.snapshot());result.put("commitVisibility",visibility.snapshot());
                if(samplerFailure.get()!=null){result.put("samplerFailure",samplerFailure.get());throw new IllegalStateException("SAMPLER_FAILED");}
                result.put("finalSample",measurements.sample(phase,elapsed()));
                result.put("finalOffsets",measurements.partitions(false));
                result.put("reconciliation",fixture.verify(jdbc,config));
                result.put("receivedToTransactionTimestampMillis",measurements.transactionTimestampLatencies());
                result.put("healthyRankingEntries",jdbc.queryForObject("SELECT count(*) FROM ranking_entry WHERE status='HEALTHY'",Long.class));
                if(plan.name().equals("soak"))check(((Number)result.get("healthyRankingEntries")).longValue()>0,"NO_HEALTHY_RANKING");
                check(samples.stream().anyMatch(s->s.containsKey("analyzerHeapUsedBytes")),"ANALYZER_HEAP_UNOBSERVED");
                result.put("correct",true);
            }
        }catch(Exception failure) {
            result.put("correct",false);result.put("failureType",failure.getClass().getSimpleName());
            if(failure.getMessage()!=null&&failure.getMessage().matches("[A-Z_0-9]+"))result.put("failureCode",failure.getMessage());
        }finally {
            try {stopSampler(scheduler);}catch(Exception failure){result.put("correct",false);result.put("samplerStopFailure",failure.getClass().getSimpleName());}
            try {result.putIfAbsent("publisherFinal",publisher.finish());}
            finally {
                try {factory.destroy();}
                finally {finalMetrics.forEach((key,metric)->result.put(key,metric.get()));result.put("finishedAt",Instant.now());checkpoint();synchronized(this){write(output,result);}}
            }
        }
        System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("correct",result.get("correct"))));
        return Boolean.TRUE.equals(result.get("correct"));
    }
    private void seedSideInputs(KafkaTemplate<String,byte[]> kafka,JdbcTemplate jdbc,TransactionTemplate tx,RuntimeEnvironment c,ProductLoadFixture fixture,Instant at,int index) throws Exception {
        var futures=new ArrayList<CompletableFuture<?>>();
        for(int i=0;i<fixture.channels.size();i++) {
            String channel=fixture.channels.get(i);var event=new BroadcastObserved(1,id("observation/"+index+"/"+channel),c.runId(),channel,at,120,1000L+i,
                    BroadcastMetadata.LiveStatus.LIVE,PublicBroadcastObservation.Quality.OBSERVED,Long.toString(10000+i),BroadcastObserved.BroadcastIdentity.SOURCE_PROVIDED,fixture.categories.get(channel),"합성 분류");
            futures.add(kafka.send(new ProducerRecord<>(c.observationsTopic(),null,at.toEpochMilli(),channel,event.encode())));
            fixture.observations.put(event.eventId().toString(),channel+"|"+at);
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10,TimeUnit.SECONDS);
        UUID spike=id("spike/"+index);Instant evaluated=Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(),10)*10);
        tx.executeWithoutResult(status->{
            jdbc.update("INSERT INTO spike_event(spike_id,run_id,channel_id,evaluated_at,algorithm_version,count,baseline,ratio,quality_warning) VALUES (?,?,?,?,?,?,?,?,NULL)",spike,c.runId(),fixture.channels.get(index%fixture.channels.size()),ts(evaluated),"chat-spike-v1",100,50,2);
            jdbc.update("INSERT INTO spike_outbox(spike_id,run_id) VALUES (?,?)",spike,c.runId());
        });
        fixture.spikes.add(spike.toString());
    }
    private static void pace(long due) {long remaining;while((remaining=due-System.nanoTime())>0){LockSupport.parkNanos(Math.min(remaining,100_000_000));if(Thread.currentThread().isInterrupted())throw new IllegalStateException("GENERATOR_INTERRUPTED");}}
    private double elapsed(){return (System.nanoTime()-startNanos)/1e9;}
    private static void stopSampler(ScheduledExecutorService scheduler) throws InterruptedException {
        scheduler.shutdown();if(!scheduler.awaitTermination(45,TimeUnit.SECONDS)){scheduler.shutdownNow();throw new IllegalStateException("SAMPLER_STOP_TIMEOUT");}
    }
    private synchronized void checkpoint() {
        result.put("phase",phase);result.put("elapsedSeconds",startNanos==0?0:elapsed());
        write(output.resolveSibling("progress.json"),Map.of("plan",result.get("plan"),"phase",phase,"elapsedSeconds",result.get("elapsedSeconds")));
    }
    private void write(Path destination,Object value) {
        try {
            Path temp=destination.resolveSibling(destination.getFileName()+".tmp");
            synchronized(result){Files.writeString(temp,JsonMapper.builder().build().writeValueAsString(value));}
            Files.move(temp,destination,StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
        }catch(Exception failure){throw new IllegalStateException("LOAD_RESULT_WRITE_FAILED");}
    }
}
