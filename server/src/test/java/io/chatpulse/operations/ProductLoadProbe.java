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
        System.exit(probe.run(Plan.named(args[0]))?0:2);
    }
    private boolean run(Plan plan) throws Exception {
        result.put("plan",plan);result.put("seed",SEED);result.put("channels",CHANNELS);result.put("startedAt",Instant.now());result.put("samples",samples);
        result.put("inputContract",Map.of("duplicateEvery",100,"observationIntervalSeconds",30,"observationTtlSeconds",120,"outboxIntervalSeconds",30,
                "outboxSource","SYNTHETIC_SQL_SEED","sourceSessions","SYNTHETIC_COLLECTION_TELEMETRY","eventTime","FIXED_BASE_PLUS_SCHEDULE","sampleIntervalSeconds",5));
        var config=RuntimeEnvironment.from(System.getenv());var fixture=new ProductLoadFixture();
        var factory=KafkaSupport.producerFactory(config.bootstrap());var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
        var publisher=new BufferedKafkaPublisher(kafka,config.topic(),new EventCodec(),new BufferedKafkaPublisher.Limits(1000,500,64));
        var scheduler=Executors.newScheduledThreadPool(2);
        try(var ds=PostgresSupport.open(config.jdbcUrl(),config.user(),config.password(),false)) {
            var jdbc=new JdbcTemplate(ds);var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
            try(var telemetry=new CollectionTelemetry(jdbc,tx,publisher);var measurements=new ProductLoadMeasurements(config,jdbc,publisher,fixture.channels.get(0))) {
                Instant base=Instant.now().minusSeconds(2).truncatedTo(ChronoUnit.MICROS);result.put("eventBase",base);result.put("channelOrder",fixture.channels);
                for(String channel:fixture.channels) {telemetry.start(channel);telemetry.connected(channel,id("session/"+channel),base);}
                telemetry.start();startNanos=System.nanoTime();result.put("inputStartedAtEpochMillis",System.currentTimeMillis());
                scheduler.scheduleWithFixedDelay(()->{
                    try {
                        var sample=measurements.sample(phase,elapsed());
                        Files.writeString(output.resolveSibling("samples.jsonl"),JsonMapper.builder().build().writeValueAsString(sample)+"\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                        synchronized(this){samples.add(sample);checkpoint();}
                    } catch(Exception failure) {samplerFailure.compareAndSet(null,failure.getClass().getSimpleName());}
                },0,5,TimeUnit.SECONDS);
                scheduler.scheduleWithFixedDelay(()->{
                    try {
                        var sample=measurements.offsetSample(phase,elapsed());
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
                        for(String channel:fixture.channels)telemetry.alive(id("session/"+channel),Instant.now());
                        for(int tick=0;tick<10;tick++) {
                            long due=startNanos+elapsedSeconds*1_000_000_000L+tick*100_000_000L;pace(due);
                            maximumPacingDelay=Math.max(maximumPacingDelay,(System.nanoTime()-due)/1e6);
                            check(elapsed()<plan.seconds()+60,"GENERATOR_TOO_SLOW");publisher.checkHealthy();
                            var frame=new ArrayList<io.chatpulse.poc.ChatEvent>();var unique=new ArrayList<io.chatpulse.poc.ChatEvent>();
                            for(int i=0;i<stage.eventsPerSecond()/10;i++) {
                                var event=fixture.event(ordinal++,stage.hot(),base.plusSeconds(elapsedSeconds).plusMillis(tick*100L+i));
                                unique.add(event);frame.add(event);if(ordinal%100==0)frame.add(event);
                            }
                            publisher.accept(frame);physical+=frame.size();unique.forEach(fixture::accepted);
                        }
                    }
                    pace(startNanos+elapsedSeconds*1_000_000_000L);
                    synchronized(this){stages.add(Map.of("name",stage.name(),"uniqueEvents",ordinal-first,"actualSeconds",(System.nanoTime()-stageStart)/1e9,"plannedSeconds",stage.seconds()));checkpoint();}
                }
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
                phase="VERIFY";stopSampler(scheduler);
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
                finally {result.put("finishedAt",Instant.now());checkpoint();synchronized(this){write(output,result);}}
            }
        }
        System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("correct",result.get("correct"))));
        return Boolean.TRUE.equals(result.get("correct"));
    }
    private void seedSideInputs(KafkaTemplate<String,byte[]> kafka,JdbcTemplate jdbc,TransactionTemplate tx,RuntimeEnvironment c,ProductLoadFixture fixture,Instant at,int index) throws Exception {
        var futures=new ArrayList<CompletableFuture<?>>();
        for(int i=0;i<CHANNELS;i++) {
            String channel=fixture.channels.get(i);var event=new BroadcastObserved(1,id("observation/"+index+"/"+channel),c.runId(),channel,at,120,1000L+i,
                    BroadcastMetadata.LiveStatus.LIVE,PublicBroadcastObservation.Quality.OBSERVED,Long.toString(10000+i),BroadcastObserved.BroadcastIdentity.SOURCE_PROVIDED,fixture.categories.get(channel),"합성 분류");
            futures.add(kafka.send(new ProducerRecord<>(c.observationsTopic(),null,at.toEpochMilli(),channel,event.encode())));
            fixture.observations.put(event.eventId().toString(),channel+"|"+at);
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10,TimeUnit.SECONDS);
        UUID spike=id("spike/"+index);Instant evaluated=Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(),10)*10);
        tx.executeWithoutResult(status->{
            jdbc.update("INSERT INTO spike_event(spike_id,run_id,channel_id,evaluated_at,algorithm_version,count,baseline,ratio,quality_warning) VALUES (?,?,?,?,?,?,?,?,NULL)",spike,c.runId(),fixture.channels.get(index%CHANNELS),ts(evaluated),"chat-spike-v1",100,50,2);
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
