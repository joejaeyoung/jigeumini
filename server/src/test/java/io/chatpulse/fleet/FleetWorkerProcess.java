package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 실제 JVM fixture. loopback 원천과 고정 진단 코드만 사용한다. */
public final class FleetWorkerProcess {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public static void main(String[] args) throws Exception {
        var diagnostic=new StartupDiagnostic(Path.of(args[4]+".diagnostic.json"));
        try {execute(args,diagnostic);}catch(Exception|LinkageError failure){diagnostic.failed(failure);throw failure;}
    }
    private static void execute(String[] args,StartupDiagnostic diagnostic) throws Exception {
        diagnostic.phase("STARTING");
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        Path report=Path.of(args[4]);String mode=args.length>5?args[5]:"NORMAL";
        var clock=new AdjustableClock(args.length>6?Long.parseLong(args[6]):0);
        var factory=KafkaSupport.producerFactory(args.length>7?args[7]:"localhost:19092");
        var producerProbe=new FleetProducerProbe();factory.addPostProcessor(producerProbe::wrap);
        boolean delayedReport=mode.equals("DELAY_REPORT");
        if(mode.equals("DELAY_PRODUCER"))factory.addPostProcessor(producer->{
            try {diagnostic.phase("PRODUCER_STARTING");Files.writeString(Path.of(report+".producer-starting"),"PRODUCER_STARTING",StandardOpenOption.CREATE_NEW);
                Thread.sleep(3000);long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while(!Files.exists(Path.of(report+".producer-release"))){if(System.nanoTime()>deadline)throw new IllegalStateException("SYNTHETIC_RELEASE_TIMEOUT");Thread.sleep(20);}
                Files.writeString(Path.of(report+".producer-ready"),"PRODUCER_READY",StandardOpenOption.CREATE_NEW);return producer;}
            catch(Exception failure){producer.close(Duration.ZERO);throw new IllegalStateException("SYNTHETIC_PRODUCER_DELAY_FAILED");}
        });
        factory.addPostProcessor(producer->{
            @SuppressWarnings("unchecked") var observed=(org.apache.kafka.clients.producer.Producer<String,byte[]>)java.lang.reflect.Proxy.newProxyInstance(
                    FleetWorkerProcess.class.getClassLoader(),new Class<?>[]{org.apache.kafka.clients.producer.Producer.class},(proxy,method,values)->{
                        boolean metadata=method.getName().equals("partitionsFor");
                        if(metadata)diagnostic.phase("METADATA_REQUEST");
                        try {var value=method.invoke(producer,values);if(metadata)diagnostic.phase("METADATA_READY");return value;}
                        catch(java.lang.reflect.InvocationTargetException failure){
                            if(metadata)diagnostic.phase(failure.getCause() instanceof org.apache.kafka.common.errors.TimeoutException?"METADATA_TIMEOUT":"METADATA_FAILED");
                            throw failure.getCause();
                        }
                    });return observed;
        });
        try(var ds=PostgresSupport.open(args[0],"chatpulse","local-chatpulse",false)) {
            diagnostic.phase("DATABASE_OPEN");
            var source=new ChatSource(){public String resolveRoom(String channel){return channel;}public String readToken(String room){return "SYNTHETIC_TOKEN";}public URI socketUri(String room){return URI.create("ws://127.0.0.1:"+args[2]);}};
            var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
            diagnostic.phase("KAFKA_TEMPLATE_READY");
            var invalidMessages=new AtomicLong();
            CollectionObserver fixtureObserver=new CollectionObserver(){public void loss(String channel,Instant from,Instant to,String reason){
                if(reason.equals("INVALID_MESSAGE"))try {atomic(Path.of(report+".invalid-observed.json"),Map.of("invalidFrames",invalidMessages.incrementAndGet(),"workerInstant",clock.instant().toString()));}
                catch(Exception failure){throw new IllegalStateException("FIXTURE_OBSERVATION_FAILED");}
            }};
            var limits=new CollectorWorker.Limits(Integer.parseInt(args[3]),Duration.ofSeconds(6),Duration.ofSeconds(delayedReport?1:90),200,Duration.ofMillis(100),new BufferedKafkaPublisher.Limits(30,20,4));
            diagnostic.phase("WORKER_CONSTRUCTING");
            try(var worker=new CollectorWorker(new CollectorLeases(mode.equals("PLAN_LOCK_GATE")?new PlanLockGate(ds,report):ds),SourceMode.SYNTHETIC,limits,source,kafka,args[1],(publisher,budget)->fixtureObserver,clock);
                var control=new ClockControl(report,clock,worker::requestStop)) {
                atomic(Path.of(report+".ready.json"),Map.of("workerId",worker.owner().toString(),"clockOffsetSeconds",clock.offset.get()));
                diagnostic.phase("WORKER_READY");
                var written=new AtomicBoolean();worker.run(result->{if(written.compareAndSet(false,true))try {
                    diagnostic.phase("PERSISTING");
                    if(delayedReport){Files.writeString(Path.of(report+".persisting"),"REPORT_WRITE_STARTED",StandardOpenOption.CREATE_NEW);Thread.sleep(5000);}
                    var recorded=new LinkedHashMap<>(result);recorded.put("fixtureClockOffsetSeconds",clock.offset.get());recorded.put("producerProbe",producerProbe.snapshot());
                    Files.writeString(report,JSON.writeValueAsString(recorded),StandardOpenOption.CREATE_NEW);diagnostic.phase("REPORT_COMPLETE");
                }catch(Exception failure){throw new IllegalStateException("FLEET_RESULT_FAILED");}});
            }
        }finally{factory.destroy();}
    }
    private static final class AdjustableClock extends Clock {
        private final AtomicLong offset;private final ZoneId zone;
        AdjustableClock(long value){this(new AtomicLong(),ZoneOffset.UTC);set(value);}
        private AdjustableClock(AtomicLong offset,ZoneId zone){this.offset=offset;this.zone=zone;}
        void set(long value){if(value!=0&&value!=300&&value!=-300)throw new IllegalArgumentException("CLOCK_OFFSET_NOT_ALLOWED");offset.set(value);}
        public ZoneId getZone(){return zone;}
        public Clock withZone(ZoneId zone){return new AdjustableClock(offset,Objects.requireNonNull(zone));}
        public Instant instant(){return Instant.now().plusSeconds(offset.get());}
    }
    private static final class ClockControl implements AutoCloseable {
        private final ScheduledExecutorService task=Executors.newSingleThreadScheduledExecutor();private long last=-1;
        ClockControl(Path report,AdjustableClock clock,Runnable stop) {
            task.scheduleWithFixedDelay(()->{
                Path command=Path.of(report+".clock-command.json");if(!Files.exists(command))return;
                try {
                    if(Files.size(command)>1024)throw new IllegalArgumentException("CLOCK_COMMAND_TOO_LARGE");
                    var value=JSON.readTree(Files.readString(command));long sequence=value.path("sequence").asLong(-1);
                    if(sequence<=last)return;clock.set(value.path("offsetSeconds").asLong(Long.MIN_VALUE));last=sequence;
                    atomic(Path.of(report+".clock-applied.json"),Map.of("sequence",sequence,"offsetSeconds",clock.offset.get(),"appliedAt",Instant.now().toString(),"workerInstant",clock.instant().toString()));
                }catch(Exception failure){try{atomic(Path.of(report+".clock-error.json"),Map.of("code","CLOCK_CONTROL_FAILED","type",failure.getClass().getSimpleName()));}catch(Exception ignored){}stop.run();}
            },0,50,TimeUnit.MILLISECONDS);
        }
        public void close() throws Exception {task.shutdownNow();if(!task.awaitTermination(2,TimeUnit.SECONDS))throw new IllegalStateException("CLOCK_CONTROL_STOP_TIMEOUT");}
    }
    private static void atomic(Path path,Object value) throws Exception {
        Path temporary=Path.of(path+".tmp");Files.writeString(temporary,JSON.writeValueAsString(value));Files.move(temporary,path,StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
    }
    /** 원문 예외 메시지는 저장하지 않고 단계·경과 시간·타입·고정 코드만 남긴다. */
    private static final class StartupDiagnostic {
        private static final Set<String> CODES=Set.of("FLEET_KAFKA_NOT_READY","FLEET_RESULT_FAILED","SYNTHETIC_PRODUCER_DELAY_FAILED","SYNTHETIC_RELEASE_TIMEOUT");
        private final Path path;private final long started=System.nanoTime();private final List<Map<String,Object>> phases=new ArrayList<>();
        StartupDiagnostic(Path path){this.path=path;}
        synchronized void phase(String name) throws java.io.IOException {
            phases.add(Map.of("phase",name,"elapsedMs",TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started)));
            write(List.of(),List.of());
        }
        synchronized void failed(Throwable failure) throws java.io.IOException {
            var types=new ArrayList<String>();var codes=new ArrayList<String>();
            for(Throwable cause=failure;cause!=null&&types.size()<8;cause=cause.getCause()){
                types.add(cause.getClass().getName());if(cause.getMessage()!=null&&CODES.contains(cause.getMessage()))codes.add(cause.getMessage());
            }
            write(types,codes);
        }
        private void write(List<String> types,List<String> codes) throws java.io.IOException {
            Path temporary=Path.of(path+".tmp");
            Files.writeString(temporary,JsonMapper.builder().build().writeValueAsString(Map.of("phases",phases,"exceptionTypes",types,"codes",codes)));
            Files.move(temporary,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
