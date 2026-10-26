package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 실제 두 JVM fixture. 원천은 loopback으로 고정하며 인증/원문을 결과에 쓰지 않는다. */
public final class FleetWorkerProcess {
    public static void main(String[] args) throws Exception {
        var diagnostic=new StartupDiagnostic(Path.of(args[4]+".diagnostic.json"));
        try { execute(args,diagnostic); }
        catch(Exception|LinkageError failure){diagnostic.failed(failure);throw failure;}
    }
    private static void execute(String[] args,StartupDiagnostic diagnostic) throws Exception {
        diagnostic.phase("STARTING");
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        var factory=KafkaSupport.producerFactory("localhost:19092");
        boolean delayedReport=args.length>5&&args[5].equals("DELAY_REPORT");
        if(args.length>5&&args[5].equals("DELAY_PRODUCER"))factory.addPostProcessor(producer->{
            try {Files.writeString(Path.of(args[4]+".producer-starting"),"PRODUCER_STARTING",StandardOpenOption.CREATE_NEW);
                Thread.sleep(3000);long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while(!Files.exists(Path.of(args[4]+".producer-release"))){if(System.nanoTime()>deadline)throw new IllegalStateException("SYNTHETIC_RELEASE_TIMEOUT");Thread.sleep(20);}
                Files.writeString(Path.of(args[4]+".producer-ready"),"PRODUCER_READY",StandardOpenOption.CREATE_NEW);return producer;}
            catch(Exception failure){producer.close(Duration.ZERO);throw new IllegalStateException("SYNTHETIC_PRODUCER_DELAY_FAILED");}
        });
        try(var ds=PostgresSupport.open(args[0],"chatpulse","local-chatpulse",false)) {
            diagnostic.phase("DATABASE_OPEN");
            var source=new ChatSource(){public String resolveRoom(String channel){return channel;}public String readToken(String room){return "SYNTHETIC_TOKEN";}public URI socketUri(String room){return URI.create("ws://127.0.0.1:"+args[2]);}};
            var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
            diagnostic.phase("KAFKA_TEMPLATE_READY");
            var limits=new CollectorWorker.Limits(Integer.parseInt(args[3]),Duration.ofSeconds(6),Duration.ofSeconds(delayedReport?1:90),200,Duration.ofMillis(100),new BufferedKafkaPublisher.Limits(30,20,4));
            diagnostic.phase("WORKER_CONSTRUCTING");
            try(var worker=new CollectorWorker(new CollectorLeases(ds),SourceMode.SYNTHETIC,limits,source,kafka,args[1],CollectionObserver.NONE)) {
                diagnostic.phase("WORKER_READY");
                var written=new AtomicBoolean();worker.run(result->{if(written.compareAndSet(false,true))try{if(delayedReport){Files.writeString(Path.of(args[4]+".persisting"),"REPORT_WRITE_STARTED",StandardOpenOption.CREATE_NEW);Thread.sleep(5000);}Files.writeString(Path.of(args[4]),JsonMapper.builder().build().writeValueAsString(result),StandardOpenOption.CREATE_NEW);diagnostic.phase("REPORT_COMPLETE");}catch(Exception failure){throw new IllegalStateException("FLEET_RESULT_FAILED");}});
            }
        } finally{factory.destroy();}
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
