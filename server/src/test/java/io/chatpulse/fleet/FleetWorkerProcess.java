package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 실제 두 JVM fixture. 원천은 loopback으로 고정하며 인증/원문을 결과에 쓰지 않는다. */
public final class FleetWorkerProcess {
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        var factory=KafkaSupport.producerFactory("localhost:19092");
        try(var ds=PostgresSupport.open(args[0],"chatpulse","local-chatpulse",false)) {
            var source=new ChatSource(){public String resolveRoom(String channel){return channel;}public String readToken(String room){return "SYNTHETIC_TOKEN";}public URI socketUri(String room){return URI.create("ws://127.0.0.1:"+args[2]);}};
            var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
            var limits=new CollectorWorker.Limits(Integer.parseInt(args[3]),Duration.ofSeconds(6),Duration.ofSeconds(90),200,Duration.ofMillis(100),new BufferedKafkaPublisher.Limits(30,20,4));
            try(var worker=new CollectorWorker(new CollectorLeases(ds),SourceMode.SYNTHETIC,limits,source,kafka,args[1],CollectionObserver.NONE)) {
                var written=new AtomicBoolean();worker.run(result->{if(written.compareAndSet(false,true))try{Files.writeString(Path.of(args[4]),JsonMapper.builder().build().writeValueAsString(result),StandardOpenOption.CREATE_NEW);}catch(Exception failure){throw new IllegalStateException("FLEET_RESULT_FAILED");}});
            }
        } finally{factory.destroy();}
    }
}
