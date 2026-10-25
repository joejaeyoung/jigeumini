package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy;
import io.chatpulse.pipeline.KafkaSupport;
import io.chatpulse.poc.ChzzkHttpSource;
import io.chatpulse.quality.CollectionTelemetry;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 조회/미리보기와 활성화를 분리한 로컬 관리 CLI. 공식 채팅 어댑터는 아직 제공하지 않는다. */
public final class FleetCli {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    private FleetCli() { }
    public static int run(String[] args) {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        try {return execute(args,System.getenv());}catch(Exception failure){System.out.println(JSON.writeValueAsString(Map.of("error","FLEET_COMMAND_FAILED","codes",io.chatpulse.operations.FailureDiagnostics.codes(failure),"types",io.chatpulse.operations.FailureDiagnostics.types(failure))));return 2;}
    }
    static int execute(String[] args,Map<String,String> env) throws Exception {
        if(args.length==0)throw new IllegalArgumentException("FLEET_ARGUMENTS");
        FleetPlan plan=null;
        if(Set.of("preview","apply").contains(args[0])&&args.length==3) {
            Path path=Path.of(args[1]);if(Files.size(path)>1_048_576)throw new IllegalArgumentException("FLEET_INPUT_LIMIT");
            byte[] bytes;try(var in=Files.newInputStream(path)){bytes=in.readNBytes(1_048_577);}if(bytes.length>1_048_576)throw new IllegalArgumentException("FLEET_INPUT_LIMIT");
            plan=FleetPlan.from(JSON.readValue(bytes,SelectionPolicy.Report.class),HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)),Integer.parseInt(args[2]),Instant.now());
            if(args[0].equals("preview")){System.out.println(JSON.writeValueAsString(Map.of("plan",plan,"activationAllowed",false,"scope","PLAN_ONLY_NO_DATABASE_OR_SOURCE_CALL")));return 0;}
            if(plan.sourceMode()==SelectionPolicy.SourceMode.UNOFFICIAL_POC&&!"true".equals(env.get("FLEET_LIVE_ENABLED")))throw new IllegalArgumentException("FLEET_LIVE_DISABLED");
        } else if(!(Set.of("migrate","status").contains(args[0])&&args.length==1)&&!(args[0].equals("worker")&&args.length==4))throw new IllegalArgumentException("FLEET_ARGUMENTS");
        if(args[0].equals("worker")&&!"true".equals(env.get("FLEET_LIVE_ENABLED")))throw new IllegalArgumentException("FLEET_LIVE_DISABLED");
        String url=env.getOrDefault("PIPELINE_JDBC_URL","jdbc:postgresql://localhost:15432/chatpulse"),user=env.getOrDefault("PIPELINE_DB_USER","chatpulse"),password=io.chatpulse.operations.RuntimeEnvironment.password(env);
        if(args[0].equals("migrate")){try(var ds=PostgresSupport.openMigrated(url,user,password)){System.out.println("{\"migrated\":true}");return 0;}}
        try(var ds=args[0].equals("worker")&&"VALIDATE".equals(env.get("PIPELINE_SCHEMA_MODE"))?PostgresSupport.openValidated(url,user,password):PostgresSupport.open(url,user,password,args[0].equals("status"))) {
            var store=new CollectorLeases(ds);
            if(args[0].equals("status")){System.out.println(JSON.writeValueAsString(store.status()));return 0;}
            if(args[0].equals("apply")){store.apply(plan);System.out.println(JSON.writeValueAsString(store.status()));return 0;}
            var limits=CollectorWorker.Limits.defaults(Integer.parseInt(args[1]),Duration.ofSeconds(Long.parseLong(args[2])));
            if(limits.capacity()>3)throw new IllegalArgumentException("FLEET_LIVE_LIMIT");
            Path output=Path.of(args[3]).toAbsolutePath();if(!Files.isDirectory(output.getParent()))throw new IllegalArgumentException("FLEET_OUTPUT_PARENT");Files.createFile(output);
            String bootstrap=env.getOrDefault("PIPELINE_BOOTSTRAP_SERVERS","localhost:19092"),topic=env.getOrDefault("PIPELINE_TOPIC","chat.received.v1");var policy=io.chatpulse.operations.RuntimeEnvironment.policy(env);KafkaSupport.ensureTopic(bootstrap,topic,policy);
            var observations=new java.util.concurrent.atomic.AtomicReference<LeasedObservations>();
            var observationPublisher=new java.util.concurrent.atomic.AtomicReference<io.chatpulse.observations.ObservationPublisher>();
            var factory=KafkaSupport.producerFactory(bootstrap);var telemetry=new java.util.concurrent.atomic.AtomicReference<CollectionTelemetry>();
            Runnable closeRuntime=()->{if(observations.get()!=null)observations.get().close();if(observationPublisher.get()!=null)observationPublisher.get().close();if(telemetry.get()!=null)telemetry.get().close();factory.destroy();};
            try {
                var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
                try(var worker=new CollectorWorker(store,SelectionPolicy.SourceMode.UNOFFICIAL_POC,limits,new ChzzkHttpSource(),kafka,topic,(publisher,budget)->{
                    var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
                    var observer=new CollectionTelemetry(new JdbcTemplate(ds),tx,publisher);telemetry.set(observer);observer.start();
                    if(!"true".equals(env.get("OBSERVATIONS_ENABLED")))return observer;
                    String observedTopic=env.getOrDefault("OBSERVATIONS_TOPIC","broadcast.observed.v1");
                    if(observedTopic.equals(topic)||observedTopic.equals(topic+".dlq")||observedTopic.equals(topic+".spikes"))throw new IllegalArgumentException("OBSERVATION_TOPIC");
                    KafkaSupport.ensureTopic(bootstrap,observedTopic,policy);
                    var observedPublisher=new io.chatpulse.observations.ObservationPublisher(bootstrap,observedTopic);observationPublisher.set(observedPublisher);
                    var observed=new LeasedObservations(budget,env.getOrDefault("PIPELINE_RUN_ID","live-v1"),
                            Integer.parseInt(env.getOrDefault("OBSERVATIONS_INTERVAL_SECONDS","60")),Integer.parseInt(env.getOrDefault("OBSERVATIONS_TTL_SECONDS","120")),
                            new io.chatpulse.metadata.ChzzkMetadataSource(budget::reserveFor)::observe,observedPublisher);observations.set(observed);
                    return io.chatpulse.poc.CollectionObserver.combine(List.of(observer,observed));
                })) {
                    var report=worker.run(result->{try{closeRuntime.run();Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");}catch(Exception failure){throw new IllegalStateException("FLEET_OUTPUT_FAILED");}});
                    System.out.println(JSON.writeValueAsString(Map.of("complete",report.get("complete"),"workerId",report.get("workerId"))));return Boolean.TRUE.equals(report.get("complete"))?0:2;
                }
            } finally {closeRuntime.run();}
        }
    }
}
