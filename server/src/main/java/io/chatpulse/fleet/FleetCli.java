package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
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
        } else if(!(Set.of("migrate","status").contains(args[0])&&args.length==1)&&!(args[0].equals("worker")&&args.length==4))throw new IllegalArgumentException("FLEET_ARGUMENTS");
        String url=env.getOrDefault("PIPELINE_JDBC_URL","jdbc:postgresql://localhost:15432/chatpulse"),user=env.getOrDefault("PIPELINE_DB_USER","chatpulse"),password=io.chatpulse.operations.RuntimeEnvironment.password(env);
        if(args[0].equals("migrate")){try(var ds=PostgresSupport.openMigrated(url,user,password)){System.out.println("{\"migrated\":true}");return 0;}}
        try(var ds=args[0].equals("worker")&&"VALIDATE".equals(env.get("PIPELINE_SCHEMA_MODE"))?PostgresSupport.openValidated(url,user,password):PostgresSupport.open(url,user,password,args[0].equals("status"))) {
            var store=new CollectorLeases(ds);
            if(args[0].equals("status")){System.out.println(JSON.writeValueAsString(store.status()));return 0;}
            if(args[0].equals("apply")){store.apply(plan);System.out.println(JSON.writeValueAsString(store.status()));return 0;}
            var limits=CollectorWorker.Limits.defaults(Integer.parseInt(args[1]),Duration.ofSeconds(Long.parseLong(args[2])));
            if(limits.capacity()>3)throw new IllegalArgumentException("FLEET_LIVE_LIMIT");
            Path output=Path.of(args[3]).toAbsolutePath();if(!Files.isDirectory(output.getParent()))throw new IllegalArgumentException("FLEET_OUTPUT_PARENT");Files.createFile(output);
            return FleetRuntime.run(store, ds, limits, env, output);
        }
    }
}
