package io.chatpulse.replay;

import com.zaxxer.hikari.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.flywaydb.core.Flyway;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.replay.ReplayFailure.Code.*;

/** 로컬 foreground 관리 명령. HTTP 포트·수집기·분석기를 기동하지 않는다. */
public final class ReplayCli {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    private ReplayCli() { }
    public static int run(String[] args) {
        // 외부 라이브러리 예외나 접속 설정을 출력하지 않고 명령 결과의 고정 코드만 기록한다.
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)
            context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        try {
            if(args.length<1) throw new ReplayFailure(INVALID_COMMAND);
            String bootstrap=env("PIPELINE_BOOTSTRAP_SERVERS","127.0.0.1:19092"),topic=env("PIPELINE_TOPIC","chat.received.v1");
            if(args[0].equals("capture")) {
                if(args.length!=2 && args.length!=4) throw new ReplayFailure(INVALID_COMMAND);
                try(var source=new ReplaySource(bootstrap,topic,UUID.randomUUID())) {
                    var manifest=source.capture(args.length==4?Instant.parse(args[2]):null,args.length==4?Instant.parse(args[3]):null);
                    Files.writeString(Path.of(args[1]),manifest.json()+"\n",StandardOpenOption.CREATE_NEW);
                    print(manifest);return 0;
                }
            }
            try(var ds=dataSource()) {
                Flyway.configure().dataSource(ds).load().migrate();var store=new ReplayStore(ds);
                switch(args[0]) {
                    case "create" -> {
                        require(args,2);var path=Path.of(args[1]);
                        if(Files.size(path)>65536) throw new ReplayFailure(INVALID_MANIFEST);
                        var manifest=ReplayManifest.parse(Files.readString(path));
                        try(var source=new ReplaySource(bootstrap,topic,UUID.randomUUID())) {source.validate(manifest);}
                        print(store.status(store.create(manifest)));
                    }
                    case "status" -> {require(args,2);store.reap();print(store.status(UUID.fromString(args[1])));}
                    case "cancel" -> {require(args,2);UUID id=UUID.fromString(args[1]);store.cancel(id);store.reap();print(store.status(id));}
                    case "worker" -> {
                        require(args,1);var id=new ReplayWorker(store,bootstrap,topic).runOne();
                        if(id==null) print(Map.of("state","IDLE"));
                        else {print(store.status(id));if(!store.get(id).state().equals("COMPLETED")) return 2;}
                    }
                    case "reap" -> {require(args,1);print(Map.of("reapedRuns",store.reap()));}
                    case "compare" -> {require(args,3);var result=store.compare(UUID.fromString(args[1]),UUID.fromString(args[2]));print(result);if(!Boolean.TRUE.equals(result.get("equal"))) return 2;}
                    default -> throw new ReplayFailure(INVALID_COMMAND);
                }
            }
            return 0;
        } catch(ReplayFailure e) {print(Map.of("error",e.code().name()));return 2;}
          catch(java.io.IOException | IllegalArgumentException e) {print(Map.of("error","INVALID_COMMAND_OR_FILE"));return 2;}
          catch(Exception e) {print(Map.of("error","REPLAY_UNAVAILABLE"));return 2;}
    }
    private static void require(String[] args,int count) {if(args.length!=count) throw new ReplayFailure(INVALID_COMMAND);}
    private static void print(Object value) {System.out.println(JSON.writeValueAsString(value));}
    private static String env(String name,String fallback) {return System.getenv().getOrDefault(name,fallback);}
    private static HikariDataSource dataSource() {
        var config=new HikariConfig();config.setJdbcUrl(env("PIPELINE_JDBC_URL","jdbc:postgresql://127.0.0.1:15432/chatpulse"));
        config.setUsername(env("PIPELINE_DB_USER","chatpulse"));config.setPassword(env("PIPELINE_DB_PASSWORD","local-chatpulse"));
        config.setMaximumPoolSize(4);config.setConnectionTimeout(3000);config.setValidationTimeout(2000);
        config.addDataSourceProperty("connectTimeout","3");config.addDataSourceProperty("socketTimeout","10");
        return new HikariDataSource(config);
    }
}
