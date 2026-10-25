package io.chatpulse.operations;

import io.chatpulse.db.PostgresSupport;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 명시적 migration/원천 등록/새 복구 group 준비. raw 오류나 비밀은 출력하지 않는다. */
public final class OperationsCli {
    private OperationsCli() { }
    public static int run(String[] args) {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        try {
            if(args.length==1&&args[0].equals("probe")) {
                var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:8080/readyz")).timeout(Duration.ofSeconds(3)).GET().build();
                try(var client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()){return client.send(request,HttpResponse.BodyHandlers.discarding()).statusCode()==200?0:2;}
            }
            if(args.length<1||!Set.of("migrate","bind-sources","initialize-groups","recovery-plan","recover-groups").contains(args[0])
                    ||args.length!=(args[0].equals("recover-groups")?2:1))throw new IllegalArgumentException("OPS_ARGUMENTS");
            var env=System.getenv();var config=RuntimeEnvironment.from(env);Object result;
            if(args[0].equals("migrate")) {
                try(var ds=PostgresSupport.openMigrated(config.jdbcUrl(),config.user(),config.password())) {
                    if("true".equals(env.get("OPS_GRANT_RUNTIME"))) {
                        var jdbc=new JdbcTemplate(ds);jdbc.execute("GRANT USAGE ON SCHEMA public TO app_runtime");
                        jdbc.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO app_runtime");
                        jdbc.execute("GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO app_runtime");
                        jdbc.execute("REVOKE INSERT,UPDATE,DELETE ON flyway_schema_history,deployment_source_identity FROM app_runtime");
                    }
                    result=Map.of("migrated",true);
                }
            } else {
                boolean readOnly=args[0].equals("recovery-plan");
                if(!readOnly&&!"true".equals(env.get("DEPLOYMENT_MAINTENANCE")))throw new IllegalArgumentException("MAINTENANCE_REQUIRED");
                try(var ds=PostgresSupport.open(config.jdbcUrl(),config.user(),config.password(),readOnly);var plan=new RecoveryPlan(new JdbcTemplate(ds),config)) {
                    result=switch(args[0]) {
                        case "bind-sources"->{plan.bindSources();yield Map.of("sourcesBound",true);}
                        case "initialize-groups"->plan.prepareNewGroups(config.group(),true);
                        case "recover-groups"->plan.prepareNewGroups(args[1],false);
                        default->plan.inspect();
                    };
                }
            }
            System.out.println(JsonMapper.builder().build().writeValueAsString(result));return 0;
        } catch(Exception failure) {
            var codes=FailureDiagnostics.codes(failure);
            try{System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("error","OPERATIONS_FAILED","codes",codes)));}catch(Exception ignored){System.out.println("{\"error\":\"OPERATIONS_FAILED\"}");}return 2;
        }
    }
}
