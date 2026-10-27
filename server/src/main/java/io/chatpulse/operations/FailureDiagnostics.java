package io.chatpulse.operations;

import java.util.*;
import tools.jackson.databind.json.JsonMapper;

/** 예외 메시지·경로·원문을 버리고 알려진 운영 코드만 출력한다. */
public final class FailureDiagnostics {
    private static final Set<String> ALLOWED=Set.of("SCHEMA_NOT_READY","SECRET_FILE_REQUIRED","SECRET_FILE_UNAVAILABLE",
            "RECOVERY_ALIGNMENT_REQUIRED","SOURCE_BIND_FAILED","SOURCE_IDENTITY_MISMATCH","RECOVERY_INSPECTION_FAILED",
            "RECOVERY_CONFIGURATION","RECOVERY_NEW_GROUP_REQUIRED","RECOVERY_RETENTION_GAP","RECOVERY_GROUP_ACTIVE",
            "RECOVERY_GROUP_EXISTS","INITIALIZE_REQUIRES_EMPTY_DATABASE","RECOVERY_GROUP_PREPARATION_FAILED",
            "MAINTENANCE_REQUIRED","OPS_ARGUMENTS","SCHEMA_MODE","FLEET_LIVE_LIMIT",
            "FLEET_KAFKA_NOT_READY","KAFKA_TOPIC_INIT_FAILED","KAFKA_TOPIC_CONFIG_MISMATCH","KAFKA_REPLICATION_FACTOR_MISMATCH","KAFKA_ELR_CONTRACT_MISMATCH",
            "KAFKA_TOPIC_POLICY","KAFKA_TOPIC_NOT_WRITABLE","TOPIC_VISIBILITY_DEADLINE","OBSERVATION_TOPIC","NOTIFICATION_TOPIC","TREND_CONFIGURATION",
            "M1 requires 3 partitions","M1 requires CreateTime timestamps","History requires delete cleanup policy");
    private FailureDiagnostics() { }
    public static List<String> codes(Throwable failure) {
        var codes=new LinkedHashSet<String>();
        for(int n=0;failure!=null&&n<12;n++,failure=failure.getCause())if(failure.getMessage()!=null&&ALLOWED.contains(failure.getMessage()))codes.add(failure.getMessage());
        return List.copyOf(codes);
    }
    public static List<String> types(Throwable failure) {
        var types=new LinkedHashSet<String>();
        for(int n=0;failure!=null&&n<12;n++,failure=failure.getCause()) {
            String name=failure.getClass().getSimpleName();if(name.matches("[A-Za-z][A-Za-z0-9]{0,100}"))types.add(name);
        }
        return List.copyOf(types);
    }
    public static void startupFailed(Throwable failure) {
        System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("error","APPLICATION_START_FAILED","codes",codes(failure),"types",types(failure))));
    }
}
