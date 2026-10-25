package io.chatpulse.operations;

import io.chatpulse.pipeline.KafkaTopicPolicy;
import java.nio.file.*;
import java.util.Map;

/** 명령행 도구와 배포가 공유하는 비밀 파일·유한 Kafka 정책. 비밀 값은 진단에 포함하지 않는다. */
public record RuntimeEnvironment(String jdbcUrl,String user,String password,String bootstrap,String topic,
        String observationsTopic,String notificationsTopic,String group,String projection,String runId,KafkaTopicPolicy policy) {
    public static RuntimeEnvironment from(Map<String,String> env) {
        String topic=env.getOrDefault("PIPELINE_TOPIC","chat.received.v1");
        return new RuntimeEnvironment(env.getOrDefault("PIPELINE_JDBC_URL","jdbc:postgresql://localhost:15432/chatpulse"),
                env.getOrDefault("PIPELINE_DB_USER","chatpulse"),password(env),
                env.getOrDefault("PIPELINE_BOOTSTRAP_SERVERS","localhost:19092"),topic,env.getOrDefault("OBSERVATIONS_TOPIC","broadcast.observed.v1"),
                env.getOrDefault("NOTIFICATIONS_TOPIC",topic+".spikes"),env.getOrDefault("PIPELINE_GROUP","chat-ranking-live-v1"),
                env.getOrDefault("PIPELINE_PROJECTION","ranking"),env.getOrDefault("PIPELINE_RUN_ID","live-v1"),policy(env));
    }
    public static KafkaTopicPolicy policy(Map<String,String> env) {
        return new KafkaTopicPolicy(KafkaTopicPolicy.Durability.valueOf(env.getOrDefault("PIPELINE_KAFKA_DURABILITY","LOCAL")),
                Long.parseLong(env.getOrDefault("PIPELINE_KAFKA_RETENTION_MS","86400000")),Long.parseLong(env.getOrDefault("PIPELINE_KAFKA_RETENTION_BYTES","268435456")),Integer.parseInt(env.getOrDefault("PIPELINE_KAFKA_SEGMENT_BYTES","33554432")));
    }
    public static String password(Map<String,String> env) {
        return secret(env.getOrDefault("PIPELINE_DB_PASSWORD","local-chatpulse"),env.getOrDefault("PIPELINE_DB_PASSWORD_FILE",""),
                "true".equals(env.get("DEPLOYMENT_ENABLED"))||"VALIDATE".equals(env.get("PIPELINE_SCHEMA_MODE")));
    }
    public static String secret(String fallback,String file,boolean required) {
        if(required&&file.isEmpty())throw new IllegalArgumentException("SECRET_FILE_REQUIRED");
        return secret(fallback,file);
    }
    public static String secret(String fallback,String file) {
        if(file.isEmpty())return fallback;
        try(var input=Files.newInputStream(Path.of(file))) {
            byte[] bytes=input.readNBytes(4097);if(bytes.length==0||bytes.length>4096)throw new IllegalArgumentException();
            String value=new String(bytes,java.nio.charset.StandardCharsets.UTF_8);
            if(value.endsWith("\n"))value=value.substring(0,value.length()-1);
            if(value.isEmpty()||value.indexOf('\n')>=0||value.indexOf('\r')>=0)throw new IllegalArgumentException();return value;
        } catch(Exception failure){throw new IllegalArgumentException("SECRET_FILE_UNAVAILABLE");}
    }
    /** record의 기본 toString은 비밀을 출력하므로 금지한다. */
    @Override public String toString(){return "RuntimeEnvironment[REDACTED]";}
}
