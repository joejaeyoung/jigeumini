package io.chatpulse.pipeline;

import java.util.Map;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** F15: 생성할 계약과 기존 토픽에서 검사할 계약을 같은 값으로 유지한다. */
@ConfigurationProperties("pipeline.kafka")
public record KafkaTopicPolicy(@DefaultValue("LOCAL") Durability durability,
        @DefaultValue("86400000") long retentionMs, @DefaultValue("268435456") long retentionBytes,
        @DefaultValue("33554432") int segmentBytes) {
    public enum Durability { LOCAL, HA }
    public KafkaTopicPolicy {
        if (durability == null || retentionMs < 60_000 || retentionMs > 30L * 86400000
                || segmentBytes < 1_048_576 || segmentBytes > 1_073_741_824
                || retentionBytes < segmentBytes || retentionBytes > 1_099_511_627_776L)
            throw new IllegalArgumentException("KAFKA_TOPIC_POLICY");
    }
    public static KafkaTopicPolicy local() { return new KafkaTopicPolicy(Durability.LOCAL, 86400000, 268435456, 33554432); }
    public static KafkaTopicPolicy ha() { return new KafkaTopicPolicy(Durability.HA, 86400000, 268435456, 33554432); }
    public int partitions() { return 3; }
    public short replicationFactor() { return (short)(durability == Durability.HA ? 3 : 1); }
    public int minInSyncReplicas() { return durability == Durability.HA ? 2 : 1; }
    public Map<String,String> configurations() {
        return Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "delete", TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime",
                TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs), TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(retentionBytes),
                TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(segmentBytes), TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minInSyncReplicas()),
                TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
    }
    void verify(TopicDescription topic, Config config) {
        if (!value(config, TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).equals("CreateTime")) throw new IllegalStateException("M1 requires CreateTime timestamps");
        if (!value(config, TopicConfig.CLEANUP_POLICY_CONFIG).equals("delete")) throw new IllegalStateException("History requires delete cleanup policy");
        if (topic.partitions().size() != partitions()) throw new IllegalStateException("M1 requires 3 partitions");
        configurations().forEach((name, expected) -> {
            if (!value(config,name).equals(expected)) throw new IllegalStateException("KAFKA_TOPIC_CONFIG_MISMATCH");
        });
        for (var partition : topic.partitions()) {
            if (partition.replicas().size() != replicationFactor()
                    || partition.replicas().stream().map(n -> n.id()).distinct().count() != replicationFactor())
                throw new IllegalStateException("KAFKA_REPLICATION_FACTOR_MISMATCH");
            if (partition.leader() == null || partition.leader().id() < 0
                    || partition.isr().size() < minInSyncReplicas()
                    || partition.isr().stream().anyMatch(n -> partition.replicas().stream().noneMatch(r -> r.id() == n.id())))
                throw new NotReady();
        }
    }
    private static String value(Config config, String name) { return config.get(name) == null ? "" : config.get(name).value(); }
    static final class NotReady extends IllegalStateException { NotReady() { super("KAFKA_TOPIC_NOT_WRITABLE"); } }
}
