package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

/** 실험 입력의 키/partition 배정. 제품의 라우팅 계약과 분리한다. */
enum ExperimentRouting {
    CHANNEL_KEY, SUFFIX_HASH, EXPLICIT_EVENT_ID;

    static final int PARTITIONS = 3;

    ProducerRecord<String, byte[]> record(String topic, ChatEvent event, byte[] bytes, long sequence) {
        String key = event.channelId() + (this == SUFFIX_HASH ? "/" + sequence % PARTITIONS : "");
        Integer partition = this == EXPLICIT_EVENT_ID ? partition(event) : null;
        return new ProducerRecord<>(topic, partition, event.receivedAt().toEpochMilli(), key, bytes);
    }

    int partition(ChatEvent event) {
        return (keyPartition(event.channelId()) + keyPartition(event.eventId().toString())) % PARTITIONS;
    }

    static int keyPartition(String key) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % PARTITIONS;
    }

    Map<String, Object> audit(Map<Integer, Long> hotCounts, long misplacedRecords) {
        long total = hotCounts.values().stream().mapToLong(Long::longValue).sum();
        long maximum = hotCounts.values().stream().mapToLong(Long::longValue).max().orElse(0);
        var counts = new ArrayList<Long>();
        for (int p = 0; p < PARTITIONS; p++) counts.add(hotCounts.getOrDefault(p, 0L));
        boolean split = total > 0 && counts.stream().allMatch(n -> n > 0) && maximum / (double) total <= .40;
        return Map.of("mode", name(), "partitionCount", PARTITIONS, "hotPhysicalRecords", total,
                "hotRecordsByPartition", counts, "hotPartitionCount", counts.stream().filter(n -> n > 0).count(),
                "maximumHotShare", total == 0 ? 0 : maximum / (double) total,
                "splitVerified", split, "misplacedRecords", misplacedRecords,
                "requiredDistributionPassed", this != EXPLICIT_EVENT_ID || split && misplacedRecords == 0);
    }
}
