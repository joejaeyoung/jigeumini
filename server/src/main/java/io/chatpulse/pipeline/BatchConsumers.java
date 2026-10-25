package io.chatpulse.pipeline;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

/** DB 반영 후 listener가 반환해야 offset을 커밋하는 공통 소비 계약. */
public final class BatchConsumers {
    private BatchConsumers() { }

    /** 오류 격리와 DB 트랜잭션은 listener가 소유한다. 모든 미처리 실패는 배치 전체를 재시도한다. */
    public static KafkaMessageListenerContainer<String, byte[]> create(
            DefaultKafkaConsumerFactory<String, byte[]> factory, String name, String[] topics,
            ConsumerAwareRebalanceListener rebalance, BatchMessageListener<String, byte[]> listener) {
        var properties = new ContainerProperties(topics);
        properties.setAckMode(ContainerProperties.AckMode.BATCH);
        properties.setSyncCommits(true);
        properties.setPollTimeout(1000);
        properties.setShutdownTimeout(10000);
        properties.setConsumerRebalanceListener(rebalance);
        properties.setMessageListener(listener);

        var container = new KafkaMessageListenerContainer<>(factory, properties);
        container.setBeanName(name);
        var errors = new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.setClassifications(Map.of(Exception.class, true), true);
        container.setCommonErrorHandler(errors);
        return container;
    }

    public static ConsumerAwareRebalanceListener initialOffsets(String reset) {
        return initialOffsets(reset, null, () -> { });
    }

    /**
     * DB 초기화가 모두 성공한 뒤에만 최초 earliest 위치를 기록하며 기존 commit은 바꾸지 않는다.
     * initialize가 null이면 DB 초기화가 없는 소비자이며, 최초 earliest 외에는 position도 조회하지 않는다.
     */
    public static ConsumerAwareRebalanceListener initialOffsets(String reset,
            BiConsumer<TopicPartition, Long> initialize, Runnable revoked) {
        return new ConsumerAwareRebalanceListener() {
            @Override public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                var committed = consumer.committed(new HashSet<>(partitions));
                var initial = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (var partition : partitions) {
                    boolean first = committed.get(partition) == null && "earliest".equals(reset);
                    if (initialize == null && !first) continue;
                    long position = consumer.position(partition);
                    if (initialize != null) initialize.accept(partition, position);
                    if (first) initial.put(partition, new OffsetAndMetadata(position));
                }
                if (!initial.isEmpty()) consumer.commitSync(initial);
            }

            @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                revoked.run();
            }
        };
    }
}
