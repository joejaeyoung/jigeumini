package io.chatpulse.pipeline;

import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

public final class ProjectionConsumer implements AutoCloseable {
    private final KafkaMessageListenerContainer<String, byte[]> container;
    private volatile String state = "STARTING";
    private final AtomicLong batches = new AtomicLong(), errors = new AtomicLong();
    public ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store) {
        this(factory, topic, store, records -> { });
    }
    // commit 직후/offset commit 직전 장애를 실제 broker 테스트에서 재현하는 seam.
    ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store,
            Consumer<List<ConsumerRecord<String, byte[]>>> afterDbCommit) {
        var props = new ContainerProperties(topic);
        props.setAckMode(ContainerProperties.AckMode.BATCH);
        if ("earliest".equals(factory.getConfigurationProperties().get("auto.offset.reset"))) {
            // 최초 설치를 명시한 실행에서만 빈 partition도 시작 offset을 기록한다.
            // 그렇지 않으면 무입력 partition이 남아 다음 none 재시작에서 실패한다.
            props.setConsumerRebalanceListener(new org.springframework.kafka.listener.ConsumerAwareRebalanceListener() {
                @Override public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                        java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                    var committed = consumer.committed(new java.util.HashSet<>(partitions));
                    var initial = new java.util.HashMap<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>();
                    for (var partition : partitions) {
                        if (committed.get(partition) == null) {
                            initial.put(partition, new org.apache.kafka.clients.consumer.OffsetAndMetadata(consumer.position(partition)));
                        }
                    }
                    if (!initial.isEmpty()) consumer.commitSync(initial);
                }
            });
        }
        props.setSyncCommits(true);
        props.setPollTimeout(1000);
        props.setShutdownTimeout(10000);
        props.setMessageListener((BatchMessageListener<String, byte[]>) records -> {
            try {
                store.apply(records);
                afterDbCommit.accept(records);
                batches.incrementAndGet();
                state = "RUNNING";
            } catch (RuntimeException e) {
                errors.incrementAndGet();
                state = e instanceof EventContractException ? "BLOCKED_INVALID_EVENT" : "RETRYING";
                throw e;
            }
        });
        container = new KafkaMessageListenerContainer<>(factory, props);
        container.setBeanName("chat-projection");
        // M1은 실패 record를 버리거나 offset을 넘기지 않는다. DLQ는 M3에서 추가한다.
        var handler = new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        container.setCommonErrorHandler(handler);
    }
    public void start() { container.start(); }
    public Map<String, Object> status() {
        var assigned = container.getAssignedPartitions();
        return Map.of("running", container.isRunning(), "state", "STARTING".equals(state) && assigned != null && !assigned.isEmpty() ? "WAITING_FOR_EVENTS" : state, "successfulBatches", batches.get(),
                "failedAttempts", errors.get(), "assignedPartitions", assigned == null ? 0 : assigned.size());
    }
    @Override public void close() { container.stop(); }
}
