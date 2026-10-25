package io.chatpulse.pipeline;

import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

public final class ProjectionConsumer implements AutoCloseable {
    private final KafkaMessageListenerContainer<String, byte[]> container;
    public record PollState(java.time.Instant at, java.util.Set<org.apache.kafka.common.TopicPartition> partitions) { }
    private final ConsumerPollTracker poll = new ConsumerPollTracker();
    public PollState pollState() {
        var snapshot = poll.snapshot();
        return new PollState(snapshot.at(), snapshot.partitions());
    }
    private volatile String state = "STARTING";
    private volatile String lastErrorCode = "NONE";
    private final AtomicLong batches = new AtomicLong(), errors = new AtomicLong();
    public ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store) {
        this(factory, topic, store, null, records -> { });
    }
    public ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store, DeadLetterRecovery recovery) {
        this(factory, topic, store, recovery, records -> { });
    }
    // commit 직후/offset commit 직전 장애를 실제 broker 테스트에서 재현하는 seam.
    ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store,
            Consumer<List<ConsumerRecord<String, byte[]>>> afterDbCommit) {
        this(factory, topic, store, null, afterDbCommit);
    }
    ProjectionConsumer(DefaultKafkaConsumerFactory<String, byte[]> factory, String topic, ProjectionStore store,
            DeadLetterRecovery recovery, Consumer<List<ConsumerRecord<String, byte[]>>> afterDbCommit) {
        factory.addPostProcessor(poll::track);
        String reset = "earliest".equals(factory.getConfigurationProperties().get("auto.offset.reset")) ? "earliest" : "none";
        var rebalance = BatchConsumers.initialOffsets(reset,
                (partition, position) -> store.initializeProgress(partition.topic(), partition.partition(), position),
                poll::revoked);
        BatchMessageListener<String, byte[]> listener = records -> {
            try {
                if (recovery == null) store.apply(records);
                else store.applyRecovering(records, recovery);
                afterDbCommit.accept(records);
                batches.incrementAndGet();
                state = "RUNNING";
                lastErrorCode = "NONE";
            } catch (RuntimeException e) {
                errors.incrementAndGet();
                state = e instanceof EventContractException ? "BLOCKED_INVALID_EVENT" : "RETRYING";
                lastErrorCode = e instanceof EventContractException || e instanceof ProjectionRetryException ? e.getMessage() : "DB_OR_PROCESSING_UNAVAILABLE";
                throw new ProjectionRetryException(lastErrorCode);
            }
        };
        container = BatchConsumers.create(factory, "chat-projection", new String[] { topic }, rebalance, listener);
    }
    public void start() { container.start(); }
    public Map<String, Object> status() {
        var assigned = container.getAssignedPartitions();
        return Map.of("running", container.isRunning(), "state", "STARTING".equals(state) && assigned != null && !assigned.isEmpty() ? "WAITING_FOR_EVENTS" : state, "successfulBatches", batches.get(),
                "failedAttempts", errors.get(), "lastErrorCode", lastErrorCode, "assignedPartitions", assigned == null ? 0 : assigned.size());
    }
    @Override public void close() { container.stop(); }
}
