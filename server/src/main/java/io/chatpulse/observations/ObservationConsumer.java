package io.chatpulse.observations;

import io.chatpulse.pipeline.BatchConsumers;
import io.chatpulse.pipeline.KafkaSupport;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.kafka.listener.*;

public final class ObservationConsumer implements AutoCloseable {
    private final KafkaMessageListenerContainer<String,byte[]> container;
    private volatile String state = "STARTING", lastError = "NONE";
    private final AtomicLong batches = new AtomicLong(), failures = new AtomicLong();
    public ObservationConsumer(String bootstrap, String topic, String group, String reset, ObservationStore store) {
        this(bootstrap, topic, group, reset, store, () -> { });
    }
    // DB commit 이후·Kafka commit 이전의 별도 JVM 종료 검증 경계.
    ObservationConsumer(String bootstrap, String topic, String group, String reset, ObservationStore store, Runnable afterDbCommit) {
        if (!Set.of("none", "earliest").contains(reset)) throw new IllegalArgumentException("OBSERVATION_RESET");
        BatchMessageListener<String, byte[]> listener = records -> {
            try { store.apply(records); afterDbCommit.run(); state = "RUNNING"; lastError = "NONE"; batches.incrementAndGet(); }
            catch (RuntimeException failure) {
                state = "RETRYING"; failures.incrementAndGet();
                lastError = failure instanceof IllegalArgumentException && failure.getMessage() != null && Set.of("OBSERVATION_CONTRACT_INVALID", "OBSERVATION_FUTURE_TIME", "OBSERVATION_ID_CONFLICT").contains(failure.getMessage())
                        ? failure.getMessage() : "OBSERVATION_PROCESSING_UNAVAILABLE";
                throw new IllegalStateException(lastError);
            }
        };
        container = BatchConsumers.create(KafkaSupport.consumerFactory(bootstrap, group, reset),
                "broadcast-observations", new String[] { topic }, BatchConsumers.initialOffsets(reset), listener);
    }
    public void start() { container.start(); }
    public Map<String,Object> status() {
        var assigned = container.getAssignedPartitions();
        return Map.of("running", container.isRunning(), "state", state.equals("STARTING") && assigned != null && !assigned.isEmpty() ? "WAITING_FOR_EVENTS" : state,
                "successfulBatches", batches.get(), "failedAttempts", failures.get(), "lastErrorCode", lastError,
                "assignedPartitions", assigned == null ? 0 : assigned.size());
    }
    @Override public void close() { container.stop(); }
}
