package io.chatpulse.notifications;

import io.chatpulse.pipeline.BatchConsumers;
import io.chatpulse.pipeline.KafkaSupport;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.kafka.listener.*;

public final class NotificationConsumer implements AutoCloseable {
    private final KafkaMessageListenerContainer<String,byte[]> container;
    private volatile String state="STARTING";
    private final AtomicLong batches=new AtomicLong(),failures=new AtomicLong();
    public NotificationConsumer(String bootstrap,String topic,String group,String reset,NotificationStore store) {
        if(!java.util.Set.of("none","earliest").contains(reset))throw new IllegalArgumentException("NOTIFICATION_RESET");
        BatchMessageListener<String, byte[]> listener = records -> {
            try {store.apply(records);state="RUNNING";batches.incrementAndGet();}
            catch(RuntimeException ignored){state="RETRYING";failures.incrementAndGet();throw new IllegalStateException("NOTIFICATION_PROCESSING_UNAVAILABLE");}
        };
        container = BatchConsumers.create(KafkaSupport.consumerFactory(bootstrap, group, reset),
                "spike-notifications", new String[] { topic }, BatchConsumers.initialOffsets(reset), listener);
    }
    public void start(){container.start();}
    public Map<String,Object> status(){var assigned=container.getAssignedPartitions();return Map.of("running",container.isRunning(),"state",state.equals("STARTING")&&assigned!=null&&!assigned.isEmpty()?"WAITING_FOR_EVENTS":state,
            "successfulBatches",batches.get(),"failedAttempts",failures.get(),"assignedPartitions",assigned==null?0:assigned.size());}
    @Override public void close(){container.stop();}
}
