package io.chatpulse.notifications;

import io.chatpulse.pipeline.KafkaSupport;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

public final class NotificationConsumer implements AutoCloseable {
    private final KafkaMessageListenerContainer<String,byte[]> container;
    private volatile String state="STARTING";
    private final AtomicLong batches=new AtomicLong(),failures=new AtomicLong();
    public NotificationConsumer(String bootstrap,String topic,String group,String reset,NotificationStore store) {
        if(!java.util.Set.of("none","earliest").contains(reset))throw new IllegalArgumentException("NOTIFICATION_RESET");
        var props=new ContainerProperties(topic);props.setAckMode(ContainerProperties.AckMode.BATCH);
        props.setSyncCommits(true);props.setPollTimeout(1000);props.setShutdownTimeout(10000);
        var factory=KafkaSupport.consumerFactory(bootstrap,group,reset);
        props.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener(){
            @Override public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?,?> consumer,java.util.Collection<org.apache.kafka.common.TopicPartition> partitions){
                var committed=consumer.committed(new java.util.HashSet<>(partitions));
                var initial=new java.util.HashMap<org.apache.kafka.common.TopicPartition,org.apache.kafka.clients.consumer.OffsetAndMetadata>();
                for(var partition:partitions)if(committed.get(partition)==null&&reset.equals("earliest"))
                    initial.put(partition,new org.apache.kafka.clients.consumer.OffsetAndMetadata(consumer.position(partition)));
                if(!initial.isEmpty())consumer.commitSync(initial);
            }
        });
        props.setMessageListener((BatchMessageListener<String,byte[]>) records -> {
            try {store.apply(records);state="RUNNING";batches.incrementAndGet();}
            catch(RuntimeException ignored){state="RETRYING";failures.incrementAndGet();throw new IllegalStateException("NOTIFICATION_PROCESSING_UNAVAILABLE");}
        });
        container=new KafkaMessageListenerContainer<>(factory,props);container.setBeanName("spike-notifications");
        var handler=new DefaultErrorHandler(new FixedBackOff(1000,FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class,true),true);container.setCommonErrorHandler(handler);
    }
    public void start(){container.start();}
    public Map<String,Object> status(){var assigned=container.getAssignedPartitions();return Map.of("running",container.isRunning(),"state",state.equals("STARTING")&&assigned!=null&&!assigned.isEmpty()?"WAITING_FOR_EVENTS":state,
            "successfulBatches",batches.get(),"failedAttempts",failures.get(),"assignedPartitions",assigned==null?0:assigned.size());}
    @Override public void close(){container.stop();}
}
