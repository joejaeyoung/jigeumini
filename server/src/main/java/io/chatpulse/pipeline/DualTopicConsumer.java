package io.chatpulse.pipeline;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.*;

public class DualTopicConsumer implements AutoCloseable {
    private final ConsumerPollTracker poll = new ConsumerPollTracker();
    private final KafkaMessageListenerContainer<String,byte[]> container;
    private final Admin admin;
    private final DualTopicProjection store;
    private final String prefix;
    private final List<String> topics;
    private final Map<String,String> identities;
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong batches = new AtomicLong(), failures = new AtomicLong();
    private volatile boolean blocked;
    private volatile String state="STARTING", sourceState="UNKNOWN", lastFailure="NONE";
    public DualTopicConsumer(String bootstrap,List<String> topics,String group,String reset,DualTopicProjection store,String prefix) {
        if(!Set.of("CATEGORY","REPORT").contains(prefix))throw new IllegalArgumentException("PROJECTION_PREFIX");
        this.prefix=prefix;
        if(topics.size()!=2 || topics.stream().distinct().count()!=2 || !Set.of("none","earliest").contains(reset)) throw new IllegalArgumentException(prefix+"_CONSUMER_CONFIGURATION");
        this.topics=List.copyOf(topics); this.store=store;
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"default.api.timeout.ms",5000,"request.timeout.ms",3000));
        try { identities=describe(); store.bindSources(identities); }
        catch(RuntimeException failure) { admin.close(Duration.ofSeconds(1)); throw failure; }
        var factory=KafkaSupport.consumerFactory(bootstrap,group,reset);
        factory.addPostProcessor(poll::track);
        var rebalance = BatchConsumers.initialOffsets(reset, store::initialize, poll::revoked);
        BatchMessageListener<String, byte[]> listener = records -> {
            try {
                verifySources(); store.apply(records); blocked=false; state="RUNNING"; lastFailure="NONE"; batches.incrementAndGet();
            } catch(RuntimeException failure) {
                blocked=true; state="RETRYING"; failures.incrementAndGet();lastFailure=code(failure);
                throw new IllegalStateException(lastFailure);
            }
        };
        container = BatchConsumers.create(factory, prefix.toLowerCase(Locale.ROOT) + "-projection",
                topics.toArray(String[]::new), rebalance, listener);
    }
    private Map<String,String> describe() {
        try {
            var values=admin.describeTopics(topics).allTopicNames().get(5,TimeUnit.SECONDS); var result=new HashMap<String,String>();
            for(String topic:topics) {
                if(values.get(topic).partitions().size()!=3)throw new IllegalStateException(prefix+"_SOURCE_CHANGED");
                result.put(topic,values.get(topic).topicId().toString());
            }
            return Map.copyOf(result);
        } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(prefix+"_SOURCE_UNAVAILABLE"); }
        catch(Exception ignored) { throw new IllegalStateException(prefix+"_SOURCE_UNAVAILABLE"); }
    }
    private void verifySources() { if(!identities.equals(describe()))throw new IllegalStateException(prefix+"_SOURCE_CHANGED"); }
    private String code(RuntimeException failure) {
        if(failure.getMessage()!=null&&Set.of(prefix+"_SOURCE_CHANGED",prefix+"_SOURCE_UNAVAILABLE",prefix+"_INPUT_TOPIC",prefix+"_FUTURE_TIME",prefix+"_OBSERVATION_CONFLICT",prefix+"_EVENT_CONFLICT").contains(failure.getMessage()))return failure.getMessage();
        if(failure instanceof org.springframework.dao.DataAccessException || failure instanceof org.springframework.transaction.TransactionException)return prefix+"_DB_UNAVAILABLE";
        if(failure instanceof IllegalArgumentException || failure instanceof io.chatpulse.pipeline.EventContractException)return prefix+"_INVALID_EVENT";
        return prefix+"_PROCESSING_UNAVAILABLE";
    }
    public void start() {
        container.start(); sampler.scheduleWithFixedDelay(this::sample,0,5,TimeUnit.SECONDS);
    }
    public void sample() {
        var snapshot=poll.snapshot(); if(snapshot.partitions().isEmpty())return;
        try {
            verifySources(); var request=new HashMap<TopicPartition,OffsetSpec>(); snapshot.partitions().forEach(p->request.put(p,OffsetSpec.latest()));
            Instant at=Instant.now(); var ends=admin.listOffsets(request).all().get(5,TimeUnit.SECONDS);
            for(var entry:ends.entrySet())store.observe(entry.getKey(),entry.getValue().offset(),at,snapshot.at(),blocked);
            sourceState="AVAILABLE";
        } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); sourceState="UNAVAILABLE"; }
        catch(Exception ignored) { sourceState="UNAVAILABLE"; try { store.unavailable(snapshot.partitions()); } catch(RuntimeException unavailable) { /* freshness 만료로 조회 보류 */ } }
    }
    public Map<String,Object> status() {
        return Map.of("running",container.isRunning(),"state",state.equals("STARTING")&&!poll.snapshot().partitions().isEmpty()?"WAITING_FOR_EVENTS":state,
                "sourceState",sourceState,"lastFailure",lastFailure,"assignedPartitions",poll.snapshot().partitions().size(),"successfulBatches",batches.get(),"failedAttempts",failures.get());
    }
    @Override public void close() {
        sampler.shutdownNow();
        try { store.unavailable(poll.snapshot().partitions()); } catch(RuntimeException ignored) { }
        container.stop(); admin.close(Duration.ofSeconds(1));
    }
}
