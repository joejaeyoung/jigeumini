package io.chatpulse.categories;

import io.chatpulse.pipeline.KafkaSupport;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

public final class CategoryConsumer implements AutoCloseable {
    private record Poll(Instant at, Set<TopicPartition> partitions) { }
    private volatile Poll poll = new Poll(null,Set.of());
    private final KafkaMessageListenerContainer<String,byte[]> container;
    private final Admin admin;
    private final CategoryStore store;
    private final List<String> topics;
    private final Map<String,String> identities;
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong batches = new AtomicLong(), failures = new AtomicLong();
    private volatile boolean blocked;
    private volatile String state="STARTING", sourceState="UNKNOWN", lastFailure="NONE";
    public CategoryConsumer(String bootstrap,List<String> topics,String group,String reset,CategoryStore store) {
        if(topics.size()!=2 || topics.stream().distinct().count()!=2 || !Set.of("none","earliest").contains(reset)) throw new IllegalArgumentException("CATEGORY_CONSUMER_CONFIGURATION");
        this.topics=List.copyOf(topics); this.store=store;
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"default.api.timeout.ms",5000,"request.timeout.ms",3000));
        try { identities=describe(); store.bindSources(identities); }
        catch(RuntimeException failure) { admin.close(Duration.ofSeconds(1)); throw failure; }
        var factory=KafkaSupport.consumerFactory(bootstrap,group,reset);
        factory.addPostProcessor(consumer -> {
            @SuppressWarnings("unchecked") var wrapped=(org.apache.kafka.clients.consumer.Consumer<String,byte[]>)java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.kafka.clients.consumer.Consumer.class.getClassLoader(),new Class<?>[]{org.apache.kafka.clients.consumer.Consumer.class},(proxy,method,args)->{
                        try {
                            Object result=method.invoke(consumer,args);
                            if(method.getName().equals("poll"))poll=new Poll(Instant.now(),Set.copyOf(consumer.assignment()));
                            return result;
                        } catch(java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            return wrapped;
        });
        var props=new ContainerProperties(topics.toArray(String[]::new));
        props.setAckMode(ContainerProperties.AckMode.BATCH); props.setSyncCommits(true); props.setPollTimeout(1000); props.setShutdownTimeout(10000);
        props.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?,?> consumer,Collection<TopicPartition> partitions) {
                var committed=consumer.committed(new HashSet<>(partitions)); var initial=new HashMap<TopicPartition,OffsetAndMetadata>();
                for(var partition:partitions) {
                    long position=consumer.position(partition); store.initialize(partition,position);
                    if(committed.get(partition)==null && reset.equals("earliest"))initial.put(partition,new OffsetAndMetadata(position));
                }
                if(!initial.isEmpty())consumer.commitSync(initial);
            }
            @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { poll=new Poll(null,Set.of()); }
        });
        props.setMessageListener((BatchMessageListener<String,byte[]>) records -> {
            try {
                verifySources(); store.apply(records); blocked=false; state="RUNNING"; lastFailure="NONE"; batches.incrementAndGet();
            } catch(RuntimeException failure) {
                blocked=true; state="RETRYING"; failures.incrementAndGet();lastFailure=code(failure);
                throw new IllegalStateException(lastFailure);
            }
        });
        container=new KafkaMessageListenerContainer<>(factory,props); container.setBeanName("category-projection");
        var handler=new DefaultErrorHandler(new FixedBackOff(1000,FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class,true),true); container.setCommonErrorHandler(handler);
    }
    private Map<String,String> describe() {
        try {
            var values=admin.describeTopics(topics).allTopicNames().get(5,TimeUnit.SECONDS); var result=new HashMap<String,String>();
            for(String topic:topics) {
                if(values.get(topic).partitions().size()!=3)throw new IllegalStateException("CATEGORY_SOURCE_CHANGED");
                result.put(topic,values.get(topic).topicId().toString());
            }
            return Map.copyOf(result);
        } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("CATEGORY_SOURCE_UNAVAILABLE"); }
        catch(Exception ignored) { throw new IllegalStateException("CATEGORY_SOURCE_UNAVAILABLE"); }
    }
    private void verifySources() { if(!identities.equals(describe()))throw new IllegalStateException("CATEGORY_SOURCE_CHANGED"); }
    private static String code(RuntimeException failure) {
        if(failure.getMessage()!=null&&Set.of("CATEGORY_SOURCE_CHANGED","CATEGORY_SOURCE_UNAVAILABLE","CATEGORY_INPUT_TOPIC","CATEGORY_FUTURE_TIME","CATEGORY_OBSERVATION_CONFLICT","CATEGORY_EVENT_CONFLICT").contains(failure.getMessage()))return failure.getMessage();
        if(failure instanceof org.springframework.dao.DataAccessException || failure instanceof org.springframework.transaction.TransactionException)return "CATEGORY_DB_UNAVAILABLE";
        if(failure instanceof IllegalArgumentException || failure instanceof io.chatpulse.pipeline.EventContractException)return "CATEGORY_INVALID_EVENT";
        return "CATEGORY_PROCESSING_UNAVAILABLE";
    }
    public void start() {
        container.start(); sampler.scheduleWithFixedDelay(this::sample,0,5,TimeUnit.SECONDS);
        sampler.scheduleWithFixedDelay(()->{try { store.cleanup(); } catch(RuntimeException ignored) { org.slf4j.LoggerFactory.getLogger(CategoryConsumer.class).warn("CATEGORY_CLEANUP_UNAVAILABLE"); }},60,60,TimeUnit.SECONDS);
    }
    public void sample() {
        var snapshot=poll; if(snapshot.partitions().isEmpty())return;
        try {
            verifySources(); var request=new HashMap<TopicPartition,OffsetSpec>(); snapshot.partitions().forEach(p->request.put(p,OffsetSpec.latest()));
            Instant at=Instant.now(); var ends=admin.listOffsets(request).all().get(5,TimeUnit.SECONDS);
            for(var entry:ends.entrySet())store.observe(entry.getKey(),entry.getValue().offset(),at,snapshot.at(),blocked);
            sourceState="AVAILABLE";
        } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); sourceState="UNAVAILABLE"; }
        catch(Exception ignored) { sourceState="UNAVAILABLE"; try { store.unavailable(snapshot.partitions()); } catch(RuntimeException unavailable) { /* freshness 만료로 조회 보류 */ } }
    }
    public Map<String,Object> status() {
        return Map.of("running",container.isRunning(),"state",state.equals("STARTING")&&!poll.partitions().isEmpty()?"WAITING_FOR_EVENTS":state,
                "sourceState",sourceState,"lastFailure",lastFailure,"assignedPartitions",poll.partitions().size(),"successfulBatches",batches.get(),"failedAttempts",failures.get());
    }
    @Override public void close() {
        sampler.shutdownNow();
        try { store.unavailable(poll.partitions()); } catch(RuntimeException ignored) { }
        container.stop(); admin.close(Duration.ofSeconds(1));
    }
}
