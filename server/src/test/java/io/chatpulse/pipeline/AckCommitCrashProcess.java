package io.chatpulse.pipeline;

import io.chatpulse.db.PostgresSupport;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** commit 직전/직후 Runtime.halt로 shutdown commit을 배제한다. AUTO_POLL_AHEAD는 의도한 잘못된 처리 순서다. */
public final class AckCommitCrashProcess {
    private static Path trace;private static String boundary,topic;private static int pollCalls,pollStarted,delivered;private static boolean dbCommitted,ackRequested;
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        String bootstrap=args[0],url=args[1],group=args[3],mode=args[4],projection=args[6];topic=args[2];boundary=args[5];trace=Path.of(args[7]);
        var settings=new HashMap<String,Object>(KafkaSupport.consumerFactory(bootstrap,group,"earliest").getConfigurationProperties());
        settings.put("enable.auto.commit",mode.startsWith("AUTO"));settings.put("auto.commit.interval.ms",100);settings.put("max.poll.records",1);
        settings.put("interceptor.classes",CommitObserver.class.getName());
        try(var ds=PostgresSupport.open(url,"chatpulse","local-chatpulse",false)) {
            var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);var store=new ProjectionStore(new JdbcTemplate(ds),tx,new EventCodec(),projection,"comparison");
            var factory=new DefaultKafkaConsumerFactory<String,byte[]>(settings,new org.apache.kafka.common.serialization.StringDeserializer(),new org.apache.kafka.common.serialization.ByteArrayDeserializer());
            factory.addPostProcessor(consumer->{
                @SuppressWarnings("unchecked") var wrapped=(org.apache.kafka.clients.consumer.Consumer<String,byte[]>)java.lang.reflect.Proxy.newProxyInstance(Consumer.class.getClassLoader(),new Class<?>[]{Consumer.class},(proxy,method,values)->{
                    try{if(method.getName().equals("poll"))pollStarted++;Object result=method.invoke(consumer,values);if(method.getName().equals("poll"))pollCalls++;return result;}
                    catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
                });return wrapped;
            });
            if(mode.equals("AUTO_POLL_AHEAD")) {
                try(var consumer=factory.createConsumer()) {
                    consumer.subscribe(List.of(topic));long end=System.nanoTime()+Duration.ofSeconds(25).toNanos();
                    while(System.nanoTime()<end){var records=consumer.poll(Duration.ofMillis(100));if(!records.isEmpty()){delivered+=records.count();write("DB_WORK_DEFERRED",Map.of());}/* DB 처리 완료 전에 다음 poll을 호출하는 잘못된 worker */}
                }
            } else {
                var properties=new ContainerProperties(topic);properties.setPollTimeout(100);properties.setSyncCommits(true);
                properties.setAckMode(mode.equals("MANUAL")?ContainerProperties.AckMode.MANUAL:ContainerProperties.AckMode.BATCH);
                properties.setMessageListener((BatchAcknowledgingMessageListener<String,byte[]>)(records,ack)->{
                    delivered+=records.size();write("BEFORE_DB",Map.of());if(boundary.equals("BEFORE_DB"))Runtime.getRuntime().halt(71);
                    store.apply(records);dbCommitted=true;write("AFTER_DB",Map.of());if(boundary.equals("AFTER_DB"))Runtime.getRuntime().halt(72);
                    if(mode.equals("MANUAL")){ackRequested=true;ack.acknowledge();}
                });
                var container=new KafkaMessageListenerContainer<>(factory,properties);container.start();Thread.sleep(25000);container.stop();
            }
        }
        System.exit(2);
    }
    public static final class CommitObserver implements ConsumerInterceptor<String,byte[]> {
        public void configure(Map<String,?> config){}public ConsumerRecords<String,byte[]> onConsume(ConsumerRecords<String,byte[]> records){return records;}
        public void onCommit(Map<TopicPartition,OffsetAndMetadata> offsets) {
            long maximum=offsets.entrySet().stream().filter(e->e.getKey().topic().equals(topic)).mapToLong(e->e.getValue().offset()).max().orElse(0);
            if(maximum>0){write("AFTER_OFFSET",Map.of("committedMaximum",maximum));if(boundary.equals("AFTER_OFFSET"))Runtime.getRuntime().halt(73);}
        }
        public void close(){}
    }
    private static synchronized void write(String phase,Map<String,Object> extra) {
        var result=new LinkedHashMap<String,Object>();result.put("phase",phase);result.put("pollCallsReturned",pollCalls);result.put("pollCallsStarted",pollStarted);result.put("delivered",delivered);result.put("databaseCommitted",dbCommitted);result.put("manualAcknowledgmentRequested",ackRequested);result.putAll(extra);
        try{Files.writeString(trace,JSON.writeValueAsString(result));}catch(Exception failure){Runtime.getRuntime().halt(74);}
    }
}
