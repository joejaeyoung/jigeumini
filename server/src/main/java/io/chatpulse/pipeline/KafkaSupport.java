package io.chatpulse.pipeline;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

public final class KafkaSupport {
    private KafkaSupport() { }
    public static void ensureTopic(String bootstrap, String topic) { ensureTopic(bootstrap, topic, KafkaTopicPolicy.local()); }
    public static void ensureTopic(String bootstrap, String topic, KafkaTopicPolicy policy) {
        try {
            var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", 10000, "request.timeout.ms", 3000));
            try {
                if (policy.durability() == KafkaTopicPolicy.Durability.HA) {
                    var features = admin.describeFeatures().featureMetadata().get(10, TimeUnit.SECONDS).finalizedFeatures();
                    var elr = features.get("eligible.leader.replicas.version");
                    if (elr == null || elr.minVersionLevel() != 1 || elr.maxVersionLevel() != 1)
                        throw new IllegalStateException("KAFKA_ELR_CONTRACT_MISMATCH");
                }
                var definition = new NewTopic(topic, policy.partitions(), policy.replicationFactor()).configs(policy.configurations());
                try { admin.createTopics(List.of(definition)).all().get(10, TimeUnit.SECONDS); }
                catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) throw e;
                }
                awaitTopic(admin, topic, policy, Duration.ofSeconds(10), System::nanoTime, Thread::sleep);
            } finally { admin.close(Duration.ofSeconds(1)); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new IllegalStateException("KAFKA_TOPIC_INIT_FAILED", interrupted);
        } catch (Exception e) { throw new IllegalStateException("KAFKA_TOPIC_INIT_FAILED", e); }
    }
    @FunctionalInterface interface Sleeper { void sleep(long millis) throws InterruptedException; }
    static void awaitTopic(Admin admin, String topic, Duration timeout, LongSupplier nanoTime, Sleeper sleeper) throws Exception {
        awaitTopic(admin, topic, KafkaTopicPolicy.local(), timeout, nanoTime, sleeper);
    }
    static void awaitTopic(Admin admin, String topic, KafkaTopicPolicy policy, Duration timeout, LongSupplier nanoTime, Sleeper sleeper) throws Exception {
        long deadline = nanoTime.getAsLong() + timeout.toNanos();
        while (true) {
            try {
                var actual = admin.describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs(remainingMillis(deadline,nanoTime)))
                        .allTopicNames().get(remainingNanos(deadline,nanoTime), TimeUnit.NANOSECONDS).get(topic);
                var resource = new org.apache.kafka.common.config.ConfigResource(
                        org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic);
                var config = admin.describeConfigs(List.of(resource), new DescribeConfigsOptions().timeoutMs(remainingMillis(deadline,nanoTime)))
                        .all().get(remainingNanos(deadline,nanoTime), TimeUnit.NANOSECONDS).get(resource);
                policy.verify(actual, config);
                return;
            } catch (KafkaTopicPolicy.NotReady notWritable) {
                sleeper.sleep(Math.min(50,remainingMillis(deadline,nanoTime)));
            } catch (ExecutionException error) {
                // 생성 ACK와 broker의 조회 가능 시점은 다르다. 관측한 일시 오류만 다시 조회한다.
                if (!(error.getCause() instanceof UnknownTopicOrPartitionException)) throw error;
                sleeper.sleep(Math.min(50,remainingMillis(deadline,nanoTime)));
            }
        }
    }
    private static long remainingNanos(long deadline, LongSupplier nanoTime) throws TimeoutException {
        long remaining = deadline - nanoTime.getAsLong();
        if (remaining <= 0) throw new TimeoutException("TOPIC_VISIBILITY_DEADLINE");
        return remaining;
    }
    private static int remainingMillis(long deadline, LongSupplier nanoTime) throws TimeoutException {
        return (int)Math.min(Integer.MAX_VALUE, Math.max(1,TimeUnit.NANOSECONDS.toMillis(remainingNanos(deadline,nanoTime))));
    }
    public static DefaultKafkaProducerFactory<String, byte[]> producerFactory(String bootstrap) {
        var settings = new HashMap<String, Object>();
        settings.put("bootstrap.servers", bootstrap);
        settings.put("acks", "all");
        settings.put("enable.idempotence", true);
        settings.put("max.in.flight.requests.per.connection", 1);
        settings.put("delivery.timeout.ms", 10000);
        settings.put("request.timeout.ms", 3000);
        settings.put("max.block.ms", 2000);
        settings.put("buffer.memory", 8 * 1024 * 1024);
        settings.put("linger.ms", 5);
        var factory = new DefaultKafkaProducerFactory<>(settings, new StringSerializer(), new ByteArraySerializer());
        factory.setPhysicalCloseTimeout(2);
        return factory;
    }
    public static DefaultKafkaConsumerFactory<String, byte[]> consumerFactory(String bootstrap, String group, String reset) {
        var settings = new HashMap<String, Object>();
        settings.put("bootstrap.servers", bootstrap);
        settings.put("group.id", group);
        settings.put("enable.auto.commit", false);
        settings.put("auto.offset.reset", reset);
        settings.put("max.poll.records", 200);
        settings.put("max.poll.interval.ms", 300000);
        settings.put("fetch.max.wait.ms", 100);
        settings.put("default.api.timeout.ms", 10000);
        settings.put("request.timeout.ms", 5000);
        settings.put("group.protocol", "classic");
        settings.put("session.timeout.ms", 6000);
        settings.put("heartbeat.interval.ms", 2000);
        return new DefaultKafkaConsumerFactory<>(settings, new StringDeserializer(), new ByteArrayDeserializer());
    }
}
