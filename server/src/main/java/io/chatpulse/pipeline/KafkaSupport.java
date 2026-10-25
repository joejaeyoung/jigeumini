package io.chatpulse.pipeline;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

public final class KafkaSupport {
    private KafkaSupport() { }
    public static void ensureTopic(String bootstrap, String topic) {
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", 10000, "request.timeout.ms", 3000))) {
            var definition = new NewTopic(topic, 3, (short) 1).configs(Map.of(
                    TopicConfig.RETENTION_MS_CONFIG, "86400000",
                    TopicConfig.RETENTION_BYTES_CONFIG, "268435456",
                    TopicConfig.SEGMENT_BYTES_CONFIG, "33554432",
                    TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"));
            try { admin.createTopics(List.of(definition)).all().get(10, TimeUnit.SECONDS); }
            catch (java.util.concurrent.ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) throw e;
            }
            var actual = admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
            var resource = new org.apache.kafka.common.config.ConfigResource(
                    org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic);
            var config = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
            if (!"CreateTime".equals(config.get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).value())) {
                throw new IllegalStateException("M1 requires CreateTime timestamps");
            }
            if (actual.partitions().size() != 3) throw new IllegalStateException("M1 requires 3 partitions");
        } catch (Exception e) { throw new IllegalStateException("KAFKA_TOPIC_INIT_FAILED", e); }
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
