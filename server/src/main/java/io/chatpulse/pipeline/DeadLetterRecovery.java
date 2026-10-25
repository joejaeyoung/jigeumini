package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 호출자의 DB 트랜잭션 안에서 ACK를 기다린 뒤 격리 기록을 저장한다. */
public final class DeadLetterRecovery implements AutoCloseable {
    private final Admin admin;
    private final DefaultKafkaProducerFactory<String, byte[]> factory;
    private final KafkaTemplate<String, byte[]> kafka;
    private final JdbcTemplate jdbc;
    private final String topic, projection, run;
    private final Uuid topicId;
    private final Consumer<DeadLetter> afterAck;
    private final JsonMapper json = JsonMapper.builder().build();

    public DeadLetterRecovery(String bootstrap, String topic, JdbcTemplate jdbc, String projection, String run) {
        this(bootstrap, topic, jdbc, projection, run, KafkaTopicPolicy.local(), failure -> { });
    }
    public DeadLetterRecovery(String bootstrap, String topic, JdbcTemplate jdbc, String projection, String run, KafkaTopicPolicy policy) {
        this(bootstrap, topic, jdbc, projection, run, policy, failure -> { });
    }
    // ACK 직후 DB commit 전 JVM 종료를 재현하는 seam.
    DeadLetterRecovery(String bootstrap, String topic, JdbcTemplate jdbc, String projection, String run, Consumer<DeadLetter> afterAck) {
        this(bootstrap, topic, jdbc, projection, run, KafkaTopicPolicy.local(), afterAck);
    }
    private DeadLetterRecovery(String bootstrap, String topic, JdbcTemplate jdbc, String projection, String run, KafkaTopicPolicy policy, Consumer<DeadLetter> afterAck) {
        KafkaSupport.ensureTopic(bootstrap, topic + ".dlq", policy);
        this.topic = topic; this.jdbc = jdbc; this.projection = projection; this.run = run; this.afterAck = afterAck;
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", 4000, "request.timeout.ms", 3000));
        try { topicId = currentTopicId(); }
        catch (RuntimeException e) { admin.close(Duration.ofSeconds(1)); throw e; }
        factory = KafkaSupport.producerFactory(bootstrap);
        kafka = new KafkaTemplate<>(factory);
        kafka.setProducerListener(null); // 실패 본문은 고정 코드로만 소비 상태에 노출한다.
    }
    private Uuid currentTopicId() {
        try {
            Uuid id = admin.describeTopics(List.of(topic)).allTopicNames().get(4, TimeUnit.SECONDS).get(topic).topicId();
            if (id.equals(Uuid.ZERO_UUID)) throw new ProjectionRetryException("SOURCE_IDENTITY_UNKNOWN");
            return id;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw new ProjectionRetryException("SOURCE_METADATA_UNAVAILABLE");
        } catch (Exception e) { throw new ProjectionRetryException("SOURCE_METADATA_UNAVAILABLE"); }
    }
    void checkSource(List<ConsumerRecord<String, byte[]>> records) {
        records.forEach(this::checkRecord);
        if (!topicId.equals(currentTopicId())) throw new ProjectionRetryException("SOURCE_IDENTITY_CHANGED");
    }
    void bindSource() {
        jdbc.update("INSERT INTO projection_source VALUES (?,?,?,?) ON CONFLICT DO NOTHING", projection, run, topic, topicId.toString());
        String bound = jdbc.queryForObject("SELECT topic_id FROM projection_source WHERE projection=? AND run_id=? AND topic=?",
                String.class, projection, run, topic);
        if (!topicId.toString().equals(bound)) throw new ProjectionRetryException("SOURCE_IDENTITY_CHANGED");
    }
    void resolve(ConsumerRecord<String, byte[]> record, ChatEvent event, EventContractException error, long deadline) {
        checkRecord(record);
        DeadLetter failure = DeadLetter.create(topicId, record, error.getMessage(), event, Instant.now());
        try {
            if (System.nanoTime() >= deadline) throw new ProjectionRetryException("DLQ_UNAVAILABLE");
            var sent = kafka.send(new ProducerRecord<>(topic + ".dlq", record.partition(), failure.failureId(), json.writeValueAsBytes(failure)));
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new ProjectionRetryException("DLQ_UNAVAILABLE");
            sent.get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw new ProjectionRetryException("DLQ_UNAVAILABLE");
        } catch (Exception e) { throw new ProjectionRetryException("DLQ_UNAVAILABLE"); }
        afterAck.accept(failure);
        jdbc.update("""
                INSERT INTO projection_failure(projection,run_id,failure_id,source_topic,topic_id,source_partition,
                    source_offset,error_code,channel_id,affected_from,affected_to)
                VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING
                """, projection, run, failure.failureId(), topic, topicId.toString(), record.partition(), record.offset(),
                failure.errorCode(), failure.channelId(), Timestamp.from(failure.affectedFrom()), Timestamp.from(failure.affectedTo()));
    }
    private void checkRecord(ConsumerRecord<String, byte[]> record) {
        if (!topic.equals(record.topic())) throw new ProjectionRetryException("SOURCE_TOPIC_MISMATCH");
    }
    @Override public void close() { factory.destroy(); admin.close(Duration.ofSeconds(2)); }
}
