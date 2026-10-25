package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration")
class PipelineIntegrationTest {
    private String schema, topic, group;
    private ProjectionStore store;
    private JdbcTemplate jdbc;
    private final EventCodec codec = new EventCodec();
    @BeforeEach void setup() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        schema = "it_" + suffix;
        topic = "it.chat." + suffix;
        group = "it-group-" + suffix;
        jdbc = new JdbcTemplate(IntegrationSupport.ds("public"));
        jdbc.execute("CREATE SCHEMA " + schema);
        var ds = IntegrationSupport.ds(schema);
        org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
        jdbc = new JdbcTemplate(ds);
        store = IntegrationSupport.store(schema);
    }
    @AfterEach void cleanup() throws Exception {
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("DROP SCHEMA " + schema + " CASCADE");
        try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
            if (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic)) admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            if (admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream().anyMatch(g -> g.groupId().equals(group))) {
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }
    private ConsumerRecord<String, byte[]> record(ChatEvent e, long offset) {
        return new ConsumerRecord<>(topic, 0, offset, e.channelId(), codec.encode(e));
    }
    @Test void duplicatesAcrossBatchAndRedeliveryOnlyCountOnceAndUseEventTime() {
        Instant time = Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(), 10) * 10 - 10);
        var a = EventCodecTest.event(time.plusSeconds(9));
        var b = EventCodecTest.event(time.plusSeconds(10));
        assertEquals(2, store.apply(List.of(record(a, 0), record(a, 1), record(b, 2))));
        assertEquals(0, store.apply(List.of(record(a, 0), record(b, 2))));
        assertEquals(2L, store.totals().get("bucketTotal"));
        assertEquals(List.of(time, time.plusSeconds(10)), jdbc.query("SELECT bucket_start FROM chat_bucket_10s ORDER BY bucket_start",
                (rs, n) -> rs.getTimestamp(1).toInstant()));
        assertEquals(3L, jdbc.queryForObject("SELECT next_resolved_offset FROM projection_progress", Long.class));
    }
    @Test void failureAfterBucketUpdateRollsBackLedgerBucketAndProgress() {
        jdbc.execute("CREATE FUNCTION reject_progress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$");
        jdbc.execute("CREATE TRIGGER reject_progress BEFORE INSERT ON projection_progress FOR EACH ROW EXECUTE FUNCTION reject_progress()");
        var e = EventCodecTest.event(Instant.now());
        assertThrows(RuntimeException.class, () -> store.apply(List.of(record(e, 0))));
        assertEquals(0L, store.totals().get("processedEvents"));
        assertEquals(0L, store.totals().get("bucketTotal"));
        assertEquals(0, store.progress().size());
        jdbc.execute("DROP TRIGGER reject_progress ON projection_progress");
        assertEquals(1, store.apply(List.of(record(e, 0))));
    }
    @Test void conflictingIdRollsBackNewEventsInSameTransaction() {
        var e = EventCodecTest.event(Instant.now());
        store.apply(List.of(record(e, 0)));
        var conflict = new ChatEvent(e.schemaVersion(), e.eventId(), e.source(), e.channelId(), e.chatRoomId(),
                e.collectionSessionId(), null, "CHAT", e.eventTime().minusSeconds(1), e.receivedAt(), false, "test");
        assertThrows(EventContractException.class, () -> store.apply(List.of(record(EventCodecTest.event(Instant.now()), 1), record(conflict, 2))));
        assertEquals(1L, store.totals().get("processedEvents"));
        assertEquals(1L, store.totals().get("bucketTotal"));
        assertThrows(EventContractException.class, () -> store.apply(List.of(record(e, 3), record(conflict, 4))));
    }
    @Test void realBrokerPublisherAndConsumerProjectAndCommit() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try (var consumer = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "earliest"), topic, store);
             var publisher = new BufferedKafkaPublisher(new KafkaTemplate<>(factory), topic, codec, 100)) {
            consumer.start();
            var e = EventCodecTest.event(Instant.now());
            publisher.accept(List.of(e, e, EventCodecTest.event(Instant.now())));
            assertEquals(true, publisher.finish().get("complete"));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(2L, store.totals().get("bucketTotal")));
            awaitCommitted(3);
        } finally { factory.destroy(); }
    }
    @Test void jvmCrashAfterDbCommitBeforeOffsetCommitRedeliversWithoutIncrement() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try {
            new KafkaTemplate<>(factory).send(topic, EventCodecTest.CHANNEL, codec.encode(EventCodecTest.event(Instant.now()))).get(15, TimeUnit.SECONDS);
        } finally { factory.destroy(); }
        Path log = Path.of("build", "crash-" + schema + ".log");
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("integration.classpath"), CrashAfterCommitProcess.class.getName(), schema, topic, group)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(40, TimeUnit.SECONDS), "crash child timeout");
            assertEquals(91, process.exitValue());
        } finally { if (process.isAlive()) process.destroyForcibly().waitFor(); }
        assertEquals(1L, store.totals().get("bucketTotal"));
        try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            assertTrue(offsets.values().stream().allMatch(x -> x.offset() == 0));
        }
        try (var consumer = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "earliest"), topic, store)) {
            consumer.start();
            awaitCommitted(1);
            assertEquals(1L, store.totals().get("processedEvents"));
            assertEquals(1L, store.totals().get("bucketTotal"));
        }
    }
    @Test void transientDbFailureRetriesWithoutSkippingOffset() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        jdbc.execute("CREATE FUNCTION reject_progress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test unavailable'; END $$");
        jdbc.execute("CREATE TRIGGER reject_progress BEFORE INSERT ON projection_progress FOR EACH ROW EXECUTE FUNCTION reject_progress()");
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try (var consumer = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "earliest"), topic, store)) {
            consumer.start();
            new KafkaTemplate<>(factory).send(topic, EventCodecTest.CHANNEL, codec.encode(EventCodecTest.event(Instant.now()))).get(15, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(20)).until(() -> ((Long) consumer.status().get("failedAttempts")) > 0);
            assertEquals(0L, store.totals().get("bucketTotal"));
            try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                assertTrue(offsets.values().stream().allMatch(x -> x.offset() == 0));
            }
            jdbc.execute("DROP TRIGGER reject_progress ON projection_progress");
            awaitCommitted(1);
            assertEquals(1L, store.totals().get("bucketTotal"));
            assertEquals("RUNNING", consumer.status().get("state"));
        } finally { factory.destroy(); }
    }
    @Test void malformedEventBlocksBatchAndDoesNotAdvanceOffset() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try (var consumer = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "earliest"), topic, store)) {
            new KafkaTemplate<>(factory).send(topic, EventCodecTest.CHANNEL, "invalid".getBytes()).get(15, TimeUnit.SECONDS);
            consumer.start();
            await().atMost(Duration.ofSeconds(20)).until(() -> ((Long) consumer.status().get("failedAttempts")) >= 2);
            assertEquals("BLOCKED_INVALID_EVENT", consumer.status().get("state"));
            assertEquals(0L, store.totals().get("processedEvents"));
            assertTrue(store.progress().isEmpty());
            try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                assertTrue(offsets.values().stream().allMatch(x -> x.offset() == 0));
            }
        } finally { factory.destroy(); }
    }
    @Test void explicitFirstStartInitializesEmptyPartitionsForStrictRestart() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        try (var first = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "earliest"), topic, store)) {
            first.start();
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
                    var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                    assertEquals(3, offsets.size());
                    assertTrue(offsets.values().stream().allMatch(x -> x.offset() == 0));
                }
            });
        }
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try (var restarted = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, "none"), topic, store)) {
            restarted.start();
            new KafkaTemplate<>(factory).send(topic, EventCodecTest.CHANNEL, codec.encode(EventCodecTest.event(Instant.now()))).get(15, TimeUnit.SECONDS);
            awaitCommitted(1);
            assertEquals(1L, store.totals().get("bucketTotal"));
        } finally { factory.destroy(); }
    }
    private void awaitCommitted(long total) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            try (var admin = Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP))) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                assertEquals(total, offsets.values().stream().mapToLong(x -> x.offset()).sum());
            }
        });
    }
}
