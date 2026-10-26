package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class TrendRecoveryIntegrationTest {
    private static final String TREND = "ranking-trend", RUN = "test-v1", CHANNEL = "a".repeat(32);
    private String schema, topic, rankGroup, trendGroup;
    private JdbcTemplate jdbc;
    private ProjectionStore trend, ranking;
    private final EventCodec codec = new EventCodec();
    private Instant bucket;
    private enum FailurePoint { PROGRESS_UPDATE, DEFERRED_COMMIT }

    @BeforeEach void setup() {
        String id = UUID.randomUUID().toString().replace("-", "");
        schema = "it_" + id; topic = "it.trend." + id;
        rankGroup = topic + "-ranking"; trendGroup = topic + "-trend";
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("CREATE SCHEMA " + schema);
        var ds = IntegrationSupport.ds(schema);
        org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
        jdbc = new JdbcTemplate(ds);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        bucket = Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(), 10) * 10 - 30);
        var clock = Clock.fixed(bucket.plusSeconds(30), ZoneOffset.UTC);
        trend = new ProjectionStore(jdbc, tx, codec, TREND, RUN, clock);
        ranking = new ProjectionStore(jdbc, tx, codec, "ranking", RUN, clock);
    }

    @AfterEach void cleanup() throws Exception {
        try (var admin = admin()) {
            var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS).stream()
                    .filter(t -> t.equals(topic) || t.equals(topic + ".dlq")).toList();
            if (!topics.isEmpty()) admin.deleteTopics(topics).all().get(10, TimeUnit.SECONDS);
            var groups = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream()
                    .map(g -> g.groupId()).filter(g -> g.equals(rankGroup) || g.equals(trendGroup)).toList();
            if (!groups.isEmpty()) admin.deleteConsumerGroups(groups).all().get(10, TimeUnit.SECONDS);
        } finally {
            new JdbcTemplate(IntegrationSupport.ds("public")).execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    @Test void duplicatesWithinAndAcrossBatchesRemainIndependentFromRanking() {
        var a = event(bucket.plusSeconds(1)); var b = event(bucket.plusSeconds(11));
        ranking.apply(List.of(record(a, 0, 0)));
        var rankBefore = snapshot("ranking");
        assertEquals(2, trend.apply(List.of(record(a, 0, 0), record(a, 0, 1), record(b, 0, 2))));
        assertEquals(0, trend.apply(List.of(record(a, 0, 3), record(b, 0, 4))));
        assertProjection(trend, Set.of(a.eventId(), b.eventId()), List.of(1L, 1L));
        assertEquals(5L, progress(TREND, 0));
        assertEquals(0L, sourceOffset(a)); assertEquals(2L, sourceOffset(b));
        assertEquals(rankBefore, snapshot("ranking"));
    }

    @Test void identityConflictsRollbackNewRowsAndPreservePreviouslyCommittedState() {
        var a = event(bucket); var b = event(bucket.plusSeconds(10));
        trend.apply(List.of(record(a, 0, 0)));
        var before = snapshot(TREND);
        var changed = new ChatEvent(a.schemaVersion(), a.eventId(), a.source(), a.channelId(), a.chatRoomId(),
                a.collectionSessionId(), null, "CHAT", a.eventTime().plusSeconds(1), a.receivedAt(), false, "test");
        var persistedConflict = assertThrows(EventContractException.class,
                () -> trend.apply(List.of(record(b, 0, 1), record(changed, 0, 2))));
        assertEquals("EVENT_ID_CONFLICT", persistedConflict.getMessage());
        assertEquals(before, snapshot(TREND));
        // 배치 내부 충돌은 SQL 전에 거부하며, 위 persistedConflict가 실제 rollback을 검증한다.
        var batchConflict = assertThrows(EventContractException.class,
                () -> trend.apply(List.of(record(b, 0, 1), record(a, 0, 2), record(changed, 0, 3))));
        assertEquals("EVENT_ID_CONFLICT", batchConflict.getMessage());
        assertEquals(before, snapshot(TREND));
        assertEquals(1, trend.apply(List.of(record(a, 0, 1), record(b, 0, 2))));
        assertProjection(trend, Set.of(a.eventId(), b.eventId()), List.of(1L, 1L));
        assertEquals(3L, progress(TREND, 0));
    }

    @Test void sqlFailureAfterBucketWritesRollsBackTrendAndPreservesRanking() {
        var a = event(bucket); var b = event(bucket.plusSeconds(1)); var c = event(bucket.plusSeconds(10));
        trend.apply(List.of(record(a, 0, 0))); ranking.apply(List.of(record(a, 0, 0)));
        var trendBefore = snapshot(TREND); var rankBefore = snapshot("ranking");
        rejectTrendProgress(FailurePoint.DEFERRED_COMMIT);
        var input = List.of(record(b, 0, 1), record(c, 1, 0));
        Throwable failure = assertThrows(RuntimeException.class, () -> trend.apply(input));
        while (failure.getCause() != null) failure = failure.getCause();
        assertInstanceOf(SQLException.class, failure);
        assertEquals("P1002", ((SQLException) failure).getSQLState());
        assertEquals(1L, triggerAttempts());
        assertEquals(trendBefore, snapshot(TREND));
        assertEquals(rankBefore, snapshot("ranking"));
        allowTrendProgress();
        assertEquals(2, trend.apply(input)); assertEquals(0, trend.apply(input));
        assertProjection(trend, Set.of(a.eventId(), b.eventId(), c.eventId()), List.of(2L, 1L));
        assertEquals(2L, progress(TREND, 0)); assertEquals(1L, progress(TREND, 1));
        assertEquals(rankBefore, snapshot("ranking"));
    }

    @Test void trendRecoversInPlaceAndStrictRestartPreservesDuplicateIdentity() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        var a = event(bucket); var b = event(bucket.plusSeconds(1)); var c = event(bucket.plusSeconds(10));
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        var producer = new KafkaTemplate<>(factory);
        try (var admin = admin();
             var rankRecovery = new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP, topic, jdbc, "ranking", RUN);
             var trendRecovery = new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP, topic, jdbc, TREND, RUN)) {
            try (var rankConsumer = consumer(rankGroup, "earliest", ranking, rankRecovery);
                 var trendConsumer = consumer(trendGroup, "earliest", trend, trendRecovery)) {
                rankConsumer.start(); trendConsumer.start();
                awaitCommitted(admin, rankGroup, 0); awaitCommitted(admin, trendGroup, 0);
                send(producer, a);
                awaitCommitted(admin, rankGroup, 1); awaitCommitted(admin, trendGroup, 1);
                var before = snapshot(TREND);
                rejectTrendProgress(FailurePoint.PROGRESS_UPDATE);
                send(producer, b); send(producer, b); send(producer, c);
                awaitCommitted(admin, rankGroup, 4);
                await().atMost(Duration.ofSeconds(30)).until(() -> (Long) trendConsumer.status().get("failedAttempts") >= 2);
                assertTrue(triggerAttempts() >= 2);
                assertEquals("RETRYING", trendConsumer.status().get("state"));
                assertEquals("DB_OR_PROCESSING_UNAVAILABLE", trendConsumer.status().get("lastErrorCode"));
                assertEquals(0L, rankConsumer.status().get("failedAttempts"));
                assertEquals(before, snapshot(TREND));
                assertCommitted(admin, trendGroup, 1);
                assertProjection(ranking, Set.of(a.eventId(), b.eventId(), c.eventId()), List.of(2L, 1L));
                assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM projection_failure", Long.class));
                var dlqEnds = admin.listOffsets(Map.of(new TopicPartition(topic + ".dlq", 0), OffsetSpec.latest(),
                        new TopicPartition(topic + ".dlq", 1), OffsetSpec.latest(),
                        new TopicPartition(topic + ".dlq", 2), OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS);
                assertEquals(0, dlqEnds.values().stream().mapToLong(v -> v.offset()).sum());
                allowTrendProgress();
                awaitCommitted(admin, trendGroup, 4);
                assertEquals("RUNNING", trendConsumer.status().get("state"));
                assertProjection(trend, Set.of(a.eventId(), b.eventId(), c.eventId()), List.of(2L, 1L));
            }
            try (var restarted = consumer(trendGroup, "none", trend, trendRecovery)) {
                restarted.start(); send(producer, a); send(producer, b);
                awaitCommitted(admin, trendGroup, 6);
                assertProjection(trend, Set.of(a.eventId(), b.eventId(), c.eventId()), List.of(2L, 1L));
                assertEquals(6L, progress(TREND, 0));
                assertEquals(0L, sourceOffset(a)); assertEquals(1L, sourceOffset(b)); assertEquals(3L, sourceOffset(c));
                assertCommitted(admin, rankGroup, 4);
                assertProjection(ranking, Set.of(a.eventId(), b.eventId(), c.eventId()), List.of(2L, 1L));
            }
        } finally { factory.destroy(); }
    }

    @Test void committedTrendSurvivesJvmHaltAndReceivesTheSameOffsetWithoutIncrement() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP, topic);
        var a = event(bucket); var b = event(bucket.plusSeconds(10));
        var factory = KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        var producer = new KafkaTemplate<>(factory);
        try (var admin = admin()) {
            send(producer, a);
            var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("integration.classpath"), TrendAfterCommitProcess.class.getName(), schema, topic, trendGroup)
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
            try { assertTrue(child.waitFor(40, TimeUnit.SECONDS)); assertEquals(91, child.exitValue()); }
            finally { if (child.isAlive()) child.destroyForcibly().waitFor(); }
            assertEquals(1L, trend.totals().get("bucketTotal"));
            assertEquals(1L, trend.totals().get("processedEvents"));
            assertEquals(1L, progress(TREND, 0)); assertCommitted(admin, trendGroup, 0);
            var committed = snapshot(TREND);
            var seen = new CopyOnWriteArrayList<Long>();
            try (var recovery = new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP, topic, jdbc, TREND, RUN);
                 var resumed = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, trendGroup, "none"),
                         topic, trend, recovery, records -> records.forEach(r -> seen.add(r.offset())))) {
                resumed.start(); awaitCommitted(admin, trendGroup, 1);
                assertEquals(Set.of(0L), Set.copyOf(seen));
                assertEquals(committed, snapshot(TREND));
                send(producer, a); send(producer, b);
                awaitCommitted(admin, trendGroup, 3);
                assertProjection(trend, Set.of(a.eventId(), b.eventId()), List.of(1L, 1L));
                assertEquals(0L, sourceOffset(a)); assertEquals(2L, sourceOffset(b));
            }
        } finally { factory.destroy(); }
    }

    private ChatEvent event(Instant at) {
        return new ChatEvent(1, UUID.randomUUID(), "CHZZK_UNOFFICIAL", CHANNEL, "synthetic-room", UUID.randomUUID(),
                null, "CHAT", at, at, false, "test");
    }
    private ConsumerRecord<String, byte[]> record(ChatEvent event, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, CHANNEL, codec.encode(event));
    }
    private Map<String, List<Map<String, Object>>> snapshot(String projection) {
        return Map.of("ledger", jdbc.queryForList("SELECT * FROM processed_event WHERE projection=? AND run_id=? ORDER BY event_id", projection, RUN),
                "buckets", jdbc.queryForList("SELECT * FROM chat_bucket_10s WHERE projection=? AND run_id=? ORDER BY bucket_start,channel_id", projection, RUN),
                "progress", jdbc.queryForList("SELECT * FROM projection_progress WHERE projection=? AND run_id=? ORDER BY topic,partition", projection, RUN));
    }
    private long progress(String projection, int partition) {
        return jdbc.queryForObject("SELECT next_resolved_offset FROM projection_progress WHERE projection=? AND run_id=? AND topic=? AND partition=?",
                Long.class, projection, RUN, topic, partition);
    }
    private void assertProjection(ProjectionStore store, Set<UUID> ids, List<Long> counts) {
        String projection = store.totals().get("projection").toString();
        assertEquals((long) ids.size(), store.totals().get("processedEvents"));
        assertEquals((long) ids.size(), store.totals().get("bucketTotal"));
        assertEquals(ids, Set.copyOf(jdbc.query("SELECT event_id FROM processed_event WHERE projection=? AND run_id=?",
                (rs, n) -> rs.getObject(1, UUID.class), projection, RUN)));
        assertEquals(counts, jdbc.queryForList("SELECT chat_count FROM chat_bucket_10s WHERE projection=? AND run_id=? ORDER BY bucket_start", Long.class, projection, RUN));
        assertEquals(List.of(Timestamp.from(bucket), Timestamp.from(bucket.plusSeconds(10))),
                jdbc.queryForList("SELECT bucket_start FROM chat_bucket_10s WHERE projection=? AND run_id=? ORDER BY bucket_start", Timestamp.class, projection, RUN));
    }
    private long sourceOffset(ChatEvent event) {
        return jdbc.queryForObject("SELECT source_offset FROM processed_event WHERE projection=? AND run_id=? AND event_id=?",
                Long.class, TREND, RUN, event.eventId());
    }
    private long triggerAttempts() {
        return jdbc.queryForObject("SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM trend_failure_attempts", Long.class);
    }
    private void rejectTrendProgress(FailurePoint point) {
        jdbc.execute("CREATE SEQUENCE trend_failure_attempts");
        // COMMIT 시점에 세 저장 영역의 실제 새 값을 확인한 뒤 실패시킨다.
        String writtenState = point == FailurePoint.DEFERRED_COMMIT ? """
                    IF (SELECT count(*) FROM processed_event WHERE projection=NEW.projection AND run_id=NEW.run_id) IS DISTINCT FROM 3
                        OR (SELECT sum(chat_count) FROM chat_bucket_10s WHERE projection=NEW.projection AND run_id=NEW.run_id) IS DISTINCT FROM 3
                        OR (SELECT next_resolved_offset FROM projection_progress WHERE projection=NEW.projection AND run_id=NEW.run_id AND topic=NEW.topic AND partition=0) IS DISTINCT FROM 2
                        OR (SELECT next_resolved_offset FROM projection_progress WHERE projection=NEW.projection AND run_id=NEW.run_id AND topic=NEW.topic AND partition=1) IS DISTINCT FROM 1 THEN
                        RAISE EXCEPTION USING ERRCODE='P1003', MESSAGE='SYNTHETIC_WRITES_NOT_VISIBLE';
                    END IF;
                """ : "";
        jdbc.execute("""
                CREATE FUNCTION reject_trend_progress() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.projection = 'ranking-trend' THEN
                """ + writtenState + """
                        PERFORM nextval('trend_failure_attempts');
                        RAISE EXCEPTION USING ERRCODE = 'P1002', MESSAGE = 'SYNTHETIC_TREND_PROGRESS_FAILURE';
                    END IF;
                    RETURN NEW;
                END $$
                """);
        String declaration = point == FailurePoint.DEFERRED_COMMIT
                ? "CREATE CONSTRAINT TRIGGER reject_trend_progress AFTER INSERT OR UPDATE ON projection_progress DEFERRABLE INITIALLY DEFERRED"
                : "CREATE TRIGGER reject_trend_progress BEFORE UPDATE ON projection_progress";
        jdbc.execute(declaration + " FOR EACH ROW EXECUTE FUNCTION reject_trend_progress()");
    }
    private void allowTrendProgress() { jdbc.execute("DROP TRIGGER reject_trend_progress ON projection_progress"); }
    private Admin admin() {
        return Admin.create(Map.of("bootstrap.servers", IntegrationSupport.BOOTSTRAP, "default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
    }
    private ProjectionConsumer consumer(String group, String reset, ProjectionStore store, DeadLetterRecovery recovery) {
        return new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, group, reset), topic, store, recovery);
    }
    private void send(KafkaTemplate<String, byte[]> producer, ChatEvent event) throws Exception {
        producer.send(topic, 0, CHANNEL, codec.encode(event)).get(15, TimeUnit.SECONDS);
    }
    private void awaitCommitted(Admin admin, String group, long position) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertCommitted(admin, group, position));
    }
    private void assertCommitted(Admin admin, String group, long position) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        assertEquals(Set.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)), offsets.keySet());
        for (var offset : offsets.entrySet()) assertEquals(offset.getKey().partition() == 0 ? position : 0, offset.getValue().offset());
    }
}
