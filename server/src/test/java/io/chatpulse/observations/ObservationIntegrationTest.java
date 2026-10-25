package io.chatpulse.observations;

import io.chatpulse.pipeline.KafkaSupport;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ObservationIntegrationTest {
    static final String BOOTSTRAP = "localhost:19092", CHANNEL = "a".repeat(32);
    private String schema, topic, group;
    private JdbcTemplate jdbc;
    private ObservationStore store;
    static PGSimpleDataSource ds(String schema) {
        var ds = new PGSimpleDataSource(); ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");
        ds.setUser("chatpulse"); ds.setPassword("local-chatpulse"); ds.setCurrentSchema(schema);
        ds.setConnectTimeout(3); ds.setSocketTimeout(10); return ds;
    }
    static ObservationStore store(String schema, Clock clock) {
        var ds = ds(schema); var tx = new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        return new ObservationStore(new JdbcTemplate(ds), tx, "test-v1", clock);
    }
    @BeforeEach void setup() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        schema = "it_obs_" + suffix; topic = "it.observations." + suffix; group = topic + "-group";
        new JdbcTemplate(ds("public")).execute("CREATE SCHEMA " + schema);
        org.flywaydb.core.Flyway.configure().dataSource(ds(schema)).defaultSchema(schema).load().migrate();
        jdbc = new JdbcTemplate(ds(schema)); store = store(schema, Clock.systemUTC());
        KafkaSupport.ensureTopic(BOOTSTRAP, topic);
    }
    @AfterEach void cleanup() throws Exception {
        new JdbcTemplate(ds("public")).execute("DROP SCHEMA " + schema + " CASCADE");
        try (var admin = Admin.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            if (admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream().anyMatch(g -> g.groupId().equals(group)))
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        }
    }
    private ConsumerRecord<String,byte[]> record(BroadcastObserved event, long offset) {
        return new ConsumerRecord<>(topic, 0, offset, event.channelId(), event.encode());
    }
    private long count() { return jdbc.queryForObject("SELECT count(*) FROM broadcast_observation", Long.class); }
    private long progress() { return jdbc.queryForObject("SELECT max(next_offset) FROM observation_progress", Long.class); }
    private ObservationStore.History history() { return store.history(CHANNEL, Instant.now().minusSeconds(3600), Instant.now().plusSeconds(1)); }
    @Test void observationAtNanosecondMinuteBoundaryStaysInItsOriginalWindow() {
        var boundary = Instant.parse("2026-10-25T12:00:00Z");
        store = store(schema, Clock.fixed(boundary.plusSeconds(10), ZoneOffset.UTC));
        var before = BroadcastObservedTest.sample(boundary.minusNanos(1), 10L);
        var after = BroadcastObservedTest.sample(boundary.plusNanos(1), 20L);
        store.apply(List.of(record(before,0),record(after,1)));
        assertEquals(List.of(before), store.history(CHANNEL,boundary.minusSeconds(60),boundary).samples());
        assertEquals(List.of(after), store.history(CHANNEL,boundary,boundary.plusSeconds(60)).samples());
    }
    @Test void reorderedDuplicateAndNullObservationsRemainActualPointsAndConflictRollsBackBatch() {
        var now = Instant.now().minusSeconds(1);
        var first = BroadcastObservedTest.sample(now.minusSeconds(120), 0L);
        var gap = BroadcastObservedTest.sample(now.minusSeconds(60), null);
        var last = BroadcastObservedTest.sample(now, 400L);
        store.apply(List.of(record(last,0),record(first,1),record(gap,2),record(first,3)));
        assertEquals(List.of(first,gap,last), history().samples()); assertEquals(3, count()); assertEquals(4, progress());
        store.apply(List.of(record(first,1),record(last,0))); assertEquals(4, progress());
        var conflict = new BroadcastObserved(last.schemaVersion(),last.eventId(),last.runId(),last.channelId(),last.observedAt(),last.ttlSeconds(),401L,
                last.liveStatus(),last.quality(),last.sourceBroadcastId(),last.broadcastIdentity(),last.categoryId(),last.categoryName());
        assertEquals("OBSERVATION_ID_CONFLICT", assertThrows(IllegalArgumentException.class,
                () -> store.apply(List.of(record(BroadcastObservedTest.sample(now, 5L),4),record(conflict,5)))).getMessage());
        assertEquals(3, count()); assertEquals(4, progress());
        jdbc.execute("CREATE FUNCTION reject_observation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_PRIVATE_SQL'; END $$");
        jdbc.execute("CREATE TRIGGER reject_observation BEFORE INSERT ON observation_progress FOR EACH ROW EXECUTE FUNCTION reject_observation()");
        assertThrows(RuntimeException.class, () -> store.apply(List.of(record(BroadcastObservedTest.sample(now, 5L),4))));
        assertEquals(3, count()); assertEquals(4, progress());
        jdbc.execute("DROP TRIGGER reject_observation ON observation_progress");
        assertEquals("OBSERVATION_FUTURE_TIME", assertThrows(IllegalArgumentException.class,
                () -> store.apply(List.of(record(BroadcastObservedTest.sample(Instant.now().plusSeconds(10), 5L),4)))).getMessage());
    }
    @Test void sevenDayCleanupDoesNotResurrectExpiredHistoryOnRedeliveryOrTouchLiveLedger() {
        var clock = new ObservationCollectorTest.MutableClock(); store = store(schema, clock);
        var boundary = BroadcastObservedTest.sample(clock.instant().minusSeconds(7 * 86400), 0L);
        var expired = BroadcastObservedTest.sample(boundary.observedAt().minusNanos(1), 5L);
        store.apply(List.of(record(boundary,0),record(expired,1)));
        var old=BroadcastObserved.observed("old-run",CHANNEL,expired.observedAt(),120,
                new io.chatpulse.metadata.PublicBroadcastObservation(io.chatpulse.metadata.BroadcastMetadata.LiveStatus.LIVE,5L,null,null,null,io.chatpulse.metadata.PublicBroadcastObservation.Quality.OBSERVED));
        jdbc.update("INSERT INTO broadcast_observation VALUES (?,?,?,?,?,?::jsonb)",old.runId(),old.eventId(),CHANNEL,
                java.sql.Timestamp.from(old.observedAt()),java.sql.Timestamp.from(old.observedAt()),new String(old.encode(),java.nio.charset.StandardCharsets.UTF_8));
        jdbc.update("INSERT INTO processed_event(projection,run_id,event_id,channel_id,collection_session_id,event_time,received_at,source_topic,source_partition,source_offset,payload_hash) VALUES ('ranking','live-v1',?,?,?,now(),now(),?,0,0,?)",
                UUID.randomUUID(),CHANNEL,UUID.randomUUID(),topic,"a".repeat(64));
        assertEquals(2, count()); assertEquals(2, progress()); assertEquals(1, store.cleanup()); assertEquals(1,count());
        clock.advance(1); assertEquals(1, store.cleanup());
        store.apply(List.of(record(boundary,0),record(expired,1))); assertEquals(0, count()); assertEquals(2, progress());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM processed_event", Integer.class));
    }
    @Test void springCollectorConfigurationConnectsFreshHttpThroughKafkaToHistory() throws Exception {
        try(var http=new io.chatpulse.metadata.ObservationHttpFixture(CHANNEL);
            var context=new org.springframework.context.annotation.AnnotationConfigApplicationContext();
            var consumer=new ObservationConsumer(BOOTSTRAP,topic,group,"earliest",store)) {
            var pipeline=org.mockito.Mockito.mock(io.chatpulse.pipeline.PipelineProperties.class);
            org.mockito.Mockito.when(pipeline.bootstrapServers()).thenReturn(BOOTSTRAP);
            org.mockito.Mockito.when(pipeline.runId()).thenReturn("test-v1");
            context.getEnvironment().setActiveProfiles("pipeline");
            context.getEnvironment().getPropertySources().addFirst(new org.springframework.core.env.MapPropertySource("role",Map.of("pipeline.role","collector")));
            context.getEnvironment().getPropertySources().addFirst(new org.springframework.core.env.SystemEnvironmentPropertySource("synthetic-env",
                    Map.of("OBSERVATIONS_ENABLED","true","OBSERVATIONS_INTERVAL_SECONDS","60","OBSERVATIONS_TTL_SECONDS","120")));
            context.registerBean(io.chatpulse.pipeline.PipelineProperties.class,()->pipeline);
            context.registerBean(io.chatpulse.poc.PocProperties.class,()->new io.chatpulse.poc.PocProperties(List.of(CHANNEL),Duration.ofSeconds(3),Duration.ZERO,Path.of("build","synthetic-poc.json")));
            context.registerBean(ObservationConfiguration.ObservationTopic.class,()->new ObservationConfiguration.ObservationTopic(topic));
            context.registerBean(io.chatpulse.metadata.PublicObservationSource.class,http::source);
            context.registerBean(io.chatpulse.poc.ChatSource.class,()->org.mockito.Mockito.mock(io.chatpulse.poc.ChatSource.class));
            var telemetry=org.mockito.Mockito.mock(io.chatpulse.poc.CollectionObserver.class);
            context.registerBean("telemetryFixture",io.chatpulse.poc.CollectionObserver.class,()->telemetry);
            context.register(ObservationConfiguration.CollectorConfiguration.class,io.chatpulse.poc.PocRunner.class);
            context.refresh(); assertNotNull(context.getBean(io.chatpulse.poc.PocRunner.class));
            var observers=context.getBeanProvider(io.chatpulse.poc.CollectionObserver.class).orderedStream().toList();
            assertEquals(2,observers.size()); assertEquals(0,http.requests.get());
            consumer.start(); awaitCommitted(0);
            var combined=io.chatpulse.poc.CollectionObserver.combine(observers); combined.start(CHANNEL);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertEquals(1,count()));
            combined.stopped(CHANNEL); awaitCommitted(1);
            var sample=history().samples().getFirst(); assertEquals(73L,sample.viewerCount()); assertEquals("123",sample.sourceBroadcastId());
            assertEquals(120,sample.ttlSeconds()); assertEquals(1,http.requests.get());
            org.mockito.Mockito.verify(telemetry).start(CHANNEL); org.mockito.Mockito.verify(telemetry).stopped(CHANNEL);
        }
    }
    @Test void realKafkaPublisherRestartAndDbRetryCommitOnlyPersistedObservations() throws Exception {
        try (var first = new ObservationConsumer(BOOTSTRAP,topic,group,"earliest",store)) {
            first.start(); awaitCommitted(0); assertEquals("WAITING_FOR_EVENTS", first.status().get("state"));
        }
        var now = Instant.now().minusSeconds(1); var later = BroadcastObservedTest.sample(now,20L); var earlier = BroadcastObservedTest.sample(now.minusSeconds(60),null);
        try (var publisher = new ObservationPublisher(BOOTSTRAP,topic)) { publisher.accept(later); publisher.accept(earlier); publisher.accept(later); }
        assertEquals(0, count());
        jdbc.execute("CREATE FUNCTION reject_observation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_PRIVATE_SQL'; END $$");
        jdbc.execute("CREATE TRIGGER reject_observation BEFORE INSERT ON observation_progress FOR EACH ROW EXECUTE FUNCTION reject_observation()");
        try (var restarted = new ObservationConsumer(BOOTSTRAP,topic,group,"none",store)) {
            restarted.start(); await().atMost(Duration.ofSeconds(20)).until(() -> ((Long) restarted.status().get("failedAttempts")) > 0);
            assertEquals(0, count()); assertCommitted(0);
            jdbc.execute("DROP TRIGGER reject_observation ON observation_progress");
            awaitCommitted(3); assertEquals(List.of(earlier,later), history().samples()); assertEquals("RUNNING", restarted.status().get("state"));
        }
        try (var restarted = new ObservationConsumer(BOOTSTRAP,topic,group,"none",store); var publisher = new ObservationPublisher(BOOTSTRAP,topic)) {
            restarted.start(); publisher.accept(later); awaitCommitted(4); assertEquals(2, count());
        }
    }
    @Test void processExitAfterDbCommitLeavesKafkaOffsetAndRestartDoesNotDuplicate() throws Exception {
        try (var publisher = new ObservationPublisher(BOOTSTRAP,topic)) { publisher.accept(BroadcastObservedTest.sample(Instant.now(), 10L)); }
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),
                "-cp",System.getProperty("integration.classpath"),ObservationCrashProcess.class.getName(),schema,topic,group)
                .redirectErrorStream(true).redirectOutput(Path.of("build","observation-crash-"+schema+".log").toFile()).start();
        try { assertTrue(process.waitFor(40,TimeUnit.SECONDS)); assertEquals(97,process.exitValue()); }
        finally { if (process.isAlive()) process.destroyForcibly().waitFor(); }
        assertEquals(1,count()); assertCommitted(0);
        try (var restarted = new ObservationConsumer(BOOTSTRAP,topic,group,"none",store)) {
            restarted.start(); awaitCommitted(1); assertEquals(1,count());
        }
    }
    private void assertCommitted(long expected) throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
            assertEquals(3,offsets.size()); assertEquals(expected,offsets.values().stream().mapToLong(x -> x.offset()).sum());
        }
    }
    private void awaitCommitted(long expected) { await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertCommitted(expected)); }
}
