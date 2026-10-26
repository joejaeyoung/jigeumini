package io.chatpulse.reports;

import io.chatpulse.metadata.*;
import io.chatpulse.observations.*;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ReportIntegrationTest {
    static final String BOOTSTRAP="localhost:19092",CHANNEL="a".repeat(32);
    private String schema,chat,observations,group;
    private PGSimpleDataSource dataSource;
    private JdbcTemplate jdbc;
    private ReportStore store;
    private final EventCodec codec=new EventCodec();
    static PGSimpleDataSource ds(String schema) {
        var ds=new PGSimpleDataSource();ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");ds.setUser("chatpulse");ds.setPassword("local-chatpulse");ds.setCurrentSchema(schema);ds.setConnectTimeout(3);ds.setSocketTimeout(10);return ds;
    }
    private TransactionTemplate tx() { var tx=new TransactionTemplate(new DataSourceTransactionManager(dataSource));tx.setTimeout(20);return tx; }
    private ReportStore store(Clock clock) { return new ReportStore(jdbc,tx(),"test-v1",chat,observations,clock); }
    @BeforeEach void setup() {
        String suffix=UUID.randomUUID().toString().replace("-","");schema="it_report_"+suffix;chat="it.report."+suffix;observations=chat+".observations";group=chat+"-group";
        new JdbcTemplate(ds("public")).execute("CREATE SCHEMA "+schema);dataSource=ds(schema);
        org.flywaydb.core.Flyway.configure().dataSource(dataSource).defaultSchema(schema).load().migrate();jdbc=new JdbcTemplate(dataSource);store=store(Clock.systemUTC());
        KafkaSupport.ensureTopic(BOOTSTRAP,chat);KafkaSupport.ensureTopic(BOOTSTRAP,observations);
    }
    @AfterEach void cleanup() throws Exception {
        new JdbcTemplate(ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");
        try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) {
            admin.deleteTopics(List.of(chat,observations)).all().get(10,TimeUnit.SECONDS);
            var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().map(g->g.groupId()).filter(g->g.startsWith(group)).toList();
            if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);
        }
    }
    private BroadcastObserved sample(Instant at,String source,BroadcastMetadata.LiveStatus state,Long viewers) {
        var quality=state==BroadcastMetadata.LiveStatus.LIVE?(viewers==null?PublicBroadcastObservation.Quality.MISSING_VIEWER_COUNT:PublicBroadcastObservation.Quality.OBSERVED):state==BroadcastMetadata.LiveStatus.OFFLINE?PublicBroadcastObservation.Quality.OFFLINE:PublicBroadcastObservation.Quality.UPSTREAM_ERROR;
        return BroadcastObserved.observed("test-v1",CHANNEL,at,120,new PublicBroadcastObservation(state,viewers,source,null,null,quality));
    }
    private BroadcastObserved live(Instant at,String source) { return sample(at,source,BroadcastMetadata.LiveStatus.LIVE,0L); }
    private ChatEvent event(Instant at,Instant received) { return new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic-room",UUID.randomUUID(),null,"CHAT",at,received,false,"test"); }
    private ConsumerRecord<String,byte[]> record(BroadcastObserved sample,long offset) { return new ConsumerRecord<>(observations,0,offset,CHANNEL,sample.encode()); }
    private ConsumerRecord<String,byte[]> record(ChatEvent event,long offset) { return new ConsumerRecord<>(chat,0,offset,CHANNEL,codec.encode(event)); }
    private long total(UUID report) { return store.detail(report).minutes().stream().mapToLong(ReportStore.Minute::count).sum(); }
    private UUID source(String source) { return store.list(CHANNEL).reports().stream().filter(s->Objects.equals(source,s.session().sourceBroadcastId())).findFirst().orElseThrow().session().id(); }
    @Test void chatOnlyBatchesPreserveStoredTtlStateRevisionAndUnknownObservations() {
        Instant at=Instant.parse("2026-10-27T09:00:00Z");store=store(Clock.fixed(at.plusSeconds(30),ZoneOffset.UTC));
        store.apply(List.of(record(live(at,"123"),0),record(event(at.plusSeconds(10),at.plusSeconds(11)),0)));
        UUID id=source("123");var initial=store.detail(id).stored();assertEquals("LIVE",initial.session().state());
        store=store(Clock.fixed(at.plusSeconds(121),ZoneOffset.UTC));var late=event(at.plusSeconds(20),at.plusSeconds(21));
        store.apply(List.of(record(late,1)));var expired=store.detail(id).stored();
        assertEquals("UNKNOWN",expired.session().state());assertEquals(initial.revision()+2,expired.revision());
        assertEquals(at.plusSeconds(121),expired.updatedAt());assertEquals(2,total(id));
        store.apply(List.of(record(late,2)));assertEquals(expired,store.detail(id).stored());
        // 벽시계가 되돌아가도 이전 derive와 같은 저장 상태를 유지한다.
        store=store(Clock.fixed(at.plusSeconds(60),ZoneOffset.UTC));
        store.apply(List.of(record(event(at.plusSeconds(30),at.plusSeconds(31)),3)));
        var live=store.detail(id).stored();assertEquals("LIVE",live.session().state());assertEquals(expired.revision()+2,live.revision());
        store.apply(List.of(record(sample(at.plusSeconds(40),null,BroadcastMetadata.LiveStatus.UNKNOWN,null),1)));
        var unknown=store.detail(id).stored();assertEquals("UNKNOWN",unknown.session().state());
        store.apply(List.of(record(event(at.plusSeconds(45),at.plusSeconds(46)),4)));
        assertEquals(unknown,store.detail(id).stored());assertEquals(3,total(id));assertEquals(1,store.list(CHANNEL).unassigned());
    }
    @Test void knownBroadcastsKeepMidnightHistoryAndLateMetadataReassignsWithoutDuplicateCounts() {
        Instant at=Instant.parse("2026-10-25T23:58:00Z");store=store(Clock.fixed(at.plusSeconds(600),ZoneOffset.UTC));
        var initial=live(at,"123");var first=event(at.plusSeconds(10),at.plusSeconds(11));var second=event(at.plusSeconds(150),at.plusSeconds(400));
        store.apply(List.of(record(first,0),record(initial,0),record(second,1)));UUID old=source("123");assertEquals(1,total(old));assertEquals(1,store.list(CHANNEL).unassigned());
        var late=live(at.plusSeconds(90),"123");store.apply(List.of(record(late,1)));assertEquals(2,total(old));assertEquals(0,store.list(CHANNEL).unassigned());assertEquals(1,store.detail(old).lateCount());
        long revision=store.detail(old).stored().revision();store.apply(List.of(record(late,2),record(first,2),record(second,3)));assertEquals(2,total(old));assertEquals(revision,store.detail(old).stored().revision());
        store.apply(List.of(record(live(at.plusSeconds(140),"456"),3)));assertEquals(1,total(old));assertEquals(1,total(source("456")));
        store.apply(List.of(record(sample(at.plusSeconds(160),null,BroadcastMetadata.LiveStatus.OFFLINE,null),4),record(event(at.plusSeconds(170),at.plusSeconds(171)),4)));
        assertEquals(1,store.list(CHANNEL).unassigned());assertEquals("OFFLINE",store.detail(source("456")).stored().session().state());
        store.apply(List.of(record(live(at.plusSeconds(200),"123"),5)));assertEquals(old,source("123"));assertEquals(at,store.detail(old).stored().session().observedFrom());
        assertEquals(2L,jdbc.queryForObject("SELECT sum(message_count) FROM report_bucket",Long.class));assertEquals(3L,jdbc.queryForObject("SELECT count(*) FROM report_event",Long.class));
    }
    @Test void internalAnchorSurvivesEarlierSamplesAndOldLinkResolvesAfterKnownIdentityArrives() {
        Instant at=Instant.now().minusSeconds(240).truncatedTo(java.time.temporal.ChronoUnit.MICROS);var original=live(at.plusSeconds(60),null);store.apply(List.of(record(original,0),record(event(at.plusSeconds(65),at.plusSeconds(66)),0)));UUID id=source(null);
        store.apply(List.of(record(live(at,null),1)));assertEquals(id,source(null));assertEquals(at,store.detail(id).stored().session().observedFrom());
        store.apply(List.of(record(live(at.plusSeconds(30),"123"),2)));UUID known=source("123");assertEquals(known,store.detail(id).stored().session().id());assertEquals(1,total(known));
        assertEquals(1L,jdbc.queryForObject("SELECT sum(message_count) FROM report_bucket",Long.class));
        store.cleanup();assertEquals(known,store.detail(id).stored().session().id());assertEquals(0L,jdbc.queryForObject("SELECT count(*) FROM report_session WHERE report_id=?",Long.class,id));
    }
    @Test void progressFailureRollsBackSessionReassignmentAndAliasesAndConflictsKeepPriorState() {
        Instant at=Instant.now().minusSeconds(90);var initial=live(at,null);var first=event(at.plusSeconds(10),at.plusSeconds(11));store.apply(List.of(record(initial,0),record(first,0)));UUID id=source(null);
        jdbc.execute("CREATE FUNCTION reject_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_PRIVATE_SQL'; END $$");jdbc.execute("CREATE TRIGGER reject_report BEFORE INSERT ON report_progress FOR EACH ROW EXECUTE FUNCTION reject_report()");
        var known=live(at.minusSeconds(1),"123");assertThrows(RuntimeException.class,()->store.apply(List.of(record(known,1),record(event(at.plusSeconds(15),at.plusSeconds(16)),1))));
        assertEquals(id,source(null));assertEquals(1,total(id));assertEquals(0L,jdbc.queryForObject("SELECT count(*) FROM report_alias",Long.class));
        jdbc.execute("DROP TRIGGER reject_report ON report_progress");store.apply(List.of(record(known,1)));assertEquals(source("123"),store.detail(id).stored().session().id());
        var altered=new ChatEvent(first.schemaVersion(),first.eventId(),first.source(),first.channelId(),first.chatRoomId(),first.collectionSessionId(),null,"CHAT",first.eventTime().minusSeconds(1),first.receivedAt(),false,"test");
        assertEquals("REPORT_EVENT_CONFLICT",assertThrows(IllegalArgumentException.class,()->store.apply(List.of(record(altered,2)))).getMessage());assertEquals(1,total(source("123")));
    }
    @Test void cleanupProtectsUnknownActiveSessionsAndInFlightTransactionsAndNeverTouchesLiveTables() throws Exception {
        Instant at=Instant.now().minusSeconds(200);store.apply(List.of(record(live(at,"123"),0),record(event(at.plusSeconds(10),at.plusSeconds(11)),0),record(sample(at.plusSeconds(20),null,BroadcastMetadata.LiveStatus.OFFLINE,null),1),record(live(at.plusSeconds(30),"456"),2),record(event(at.plusSeconds(40),at.plusSeconds(41)),1)));
        UUID active=source("456");store=store(Clock.fixed(Instant.now().plusSeconds(7*86400),ZoneOffset.UTC));
        try(var connection=dataSource.getConnection()) {
            connection.setAutoCommit(false);try(var statement=connection.prepareStatement("SELECT pg_advisory_xact_lock_shared(hashtextextended(?,0))")) {statement.setString(1,"report-cleanup/test-v1");statement.execute();}
            store.cleanup();assertEquals(2L,jdbc.queryForObject("SELECT count(*) FROM report_session",Long.class));connection.rollback();
        }
        store.cleanup();assertEquals(1L,jdbc.queryForObject("SELECT count(*) FROM report_session",Long.class));assertEquals(1,total(active));assertEquals(0L,jdbc.queryForObject("SELECT count(*) FROM processed_event",Long.class));
        store.apply(List.of(record(event(at.plusSeconds(10),at.plusSeconds(11)),10)));assertEquals(1L,jdbc.queryForObject("SELECT count(*) FROM report_event",Long.class));
    }
    @Test void partialCleanupNeverRebuildsExpiredSessionFromRemainingChildren() {
        Instant at=Instant.parse("2026-10-10T12:00:00Z"),now=at.plusSeconds(8*86400);store=store(Clock.fixed(at.plusSeconds(60),ZoneOffset.UTC));
        var first=live(at,null);var message=event(at.plusSeconds(1),at.plusSeconds(2));
        store.apply(List.of(record(first,0),record(message,0),record(sample(at.plusSeconds(20),null,BroadcastMetadata.LiveStatus.OFFLINE,null),1)));
        UUID old=source(null);
        jdbc.update("""
                INSERT INTO report_observation SELECT run_id,id,channel_id,observed_at,ttl_seconds,live,
                  jsonb_set(payload,'{eventId}',to_jsonb(id::text)),payload_hash,report_id
                FROM report_observation CROSS JOIN (SELECT gen_random_uuid() id FROM generate_series(1,10004)) copies WHERE event_id=?
                """,first.eventId());
        jdbc.update("""
                INSERT INTO report_event SELECT run_id,gen_random_uuid(),channel_id,event_time,received_at,payload_hash,late,observation_id,report_id
                FROM report_event CROSS JOIN generate_series(1,10004) WHERE event_id=?
                """,message.eventId());
        jdbc.update("UPDATE report_bucket SET message_count=10005");
        store=store(Clock.fixed(now,ZoneOffset.UTC));store.cleanup();
        assertEquals(5L,jdbc.queryForObject("SELECT count(*) FROM report_event WHERE report_id=?",Long.class,old));
        assertEquals(6L,jdbc.queryForObject("SELECT count(*) FROM report_observation WHERE report_id=?",Long.class,old));
        jdbc.update("DELETE FROM report_observation WHERE event_id=?",first.eventId());
        assertNull(store.detail(old));
        store.apply(List.of(record(live(now.minusSeconds(30),null),2),record(event(now.minusSeconds(20),now.minusSeconds(19)),1)));
        UUID current=source(null);assertNotEquals(old,current);assertEquals(1,total(current));assertEquals(now.minusSeconds(30),store.detail(current).stored().session().observedFrom());
        assertEquals(at.plusSeconds(20),jdbc.queryForObject("SELECT ended_at FROM report_session WHERE report_id=?",java.sql.Timestamp.class,old).toInstant());
        store.cleanup();assertEquals(1L,jdbc.queryForObject("SELECT count(*) FROM report_session",Long.class));assertEquals(1,total(current));
    }
    @Test void reusedExpiredSourceWaitsForCleanupAndNeverRevivesPartiallyDeletedCounts() {
        Instant at=Instant.parse("2026-10-10T12:00:00Z"),now=at.plusSeconds(8*86400);store=store(Clock.fixed(at.plusSeconds(60),ZoneOffset.UTC));
        store.apply(List.of(record(live(at,"123"),0),record(event(at.plusSeconds(1),at.plusSeconds(2)),0),record(sample(at.plusSeconds(20),null,BroadcastMetadata.LiveStatus.OFFLINE,null),1)));
        UUID old=source("123");store=store(Clock.fixed(now,ZoneOffset.UTC));
        // 정리 도중 bucket만 먼저 지워진 상태를 명시적으로 재현한다.
        jdbc.update("DELETE FROM report_bucket WHERE report_id=?",old);
        var incoming=List.of(record(live(now.minusSeconds(30),"123"),2),record(event(now.minusSeconds(20),now.minusSeconds(19)),1));
        assertEquals("REPORT_EXPIRED_ID_REUSE",assertThrows(IllegalStateException.class,()->store.apply(incoming)).getMessage());
        assertEquals(1L,jdbc.queryForObject("SELECT count(*) FROM report_event",Long.class));
        store.cleanup();store.apply(incoming);assertEquals(old,source("123"));assertEquals(1,total(old));assertEquals(now.minusSeconds(30),store.detail(old).stored().session().observedFrom());
    }
    @Test void retainedReportInputsOlderThanLiveWindowResumeAndExpiredInputsOnlyAdvanceProgress() {
        Instant now=Instant.parse("2026-10-25T12:00:00Z"),at=now.minusSeconds(3*86400);store=store(Clock.fixed(now,ZoneOffset.UTC));
        store.apply(List.of(record(live(at,"123"),0),record(event(at.plusSeconds(1),at.plusSeconds(2)),0)));
        assertEquals(1,total(source("123")));
        store.apply(List.of(record(event(now.minusSeconds(8*86400),now.minusSeconds(8*86400)),1)));
        assertEquals(1,total(source("123")));assertEquals(2L,jdbc.queryForObject("SELECT next_offset FROM report_progress WHERE topic=?",Long.class,chat));
        assertEquals("REPORT_FUTURE_TIME",assertThrows(IllegalArgumentException.class,()->store.apply(List.of(record(event(now,now.plusSeconds(6)),2)))).getMessage());
    }
    @Test void independentReportConsumerRestartsWithoutDuplicationWhileRankingContinues() throws Exception {
        var ranking=new ProjectionStore(jdbc,tx(),codec,"ranking","test-v1");var factory=KafkaSupport.producerFactory(BOOTSTRAP);var kafka=new KafkaTemplate<>(factory);
        var reports=new DualTopicConsumer(BOOTSTRAP,List.of(chat,observations),group,"earliest",store,"REPORT");
        try(var rankingConsumer=new ProjectionConsumer(KafkaSupport.consumerFactory(BOOTSTRAP,group+"-ranking","earliest"),chat,ranking);var publisher=new ObservationPublisher(BOOTSTRAP,observations)) {
            rankingConsumer.start();reports.start();awaitCommitted(0);Instant at=Instant.now().minusSeconds(30);publisher.accept(live(at,"123"));var first=event(at.plusSeconds(1),at.plusSeconds(2));kafka.send(chat,CHANNEL,codec.encode(first)).get(10,TimeUnit.SECONDS);awaitCommitted(2);UUID id=source("123");assertEquals(1,total(id));
            reports.close();kafka.send(chat,CHANNEL,codec.encode(event(at.plusSeconds(3),at.plusSeconds(4)))).get(10,TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertEquals(2L,ranking.totals().get("bucketTotal")));assertEquals(1,total(id));
            try(var restarted=new DualTopicConsumer(BOOTSTRAP,List.of(chat,observations),group,"none",store,"REPORT")) {
                restarted.start();awaitCommitted(3);assertEquals(2,total(id));kafka.send(chat,CHANNEL,codec.encode(first)).get(10,TimeUnit.SECONDS);awaitCommitted(4);assertEquals(2,total(id));
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertTrue(store.list(CHANNEL).fresh()));
            }
        } finally { reports.close();factory.destroy(); }
    }
    private void awaitCommitted(long expected) {
        await().atMost(Duration.ofSeconds(40)).untilAsserted(()->{try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))){var offsets=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);assertEquals(6,offsets.size());assertEquals(expected,offsets.values().stream().mapToLong(o->o.offset()).sum());}});
    }
    @Test void summaryExcludesIncompleteMinutesKeepsHealthyZeroAndRevisesWithoutRealtimeAlerts() throws Exception {
        Instant at=Instant.parse("2026-10-25T12:00:00Z"),now=at.plusSeconds(600);Clock clock=Clock.fixed(now,ZoneOffset.UTC);store=store(clock);
        var inputs=new ArrayList<ConsumerRecord<String,byte[]>>();
        for(int i=0;i<3;i++)inputs.add(record(live(at.plusSeconds(i*60L),"123"),i));
        inputs.add(record(sample(at.plusSeconds(180),null,BroadcastMetadata.LiveStatus.OFFLINE,null),3));
        for(int i=0;i<14;i++)inputs.add(record(event(at.plusSeconds(i<11?61:121),at.plusSeconds(i<11?62:122)),i));
        store.apply(inputs);UUID id=source("123");
        jdbc.update("INSERT INTO collection_session VALUES (?,?,?,?,?,?,?,'CLOSED')",UUID.randomUUID(),CHANNEL,UUID.randomUUID(),java.sql.Timestamp.from(at),java.sql.Timestamp.from(at.plusSeconds(180)),java.sql.Timestamp.from(at.plusSeconds(180)),java.sql.Timestamp.from(at.plusSeconds(180)));
        for(int i=1;i<=3;i++) {
            UUID snapshot=UUID.randomUUID();jdbc.update("INSERT INTO ranking_snapshot(snapshot_id,projection,run_id,window_sec,evaluated_at,algorithm_version) VALUES (?,'ranking','test-v1',60,?,?)",snapshot,java.sql.Timestamp.from(at.plusSeconds(i*60L)),io.chatpulse.quality.RankingStore.VERSION);
            jdbc.update("INSERT INTO ranking_entry VALUES (?,?,1,0,0,1,'HEALTHY','[]',NULL,NULL)",snapshot,CHANNEL);
        }
        jdbc.update("INSERT INTO collection_loss VALUES (?,?,?,?,?)",UUID.randomUUID(),CHANNEL,java.sql.Timestamp.from(at.plusSeconds(70)),java.sql.Timestamp.from(at.plusSeconds(71)),"SYNTHETIC_DROP");
        for(String topic:List.of(chat,observations))for(int p=0;p<3;p++) {
            var partition=new TopicPartition(topic,p);store.initialize(partition,0);
            long end=jdbc.queryForObject("SELECT next_offset FROM report_progress WHERE run_id='test-v1' AND topic=? AND partition=?",Long.class,topic,p);store.observe(partition,end,now,now,false);
        }
        var history=new io.chatpulse.quality.QualityHistory(jdbc,tx(),"test-v1",chat,"ranking",clock);
        var queries=new ReportQueries(store,history,clock);var result=queries.detail(id);
        assertEquals(14,result.referenceCount());assertEquals(3L,result.qualifiedCount());assertEquals(3L,result.maximumPerMinute());assertEquals(0L,result.header().viewerMaximum());assertEquals(3,result.completedMinutes());assertEquals(2,result.healthyMinutes());
        assertEquals(0L,result.points().getFirst().count());assertNull(result.points().get(1).count());assertEquals(List.of("KNOWN_LOSS"),result.points().get(1).reasons());assertEquals(1,result.peaks().size());assertEquals(at.plusSeconds(120).toString(),result.peaks().getFirst().from());
        assertEquals("OFFLINE",result.header().state());assertTrue(result.partialObservation());assertEquals(result.header().reportVersion(),queries.detail(id).header().reportVersion());
        var legacy=new io.chatpulse.quality.RankingStore(jdbc,tx(),"ranking","ranking-trend","test-v1",chat,clock).activity(CHANNEL,at,at.plusSeconds(180));
        assertEquals(legacy.points().stream().map(p->p.status().equals("HEALTHY")).toList(),history.minutes(CHANNEL,at,at.plusSeconds(180)).stream().map(io.chatpulse.quality.QualityHistory.Minute::healthy).toList());
        var json=tools.jackson.databind.json.JsonMapper.builder().build();
        try(var fixture=getClass().getResourceAsStream("/broadcast-report.json")){assertNotNull(fixture);assertEquals(json.readTree(fixture),json.readTree(json.writeValueAsString(result)));}
        var late=record(event(at.plusSeconds(121),at.plusSeconds(241)),14);store.apply(List.of(late));var revised=queries.detail(id);
        assertEquals(15,revised.referenceCount());assertEquals(4L,revised.qualifiedCount());assertEquals(1,revised.lateCount());assertNotEquals(result.header().reportVersion(),revised.header().reportVersion());
        store.apply(List.of(late));assertEquals(revised.header().reportVersion(),queries.detail(id).header().reportVersion());
        jdbc.update("INSERT INTO snapshot_quality_revision SELECT snapshot_id,channel_id,'COLLECTION_REVISED',? FROM ranking_entry WHERE snapshot_id=(SELECT snapshot_id FROM ranking_snapshot WHERE evaluated_at=?)",java.sql.Timestamp.from(now),java.sql.Timestamp.from(at.plusSeconds(180)));
        var incomplete=queries.detail(id);assertEquals(0L,incomplete.maximumPerMinute());assertEquals(0L,incomplete.qualifiedCount());assertTrue(incomplete.peaks().isEmpty());assertNotEquals(revised.header().reportVersion(),incomplete.header().reportVersion());assertEquals(0L,jdbc.queryForObject("SELECT count(*) FROM spike_outbox",Long.class));
        store.unavailable(List.of(new TopicPartition(chat,0)));assertNull(queries.detail(id).qualifiedCount());assertNull(queries.detail(id).maximumPerMinute());
    }
    @Test void executableAnalyzerProvidesReportListAndNonemptyPartialSummary() throws Exception {
        int port;try(var socket=new java.net.ServerSocket(0)){port=socket.getLocalPort();}
        var process=new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"),"bin","java").toString(),"-jar",java.nio.file.Path.of("build/libs/chat-pulse-poc.jar").toAbsolutePath().toString(),
                "--spring.profiles.active=pipeline","--pipeline.role=analyzer","--spring.main.web-application-type=servlet","--server.address=127.0.0.1","--server.port="+port,
                "--pipeline.bootstrap-servers="+BOOTSTRAP,"--pipeline.topic="+chat,"--pipeline.group="+group,"--observations.topic="+observations,"--pipeline.run-id=test-v1","--pipeline.offset-reset=earliest","--quality.trend-offset-reset=earliest","--metadata.enabled=false",
                "--pipeline.jdbc-url=jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema,"--logging.level.root=OFF").redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        var factory=KafkaSupport.producerFactory(BOOTSTRAP);
        try(var http=java.net.http.HttpClient.newHttpClient();var publisher=new ObservationPublisher(BOOTSTRAP,observations)) {
            Instant at=Instant.now().minusSeconds(130);publisher.accept(live(at,"123"));new KafkaTemplate<>(factory).send(chat,CHANNEL,codec.encode(event(at.plusSeconds(1),at.plusSeconds(2)))).get(10,TimeUnit.SECONDS);
            var root=java.net.URI.create("http://127.0.0.1:"+port);var json=tools.jackson.databind.json.JsonMapper.builder().build();
            await().atMost(Duration.ofSeconds(40)).ignoreExceptions().untilAsserted(()->{
                assertTrue(process.isAlive());var response=http.send(java.net.http.HttpRequest.newBuilder(root.resolve("/api/v1/channels/"+CHANNEL+"/reports")).timeout(Duration.ofSeconds(5)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                assertEquals(200,response.statusCode());assertEquals("no-store",response.headers().firstValue("Cache-Control").orElse(""));var body=json.readTree(response.body());assertEquals(1,body.path("reports").size());
                String id=body.path("reports").get(0).path("reportId").asText();var detail=http.send(java.net.http.HttpRequest.newBuilder(root.resolve("/api/v1/reports/"+id)).timeout(Duration.ofSeconds(5)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                assertEquals(200,detail.statusCode());var report=json.readTree(detail.body());assertEquals(1,report.path("referenceCount").asLong());assertTrue(report.path("qualifiedCount").isNull());assertEquals("UNKNOWN",report.path("header").path("state").asText());assertEquals("UNCONFIRMED",report.path("header").path("endReason").asText());assertTrue(report.path("partialObservation").asBoolean());
            });
            var status=http.send(java.net.http.HttpRequest.newBuilder(root.resolve("/api/operations/reports")).timeout(Duration.ofSeconds(5)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());assertEquals(6,json.readTree(status.body()).path("assignedPartitions").asInt());
            var missing=http.send(java.net.http.HttpRequest.newBuilder(root.resolve("/api/v1/reports/"+UUID.randomUUID())).timeout(Duration.ofSeconds(5)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());assertEquals(404,missing.statusCode());
        } finally { process.destroy();if(!process.waitFor(20,TimeUnit.SECONDS))process.destroyForcibly().waitFor();factory.destroy();try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))){admin.deleteTopics(List.of(chat+".dlq",chat+".spikes")).all().get(10,TimeUnit.SECONDS);} }
    }
}
