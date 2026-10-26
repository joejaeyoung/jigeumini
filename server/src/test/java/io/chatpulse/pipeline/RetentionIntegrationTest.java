package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.retention.*;
import io.chatpulse.replay.*;
import io.chatpulse.observations.*;
import io.chatpulse.categories.CategoryStore;
import io.chatpulse.reports.ReportStore;
import io.chatpulse.metadata.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class RetentionIntegrationTest {
    private static final String RUN="test-v1",CHANNEL="a".repeat(32);
    private String schema,topic;
    private JdbcTemplate jdbc;
    private TransactionTemplate tx;
    private MutableClock clock;
    private Instant started;
    private final EventCodec codec=new EventCodec();
    static final class MutableClock extends Clock {
        Instant now;
        MutableClock(Instant now){this.now=now;}
        public ZoneId getZone(){return ZoneOffset.UTC;}
        public Clock withZone(ZoneId zone){return this;}
        public Instant instant(){return now;}
    }
    @BeforeEach void setup() {
        String suffix=UUID.randomUUID().toString().replace("-","");schema="it_retention_"+suffix;topic="it.retention."+suffix;
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("CREATE SCHEMA "+schema);
        var ds=IntegrationSupport.ds(schema);org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
        jdbc=new JdbcTemplate(ds);tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
        started=Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(),10)*10);clock=new MutableClock(started);
    }
    @AfterEach void cleanup() throws Exception {
        try(var admin=Admin.create(Map.of("bootstrap.servers",IntegrationSupport.BOOTSTRAP))) {
            var owned=admin.listTopics().names().get(10,TimeUnit.SECONDS).stream().filter(n->n.equals(topic)||n.equals(topic+".dlq")).toList();
            if(!owned.isEmpty())admin.deleteTopics(owned).all().get(10,TimeUnit.SECONDS);
        } finally {new JdbcTemplate(IntegrationSupport.ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");}
    }
    private ProjectionStore live(String projection){return new ProjectionStore(jdbc,tx,codec,projection,RUN,clock);}
    private ProjectionRetention retention(String projection){return new ProjectionRetention(jdbc,tx,projection,RUN,clock);}
    private ChatEvent event(Instant time,Instant received){return new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic-room",UUID.randomUUID(),null,"CHAT",time,received,false,"retention-test");}
    private ConsumerRecord<String,byte[]> record(ChatEvent event,long offset){return new ConsumerRecord<>(topic,0,offset,CHANNEL,codec.encode(event));}
    private long count(String table){return jdbc.queryForObject("SELECT count(*) FROM "+table,Long.class);}
    @Test void exactFloorsIndependentWindowsAndClockRollbackNeverResurrectLiveCounts() {
        var store=live("ranking");var current=event(started,started);var old=event(started.minusSeconds(6*86400),started);
        store.apply(List.of(record(current,0),record(old,1)));live("ranking-trend").apply(List.of(record(current,0)));
        clock.now=started.plusSeconds(72*3600);
        var exact=retention("ranking").cleanup();assertThat(exact.get("ledgerDeleted")).isEqualTo(0);assertThat(exact.get("bucketsDeleted")).isEqualTo(1);
        clock.now=clock.now.plusSeconds(1);assertThat(retention("ranking").cleanup().get("ledgerDeleted")).isEqualTo(2);
        assertThat(store.totals()).containsEntry("processedEvents",0L).containsEntry("bucketTotal",1L);
        assertThat(live("ranking-trend").totals()).containsEntry("processedEvents",1L).containsEntry("bucketTotal",1L);
        clock.now=started;store=live("ranking");var restarted=store;
        assertThatThrownBy(()->restarted.apply(List.of(record(current,2)))).hasMessage("OUTSIDE_LIVE_RETENTION");
        retention("ranking").cleanup();assertThat(restarted.retention().get("received_floor")).isEqualTo(Timestamp.from(started.plusSeconds(1)));
        assertThat(jdbc.queryForObject("SELECT next_resolved_offset FROM projection_progress WHERE projection='ranking'",Long.class)).isEqualTo(2);
    }
    @Test void kafkaRedeliveryUsesProgressAndExpiredNewOffsetsUseExistingDlqContract() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);var event=event(started,started);var store=live("ranking");
        var factory=KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try(var recovery=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,topic,jdbc,"ranking",RUN)) {
            var producer=new KafkaTemplate<>(factory);producer.send(topic,0,CHANNEL,codec.encode(event)).get(10,TimeUnit.SECONDS);
            assertThat(store.applyRecovering(readKafka(1),recovery)).isEqualTo(1);
            clock.now=started.plusSeconds(72*3600+1);retention("ranking").cleanup();clock.now=started;
            producer.send(topic,0,CHANNEL,codec.encode(event)).get(10,TimeUnit.SECONDS);
            var records=readKafka(2);assertThat(store.applyRecovering(records,recovery)).isZero();
            assertThat(store.applyRecovering(records,recovery)).isZero();assertThat(count("projection_failure")).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT error_code FROM projection_failure",String.class)).isEqualTo("OUTSIDE_LIVE_RETENTION");
            assertThat(store.totals()).containsEntry("processedEvents",0L).containsEntry("bucketTotal",1L);
            assertThat(jdbc.queryForObject("SELECT next_resolved_offset FROM projection_progress",Long.class)).isEqualTo(2);
        } finally {factory.destroy();}
    }
    private List<ConsumerRecord<String,byte[]>> readKafka(int expected) {
        try(var source=new ReplaySource(IntegrationSupport.BOOTSTRAP,topic,UUID.randomUUID())) {
            source.assign(source.capture(null,null));var result=new ArrayList<ConsumerRecord<String,byte[]>>();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while(result.size()<expected&&System.nanoTime()<deadline)source.poll().forEach(result::add);
            assertThat(result).hasSize(expected);return result;
        }
    }
    @Test void failedDeleteRollsBackBatchButCommittedFloorSurvivesRestart() {
        var store=live("ranking");var event=event(started,started);store.apply(List.of(record(event,0)));
        jdbc.execute("CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_FAILURE'; END $$");
        jdbc.execute("CREATE TRIGGER reject_cleanup BEFORE DELETE ON processed_event FOR EACH ROW EXECUTE FUNCTION reject_cleanup()");
        clock.now=started.plusSeconds(72*3600+1);
        assertThatThrownBy(()->retention("ranking").cleanup()).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("processed_event")).isEqualTo(1);
        jdbc.execute("DROP TRIGGER reject_cleanup ON processed_event");assertThat(retention("ranking").cleanup().get("ledgerDeleted")).isEqualTo(1);
        clock.now=started;assertThatThrownBy(()->live("ranking").apply(List.of(record(event,1)))).hasMessage("OUTSIDE_LIVE_RETENTION");
        assertThat(store.totals().get("bucketTotal")).isEqualTo(1L);
    }
    @Test void activeWriterBlocksFloorAdvanceWithinLockBudgetThenCleanupResumes() throws Exception {
        live("ranking");var cleanup=retention("ranking");
        try(var connection=IntegrationSupport.ds(schema).getConnection()) {
            connection.setAutoCommit(false);
            try(var statement=connection.createStatement()){statement.execute("SELECT * FROM retention_window WHERE scope='projection/ranking' FOR SHARE");}
            long start=System.nanoTime();assertThatThrownBy(cleanup::cleanup).isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(Duration.ofNanos(System.nanoTime()-start)).isLessThan(Duration.ofSeconds(2));
            assertThat(jdbc.queryForObject("SELECT received_floor IS NULL FROM retention_window WHERE scope='projection/ranking'",Boolean.class)).isTrue();
            connection.rollback();
        }
        assertThat(cleanup.cleanup().get("deletedRows")).isEqualTo(0);
    }
    private ReplayManifest manifest(){return new ReplayManifest(1,topic,org.apache.kafka.common.Uuid.randomUuid().toString(),List.of(new ReplayManifest.Range(0,0,1)),null,null,1,ReplayManifest.ALGORITHM,started);}
    @Test void replayExpiryIsExplicitProtectsActiveAndCannotBeRecreatedAfterMetadataPurge() {
        var jobs=new ReplayStore(IntegrationSupport.ds(schema));var manifest=manifest();var id=jobs.create(manifest);var job=jobs.claim(UUID.randomUUID());
        jobs.apply(job,List.of(record(event(started,started),0)),Map.of(0,1L));jobs.complete(job);
        var active=jobs.create(manifest);jobs.claim(UUID.randomUUID());var queued=jobs.create(manifest);
        jdbc.update("UPDATE replay_run SET finished_at=? WHERE run_id=?",Timestamp.from(started),id);
        var retention=new ReplayRetention(jdbc,tx,clock);clock.now=started.plusSeconds(7*86400);retention.cleanup();
        assertThat(jobs.status(id).get("resultState")).isEqualTo("AVAILABLE");
        clock.now=clock.now.plusSeconds(1);retention.cleanup();assertThat(jobs.status(id)).containsEntry("resultState","EXPIRED").containsEntry("totals",null).containsEntry("progress",null);
        assertThat(jobs.get(active).state()).isEqualTo("RUNNING");assertThat(jobs.get(queued).state()).isEqualTo("QUEUED");
        assertThatThrownBy(()->jobs.compare(id,active)).hasMessage("RESULT_EXPIRED");
        var stale=new ProjectionStore(jdbc,tx,codec,"replay",id.toString());
        assertThatThrownBy(()->stale.applyReplay(List.of(record(event(started,started),0)))).hasMessage("RESULT_EXPIRED");
        clock.now=started.plusSeconds(30*86400+1);retention.cleanup();assertThatThrownBy(()->jobs.get(id)).hasMessage("RUN_NOT_FOUND");
        assertThatThrownBy(()->stale.applyReplay(List.of(record(event(started,started),0)))).hasMessage("RUN_NOT_FOUND");
        assertThat(count("processed_event")).isZero();
    }
    @Test void replayPartialCleanupNeverPresentsPartialTotalsAndRetryFinishes() {
        var jobs=new ReplayStore(IntegrationSupport.ds(schema));var id=jobs.create(manifest());var job=jobs.claim(UUID.randomUUID());
        jobs.apply(job,List.of(record(event(started,started),0)),Map.of(0,1L));jobs.complete(job);
        jdbc.update("UPDATE replay_run SET finished_at=? WHERE run_id=?",Timestamp.from(started),id);
        jdbc.execute("CREATE FUNCTION reject_bucket_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_FAILURE'; END $$");
        jdbc.execute("CREATE TRIGGER reject_bucket_cleanup BEFORE DELETE ON chat_bucket_10s FOR EACH ROW EXECUTE FUNCTION reject_bucket_cleanup()");
        clock.now=started.plusSeconds(7*86400+1);
        assertThatThrownBy(()->new ReplayRetention(jdbc,tx,clock).cleanup()).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("processed_event")).isZero();assertThat(count("chat_bucket_10s")).isEqualTo(1);
        assertThat(jobs.status(id)).containsEntry("resultState","EXPIRED").containsEntry("totals",null);
        jdbc.execute("DROP TRIGGER reject_bucket_cleanup ON chat_bucket_10s");new ReplayRetention(jdbc,tx,clock).cleanup();
        assertThat(count("chat_bucket_10s")).isZero();assertThat(jdbc.queryForObject("SELECT result_purged_at IS NOT NULL FROM replay_run",Boolean.class)).isTrue();
    }
    @Test void replayQueueHasFiniteAdmissionAndTerminalRunsDoNotOccupySlots() {
        var jobs=new ReplayStore(IntegrationSupport.ds(schema));var ids=new ArrayList<UUID>();for(int i=0;i<32;i++)ids.add(jobs.create(manifest()));
        assertThatThrownBy(()->jobs.create(manifest())).hasMessage("QUEUE_FULL");jobs.cancel(ids.getFirst());assertThat(jobs.create(manifest())).isNotNull();
    }
    @Test void categoryReportAndObservationFloorsSurviveBackwardClock() {
        var observations=new ObservationStore(jdbc,tx,RUN,clock);var categories=new CategoryStore(jdbc,tx,RUN,topic,topic+".observations",clock);
        var reports=new ReportStore(jdbc,tx,RUN,topic,topic+".observations",clock);
        var sample=BroadcastObserved.observed(RUN,CHANNEL,started,120,new PublicBroadcastObservation(BroadcastMetadata.LiveStatus.LIVE,1L,"1",null,null,PublicBroadcastObservation.Quality.OBSERVED));
        var observation=new ConsumerRecord<String,byte[]>(topic+".observations",0,0,CHANNEL,sample.encode());var chat=record(event(started,started),0);
        observations.apply(List.of(observation));categories.apply(List.of(observation,chat));reports.apply(List.of(observation,chat));
        clock.now=started.plusSeconds(8*86400);observations.cleanup();categories.cleanup();reports.cleanup();
        long retainedActive=count("report_event");assertThat(retainedActive).isEqualTo(1);
        clock.now=started;observations.apply(List.of(observation));categories.apply(List.of(observation,chat));reports.apply(List.of(observation,chat));
        assertThat(count("broadcast_observation")).isZero();assertThat(count("category_event")).isZero();assertThat(count("report_event")).isEqualTo(retainedActive);
    }
    @Test void boundedBatchResumesAndDiagnosticsRecordFailureWithoutRawException() {
        live("ranking");
        jdbc.update("""
                INSERT INTO processed_event(projection,run_id,event_id,channel_id,collection_session_id,event_time,received_at,source_topic,source_partition,source_offset,payload_hash)
                SELECT 'ranking',?,md5(i::text)::uuid,?,md5('session')::uuid,?::timestamptz,?::timestamptz,?,0,i,repeat('a',64)
                FROM generate_series(1,10001) i
                """,RUN,CHANNEL,Timestamp.from(started.minusSeconds(4*86400)),Timestamp.from(started.minusSeconds(4*86400)),topic);
        assertThat(retention("ranking").cleanup()).containsEntry("ledgerDeleted",10000).containsEntry("limited",true);
        assertThat(count("processed_event")).isEqualTo(1);
        assertThat(retention("ranking").cleanup()).containsEntry("ledgerDeleted",1).containsEntry("limited",false);
        try(var worker=new RetentionMaintenance(jdbc,RUN,List.of(
                new RetentionMaintenance.Task("failure",()->{throw new IllegalStateException("SYNTHETIC_PRIVATE_FAILURE");}),
                new RetentionMaintenance.Task("success",()->RetentionMaintenance.counted(3))))) {
            worker.tick();worker.tick();
        }
        assertThat(jdbc.queryForObject("SELECT failures FROM retention_task WHERE task='failure'",Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT state FROM retention_task WHERE task='success'",String.class)).isEqualTo("SUCCEEDED");
        var diagnostic=new RetentionDiagnostics(jdbc).inspect(RUN);
        assertThat(diagnostic.toString()).doesNotContain("SYNTHETIC_PRIVATE_FAILURE");
        assertThat(diagnostic).containsKeys("relations","wal","protected","windows");
    }
    @Test void openReportsProtectHistoricalQualityAndClosedExpiredReportsReleaseIt() {
        UUID report=UUID.randomUUID(),snapshot=UUID.randomUUID();Instant old=started.minusSeconds(10*86400);
        jdbc.update("INSERT INTO report_session(run_id,report_id,channel_id,observed_from,observed_to,payload,updated_at) VALUES (?,?,?,?,?,'{}',?)",
                RUN,report,CHANNEL,Timestamp.from(old.minusSeconds(86400)),Timestamp.from(started),Timestamp.from(started));
        jdbc.update("INSERT INTO ranking_snapshot(snapshot_id,projection,run_id,window_sec,evaluated_at,algorithm_version) VALUES (?,'ranking',?,60,?,'test')",snapshot,RUN,Timestamp.from(old));
        jdbc.update("INSERT INTO ranking_entry(snapshot_id,channel_id,reference_count,coverage,status,reasons) VALUES (?,?,1,1,'HEALTHY','[]')",snapshot,CHANNEL);
        jdbc.update("INSERT INTO snapshot_quality_revision VALUES (?,?,'KNOWN_LOSS',?)",snapshot,CHANNEL,Timestamp.from(old));
        jdbc.update("INSERT INTO collection_loss VALUES (?,?,?,?,?)",UUID.randomUUID(),CHANNEL,Timestamp.from(old.minusSeconds(10)),Timestamp.from(old),"TEST");
        var history=new HistoryRetention(jdbc,tx,RUN,clock);
        assertThat(history.cleanup()).containsEntry("protectedByReports",true);assertThat(count("ranking_snapshot")).isEqualTo(1);assertThat(count("collection_loss")).isEqualTo(1);
        jdbc.update("UPDATE report_session SET ended_at=? WHERE report_id=?",Timestamp.from(started.minusSeconds(9*86400)),report);
        assertThat(history.cleanup()).containsEntry("protectedByReports",false);
        for(String table:List.of("ranking_snapshot","ranking_entry","snapshot_quality_revision","collection_loss"))assertThat(count(table)).isZero();
    }
    @Test void notificationReceiptIsKeptUntilOutboxAcknowledgementCanBeRetired() {
        var feed=new io.chatpulse.notifications.NotificationStore(jdbc,tx,RUN,clock);UUID id=UUID.randomUUID();Instant old=started.minusSeconds(10*86400);
        var spike=new io.chatpulse.notifications.SpikeDetected(1,id,RUN,CHANNEL,old,"chat-spike-v1",100,new java.math.BigDecimal(50),new java.math.BigDecimal(2),"HEALTHY_AT_DETECTION");
        jdbc.update("INSERT INTO spike_event(spike_id,run_id,channel_id,evaluated_at,algorithm_version,count,baseline,ratio) VALUES (?,?,?,?,'chat-spike-v1',100,50,2)",id,RUN,CHANNEL,Timestamp.from(old));
        jdbc.update("INSERT INTO spike_outbox(spike_id,run_id,created_at) VALUES (?,?,?)",id,RUN,Timestamp.from(old));
        clock.now=old;var record=new ConsumerRecord<String,byte[]>(topic,0,0,CHANNEL,spike.encode());feed.apply(List.of(record));
        clock.now=started;assertThat(feed.cleanup()).isZero();assertThat(count("notification_event")).isEqualTo(1);
        jdbc.update("UPDATE spike_outbox SET published_at=?",Timestamp.from(started.minusSeconds(8*86400)));
        try(var outbox=new io.chatpulse.notifications.SpikeOutbox(jdbc,tx,IntegrationSupport.BOOTSTRAP,topic,RUN)){assertThat(outbox.cleanup()).isEqualTo(1);}
        assertThat(feed.cleanup()).isEqualTo(1);clock.now=old;feed.apply(List.of(record));assertThat(count("notification_event")).isZero();
    }
    @Test void delayedFirstDeliveryConfirmsOldOutboxWithoutReturningItToTheFeed() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);
        var feed=new io.chatpulse.notifications.NotificationStore(jdbc,tx,RUN,clock);
        UUID id=UUID.randomUUID();Instant old=started.minusSeconds(8*86400);
        jdbc.update("INSERT INTO spike_event(spike_id,run_id,channel_id,evaluated_at,algorithm_version,count,baseline,ratio) VALUES (?,?,?,?,'chat-spike-v1',100,50,2)",id,RUN,CHANNEL,Timestamp.from(old));
        jdbc.update("INSERT INTO spike_outbox(spike_id,run_id,created_at) VALUES (?,?,?)",id,RUN,Timestamp.from(old));
        assertThat(feed.cleanup()).isZero();
        try(var outbox=new io.chatpulse.notifications.SpikeOutbox(jdbc,tx,IntegrationSupport.BOOTSTRAP,topic,RUN)) {
            assertThat(outbox.publishOne()).isTrue();feed.apply(readKafka(1));
            assertThat(count("notification_event")).isEqualTo(1);assertThat(feed.cleanup()).isZero();
            assertThat(feed.page(List.of(CHANNEL),started,UUID.randomUUID(),20).events()).isEmpty();
            assertThat(outbox.cleanup()).isZero();
            jdbc.update("UPDATE spike_outbox SET published_at=?",Timestamp.from(old));
            assertThat(outbox.cleanup()).isEqualTo(1);assertThat(feed.cleanup()).isEqualTo(1);
        }
        assertThat(jdbc.queryForObject("SELECT next_offset FROM notification_progress",Long.class)).isEqualTo(1);
    }
    @Test void reportParentLimitIsReportedEvenWhenNoChildRowsExist() {
        var reports=new ReportStore(jdbc,tx,RUN,topic,topic+".observations",clock);
        jdbc.update("""
                INSERT INTO report_session(run_id,report_id,channel_id,observed_from,observed_to,ended_at,payload,updated_at)
                SELECT ?,md5(i::text)::uuid,?,?::timestamptz,?::timestamptz,?::timestamptz,'{}',?::timestamptz
                FROM generate_series(1,1001) i
                """,RUN,CHANNEL,Timestamp.from(started.minusSeconds(9*86400)),Timestamp.from(started.minusSeconds(8*86400)),Timestamp.from(started.minusSeconds(8*86400)),Timestamp.from(started));
        assertThat(reports.cleanup().summary()).containsEntry("deletedRows",1000).containsEntry("limited",true);
        assertThat(count("report_session")).isEqualTo(1);
        assertThat(reports.cleanup().summary()).containsEntry("deletedRows",1).containsEntry("limited",false);
    }
}
