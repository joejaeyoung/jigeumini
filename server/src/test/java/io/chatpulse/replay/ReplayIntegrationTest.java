package io.chatpulse.replay;

import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.replay.ReplayTestSupport.*;

@Tag("integration")
class ReplayIntegrationTest {
    private String schema,topic,liveGroup;
    private JdbcTemplate jdbc;
    private ReplayStore store;
    private Admin admin;
    private KafkaProducer<String,byte[]> producer;
    private final EventCodec codec=new EventCodec();
    private Instant t;
    private Path executableJar=Path.of("build/libs/chat-pulse-poc.jar").toAbsolutePath();
    @BeforeEach void setup() throws Exception {
        schema="it_"+UUID.randomUUID().toString().replace("-","");topic="it.replay."+UUID.randomUUID();liveGroup=topic+"-live";
        new JdbcTemplate(ds("public")).execute("CREATE SCHEMA "+schema);
        Flyway.configure().dataSource(ds(schema)).defaultSchema(schema).load().migrate();jdbc=new JdbcTemplate(ds(schema));store=new ReplayStore(ds(schema));
        admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP,"default.api.timeout.ms",10000,"request.timeout.ms",3000));
        KafkaSupport.ensureTopic(BOOTSTRAP,topic);
        producer=new KafkaProducer<>(Map.of("bootstrap.servers",BOOTSTRAP,"acks","all","enable.idempotence",true),new StringSerializer(),new ByteArraySerializer());
        t=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    }
    @AfterEach void cleanup() throws Exception {
        if(producer!=null) producer.close(Duration.ofSeconds(3));
        try {
            if(admin!=null) {
                admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);
                if(admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().anyMatch(g->g.groupId().equals(liveGroup))) admin.deleteConsumerGroups(List.of(liveGroup)).all().get(10,TimeUnit.SECONDS);
            }
        } finally {if(admin!=null)admin.close(Duration.ofSeconds(3));new JdbcTemplate(ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");}
    }
    private void send(int partition,ChatEvent event) throws Exception {
        producer.send(new ProducerRecord<>(topic,partition,event.receivedAt().toEpochMilli(),event.channelId(),codec.encode(event))).get(10,TimeUnit.SECONDS);
    }
    private ReplayManifest capture(Instant from,Instant to) {
        try(var source=new ReplaySource(BOOTSTRAP,topic,UUID.randomUUID())) {return source.capture(from,to);}
    }
    private UUID execute(ReplayManifest manifest) throws Exception {
        var id=store.create(manifest);assertThat(new ReplayWorker(store,BOOTSTRAP,topic).runOne()).isEqualTo(id);return id;
    }
    private long value(UUID id,String name) {return jdbc.queryForObject("SELECT "+name+" FROM replay_run WHERE run_id=?",Long.class,id);}
    private tools.jackson.databind.JsonNode command(String... arguments) throws Exception {return commandExpected(0,arguments);}
    private tools.jackson.databind.JsonNode commandExpected(int exitCode,String... arguments) throws Exception {
        var cmd=new ArrayList<String>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-jar",executableJar.toString(),"replay"));
        cmd.addAll(List.of(arguments));
        var output=Path.of("../work/replay-cli-"+UUID.randomUUID()+".json").toAbsolutePath();Files.createDirectories(output.getParent());
        var builder=new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().put("PIPELINE_BOOTSTRAP_SERVERS",BOOTSTRAP);builder.environment().put("PIPELINE_TOPIC",topic);
        builder.environment().put("PIPELINE_JDBC_URL","jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema);
        builder.environment().put("PIPELINE_DB_USER","chatpulse");builder.environment().put("PIPELINE_DB_PASSWORD","local-chatpulse");
        var process=builder.start();
        try {assertThat(process.waitFor(25,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isEqualTo(exitCode);}
        finally {if(process.isAlive())process.destroyForcibly();}
        return tools.jackson.databind.json.JsonMapper.builder().build().readTree(Files.readString(output));
    }
    @Test void executableJarSupportsCaptureCreateWorkerStatusCompareAndCancel() throws Exception {
        send(0,event(1,t.minusSeconds(10),t));
        var file=Path.of("../work/replay-manifest-"+UUID.randomUUID()+".json").toAbsolutePath();
        assertThat(command("capture",file.toString()).path("partitions").size()).isEqualTo(3);
        var a=command("create",file.toString()).path("runId").asText();
        assertThat(command("worker").path("state").asText()).isEqualTo("COMPLETED");
        assertThat(command("status",a).path("metrics").path("unique_count").asLong()).isEqualTo(1);
        var b=command("create",file.toString()).path("runId").asText();command("worker");
        assertThat(command("compare",a,b).path("summary").path("mismatched_buckets").asLong()).isZero();
        assertThat(commandExpected(2,"compare",a,a).path("error").asText()).isEqualTo("INCOMPATIBLE_RUNS");
        assertThat(command("status",a).path("metrics").path("worker_build").asText()).matches("[a-f0-9]{64}");
        var retained=command("retention",file.toString());
        assertThat(retained.path("timestampMeaning").asText()).isEqualTo("KAFKA_RECEIVED_AT_NOT_EVENT_TIME");
        assertThat(retained.path("partitions").get(0).path("availableCapturedOffsetSpan").asLong()).isEqualTo(1);
        jdbc.update("UPDATE chat_bucket_10s SET chat_count=chat_count+1 WHERE projection='replay' AND run_id=?",b);
        assertThat(commandExpected(2,"compare",a,b).path("equal").asBoolean()).isFalse();
        var c=command("create",file.toString()).path("runId").asText();
        assertThat(command("cancel",c).path("state").asText()).isEqualTo("CANCELLED");
    }
    @Test void emptyAndFilteredOnlyRangesCompleteAndExecutionDeadlineIsFinite() throws Exception {
        var empty=execute(capture(null,null));assertThat(store.get(empty).state()).isEqualTo("COMPLETED");assertThat(value(empty,"read_count")).isZero();
        send(0,event(1,t.minusSeconds(10),t));
        var filtered=execute(capture(t.minusSeconds(5),t));assertThat(store.get(filtered).state()).isEqualTo("COMPLETED");
        assertThat(value(filtered,"filtered_count")).isEqualTo(1);assertThat(value(filtered,"unique_count")).isZero();
        var timed=store.create(capture(null,null));
        new ReplayWorker(store,BOOTSTRAP,topic,job->{},Duration.ZERO,Duration.ofSeconds(30)).runOne();
        assertThat(store.get(timed).state()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,timed)).isEqualTo("TIME_LIMIT");
    }
    @Test void fixedRangesReplayTwiceMatchesKnownBucketsAndLeavesLiveStateUntouched() throws Exception {
        var newer=event(1,t.minusSeconds(10),t);var older=event(2,t.minusSeconds(50),t);
        send(0,newer);send(0,older);send(0,older);send(1,event(3,t.minusSeconds(30),t));send(1,event(4,t.minusSeconds(120),t));
        var manifest=capture(t.minusSeconds(60),t);assertThat(manifest.partitions()).hasSize(3);
        var live=new ProjectionStore(jdbc,new TransactionTemplate(new DataSourceTransactionManager(ds(schema))),codec,"ranking","live-v1");
        live.apply(List.of(new ConsumerRecord<>(topic,0,0,CHANNEL,codec.encode(newer))));
        var liveOffsets=Map.of(new TopicPartition(topic,0),new OffsetAndMetadata(1));admin.alterConsumerGroupOffsets(liveGroup,liveOffsets).all().get(10,TimeUnit.SECONDS);
        jdbc.update("INSERT INTO ranking_snapshot(snapshot_id,projection,run_id,window_sec,evaluated_at,algorithm_version) VALUES (?,?,?,?,?,?)",UUID.randomUUID(),"ranking","live-v1",60,java.sql.Timestamp.from(t),"quality-ranking-v1");
        jdbc.update("INSERT INTO spike_event VALUES (?,?,?,?,?,?,?,?,NULL)",UUID.randomUUID(),"live-v1",CHANNEL,java.sql.Timestamp.from(t),"chat-spike-v1",100,50,2);
        var before=live.totals();
        send(2,event(5,t.minusSeconds(20),t)); // capture 이후 입력은 같은 시간 필터여도 포함하지 않는다.
        var a=execute(manifest);var b=execute(ReplayManifest.parse(manifest.json()));
        for(var id:List.of(a,b)) {
            assertThat(store.get(id).state()).isEqualTo("COMPLETED");
            assertThat(value(id,"read_count")).isEqualTo(5);assertThat(value(id,"filtered_count")).isEqualTo(1);
            assertThat(value(id,"unique_count")).isEqualTo(3);assertThat(value(id,"duplicate_count")).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT sum(chat_count)::bigint FROM chat_bucket_10s WHERE projection='replay' AND run_id=?",Long.class,id.toString())).isEqualTo(3);
            var expected=List.of(50L,30L,10L).stream().map(seconds->Instant.ofEpochSecond(Math.floorDiv(t.minusSeconds(seconds).getEpochSecond(),10)*10)).toList();
            assertThat(jdbc.query("SELECT bucket_start FROM chat_bucket_10s WHERE projection='replay' AND run_id=? ORDER BY bucket_start",(rs,n)->rs.getTimestamp(1).toInstant(),id.toString())).isEqualTo(expected);
            assertThat(jdbc.queryForList("SELECT next_resolved_offset FROM projection_progress WHERE projection='replay' AND run_id=? ORDER BY partition",Long.class,id.toString())).containsExactly(3L,2L,0L);
        }
        var summary=(Map<?,?>)store.compare(a,b).get("summary");assertThat(((Number)summary.get("mismatched_buckets")).longValue()).isZero();
        assertThat(((Number)summary.get("matched_buckets")).longValue()).isEqualTo(3);
        assertThat(live.totals()).isEqualTo(before);
        assertThat(admin.listConsumerGroupOffsets(liveGroup).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS)).isEqualTo(liveOffsets);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ranking_snapshot",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM spike_event",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM channel_alert_state",Long.class)).isZero();
        assertThat(admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS)).noneMatch(g->g.groupId().equals("replay-"+a));
        var evidence=Map.of("manifest",manifest,"firstRun",store.status(a),"secondRun",store.status(b),"comparison",store.compare(a,b),
                "liveUnchanged",Map.of("ledgerAndBuckets",true,"groupOffsets",true,"snapshots",true,"spikes",true,"alertState",true));
        var evidencePath=Path.of("../work/replay-verification.json");Files.createDirectories(evidencePath.toAbsolutePath().getParent());
        Files.writeString(evidencePath,tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString(evidence));
        jdbc.update("UPDATE chat_bucket_10s SET chat_count=chat_count+1 WHERE projection='replay' AND run_id=?",b.toString());
        assertThat(((Number)((Map<?,?>)store.compare(a,b).get("summary")).get("mismatched_buckets")).longValue()).isEqualTo(3);
        var different=execute(capture(null,null));assertThatThrownBy(()->store.compare(a,different)).hasMessage("INCOMPATIBLE_RUNS");
    }
    @Test void duplicateAndPayloadConflictAcrossPollBatchesUseThePersistentLedger() throws Exception {
        for(int i=0;i<200;i++) send(0,event(i,t.minusSeconds(10),t));
        send(0,event(0,t.minusSeconds(10),t)); // max.poll.records=200보다 뒤의 재전달
        var id=execute(capture(null,null));assertThat(store.get(id).state()).isEqualTo("COMPLETED");
        assertThat(value(id,"read_count")).isEqualTo(201);assertThat(value(id,"unique_count")).isEqualTo(200);assertThat(value(id,"duplicate_count")).isEqualTo(1);
        send(0,event(0,t.minusSeconds(20),t));
        var conflict=execute(capture(null,null));assertThat(store.get(conflict).state()).isEqualTo("FAILED");
        assertThat(value(conflict,"read_count")).isBetween(1L,201L);assertThat(value(conflict,"error_count")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,conflict)).isEqualTo("INVALID_EVENT");
        assertThat(jdbc.queryForObject("SELECT sum(chat_count)::bigint FROM chat_bucket_10s WHERE projection='replay' AND run_id=?",Long.class,conflict.toString())).isEqualTo(value(conflict,"unique_count"));
    }
    @Test void queuedAndRunningCancellationStopWithoutCompletingOrChangingLaterProgress() throws Exception {
        for(int i=0;i<250;i++) send(0,event(i,t.minusSeconds(10),t));
        var manifest=capture(null,null);var queued=store.create(manifest);store.cancel(queued);
        assertThat(store.get(queued).state()).isEqualTo("CANCELLED");
        assertThat(new ReplayWorker(store,BOOTSTRAP,topic).runOne()).isNull();
        var running=store.create(manifest);
        new ReplayWorker(store,BOOTSTRAP,topic,job->store.cancel(job.id()),Duration.ofMinutes(1),Duration.ofSeconds(10)).runOne();
        assertThat(store.get(running).state()).isEqualTo("CANCELLED");assertThat(value(running,"read_count")).isBetween(1L,200L);
        assertThatThrownBy(()->store.complete(store.get(running))).hasMessage("OWNER_LOST");
        assertThatThrownBy(()->store.compare(queued,running)).hasMessage("INCOMPATIBLE_RUNS");
    }
    @Test void crashAfterBatchCommitIsFencedReapedAndOnlyNewRunCanReplay() throws Exception {
        send(0,event(1,t.minusSeconds(10),t));var manifest=capture(null,null);var id=store.create(manifest);
        var output=Path.of("../work/replay-crash-"+id+".log");Files.createDirectories(output.toAbsolutePath().getParent());
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",System.getProperty("integration.classpath"),ReplayCrashProcess.class.getName(),schema,topic).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isEqualTo(91);}
        finally {if(child.isAlive())child.destroyForcibly();}
        var abandoned=store.get(id);assertThat(abandoned.state()).isEqualTo("RUNNING");assertThat(value(id,"unique_count")).isEqualTo(1);
        jdbc.update("UPDATE replay_run SET heartbeat_at=now()-interval '31 seconds' WHERE run_id=?",id);
        assertThat(store.reap()).isEqualTo(1);assertThat(store.get(id).state()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,id)).isEqualTo("WORKER_LOST");
        assertThatThrownBy(()->store.complete(abandoned)).hasMessage("OWNER_LOST");
        assertThat(new ReplayWorker(store,BOOTSTRAP,topic).runOne()).isNull();
        var retried=execute(manifest);assertThat(retried).isNotEqualTo(id);assertThat(value(retried,"unique_count")).isEqualTo(1);
    }
    @Test void retentionExpiryAndTopicRecreationFailWithExplicitReason() throws Exception {
        send(0,event(1,t.minusSeconds(10),t));send(0,event(2,t.minusSeconds(10),t));
        var manifest=capture(null,null);var expired=store.create(manifest);
        admin.deleteRecords(Map.of(new TopicPartition(topic,0),RecordsToDelete.beforeOffset(1))).all().get(10,TimeUnit.SECONDS);
        new ReplayWorker(store,BOOTSTRAP,topic).runOne();
        assertThat(store.get(expired).state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,expired)).isEqualTo("RETENTION_EXPIRED");
        var replaced=store.create(capture(null,null));admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);
        KafkaSupport.ensureTopic(BOOTSTRAP,topic);
        new ReplayWorker(store,BOOTSTRAP,topic).runOne();
        assertThat(store.get(replaced).state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,replaced)).isEqualTo("TOPIC_CHANGED");
        var expanded=store.create(capture(null,null));admin.createPartitions(Map.of(topic,NewPartitions.increaseTo(4))).all().get(10,TimeUnit.SECONDS);
        new ReplayWorker(store,BOOTSTRAP,topic).runOne();
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,expanded)).isEqualTo("PARTITIONS_CHANGED");
    }
    @Test void poisonPayloadAndIdentityConflictRollbackWholeBatchAndNeverExposePayload() throws Exception {
        var a=event(1,t.minusSeconds(10),t);send(0,a);
        producer.send(new ProducerRecord<>(topic,0,CHANNEL,"SYNTHETIC_PRIVATE_PAYLOAD".getBytes())).get(10,TimeUnit.SECONDS);
        var id=execute(capture(null,null));assertThat(store.get(id).state()).isEqualTo("FAILED");assertThat(value(id,"read_count")).isZero();
        assertThat(new tools.jackson.databind.json.JsonMapper().writeValueAsString(store.status(id))).doesNotContain("SYNTHETIC_PRIVATE_PAYLOAD");
        // 같은 ID의 다른 payload도 selected ledger의 hash 충돌로 실패한다.
        var manifest=capture(null,null);var conflict=store.create(manifest);var job=store.claim(UUID.randomUUID());assertThat(job.id()).isEqualTo(conflict);
        var changed=event(1,t.minusSeconds(20),t);
        assertThatThrownBy(()->store.apply(job,List.of(new ConsumerRecord<>(topic,0,0,CHANNEL,codec.encode(a)),new ConsumerRecord<>(topic,0,1,CHANNEL,codec.encode(changed))),Map.of(0,2L))).isInstanceOf(EventContractException.class);
        assertThat(value(conflict,"unique_count")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE run_id=?",Long.class,conflict.toString())).isZero();
    }
    @Test void sqlFailureRollsBackLedgerBucketCountersAndOffset() throws Exception {
        send(0,event(1,t.minusSeconds(10),t));
        jdbc.execute("CREATE FUNCTION reject_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.projection='replay' THEN RAISE EXCEPTION 'SYNTHETIC_FAILURE'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_replay BEFORE INSERT ON chat_bucket_10s FOR EACH ROW EXECUTE FUNCTION reject_replay()");
        var id=execute(capture(null,null));assertThat(store.get(id).state()).isEqualTo("FAILED");
        assertThat(value(id,"unique_count")).isZero();assertThat(value(id,"read_count")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE run_id=?",Long.class,id.toString())).isZero();
        assertThat(jdbc.queryForObject("SELECT sum(next_resolved_offset)::bigint FROM projection_progress WHERE run_id=?",Long.class,id.toString())).isZero();
    }
    @Test void liveAgeIsNotRelaxedAndWatchdogDoesNotReapAnActiveWorker() throws Exception {
        var old=event(1,t.minusSeconds(49*3600),t.minusSeconds(49*3600));
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds(schema)));
        var live=new ProjectionStore(jdbc,tx,codec,"ranking","live-v1");
        var record=new ConsumerRecord<String,byte[]>(topic,0,0,CHANNEL,codec.encode(old));
        assertThatThrownBy(()->live.apply(List.of(record))).isInstanceOf(EventContractException.class);
        assertThatThrownBy(()->live.applyReplay(List.of(record))).hasMessage("REPLAY_NAMESPACE_REQUIRED");
        var replay=new ProjectionStore(jdbc,tx,codec,"replay",UUID.randomUUID().toString());assertThat(replay.applyReplay(List.of(record))).isEqualTo(1);
        var id=store.create(capture(null,null));var claimed=store.claim(UUID.randomUUID());
        jdbc.update("UPDATE replay_run SET heartbeat_at=now()-interval '31 seconds' WHERE run_id=?",id);
        try(var lock=store.tryWorkerLock()) {assertThat(lock).isNotNull();assertThat(store.reap()).isZero();assertThatThrownBy(()->new ReplayWorker(store,BOOTSTRAP,topic).runOne()).hasMessage("WORKER_BUSY");assertThat(store.get(id).state()).isEqualTo("RUNNING");}
        assertThat(store.reap()).isEqualTo(1);assertThatThrownBy(()->store.complete(claimed)).hasMessage("OWNER_LOST");
    }
    @Test void readonlyCliDoesNotMigrateOrReapAndDifferentExecutionBuildsCannotCompare() throws Exception {
        var first=execute(capture(null,null));var second=execute(capture(null,null));
        jdbc.update("UPDATE replay_run SET worker_build=? WHERE run_id=?","b".repeat(64),second);
        assertThatThrownBy(()->store.compare(first,second)).hasMessage("INCOMPATIBLE_RUNS");
        var stale=store.create(capture(null,null));store.claim(UUID.randomUUID());
        jdbc.update("UPDATE replay_run SET heartbeat_at=now()-interval '60 seconds' WHERE run_id=?",stale);
        jdbc.execute("CREATE FUNCTION reject_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_PRIVATE_SQL'; END $$");
        jdbc.execute("CREATE TRIGGER reject_replay BEFORE UPDATE ON replay_run FOR EACH ROW EXECUTE FUNCTION reject_replay()");
        assertThat(command("status",stale.toString()).path("state").asText()).isEqualTo("RUNNING");
        jdbc.execute("DROP TRIGGER reject_replay ON replay_run");
        jdbc.execute("DROP TABLE flyway_schema_history");
        assertThat(command("status",first.toString()).path("state").asText()).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("SELECT to_regclass('flyway_schema_history')",String.class)).isNull();
        assertThat(commandExpected(2,"compare",first.toString(),first.toString()).path("error").asText()).isEqualTo("INCOMPATIBLE_RUNS");
    }
    @Test void tailControlRecordTimesOutButAnIntermediateControlOffsetIsAGap() throws Exception {
        var settings=new HashMap<String,Object>();settings.put("bootstrap.servers",BOOTSTRAP);settings.put("transactional.id","it-replay-"+UUID.randomUUID());settings.put("acks","all");
        try(var transactional=new KafkaProducer<String,byte[]>(settings,new StringSerializer(),new ByteArraySerializer())) {
            transactional.initTransactions();transactional.beginTransaction();
            transactional.send(new ProducerRecord<>(topic,0,t.toEpochMilli(),CHANNEL,codec.encode(event(700,t.minusSeconds(10),t)))).get(10,TimeUnit.SECONDS);
            transactional.commitTransaction();
        }
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(()->
                assertThat(admin.listOffsets(Map.of(new TopicPartition(topic,0),OffsetSpec.latest())).all().get(5,TimeUnit.SECONDS).get(new TopicPartition(topic,0)).offset()).isEqualTo(2));
        var manifest=capture(null,null);assertThat(manifest.partitions().getFirst().end()).isEqualTo(2);
        var tail=store.create(manifest);new ReplayWorker(store,BOOTSTRAP,topic,job->{},Duration.ofSeconds(10),Duration.ofSeconds(1)).runOne();
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,tail)).isEqualTo("NO_PROGRESS");
        send(0,event(701,t.minusSeconds(9),t));var middle=execute(capture(null,null));
        assertThat(jdbc.queryForObject("SELECT reason FROM replay_run WHERE run_id=?",String.class,middle)).isEqualTo("OFFSET_GAP");
    }
    @Test void anotherActiveWorkerDefersReapingADeadRunUntilTheGlobalLockIsReleased() throws Exception {
        var manifest=capture(null,null);var stale=store.create(manifest);store.claim(UUID.randomUUID());
        jdbc.update("UPDATE replay_run SET heartbeat_at=now()-interval '60 seconds' WHERE run_id=?",stale);
        var active=store.create(manifest);
        try(var lock=store.tryWorkerLock()) {
            assertThat(lock).isNotNull();var claimed=store.claim(UUID.randomUUID());assertThat(claimed.id()).isEqualTo(active);
            assertThat(store.reap()).isZero();assertThat(store.get(stale).state()).isEqualTo("RUNNING");store.complete(claimed);
        }
        assertThat(store.reap()).isEqualTo(1);assertThat(store.get(stale).state()).isEqualTo("FAILED");assertThat(store.get(active).state()).isEqualTo("COMPLETED");
    }
    @Test void retentionReportsOffsetLossAndKafkaTimestampSamplesWithoutInferringEventTimeCompleteness() throws Exception {
        send(0,event(801,t.minusSeconds(600),t));send(0,event(802,t.minusSeconds(900),t.minusSeconds(30)));
        var manifest=capture(null,null);
        try(var source=new ReplaySource(BOOTSTRAP,topic,UUID.randomUUID())) {
            var json=tools.jackson.databind.json.JsonMapper.builder().build().valueToTree(source.retention(manifest));var partition=json.path("partitions").get(0);
            assertThat(partition.path("capturedOffsetSpan").asLong()).isEqualTo(2);
            assertThat(partition.path("firstProbe").path("kafkaTimestamp").asText()).isEqualTo(t.toString());
            assertThat(partition.path("lastProbe").path("kafkaTimestamp").asText()).isEqualTo(t.minusSeconds(30).toString());
            admin.deleteRecords(Map.of(new TopicPartition(topic,0),RecordsToDelete.beforeOffset(1))).all().get(10,TimeUnit.SECONDS);
            json=tools.jackson.databind.json.JsonMapper.builder().build().valueToTree(source.retention(manifest));partition=json.path("partitions").get(0);
            assertThat(partition.path("state").asText()).isEqualTo("RETENTION_EXPIRED");assertThat(partition.path("missingPrefixOffsetSpan").asLong()).isEqualTo(1);
            assertThat(partition.path("availableCapturedOffsetSpan").asLong()).isEqualTo(1);
        }
    }
    @Test void invalidDatesMissingTopicsAndUnavailableBrokersHaveDistinctCodes() throws Exception {
        assertThat(commandExpected(2,"capture","unused.json","not-a-date",t.toString()).path("error").asText()).isEqualTo("INVALID_DATE");
        try(var source=new ReplaySource(BOOTSTRAP,"it.absent."+UUID.randomUUID(),UUID.randomUUID())) {
            assertThatThrownBy(()->source.capture(null,null)).hasMessage("TOPIC_NOT_FOUND");
        }
        int port;try(var socket=new java.net.ServerSocket(0)){port=socket.getLocalPort();}
        try(var source=new ReplaySource("127.0.0.1:"+port,topic,UUID.randomUUID())) {
            assertThatThrownBy(()->source.capture(null,null)).hasMessage("SOURCE_UNAVAILABLE");
        }
    }
    @Test void missingBuildResourceReturnsOnlyAFixedCodeAndDoesNotCreateARun() throws Exception {
        var manifest=Path.of("../work/replay-build-manifest-"+UUID.randomUUID()+".json").toAbsolutePath();Files.createDirectories(manifest.getParent());Files.writeString(manifest,capture(null,null).json());
        var altered=Files.createTempFile(manifest.getParent(),"replay-build-missing-",".jar");Files.copy(executableJar,altered,StandardCopyOption.REPLACE_EXISTING);
        try {
            try(var zip=FileSystems.newFileSystem(altered)) { Files.delete(zip.getPath("/BOOT-INF/classes/replay-build-id.txt")); }
            executableJar=altered;
            assertThat(commandExpected(2,"create",manifest.toString()).path("error").asText()).isEqualTo("BUILD_UNAVAILABLE");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM replay_run",Long.class)).isZero();
        } finally { Files.deleteIfExists(altered);Files.deleteIfExists(manifest); }
    }
}
