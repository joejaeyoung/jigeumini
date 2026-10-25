package io.chatpulse.notifications;

import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.nio.file.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import static io.chatpulse.notifications.NotificationFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration")
class NotificationIntegrationTest {
    private String schema,topic;
    private JdbcTemplate jdbc;
    private Instant at;
    @BeforeEach void setup(){
        schema="it_"+UUID.randomUUID().toString().replace("-","");topic="it.notifications."+UUID.randomUUID();
        new JdbcTemplate(ds("public")).execute("CREATE SCHEMA "+schema);
        var data=ds(schema);org.flywaydb.core.Flyway.configure().dataSource(data).defaultSchema(schema).load().migrate();jdbc=new JdbcTemplate(data);
        KafkaSupport.ensureTopic(BOOTSTRAP,topic);at=Instant.ofEpochSecond(Math.floorDiv(Instant.now().minusSeconds(10).getEpochSecond(),10)*10);
    }
    @AfterEach void cleanup() throws Exception {
        new JdbcTemplate(ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");
        try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))){
            var topics=admin.listTopics().names().get(10,TimeUnit.SECONDS).stream().filter(t->t.equals(topic)||t.equals(topic+".chat")).toList();
            if(!topics.isEmpty())admin.deleteTopics(topics).all().get(10,TimeUnit.SECONDS);
            var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().map(g->g.groupId()).filter(g->g.startsWith(topic)).toList();
            if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);
        }
    }
    private long count(String table){return jdbc.queryForObject("SELECT count(*) FROM "+table,Long.class);}
    private SpikeOutbox outbox(){return new SpikeOutbox(jdbc,tx(jdbc),BOOTSTRAP,topic,RUN);}
    private NotificationConsumer consumer(){return new NotificationConsumer(BOOTSTRAP,topic,topic+"-feed","earliest",store(jdbc,Clock.systemUTC()));}
    private void insert(UUID id,Instant time){tx(jdbc).executeWithoutResult(s->NotificationFixture.insert(jdbc,id,time,CHANNEL));}
    private long end() throws Exception {try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))){var positions=new HashMap<TopicPartition,org.apache.kafka.clients.admin.OffsetSpec>();for(int p=0;p<3;p++)positions.put(new TopicPartition(topic,p),org.apache.kafka.clients.admin.OffsetSpec.latest());return admin.listOffsets(positions).all().get(10,TimeUnit.SECONDS).values().stream().mapToLong(v->v.offset()).sum();}}
    private long committed() throws Exception {try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))){return admin.listConsumerGroupOffsets(topic+"-feed").partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS).values().stream().mapToLong(v->v.offset()).sum();}}
    private void crash(String mode,UUID id) throws Exception {
        var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",System.getProperty("integration.classpath"),
                NotificationCrashProcess.class.getName(),schema,topic,mode,id.toString(),at.toString()).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        try {assertThat(process.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isEqualTo(mode.equals("commit")?95:96);}
        finally {if(process.isAlive())process.destroyForcibly();}
    }
    @Test void outboxCommitSurvivesJvmExitAndAckExitRepublishesWithoutDuplicateFeed() throws Exception {
        var id=UUID.randomUUID();crash("commit",id);
        assertThat(count("spike_event")).isEqualTo(1);assertThat(count("spike_outbox")).isEqualTo(1);assertThat(end()).isZero();
        crash("ack",id);assertThat(end()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT published_at IS NULL FROM spike_outbox",Boolean.class)).isTrue();
        try(var outbox=outbox();var consumer=consumer()){
            assertThat(outbox.publishOne()).isTrue();assertThat(outbox.publishOne()).isFalse();consumer.start();
            await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{assertThat(count("notification_event")).isEqualTo(1);assertThat(committed()).isEqualTo(2);});
            assertThat(end()).isEqualTo(2);
        }
        try(var restarted=consumer()){restarted.start();await().atMost(Duration.ofSeconds(15)).until(()->(int)restarted.status().get("assignedPartitions")==3);assertThat(count("notification_event")).isEqualTo(1);}
    }
    @Test void brokerRejectionKeepsOutboxPendingUntilRecovery() throws Exception {
        insert(UUID.randomUUID(),at);
        var resource=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,topic);
        try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP));var outbox=outbox()){
            admin.incrementalAlterConfigs(Map.of(resource,List.of(new org.apache.kafka.clients.admin.AlterConfigOp(new org.apache.kafka.clients.admin.ConfigEntry("max.message.bytes","64"),org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);
            assertThatThrownBy(outbox::publishOne).hasMessage("OUTBOX_DELIVERY_UNAVAILABLE");
            assertThat(end()).isZero();assertThat(jdbc.queryForObject("SELECT published_at IS NULL FROM spike_outbox",Boolean.class)).isTrue();
            admin.incrementalAlterConfigs(Map.of(resource,List.of(new org.apache.kafka.clients.admin.AlterConfigOp(new org.apache.kafka.clients.admin.ConfigEntry("max.message.bytes","1048588"),org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);
            assertThat(outbox.publishOne()).isTrue();assertThat(end()).isEqualTo(1);
        }
    }
    @Test void feedRollbackDuplicatesConflictsQualityCorrectionsDelayAndCursor() {
        var first=UUID.randomUUID();insert(first,at);
        var second=UUID.randomUUID();insert(second,at.minusSeconds(10));
        var store=store(jdbc,Clock.fixed(at.plusSeconds(601),ZoneOffset.UTC));
        var one=new SpikeDetected(1,first,RUN,CHANNEL,at,"chat-spike-v1",100,new java.math.BigDecimal(50),new java.math.BigDecimal(2),"HEALTHY_AT_DETECTION");
        var two=new SpikeDetected(1,second,RUN,CHANNEL,at.minusSeconds(10),"chat-spike-v1",100,new java.math.BigDecimal(50),new java.math.BigDecimal(2),"HEALTHY_AT_DETECTION");
        var r1=new ConsumerRecord<>(topic,0,0,CHANNEL,one.encode());var r2=new ConsumerRecord<>(topic,0,1,CHANNEL,two.encode());
        jdbc.execute("CREATE FUNCTION reject_progress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_DB_FAILURE'; END $$");
        jdbc.execute("CREATE TRIGGER reject_progress BEFORE INSERT ON notification_progress FOR EACH ROW EXECUTE FUNCTION reject_progress()");
        assertThatThrownBy(()->store.apply(List.of(r1,r2))).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(count("notification_event")).isZero();
        jdbc.execute("DROP TRIGGER reject_progress ON notification_progress");store.apply(List.of(r1,r2,r1));assertThat(count("notification_event")).isEqualTo(2);
        var page=store.page(List.of(CHANNEL),at.plusSeconds(1),new UUID(-1,-1),1);assertThat(page.events()).hasSize(1);assertThat(page.events().getFirst().delayed()).isTrue();assertThat(page.nextCursor()).isNotNull();
        var parts=page.nextCursor().split("\\|");assertThat(store.page(List.of(CHANNEL),Instant.parse(parts[0]),UUID.fromString(parts[1]),1).events().getFirst().spikeId()).isEqualTo(second.toString());
        assertThat(store.page(List.of("b".repeat(32)),at.plusSeconds(1),new UUID(-1,-1),10).events()).isEmpty();
        jdbc.update("UPDATE spike_event SET quality_warning='COLLECTION_REVISED' WHERE spike_id=?",first);
        assertThat(store.page(List.of(CHANNEL),at.plusSeconds(1),new UUID(-1,-1),10).events().getFirst().valid()).isFalse();
        var conflict=new SpikeDetected(1,first,RUN,CHANNEL,at,"chat-spike-v1",150,new java.math.BigDecimal(50),new java.math.BigDecimal(3),"HEALTHY_AT_DETECTION");
        assertThatThrownBy(()->store.apply(List.of(new ConsumerRecord<>(topic,0,2,CHANNEL,conflict.encode())))).hasMessage("SPIKE_ID_CONFLICT");
        assertThat(jdbc.queryForObject("SELECT next_offset FROM notification_progress",Long.class)).isEqualTo(2);
    }
    @Test void cleanupProtectsPendingAndUnconsumedOutboxAndKeepsDeduplicationLedger(){
        insert(UUID.randomUUID(),at);insert(UUID.randomUUID(),at.minusSeconds(10));insert(UUID.randomUUID(),at.minusSeconds(20));
        jdbc.update("UPDATE spike_outbox SET published_at=now()-interval '8 days' WHERE spike_id IN (SELECT spike_id FROM spike_event WHERE evaluated_at<?)",java.sql.Timestamp.from(at));
        var id=jdbc.queryForObject("SELECT spike_id FROM spike_event WHERE evaluated_at=?",UUID.class,java.sql.Timestamp.from(at.minusSeconds(20)));
        var event=new SpikeDetected(1,id,RUN,CHANNEL,at.minusSeconds(20),"chat-spike-v1",100,new java.math.BigDecimal(50),new java.math.BigDecimal(2),"HEALTHY_AT_DETECTION");
        store(jdbc,Clock.systemUTC()).apply(List.of(new ConsumerRecord<>(topic,0,0,CHANNEL,event.encode())));
        try(var outbox=outbox()){assertThat(outbox.cleanup()).isEqualTo(1);}
        assertThat(count("spike_outbox")).isEqualTo(2);assertThat(count("notification_event")).isEqualTo(1);
    }
    @Test void sixtySecondFeedPauseDoesNotStopRankingAndThenCatchesUp() throws Exception {
        String chat=topic+".chat";KafkaSupport.ensureTopic(BOOTSTRAP,chat);
        var rankingStore=new ProjectionStore(jdbc,tx(jdbc),new EventCodec(),"ranking",RUN);
        var factory=KafkaSupport.producerFactory(BOOTSTRAP);
        try(var ranking=new ProjectionConsumer(KafkaSupport.consumerFactory(BOOTSTRAP,topic+"-ranking","earliest"),chat,rankingStore);
            var feed=consumer();var outbox=outbox()){
            ranking.start();feed.start();await().atMost(Duration.ofSeconds(15)).until(()->(int)feed.status().get("assignedPartitions")==3);
            feed.close();long stopped=System.nanoTime();
            var kafka=new KafkaTemplate<>(factory);
            for(int i=0;i<3;i++){
                insert(UUID.randomUUID(),at.minusSeconds(i*10L));assertThat(outbox.publishOne()).isTrue();
                Instant now=Instant.now();var event=new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic-room",UUID.randomUUID(),null,"CHAT",now,now,false,"test");
                kafka.send(chat,CHANNEL,new EventCodec().encode(event)).get(10,TimeUnit.SECONDS);
            }
            await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(rankingStore.totals().get("bucketTotal")).isEqualTo(3L));
            assertThat(count("notification_event")).isZero();
            long remaining=TimeUnit.SECONDS.toNanos(60)-(System.nanoTime()-stopped);
            if(remaining>0)TimeUnit.NANOSECONDS.sleep(remaining);
            assertThat(count("notification_event")).isZero();assertThat(rankingStore.totals().get("bucketTotal")).isEqualTo(3L);
            feed.start();await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{assertThat(count("notification_event")).isEqualTo(3);assertThat(committed()).isEqualTo(3);});
        }finally{factory.destroy();}
    }

}
