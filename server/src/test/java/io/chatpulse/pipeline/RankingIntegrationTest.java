package io.chatpulse.pipeline;

import io.chatpulse.quality.*;
import java.time.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration")
class RankingIntegrationTest {
    private String schema, topic;
    private JdbcTemplate jdbc;
    private RankingStore store;
    private final String a="a".repeat(32),b="b".repeat(32),c="c".repeat(32);
    private final UUID collector=UUID.randomUUID();
    private final MutableClock clock=new MutableClock();
    private Instant t;
    @BeforeEach void setup() {
        schema="it_"+UUID.randomUUID().toString().replace("-","");topic="it.quality."+UUID.randomUUID();
        jdbc=new JdbcTemplate(IntegrationSupport.ds("public"));jdbc.execute("CREATE SCHEMA "+schema);
        var ds=IntegrationSupport.ds(schema);
        org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();jdbc=new JdbcTemplate(ds);
        t=Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(),60)*60);clock.now=t;
        store=new RankingStore(jdbc,new TransactionTemplate(new DataSourceTransactionManager(ds)),"ranking","ranking-trend","test-v1",topic,clock);
    }
    @AfterEach void cleanup() throws Exception {
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");
        try(var admin=Admin.create(Map.of("bootstrap.servers",IntegrationSupport.BOOTSTRAP))) {
            if(admin.listTopics().names().get(10,TimeUnit.SECONDS).contains(topic))admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);
            var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().filter(g->g.groupId().startsWith(topic)).map(g->g.groupId()).toList();
            if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);
        }
    }
    private Timestamp ts(Instant value){return Timestamp.from(value);}
    private void healthy(String id) {
        jdbc.update("INSERT INTO collection_channel VALUES (?,?,true,?,'CONNECTED',?) ON CONFLICT(channel_id) DO UPDATE SET updated_at=excluded.updated_at",id,collector,ts(t.minusSeconds(600)),ts(clock.now));
        jdbc.update("INSERT INTO collection_session VALUES (?,?,?,?,?,?,NULL,NULL)",UUID.randomUUID(),id,collector,ts(t.minusSeconds(600)),ts(clock.now),ts(clock.now));
        jdbc.update("INSERT INTO collector_channel_buffer VALUES (?,?,?,NULL,0) ON CONFLICT(channel_id) DO UPDATE SET observed_at=excluded.observed_at",id,collector,ts(clock.now));
        for(String projection:List.of("ranking","ranking-trend"))for(int partition=0;partition<3;partition++)
            jdbc.update("INSERT INTO projection_observation VALUES (?,?,?,?,?,0,?,?,false) ON CONFLICT(projection,run_id,topic,partition) DO UPDATE SET observed_at=excluded.observed_at,last_caught_up_at=excluded.last_caught_up_at,poll_heartbeat_at=excluded.poll_heartbeat_at",projection,"test-v1",topic,partition,ts(clock.now),ts(clock.now),ts(clock.now));
    }
    private void bucket(String projection,String id,Instant at,long count) {
        jdbc.update("INSERT INTO chat_bucket_10s VALUES (?,?,?,?,?) ON CONFLICT(projection,run_id,channel_id,bucket_start) DO UPDATE SET chat_count=excluded.chat_count",projection,"test-v1",id,ts(at),count);
    }
    @Test void completedWindowsHealthyZeroTiesAndImmutableSnapshot() {
        healthy(a);healthy(b);healthy(c);
        bucket("ranking",a,t.minusSeconds(60),5);bucket("ranking",a,t.minusSeconds(10),5);bucket("ranking",a,t,999);
        bucket("ranking",b,t.minusSeconds(20),10);
        store.evaluate(t);var result=store.rankings(60,20);
        assertThat(result.status()).isEqualTo("READY");assertThat(result.entries()).extracting(RankingStore.Entry::count).containsExactly(10L,10L,0L);
        assertThat(result.entries()).extracting(RankingStore.Entry::rank).containsExactly(1,1,3);
        bucket("ranking",a,t.minusSeconds(10),100);store.evaluate(t);
        assertThat(store.rankings(60,20).entries().getFirst().count()).isEqualTo(10);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ranking_snapshot",Long.class)).isEqualTo(2);
        var activity=store.activity(a,t.minusSeconds(60),t);
        assertThat(activity.points().getFirst().count()).isEqualTo(105); // 지연 입력은 추이만 보정한다.
        clock.now=t.plusSeconds(21);assertThat(store.rankings(60,20).status()).isEqualTo("STALE");
    }
    @Test void knownLossLowCoverageWarmupAndLagAreExcluded() {
        healthy(a);healthy(b);healthy(c);
        jdbc.update("INSERT INTO collection_loss VALUES (?,?,?,?,?)",UUID.randomUUID(),a,ts(t.minusSeconds(30)),ts(t.minusSeconds(29)),"SYNTHETIC_DROP");
        jdbc.update("UPDATE collection_channel SET observed_since=? WHERE channel_id=?",ts(t.minusSeconds(20)),b);
        jdbc.update("UPDATE collector_channel_buffer SET oldest_unacked_at=? WHERE channel_id=?",ts(t.minusSeconds(11)),c);
        store.evaluate(t);var result=store.rankings(60,20);
        assertThat(result.entries()).isEmpty();assertThat(result.excludedChannels()).extracting(RankingStore.Entry::status).containsExactly("INCOMPLETE","WARMING_UP","LAGGING");
        assertThat(result.excludedChannels()).allMatch(e->e.count()==null&&e.rank()==null);
    }
    @Test void consecutiveTrendUsesIndependentBucketsAndInvalidatesPastAlertAfterDiscoveredLoss() {
        healthy(a);bucket("ranking-trend",a,t.minusSeconds(300),250);bucket("ranking-trend",a,t.minusSeconds(50),100);
        bucket("ranking",a,t.minusSeconds(50),1);
        store.evaluate(t);assertThat(store.spikes(t.plusSeconds(1),new UUID(-1,-1),20).events()).isEmpty();
        clock.now=t.plusSeconds(10);store.evaluate(clock.now);
        var alert=store.spikes(clock.now.plusSeconds(1),new UUID(-1,-1),20).events().getFirst();
        assertThat(alert.count()).isEqualTo(100);assertThat(alert.baseline()).isEqualByComparingTo("50");assertThat(alert.ratio()).isEqualByComparingTo("2");assertThat(alert.valid()).isTrue();
        store.evaluate(clock.now);assertThat(jdbc.queryForObject("SELECT count(*) FROM spike_event",Long.class)).isEqualTo(1);
        jdbc.update("INSERT INTO collection_loss VALUES (?,?,?,?,?)",UUID.randomUUID(),a,ts(t.minusSeconds(30)),ts(t.minusSeconds(29)),"LATE_DISCOVERED_GAP");
        clock.now=t.plusSeconds(20);store.evaluate(clock.now);
        assertThat(store.spikes(clock.now.plusSeconds(1),new UUID(-1,-1),20).events().getFirst().valid()).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM snapshot_quality_revision",Long.class)).isGreaterThan(0);
        assertThat(jdbc.queryForObject("SELECT count FROM ranking_entry WHERE snapshot_id=(SELECT snapshot_id FROM ranking_snapshot WHERE window_sec=60 AND evaluated_at=?)",Long.class,ts(t))).isEqualTo(1);
    }
    @Test void heartbeatTimeoutRevisesPreviouslyProvisionalCoverage() {
        healthy(a);
        jdbc.update("UPDATE collection_session SET source_alive_at=?",ts(t.minusSeconds(20)));
        store.evaluate(t);assertThat(store.rankings(60,20).entries()).hasSize(1);
        clock.now=t.plusSeconds(15);store.evaluate(t.plusSeconds(10));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM snapshot_quality_revision",Long.class)).isGreaterThan(0);
        assertThat(store.rankings(60,20).entries()).isEmpty();
    }
    @Test void telemetryPersistsSourceLivenessLossAndStoppedStateWithoutPayload() {
        var publisher=org.mockito.Mockito.mock(BufferedKafkaPublisher.class);
        org.mockito.Mockito.when(publisher.bufferSnapshot()).thenReturn(Map.of());
        var ds=IntegrationSupport.ds(schema);var session=UUID.randomUUID();
        try(var telemetry=new CollectionTelemetry(jdbc,new TransactionTemplate(new DataSourceTransactionManager(ds)),publisher)) {
            telemetry.start(a);telemetry.connected(a,session,t.minusSeconds(40));telemetry.alive(session,t.minusSeconds(1));
            telemetry.loss(a,t.minusSeconds(5),t.minusSeconds(4),"SYNTHETIC_DROP");telemetry.flush(t);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM collection_loss",Long.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT source_alive_at FROM collection_session",Timestamp.class).toInstant()).isEqualTo(t.minusSeconds(1));
            telemetry.ended(session,t,"HEARTBEAT_TIMEOUT");telemetry.stopped(a);telemetry.flush(t);
            assertThat(jdbc.queryForObject("SELECT ended_at FROM collection_session",Timestamp.class).toInstant()).isEqualTo(t.minusSeconds(1));
            assertThat(jdbc.queryForObject("SELECT enabled FROM collection_channel",Boolean.class)).isFalse();
        }
    }
    @Test void previousSnapshotAndCursorStayStableAndDisabledChannelsDisappearImmediately() {
        healthy(a);bucket("ranking",a,t.minusSeconds(50),10);store.evaluate(t);
        clock.now=t.plusSeconds(60);healthy(a);store.evaluate(clock.now);
        assertThat(store.rankings(60,20).entries().getFirst().previousRank()).isEqualTo(1);
        assertThat(store.rankings(60,20).entries().getFirst().previousReason()).isNull();
        jdbc.update("UPDATE collection_channel SET enabled=false WHERE channel_id=?",a);
        assertThat(store.rankings(60,20).entries()).isEmpty();
        assertThat(store.rankings(60,20).excludedChannels().getFirst().status()).isEqualTo("DISABLED");
        for(int i=0;i<3;i++)jdbc.update("INSERT INTO spike_event VALUES (?,?,?,?,?,?,?,?,NULL)",new UUID(0,i+1),"test-v1",a,ts(t.minusSeconds(i*10)),TrendPolicy.VERSION,100,50,2);
        var first=store.spikes(t.plusSeconds(1),new UUID(-1,-1),2);
        assertThat(first.events()).hasSize(2);assertThat(first.nextCursor()).isNotNull();
        var parts=first.nextCursor().split("\\|");
        var next=store.spikes(Instant.parse(parts[0]),UUID.fromString(parts[1]),2);
        assertThat(next.events()).hasSize(1);assertThat(next.nextCursor()).isNull();
        assertThat(next.events().getFirst().spikeId()).isNotIn(first.events().stream().map(RankingStore.Spike::spikeId).toList());
    }
    @Test void quietRealPartitionsHavePollHeartbeatAndTwoIndependentConsumersCatchUp() throws Exception {
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);
        var ds=IntegrationSupport.ds(schema);
        var rankingStore=new ProjectionStore(jdbc,new TransactionTemplate(new DataSourceTransactionManager(ds)),new EventCodec(),"ranking","test-v1");
        var trendStore=new ProjectionStore(jdbc,new TransactionTemplate(new DataSourceTransactionManager(ds)),new EventCodec(),"ranking-trend","test-v1");
        try(var rankConsumer=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,topic+"-ranking","earliest"),topic,rankingStore);
            var trendConsumer=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,topic+"-trend","earliest"),topic,trendStore);
            var observer=new ProjectionObserver(IntegrationSupport.BOOTSTRAP,jdbc,rankConsumer,"ranking","test-v1")) {
            rankConsumer.start();trendConsumer.start();
            await().atMost(Duration.ofSeconds(15)).until(()->rankConsumer.pollState().partitions().size()==3&&trendConsumer.pollState().partitions().size()==3);
            observer.observe();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM projection_observation WHERE last_caught_up_at IS NOT NULL AND poll_heartbeat_at IS NOT NULL",Long.class)).isEqualTo(3);
            var factory=KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
            try(var publisher=new BufferedKafkaPublisher(new org.springframework.kafka.core.KafkaTemplate<>(factory),topic,new EventCodec(),10)) {
                var event=EventCodecTest.event(Instant.now());
                publisher.accept(List.of(event,event));assertThat(publisher.finish().get("complete")).isEqualTo(true);
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->{
                    for(String projection:List.of("ranking","ranking-trend")) assertThat(jdbc.queryForObject("SELECT sum(next_resolved_offset)::bigint FROM projection_progress WHERE projection=?",Long.class,projection)).isEqualTo(2L);
                    assertThat(rankingStore.totals().get("bucketTotal")).isEqualTo(1L);assertThat(trendStore.totals().get("bucketTotal")).isEqualTo(1L);});
            }finally{factory.destroy();}
            rankConsumer.close();
            var nextFactory=KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
            try(var publisher=new BufferedKafkaPublisher(new org.springframework.kafka.core.KafkaTemplate<>(nextFactory),topic,new EventCodec(),10)) {
                publisher.accept(List.of(EventCodecTest.event(Instant.now())));assertThat(publisher.finish().get("complete")).isEqualTo(true);
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(trendStore.totals().get("bucketTotal")).isEqualTo(2L));
                assertThat(rankingStore.totals().get("bucketTotal")).isEqualTo(1L);
            }finally{nextFactory.destroy();}
        }
    }
    static final class MutableClock extends Clock {
        Instant now;
        public Instant instant(){return now;}public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}
    }
}
