package io.chatpulse.categories;

import io.chatpulse.metadata.*;
import io.chatpulse.observations.*;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
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
class CategoryIntegrationTest {
    static final String BOOTSTRAP="localhost:19092",A="a".repeat(32),B="b".repeat(32);
    private String schema,chat,observations,group;
    private JdbcTemplate jdbc;
    private PGSimpleDataSource dataSource;
    private CategoryStore store;
    private final EventCodec codec=new EventCodec();
    static PGSimpleDataSource ds(String schema) {
        var ds=new PGSimpleDataSource();ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");ds.setUser("chatpulse");ds.setPassword("local-chatpulse");
        ds.setCurrentSchema(schema);ds.setConnectTimeout(3);ds.setSocketTimeout(10);return ds;
    }
    private TransactionTemplate tx() { var tx=new TransactionTemplate(new DataSourceTransactionManager(dataSource));tx.setTimeout(20);return tx; }
    private CategoryStore store(Clock clock) { return new CategoryStore(jdbc,tx(),"test-v1",chat,observations,clock); }
    @BeforeEach void setup() {
        String suffix=UUID.randomUUID().toString().replace("-","");schema="it_cat_"+suffix;chat="it.category."+suffix;observations=chat+".observations";group=chat+"-group";
        new JdbcTemplate(ds("public")).execute("CREATE SCHEMA "+schema);
        dataSource=ds(schema);org.flywaydb.core.Flyway.configure().dataSource(dataSource).defaultSchema(schema).load().migrate();jdbc=new JdbcTemplate(dataSource);store=store(Clock.systemUTC());
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
    private BroadcastObserved sample(String channel,Instant at,String category) {
        return BroadcastObserved.observed("test-v1",channel,at,120,new PublicBroadcastObservation(BroadcastMetadata.LiveStatus.LIVE,0L,"123",category,
                category==null?null:"합성 "+category,PublicBroadcastObservation.Quality.OBSERVED));
    }
    private ChatEvent event(String channel,Instant at) { return new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",channel,"synthetic-room",UUID.randomUUID(),null,"CHAT",at,at,false,"test"); }
    private ConsumerRecord<String,byte[]> record(BroadcastObserved sample,long offset) { return new ConsumerRecord<>(observations,0,offset,sample.channelId(),sample.encode()); }
    private ConsumerRecord<String,byte[]> record(ChatEvent event,long offset) { return new ConsumerRecord<>(chat,0,offset,event.channelId(),codec.encode(event)); }
    private long count(String category) { return jdbc.queryForObject("SELECT coalesce(sum(message_count),0) FROM category_bucket WHERE category_key=?",Long.class,CategoryStore.key(category)); }
    private long rows(String table) { return jdbc.queryForObject("SELECT count(*) FROM "+table,Long.class); }
    private long revision() { return jdbc.queryForObject("SELECT revision FROM category_revision",Long.class); }
    @Test void temporalJoinUsesPastSamplesAndRevisesOnlyTheirValidityRangeRegardlessOfArrivalOrder() {
        Instant base=Instant.parse("2026-10-25T12:00:00Z");store=store(Clock.fixed(base.plusSeconds(600),ZoneOffset.UTC));
        var first=sample(A,base,"game");store.apply(List.of(record(first,0)));
        long[] seconds={-1,0,89,90,119,120,209,210};var records=new ArrayList<ConsumerRecord<String,byte[]>>();
        for(int i=0;i<seconds.length;i++)records.add(record(event(A,base.plusSeconds(seconds[i])),i));store.apply(records);
        assertEquals(4,count("game"));assertEquals(4,count(null));
        var changed=sample(A,base.plusSeconds(90),"cook");store.apply(List.of(record(changed,1)));
        assertEquals(2,count("game"));assertEquals(4,count("cook"));assertEquals(2,count(null));assertEquals(8,rows("category_event"));
        long revision=revision();store.apply(records);store.apply(List.of(record(changed,2)));
        assertEquals(revision,revision());assertEquals(4,count("cook"));
        store.apply(List.of(record(sample(A,base.plusSeconds(240),"future"),3)));
        assertEquals(0,count("future"));assertEquals(4,count("cook"));
        store.apply(List.of(record(sample(A,base.plusSeconds(30),"late"),4)));
        assertEquals(1,count("game"));assertEquals(1,count("late"));assertEquals(4,count("cook"));assertEquals(2,count(null));
        var window=store.window(base,base.plusSeconds(300));
        assertTrue(window.memberships().stream().anyMatch(m->m.categoryKey().equals(CategoryStore.UNKNOWN)&&m.from().equals(base.plusSeconds(210))&&m.to().equals(base.plusSeconds(240))));
        assertFalse(window.fresh());
    }
    @Test void metadataCorrectionAndNewEventsRollbackTogetherAndConflictsPreserveOldState() {
        Instant base=Instant.now().minusSeconds(60);var observed=sample(A,base,"game");var original=event(A,base.plusSeconds(10));
        store.apply(List.of(record(observed,0),record(original,0)));long revision=revision();
        jdbc.execute("CREATE FUNCTION reject_category() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'SYNTHETIC_PRIVATE_SQL'; END $$");
        jdbc.execute("CREATE TRIGGER reject_category BEFORE INSERT ON category_progress FOR EACH ROW EXECUTE FUNCTION reject_category()");
        var changed=sample(A,base.plusSeconds(5),"cook");var next=event(A,base.plusSeconds(20));
        assertThrows(RuntimeException.class,()->store.apply(List.of(record(changed,1),record(next,1))));
        assertEquals(1,count("game"));assertEquals(0,count("cook"));assertEquals(1,rows("category_event"));assertEquals(1,rows("category_observation"));assertEquals(revision,revision());
        jdbc.execute("DROP TRIGGER reject_category ON category_progress");store.apply(List.of(record(changed,1),record(next,1)));assertEquals(2,count("cook"));
        var conflict=new ChatEvent(original.schemaVersion(),original.eventId(),original.source(),original.channelId(),original.chatRoomId(),original.collectionSessionId(),null,"CHAT",original.eventTime().minusSeconds(1),original.receivedAt(),false,"test");
        assertEquals("CATEGORY_EVENT_CONFLICT",assertThrows(IllegalArgumentException.class,()->store.apply(List.of(record(event(A,base.plusSeconds(30)),2),record(conflict,3)))).getMessage());
        assertEquals(2,count("cook"));assertEquals(2,rows("category_event"));
        var altered=new BroadcastObserved(changed.schemaVersion(),changed.eventId(),changed.runId(),changed.channelId(),changed.observedAt(),changed.ttlSeconds(),1L,
                changed.liveStatus(),changed.quality(),changed.sourceBroadcastId(),changed.broadcastIdentity(),changed.categoryId(),changed.categoryName());
        assertEquals("CATEGORY_OBSERVATION_CONFLICT",assertThrows(IllegalArgumentException.class,()->store.apply(List.of(record(altered,2)))).getMessage());
    }
    @Test void concurrentChannelsSharingOneCategoryDoNotLoseCountsAndExpiredHistoryStaysExpired() throws Exception {
        Instant base=Instant.now().minusSeconds(60);store.apply(List.of(record(sample(A,base,"game"),0),record(sample(B,base,"game"),1)));
        try(var workers=Executors.newFixedThreadPool(2)) {
            var tasks=new ArrayList<Future<?>>();
            for(String channel:List.of(A,B))tasks.add(workers.submit(()->{
                var records=new ArrayList<ConsumerRecord<String,byte[]>>();for(int i=0;i<50;i++){var event=event(channel,base.plusSeconds(10));records.add(record(event,i*2));records.add(record(event,i*2+1));}
                store.apply(records);
            }));
            for(var task:tasks)task.get(20,TimeUnit.SECONDS);
        }
        assertEquals(100,count("game"));assertEquals(100,rows("category_event"));
        store=store(Clock.fixed(Instant.now().plusSeconds(7*86400+60),ZoneOffset.UTC));store.cleanup();
        assertEquals(0,rows("category_event"));assertEquals(0,rows("category_bucket"));
        store.apply(List.of(record(event(A,base.plusSeconds(10)),101)));assertEquals(0,rows("category_event"));
    }
    @Test void independentConsumerPauseLetsRankingContinueAndRestartRestoresCategoryWithoutDuplication() throws Exception {
        var ranking=new ProjectionStore(jdbc,tx(),codec,"ranking","test-v1");
        var factory=KafkaSupport.producerFactory(BOOTSTRAP);var kafka=new KafkaTemplate<>(factory);
        var categories=new CategoryConsumer(BOOTSTRAP,List.of(chat,observations),group,"earliest",store);
        try(var rankingConsumer=new ProjectionConsumer(KafkaSupport.consumerFactory(BOOTSTRAP,group+"-ranking","earliest"),chat,ranking);
            var publisher=new ObservationPublisher(BOOTSTRAP,observations)) {
            rankingConsumer.start();categories.start();awaitCommitted(0);
            Instant base=Instant.now().minusSeconds(10);publisher.accept(sample(A,base,"game"));var one=event(A,base.plusSeconds(1));
            kafka.send(chat,A,codec.encode(one)).get(10,TimeUnit.SECONDS);awaitCommitted(2);assertEquals(1,count("game"));
            categories.close();
            for(int i=0;i<2;i++)kafka.send(chat,A,codec.encode(event(A,base.plusSeconds(2+i)))).get(10,TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertEquals(3L,ranking.totals().get("bucketTotal")));
            assertEquals(1,count("game"));
            try(var restarted=new CategoryConsumer(BOOTSTRAP,List.of(chat,observations),group,"none",store)) {
                restarted.start();awaitCommitted(4);assertEquals(3,count("game"));
                kafka.send(chat,A,codec.encode(one)).get(10,TimeUnit.SECONDS);awaitCommitted(5);assertEquals(3,count("game"));
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertTrue(store.window(base.minusSeconds(300),Instant.ofEpochSecond(Math.floorDiv(base.getEpochSecond(),60)*60)).fresh(),
                        ()->restarted.status()+" "+jdbc.queryForList("SELECT partition,observed_at,poll_at,source_end_offset,caught_up_at,blocked FROM category_partition")));
            }
            try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) { admin.deleteTopics(List.of(observations)).all().get(10,TimeUnit.SECONDS); }
            KafkaSupport.ensureTopic(BOOTSTRAP,observations);
            assertThrows(IllegalStateException.class,()->new CategoryConsumer(BOOTSTRAP,List.of(chat,observations),group,"none",store));assertEquals(3,count("game"));
        } finally { categories.close();factory.destroy(); }
    }
    @Test void readinessRequiresBothTopicsAndRecentPollsAndFailsClosedAfterStop() {
        Instant at=Instant.parse("2026-10-25T12:00:10Z"),end=at.minusSeconds(10);store=store(Clock.fixed(at,ZoneOffset.UTC));
        var partitions=new ArrayList<TopicPartition>();
        for(String topic:List.of(chat,observations))for(int i=0;i<3;i++) {
            var partition=new TopicPartition(topic,i);partitions.add(partition);store.initialize(partition,0);
            store.observe(partition,0,at,at,false);
        }
        assertTrue(store.window(end.minusSeconds(300),end).fresh());
        store.unavailable(List.of(partitions.getFirst()));assertFalse(store.window(end.minusSeconds(300),end).fresh());
        store.observe(partitions.getFirst(),0,at,at,false);assertTrue(store.window(end.minusSeconds(300),end).fresh());
        store.observe(partitions.getFirst(),1,at,at,false);assertFalse(store.window(end.minusSeconds(300),end).fresh());
        store.observe(partitions.getFirst(),0,at,at,false);
        store=store(Clock.fixed(at.plusSeconds(16),ZoneOffset.UTC));assertFalse(store.window(end.minusSeconds(300),end).fresh());
    }
    @Test void shorterLateObservationAlsoInvalidatesPreviousCategoryAfterItsOwnExpiry() {
        Instant base=Instant.now().minusSeconds(180);store.apply(List.of(record(sample(A,base,"game"),0),record(event(A,base.plusSeconds(100)),0)));
        assertEquals(1,count("game"));var source=sample(A,base.plusSeconds(30),"short");
        var shorter=new BroadcastObserved(source.schemaVersion(),source.eventId(),source.runId(),source.channelId(),source.observedAt(),60,source.viewerCount(),
                source.liveStatus(),source.quality(),source.sourceBroadcastId(),source.broadcastIdentity(),source.categoryId(),source.categoryName());
        store.apply(List.of(record(shorter,1)));assertEquals(0,count("game"));assertEquals(0,count("short"));assertEquals(1,count(null));
    }
    private void awaitCommitted(long expected) {
        await().atMost(Duration.ofSeconds(40)).untilAsserted(()->{
            try(var admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) {
                var offsets=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
                assertEquals(6,offsets.size());assertEquals(expected,offsets.values().stream().mapToLong(offset->offset.offset()).sum());
            }
        });
    }
}
