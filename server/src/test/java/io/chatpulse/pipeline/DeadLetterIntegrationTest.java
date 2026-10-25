package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration")
class DeadLetterIntegrationTest {
    private String schema,topic,group;
    private JdbcTemplate jdbc;
    private ProjectionStore store;
    private DeadLetterRecovery recovery;
    private Admin admin;
    private DefaultKafkaProducerFactory<String,byte[]> producer;
    private KafkaTemplate<String,byte[]> kafka;
    private final EventCodec codec=new EventCodec();
    @BeforeEach void setup() {
        String id=UUID.randomUUID().toString().replace("-","");
        schema="it_"+id; topic="it.dlq."+id; group=topic+"-group";
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("CREATE SCHEMA "+schema);
        var ds=IntegrationSupport.ds(schema);
        org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
        jdbc=new JdbcTemplate(ds);
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        store=new ProjectionStore(jdbc,tx,codec,"ranking","test-v1");
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);
        recovery=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,topic,jdbc,"ranking","test-v1");
        producer=KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP); kafka=new KafkaTemplate<>(producer);
        admin=Admin.create(Map.of("bootstrap.servers",IntegrationSupport.BOOTSTRAP,"default.api.timeout.ms",10000,"request.timeout.ms",5000));
    }
    @AfterEach void cleanup() throws Exception {
        if(recovery!=null)recovery.close(); if(producer!=null)producer.destroy();
        if(admin!=null) {
            admin.deleteTopics(List.of(topic,topic+".dlq")).all().get(10,TimeUnit.SECONDS);
            var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().filter(g->g.groupId().startsWith(group)).map(g->g.groupId()).toList();
            if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);
            admin.close();
        }
        new JdbcTemplate(IntegrationSupport.ds("public")).execute("DROP SCHEMA "+schema+" CASCADE");
    }
    private void send(byte[] value) throws Exception {
        kafka.send(new ProducerRecord<>(topic,0,System.currentTimeMillis(),EventCodecTest.CHANNEL,value)).get(15,TimeUnit.SECONDS);
    }
    private void mixedInput() throws Exception {
        send(codec.encode(EventCodecTest.event(Instant.now())));
        send("{PRIVATE_SYNTHETIC_CANARY".getBytes());
        send(codec.encode(EventCodecTest.event(Instant.now())));
    }
    private ProjectionConsumer consumer(String reset) {
        return new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,group,reset),topic,store,recovery);
    }
    private long committed() throws Exception {
        var value=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS).get(new TopicPartition(topic,0));
        return value==null?0:value.offset();
    }
    private void awaitCommitted(long count) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(()->assertEquals(count,committed()));
    }
    private long dlqCount() throws Exception {
        return admin.listOffsets(Map.of(new TopicPartition(topic+".dlq",0),OffsetSpec.latest())).all().get(5,TimeUnit.SECONDS)
                .get(new TopicPartition(topic+".dlq",0)).offset();
    }
    private void assertStored(long valid,long failures) {
        assertEquals(valid,store.totals().get("bucketTotal")); assertEquals(valid,store.totals().get("processedEvents"));
        assertEquals(failures,jdbc.queryForObject("SELECT count(*) FROM projection_failure WHERE projection='ranking'",Long.class));
    }
    private RecoveryDiagnostics diagnostics(ProjectionConsumer consumer) {
        return new RecoveryDiagnostics(IntegrationSupport.BOOTSTRAP,topic,"test-v1",jdbc,List.of(new RecoveryDiagnostics.Target(group,"ranking",consumer)));
    }
    @Test void permanentErrorsAreIsolatedAndBothIndependentProjectionsContinue() throws Exception {
        mixedInput();
        var original=EventCodecTest.event(Instant.now()); send(codec.encode(original)); send(codec.encode(original));
        var conflict=new ChatEvent(1,original.eventId(),original.source(),original.channelId(),original.chatRoomId(),original.collectionSessionId(),null,
                "CHAT",original.eventTime().minusSeconds(1),original.receivedAt(),false,"test");
        send(codec.encode(conflict));
        send(new String(codec.encode(EventCodecTest.event(Instant.now()))).replace("\"schemaVersion\":1","\"schemaVersion\":2").getBytes());
        var tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
        var trendStore=new ProjectionStore(jdbc,tx,codec,"ranking-trend","test-v1");
        try(var trendRecovery=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,topic,jdbc,"ranking-trend","test-v1");
            var trend=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,group+"-trend","earliest"),topic,trendStore,trendRecovery);
            var consumer=consumer("earliest")) {
            consumer.start(); trend.start(); awaitCommitted(7);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertEquals(2L,trendStore.totals().get("bucketTotal")));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertEquals(10L,jdbc.queryForObject("SELECT count(*) FROM projection_failure",Long.class)));
            assertStored(2,5);
            assertEquals(5L,jdbc.queryForObject("SELECT count(DISTINCT failure_id) FROM projection_failure",Long.class));
            assertEquals(Set.of("INVALID_EVENT_JSON","EVENT_ID_CONFLICT","INVALID_EVENT_CONTRACT"),
                    new HashSet<>(jdbc.queryForList("SELECT error_code FROM projection_failure",String.class)));
            // 같은 배치의 충돌 후보는 모두 제외했다. 이후 단독 정상 입력을 확정한 뒤의 충돌은 기존 집계를 보존한다.
            send(codec.encode(original)); awaitCommitted(8); assertStored(3,5);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertEquals(3L,trendStore.totals().get("bucketTotal")));
            send(codec.encode(conflict)); awaitCommitted(9); assertStored(3,6);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertEquals(12L,jdbc.queryForObject("SELECT count(*) FROM projection_failure",Long.class)));
        }
        long before=dlqCount();
        try(var consumer=consumer("none")) { consumer.start(); awaitCommitted(9); assertStored(3,6); assertEquals(before,dlqCount()); }
    }
    @Test void brokerRejectsDlqAckThenRecoversWithoutAdvancingSourceOrDb() throws Exception {
        var config=new ConfigResource(ConfigResource.Type.TOPIC,topic+".dlq");
        setMaxBytes(config,"64"); // DLQ 토픽만 작은 한도로 바꿔 실제 broker의 발행 거부를 유도한다.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(()->assertEquals("64",admin.describeConfigs(List.of(config)).all()
                .get(5,TimeUnit.SECONDS).get(config).get("max.message.bytes").value()));
        mixedInput();
        try(var consumer=consumer("earliest");var diagnostics=diagnostics(consumer)) {
            consumer.start();
            await().atMost(Duration.ofSeconds(35)).untilAsserted(()->assertEquals("DLQ_UNAVAILABLE",consumer.status().get("lastErrorCode")));
            assertEquals(0,committed()); assertStored(0,0); assertEquals(0,dlqCount());
            diagnostics.sample(); var snapshot=diagnostics.snapshot(); assertEquals("AVAILABLE",snapshot.state());
            var partition=snapshot.groups().getFirst().partitions().getFirst();
            assertEquals(3,partition.lag()); assertNotNull(partition.firstPendingAt()); assertEquals("PENDING",partition.state());
            setMaxBytes(config,"1048588"); awaitCommitted(3); assertStored(2,1);
            diagnostics.sample(); var groupView=diagnostics.snapshot().groups().getFirst();
            assertEquals(0,groupView.partitions().getFirst().lag()); assertEquals(1,groupView.errors().getFirst().count());
            assertEquals("AVAILABLE",groupView.recentFailures().getFirst().sourceAvailability());
            admin.deleteRecords(Map.of(new TopicPartition(topic,0),RecordsToDelete.beforeOffset(3))).all().get(10,TimeUnit.SECONDS);
            diagnostics.sample(); assertEquals("EXPIRED",diagnostics.snapshot().groups().getFirst().recentFailures().getFirst().sourceAvailability());
        } finally { setMaxBytes(config,"1048588"); }
    }
    private void setMaxBytes(ConfigResource resource,String value) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource,List.of(new AlterConfigOp(new ConfigEntry("max.message.bytes",value),AlterConfigOp.OpType.SET))))
                .all().get(10,TimeUnit.SECONDS);
    }
    @Test void databaseFailureNeverQuarantinesValidInputAndRecovers() throws Exception {
        jdbc.execute("CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic unavailable'; END $$");
        jdbc.execute("CREATE TRIGGER reject_event BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION reject_event()");
        send(codec.encode(EventCodecTest.event(Instant.now())));
        try(var consumer=consumer("earliest")) {
            consumer.start();
            await().atMost(Duration.ofSeconds(25)).until(()->(Long)consumer.status().get("failedAttempts")>=2);
            assertEquals(0,committed()); assertStored(0,0); assertEquals(0,dlqCount());
            jdbc.execute("DROP TRIGGER reject_event ON processed_event");
            awaitCommitted(1); assertStored(1,0); assertEquals(0,dlqCount());
        }
    }
    @Test void databaseFailureAfterAckRollsBackFailureLedgerBucketsAndProgress() throws Exception {
        jdbc.execute("CREATE FUNCTION reject_advance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic progress unavailable'; END $$");
        jdbc.execute("CREATE TRIGGER reject_advance BEFORE UPDATE ON projection_progress FOR EACH ROW EXECUTE FUNCTION reject_advance()");
        mixedInput();
        try(var consumer=consumer("earliest")) {
            consumer.start();
            await().atMost(Duration.ofSeconds(25)).until(()->(Long)consumer.status().get("failedAttempts")>=1);
            assertEquals(0,committed()); assertStored(0,0); assertTrue(dlqCount()>=1);
            jdbc.execute("DROP TRIGGER reject_advance ON projection_progress");
            awaitCommitted(3); assertStored(2,1); assertTrue(dlqCount()>=2);
        }
    }
    @ParameterizedTest @ValueSource(strings={"ack","db"})
    void crashRedeliveryUsesSameFailureIdAndNeverDoubleCounts(String point) throws Exception {
        mixedInput();
        var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",
                System.getProperty("integration.classpath"),DlqCrashProcess.class.getName(),schema,topic,group,point)
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try { assertTrue(process.waitFor(45,TimeUnit.SECONDS)); assertEquals(point.equals("ack")?93:94,process.exitValue()); }
        finally { if(process.isAlive())process.destroyForcibly().waitFor(); }
        assertEquals(0,committed()); assertEquals(1,dlqCount());
        assertStored(point.equals("ack")?0:2,point.equals("ack")?0:1);
        try(var consumer=consumer("none")) { consumer.start(); awaitCommitted(3); assertStored(2,1); }
        assertEquals(point.equals("ack")?2:1,dlqCount());
        var factory=KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,group+"-probe","none");
        try(var probe=factory.createConsumer()) {
            probe.assign(List.of(new TopicPartition(topic+".dlq",0))); probe.seek(new TopicPartition(topic+".dlq",0),0);
            var ids=new HashSet<String>(); var json=JsonMapper.builder().build(); long count=dlqCount();
            await().atMost(Duration.ofSeconds(10)).until(()->{
                for(var record:probe.poll(Duration.ofMillis(300))) {
                    String body=new String(record.value()); assertFalse(body.contains("PRIVATE"));
                    ids.add(json.readTree(record.value()).path("failureId").asText());
                }
                return probe.position(new TopicPartition(topic+".dlq",0))==count;
            });
            assertEquals(1,ids.size());
            assertEquals(ids.iterator().next(),jdbc.queryForObject("SELECT failure_id FROM projection_failure",String.class));
        }
    }
    @Test void recreatedSourceCannotReuseOldFailureIdentityOrProjection() throws Exception {
        var record=new ConsumerRecord<String,byte[]>(topic,0,0,"bad","invalid".getBytes());
        store.applyRecovering(List.of(record),recovery);
        admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).until(()->!admin.listTopics().names().get().contains(topic));
        KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);
        assertThrows(ProjectionRetryException.class,()->store.applyRecovering(List.of(record),recovery));
        try(var next=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,topic,jdbc,"ranking","test-v1")) {
            assertThrows(ProjectionRetryException.class,()->store.applyRecovering(List.of(record),next));
        }
        assertStored(0,1);
        try(var consumer=consumer("earliest");var diagnostics=diagnostics(consumer)) {
            diagnostics.sample(); var result=diagnostics.snapshot(); assertEquals("AVAILABLE",result.state());
            assertEquals("SOURCE_IDENTITY_CHANGED",result.groups().getFirst().partitions().getFirst().state());
            assertEquals("TOPIC_CHANGED",result.groups().getFirst().recentFailures().getFirst().sourceAvailability());
        }
    }
    @Test void executableAnalyzerExposesBothGroupsAndRecoveryPage() throws Exception {
        mixedInput();
        int port;
        try(var socket=new java.net.ServerSocket(0)) { port=socket.getLocalPort(); }
        var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-jar",
                Path.of("build/libs/chat-pulse-poc.jar").toAbsolutePath().toString(),"--spring.profiles.active=pipeline",
                "--pipeline.role=analyzer","--spring.main.web-application-type=servlet","--server.address=127.0.0.1","--server.port="+port,
                "--pipeline.bootstrap-servers="+IntegrationSupport.BOOTSTRAP,"--pipeline.topic="+topic,"--pipeline.group="+group,
                "--pipeline.run-id=test-v1","--pipeline.offset-reset=earliest","--quality.trend-offset-reset=earliest","--metadata.enabled=false",
                "--pipeline.jdbc-url=jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema,"--logging.level.root=OFF")
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try(var http=java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()) {
            var endpoint=java.net.URI.create("http://127.0.0.1:"+port);
            await().atMost(Duration.ofSeconds(40)).ignoreExceptions().untilAsserted(()->{
                assertTrue(process.isAlive());
                var response=http.send(java.net.http.HttpRequest.newBuilder(endpoint.resolve("/api/operations/recovery")).timeout(Duration.ofSeconds(2)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                assertEquals(200,response.statusCode()); assertFalse(response.body().contains("PRIVATE"));
                var json=JsonMapper.builder().build().readTree(response.body());
                assertEquals("AVAILABLE",json.path("state").asText()); assertEquals(2,json.path("groups").size());
                for(var view:json.path("groups")) {
                    assertEquals(3,view.path("partitions").size());
                    assertEquals(1,view.path("errors").get(0).path("count").asLong());
                    assertEquals(0,view.path("partitions").get(0).path("lag").asLong());
                }
            });
            var page=http.send(java.net.http.HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(2)).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(200,page.statusCode()); assertTrue(page.body().contains("소비 복구 진단"));
            assertStored(2,1);
        } finally {
            process.destroy(); if(!process.waitFor(20,TimeUnit.SECONDS))process.destroyForcibly().waitFor();
        }
    }
    @ParameterizedTest @ValueSource(strings={"age","conflict"})
    void committedNormalOffsetsAreNeverReclassifiedAfterCrash(String scenario) throws Exception {
        Instant firstTime=Instant.now();
        var event=EventCodecTest.event(scenario.equals("age")?firstTime.minusSeconds(48*3600-1):firstTime);
        send(codec.encode(event));
        var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",
                System.getProperty("integration.classpath"),DlqCrashProcess.class.getName(),schema,topic,group,"db",firstTime.toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try { assertTrue(process.waitFor(45,TimeUnit.SECONDS)); assertEquals(94,process.exitValue()); }
        finally { if(process.isAlive())process.destroyForcibly().waitFor(); }
        assertEquals(0,committed()); assertStored(1,0); assertEquals(0,dlqCount());
        if(scenario.equals("conflict")) {
            send(codec.encode(new ChatEvent(1,event.eventId(),event.source(),event.channelId(),event.chatRoomId(),event.collectionSessionId(),
                    null,"CHAT",event.eventTime().minusSeconds(1),event.receivedAt(),false,"test")));
        }
        var tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
        var restartedStore=new ProjectionStore(jdbc,tx,codec,"ranking","test-v1",Clock.fixed(firstTime.plusSeconds(2),ZoneOffset.UTC));
        try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,group,"none"),topic,restartedStore,recovery)) {
            consumer.start(); awaitCommitted(scenario.equals("age")?1:2);
        }
        long newFailures=scenario.equals("age")?0:1;
        assertStored(1,newFailures); assertEquals(newFailures,dlqCount());
        assertEquals(0L,jdbc.queryForObject("SELECT count(*) FROM projection_failure WHERE source_offset=0",Long.class));
        if(scenario.equals("conflict")) assertEquals(1L,jdbc.queryForObject("SELECT source_offset FROM projection_failure",Long.class));
    }
}
