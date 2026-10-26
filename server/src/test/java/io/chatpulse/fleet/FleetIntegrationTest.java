package io.chatpulse.fleet;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.ReplaySource;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.Admin;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration") @Timeout(120)
class FleetIntegrationTest {
    private static final String CHANNEL="a".repeat(32),SECOND="b".repeat(32);private static final JsonMapper JSON=JsonMapper.builder().build();
    private Long stalePostResumeAccepted;
    private String schema,topic,group,scenario;private com.zaxxer.hikari.HikariDataSource root,ds;private JdbcTemplate jdbc;private CollectorLeases leases;
    private final List<Process> children=new ArrayList<>();private final List<Path> reports=new ArrayList<>();private SyntheticServer source;
    @BeforeEach void setup(TestInfo info) throws Exception {
        scenario=info.getTestMethod().orElseThrow().getName();
        String id=UUID.randomUUID().toString().replace("-","");schema="fleet_"+id;topic="fleet.chat."+id;group="fleet-group-"+id;
        root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false);new JdbcTemplate(root).execute("CREATE SCHEMA "+schema);
        ds=PostgresSupport.openMigrated(url(15432),"chatpulse","local-chatpulse");jdbc=new JdbcTemplate(ds);leases=new CollectorLeases(ds);
        KafkaSupport.ensureTopic("localhost:19092",topic);source=new SyntheticServer();
    }
    @AfterEach void cleanup() throws Exception {
        for(Process child:children)if(child.isAlive()){signal(child,"CONT");child.destroy();if(!child.waitFor(35,TimeUnit.SECONDS))child.destroyForcibly().waitFor();}
        for(Path path:reports)if(Files.exists(path))try {
            var report=JSON.readTree(Files.readString(path));var delivery=report.path("delivery");
            System.out.println(JSON.writeValueAsString(Map.of("scenario",scenario,"worker",report.path("workerId").asText(),"complete",report.path("complete").asBoolean(),
                    "failureCode",report.path("failureCode").asText(),"deliveryCode",delivery.path("failureCode").asText(),"accepted",delivery.path("accepted").asLong(),
                    "acknowledged",delivery.path("acknowledged").asLong(),"rejected",delivery.path("rejected").asLong())));
        } catch(java.io.IOException|RuntimeException invalidReport){System.out.println("FLEET_CHILD_REPORT_INVALID");}
        for(int index=0;index<reports.size();index++){
            Path path=Path.of(reports.get(index)+".diagnostic.json");
            if(Files.exists(path))System.out.println(JSON.writeValueAsString(Map.of("scenario",scenario,"child",index,
                    "reportPresent",Files.exists(reports.get(index)),"exitCode",children.get(index).exitValue(),"startup",JSON.readTree(Files.readString(path)))));
            else System.out.println(JSON.writeValueAsString(Map.of("scenario",scenario,"child",index,"startupDiagnosticPresent",false,"exitCode",children.get(index).exitValue())));
        }
        if(source!=null)source.close();if(ds!=null)ds.close();
        if(root!=null){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");root.close();}
        try(var admin=Admin.create(Map.of("bootstrap.servers","localhost:19092"))){if(admin.listTopics().names().get(10,TimeUnit.SECONDS).contains(topic))admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);try{admin.deleteConsumerGroups(List.of(group)).all().get(10,TimeUnit.SECONDS);}catch(ExecutionException ignored){}}
    }
    private String url(int port){return "jdbc:postgresql://localhost:"+port+"/chatpulse?currentSchema="+schema;}
    private void plan(List<String> channels){leases.apply(new FleetPlan("a".repeat(64),SourceMode.SYNTHETIC,channels.size(),channels.size(),channels,120));}
    private CollectorLeases.Grant tick(UUID owner,int capacity,Map<String,Long> held){return leases.tick(owner,capacity,Duration.ofSeconds(6),SourceMode.SYNTHETIC,held,Set.of(),true);}
    @Test void concurrentClaimsRespectUniqueOwnerCapacityAndOldEpochCannotRenewOrReleaseNewOwner() throws Exception {
        plan(List.of(CHANNEL,SECOND));UUID a=UUID.randomUUID(),b=UUID.randomUUID();
        List<CollectorLeases.Grant> grants;
        try(var workers=Executors.newFixedThreadPool(2)){var first=workers.submit(()->tick(a,1,Map.of()));var second=workers.submit(()->tick(b,1,Map.of()));grants=List.of(first.get(10,TimeUnit.SECONDS),second.get(10,TimeUnit.SECONDS));}
        assertEquals(2,grants.stream().flatMap(g->g.leases().stream()).map(CollectorLeases.Lease::channelId).distinct().count());
        var original=grants.getFirst().leases().getFirst();var held=Map.of(original.channelId(),original.epoch());
        assertThrows(IllegalStateException.class,()->tick(a,2,held));
        jdbc.update("UPDATE collector_lease SET valid_until=clock_timestamp()-interval '1s' WHERE channel_id=?",original.channelId());
        assertThrows(IllegalStateException.class,()->tick(a,1,held));
        UUID next=UUID.randomUUID();var acquired=tick(next,1,Map.of()).leases().getFirst();assertEquals(original.epoch()+1,acquired.epoch());
        leases.release(a,held,false);assertEquals(next,jdbc.queryForObject("SELECT owner FROM collector_lease WHERE channel_id=?",UUID.class,original.channelId()));
        assertEquals(2,((Number)leases.status().get("assigned")).intValue());assertNull(leases.status().get("healthyWindow"));
    }
    @Test void selectionRemovalAndPlanExpiryDoNotRenewAnOldLease() {
        plan(List.of(CHANNEL));UUID owner=UUID.randomUUID();var lease=tick(owner,1,Map.of()).leases().getFirst();
        plan(List.of(SECOND));assertThrows(IllegalStateException.class,()->tick(owner,1,Map.of(CHANNEL,lease.epoch())));
        assertEquals(owner,jdbc.queryForObject("SELECT owner FROM collector_lease WHERE channel_id=?",UUID.class,CHANNEL));
        jdbc.update("UPDATE collector_plan SET valid_until=clock_timestamp()-interval '1s'");assertThrows(IllegalStateException.class,()->tick(UUID.randomUUID(),1,Map.of()));
        assertEquals("PLAN_EXPIRED",leases.status().get("pendingReason"));
    }
    @RepeatedTest(5) void twoProcessesCollectDisjointChannelsAndGracefulHandoffPreservesAcknowledgedDatabaseCounts() throws Exception {
        plan(List.of(CHANNEL,SECOND));var first=start(15432,1);var second=start(15432,1);awaitConnected(2);
        assertEquals(2,jdbc.queryForObject("SELECT count(DISTINCT owner) FROM collector_lease WHERE connected",Integer.class));
        stop(first);stop(second);assertEquals(0,((Number)leases.status().get("assigned")).intValue());
        var third=start(15432,2);awaitConnected(2);stop(third);
        assertEquals(2L,jdbc.queryForObject("SELECT min(epoch) FROM collector_lease WHERE selected",Long.class));
        for(Path report:reports){var value=JSON.readTree(Files.readString(report));assertTrue(value.path("complete").asBoolean());assertTrue(value.path("released").asBoolean());}
        assertReconciled();
    }
    @Test void stoppedProcessExpiresAndCannotPublishItsQueuedSocketInputAfterAnotherProcessTakesOver() throws Exception {
        plan(List.of(CHANNEL));var first=start(15432,1);awaitConnected(1);
        await().atMost(Duration.ofSeconds(10)).until(()->!readBrokerEvents().isEmpty());
        var oldSessions=readBrokerEvents().stream().map(ChatEvent::collectionSessionId).collect(java.util.stream.Collectors.toSet());
        UUID original=jdbc.queryForObject("SELECT owner FROM collector_lease WHERE channel_id=?",UUID.class,CHANNEL);signal(first,"STOP");
        var second=start(15432,1);
        await().atMost(Duration.ofSeconds(25)).untilAsserted(()->{assertNotEquals(original,jdbc.queryForObject("SELECT owner FROM collector_lease WHERE channel_id=?",UUID.class,CHANNEL));assertEquals(2L,jdbc.queryForObject("SELECT epoch FROM collector_lease WHERE channel_id=?",Long.class,CHANNEL));});
        Instant resumedAt=Instant.now();signal(first,"CONT");assertTrue(first.waitFor(35,TimeUnit.SECONDS));awaitConnected(1);stop(second);
        stalePostResumeAccepted=readBrokerEvents().stream().filter(e->oldSessions.contains(e.collectionSessionId())&&!e.receivedAt().isBefore(resumedAt)).count();
        assertEquals(0L,stalePostResumeAccepted);
        JsonNode stale=JSON.readTree(Files.readString(reports.getFirst()));assertFalse(stale.path("complete").asBoolean());assertNotEquals("NONE",stale.path("failureCode").asText());
        assertReconciled();
    }
    @Test void actualDatabaseSocketDisconnectionFencesWorkerAndAllowsExpiryHandoff() throws Exception {
        plan(List.of(CHANNEL));
        try(var proxy=new DatabaseProxy()) {
            var first=start(proxy.port(),1);awaitConnected(1);proxy.cut();assertTrue(first.waitFor(35,TimeUnit.SECONDS));
            JsonNode failed=JSON.readTree(Files.readString(reports.getFirst()));assertFalse(failed.path("complete").asBoolean());assertFalse(failed.path("released").asBoolean());
            var second=start(15432,1);await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertEquals(2L,jdbc.queryForObject("SELECT epoch FROM collector_lease WHERE channel_id=?",Long.class,CHANNEL)));awaitConnected(1);stop(second);assertReconciled();
        }
    }
    @Test void sigtermDuringMainReportWriteWaitsForCompletedDiagnosticFile() throws Exception {
        plan(List.of(CHANNEL));var child=start(15432,1,true);Path report=reports.getFirst();
        await().atMost(Duration.ofSeconds(25)).until(()->Files.exists(Path.of(report+".persisting")));
        assertTrue(child.isAlive());signal(child,"TERM");assertTrue(child.waitFor(20,TimeUnit.SECONDS));
        var diagnostic=JSON.readTree(Files.readString(report));assertTrue(diagnostic.path("complete").asBoolean());
        assertTrue(diagnostic.path("released").asBoolean());assertReconciled();
    }
    @Test void slowProducerStartupCompletesBeforeLeaseAndSourceAdmission() throws Exception {
        plan(List.of(CHANNEL));var child=start(15432,1,"DELAY_PRODUCER");Path report=reports.getFirst();
        await().atMost(Duration.ofSeconds(15)).until(()->Files.exists(Path.of(report+".producer-starting")));
        assertFalse(Files.exists(Path.of(report+".producer-ready")));
        assertEquals(0,((Number)leases.status().get("assigned")).intValue());assertTrue(source.accepted.isEmpty());
        Files.writeString(Path.of(report+".producer-release"),"RELEASE",StandardOpenOption.CREATE_NEW);
        awaitConnected(1);assertTrue(Files.exists(Path.of(report+".producer-ready")));stop(child);
        var diagnostic=JSON.readTree(Files.readString(report));assertTrue(diagnostic.path("complete").asBoolean());
        assertEquals(0,diagnostic.path("delivery").path("rejected").asInt());assertReconciled();
    }
    private Process start(int dbPort,int capacity) throws Exception {return start(dbPort,capacity,false);}
    private Process start(int dbPort,int capacity,boolean delayedReport) throws Exception {return start(dbPort,capacity,delayedReport?"DELAY_REPORT":null);}
    private Process start(int dbPort,int capacity,String fixtureMode) throws Exception {
        Path report=Path.of("build","fleet-"+schema+"-"+reports.size()+".json").toAbsolutePath();reports.add(report);
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx128m","-cp",System.getProperty("integration.classpath"),FleetWorkerProcess.class.getName(),url(dbPort),topic,Integer.toString(source.getPort()),Integer.toString(capacity),report.toString()));
        if(fixtureMode!=null)command.add(fixtureMode);
        var child=new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();children.add(child);return child;
    }
    private void stop(Process process) throws Exception {process.destroy();assertTrue(process.waitFor(35,TimeUnit.SECONDS));}
    private static void signal(Process process,String name) throws Exception {if(process.isAlive())assertEquals(0,new ProcessBuilder("kill","-"+name,Long.toString(process.pid())).start().waitFor());}
    private void awaitConnected(int count){await().atMost(Duration.ofSeconds(25)).untilAsserted(()->assertEquals(count,((Number)leases.status().get("connected")).intValue()));}
    private void assertReconciled() throws Exception {
        long accepted=0,acked=0;
        for(Path path:reports){var d=JSON.readTree(Files.readString(path));var p=d.path("delivery");assertEquals(p.path("accepted").asLong(),p.path("acknowledged").asLong()+p.path("unconfirmed").asLong());accepted+=p.path("accepted").asLong();acked+=p.path("acknowledged").asLong();}
        assertTrue(acked>0);var records=readBrokerEvents();var brokerIds=new HashSet<UUID>();records.forEach(e->brokerIds.add(e.eventId()));long physical=records.size();
        assertTrue(physical>=acked&&physical<=accepted);assertEquals(physical,brokerIds.size());
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);var store=new ProjectionStore(jdbc,tx,new EventCodec(),"fleet-test",schema);
        try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory("localhost:19092",group,"earliest"),topic,store)){consumer.start();await().atMost(Duration.ofSeconds(20)).untilAsserted(()->assertEquals((long)brokerIds.size(),store.totals().get("bucketTotal")));}
        assertEquals(brokerIds,new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection='fleet-test'",(rs,n)->rs.getObject(1,UUID.class))));
        for(Path path:reports)assertFalse(Files.readString(path).contains("PRIVATE_CANARY"));
        var evidence=new LinkedHashMap<String,Object>();evidence.put("scenario",scenario);evidence.put("recordedAt",Instant.now().toString());evidence.put("accepted",accepted);evidence.put("acknowledged",acked);evidence.put("unconfirmed",accepted-acked);evidence.put("brokerPhysical",physical);evidence.put("brokerUnique",brokerIds.size());evidence.put("databaseUnique",brokerIds.size());evidence.put("brokerDatabaseDifference",0);evidence.put("children",reports.stream().map(path->{try{return JSON.readValue(Files.readString(path),Map.class);}catch(Exception failure){throw new RuntimeException(failure);}}).toList());
        if(stalePostResumeAccepted!=null)evidence.put("stalePostResumeAccepted",stalePostResumeAccepted);
        Files.writeString(Path.of("build","fleet-evidence-"+scenario+".json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence)+"\n");
    }
    private List<ChatEvent> readBrokerEvents() throws Exception {
        var events=new ArrayList<ChatEvent>();
        try(var source=new ReplaySource("localhost:19092",topic,UUID.randomUUID())) {
            var manifest=source.capture(null,null);source.assign(manifest);var next=new HashMap<Integer,Long>();manifest.partitions().forEach(p->next.put(p.partition(),p.start()));long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(manifest.partitions().stream().anyMatch(p->next.get(p.partition())<p.end())) {assertTrue(System.nanoTime()<end);for(var r:source.poll()){events.add(new EventCodec().decode(r.value(),r.key()));next.put(r.partition(),r.offset()+1);}}
        }
        return List.copyOf(events);
    }
    private static final class SyntheticServer extends WebSocketServer implements AutoCloseable {
        final CountDownLatch ready=new CountDownLatch(1);final ScheduledExecutorService sender=Executors.newSingleThreadScheduledExecutor();
        final Set<WebSocket> accepted=ConcurrentHashMap.newKeySet();
        SyntheticServer() throws Exception {super(new InetSocketAddress("127.0.0.1",0),1);start();assertTrue(ready.await(5,TimeUnit.SECONDS));sender.scheduleWithFixedDelay(()->{long at=System.currentTimeMillis();for(var c:accepted)if(c.isOpen())c.send("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":"+at+",\"msg\":\"PRIVATE_CANARY\"}]}");},0,100,TimeUnit.MILLISECONDS);}
        public void onStart(){ready.countDown();}public void onOpen(WebSocket c,ClientHandshake h){}public void onError(WebSocket c,Exception e){}public void onClose(WebSocket c,int code,String reason,boolean remote){accepted.remove(c);}
        public void onMessage(WebSocket c,String raw){var node=JSON.readTree(raw);if(node.path("cmd").asInt()==100){c.send("{\"cmd\":10100,\"retCode\":0,\"bdy\":{\"sid\":\"synthetic\"}}");accepted.add(c);}else if(node.path("cmd").asInt()==0)c.send("{\"cmd\":10000}");}
        public void close() throws Exception {sender.shutdownNow();stop(1000);}
    }
    private static final class DatabaseProxy implements AutoCloseable {
        final ServerSocket listener=new ServerSocket(0,20,InetAddress.getLoopbackAddress());final Set<Socket> sockets=ConcurrentHashMap.newKeySet();final ExecutorService workers=Executors.newVirtualThreadPerTaskExecutor();volatile boolean cut;
        DatabaseProxy() throws Exception {workers.submit(()->{while(!cut)try{var client=listener.accept();var target=new Socket("127.0.0.1",15432);sockets.add(client);sockets.add(target);workers.submit(()->copy(client,target));workers.submit(()->copy(target,client));}catch(Exception failure){if(!cut)throw new RuntimeException(failure);}});}
        int port(){return listener.getLocalPort();}
        void copy(Socket from,Socket to){try{from.getInputStream().transferTo(to.getOutputStream());}catch(Exception ignored){}finally{try{from.close();to.close();}catch(Exception ignored){}}}
        void cut() throws Exception {cut=true;listener.close();for(var socket:sockets)socket.close();}
        public void close() throws Exception {cut();workers.shutdownNow();workers.awaitTermination(5,TimeUnit.SECONDS);}
    }
}
