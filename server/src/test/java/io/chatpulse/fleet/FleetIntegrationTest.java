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
    private boolean recoverFuture;
    private final Map<String,Object> boundaryEvidence=new LinkedHashMap<>();
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
        for(int index=0;index<reports.size();index++)try {
            Path path=Path.of(reports.get(index)+".diagnostic.json");
            if(Files.exists(path))System.out.println(JSON.writeValueAsString(Map.of("scenario",scenario,"child",index,
                    "reportPresent",Files.exists(reports.get(index)),"exitCode",children.get(index).exitValue(),"startup",JSON.readTree(Files.readString(path)))));
            else System.out.println(JSON.writeValueAsString(Map.of("scenario",scenario,"child",index,"startupDiagnosticPresent",false,"exitCode",children.get(index).exitValue())));
        }catch(java.io.IOException|RuntimeException invalidDiagnostic){System.out.println("FLEET_CHILD_STARTUP_DIAGNOSTIC_INVALID");}
        try {if(!boundaryEvidence.isEmpty())Files.writeString(Path.of("build","fleet-observed-"+scenario+".json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(boundaryEvidence)+"\n");}
        catch(java.io.IOException|RuntimeException invalidEvidence){System.out.println("FLEET_BOUNDARY_DIAGNOSTIC_INVALID");}
        if(source!=null)source.close();if(ds!=null)ds.close();
        if(root!=null){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");root.close();}
        try(var admin=Admin.create(Map.of("bootstrap.servers","localhost:19092"))){var owned=admin.listTopics().names().get(10,TimeUnit.SECONDS).stream().filter(t->t.equals(topic)||t.equals(topic+".dlq")).toList();if(!owned.isEmpty())admin.deleteTopics(owned).all().get(10,TimeUnit.SECONDS);try{admin.deleteConsumerGroups(List.of(group)).all().get(10,TimeUnit.SECONDS);}catch(ExecutionException ignored){}}
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
    @Test void oppositeWallClocksAndRuntimeJumpsKeepDatabaseLeaseEpochWhileReceiptErrorsRemainVisible() throws Exception {
        source.automatic=false;recoverFuture=true;plan(List.of(CHANNEL,SECOND));
        var a=start(15432,source.getPort(),1,"CLOCK",300);var ownerA=ready(0);
        var b=start(15432,source.getPort(),1,"CLOCK",-300);var ownerB=ready(1);awaitConnected(2);
        var before=leaseViews();var first=source.marker(false);awaitMarker(first,2);
        assertReceiptOffsets(first,Map.of(ownerA,300L,ownerB,-300L));
        // 실제 현재 source timestamp는 -5분 receipt에서 거부된다. 임대 시험용 과거 marker와 구별한다.
        var current=source.marker(true);awaitMarker(current,1);
        Path rejected=Path.of(reports.get(1)+".invalid-observed.json");
        await().atMost(Duration.ofSeconds(5)).until(()->Files.exists(rejected)&&JSON.readTree(Files.readString(rejected)).path("invalidFrames").asLong()==1);
        changeClock(0,1,-300);changeClock(1,1,300);var postClockBaseline=leaseViews();
        for(int i=0;i<before.size();i++){assertEquals(before.get(i).get("owner"),postClockBaseline.get(i).get("owner"));assertEquals(before.get(i).get("epoch"),postClockBaseline.get(i).get("epoch"));}
        await().atMost(Duration.ofSeconds(8)).untilAsserted(()->assertRenewedWithoutHandoff(postClockBaseline,leaseViews()));
        var afterJump=source.marker(false);awaitMarker(afterJump,2);
        assertReceiptOffsets(afterJump,Map.of(ownerA,-300L,ownerB,300L));
        var after=leaseViews();stop(a);stop(b);
        var invalid=report(1).path("leases").get(0).path("stats").path("skipped").path("INVALID_TIME").asLong();
        assertTrue(invalid>=1);assertEquals(1,eventsAt(current).size());
        boundaryEvidence.put("initialLeases",before);boundaryEvidence.put("postClockBaseline",postClockBaseline);boundaryEvidence.put("renewedLeases",after);
        boundaryEvidence.put("clockOffsetsSeconds",List.of(300,-300));boundaryEvidence.put("clockJumpsSeconds",List.of(-600,600));
        boundaryEvidence.put("currentTimestampInvalidTime",invalid);boundaryEvidence.put("markerEventCounts",List.of(2,1,2));
        assertReconciled();
    }
    @Test void databaseBlackholeExpiresLocalPermitWhileSourcePathLivesAndSecondProcessTakesOver() throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));
        try(var database=new ControlledTcpProxy(15432);var websocket=new ControlledTcpProxy(source.getPort())) {
            var a=start(database.port(),websocket.port(),1,"PLAN_LOCK_GATE",0);var original=ready(0);awaitConnected(1);
            var before=source.marker(false);awaitMarker(before,1);var oldSession=eventsAt(before).getFirst().collectionSessionId();
            var b=start(15432,source.getPort(),1,"NORMAL",0);var standby=ready(1);
            await().atMost(Duration.ofSeconds(5)).until(()->jdbc.queryForObject("SELECT count(*) FROM collector_worker WHERE worker_id=?",Integer.class,standby)==1);
            var lease=leaseViews().getFirst();long began=System.nanoTime(),wsBytes=websocket.forwardedBytes();
            var backendSamples=new ArrayList<Object>();boundaryEvidence.put("backendSamples",backendSamples);backendSamples.add(backendStates());
            Path gate=reports.getFirst();Files.writeString(Path.of(gate+".plan-lock-arm"),"ARM",StandardOpenOption.CREATE_NEW);
            try {
                await().atMost(Duration.ofSeconds(5)).until(()->Files.exists(Path.of(gate+".plan-locked")));
                int lockedPid=Integer.parseInt(Files.readString(Path.of(gate+".plan-locked")));
                boundaryEvidence.put("planLockBackendPid",lockedPid);backendSamples.add(backendStates());
                database.mode(ControlledTcpProxy.Mode.BLACKHOLE);
                // 잠금 응답은 수신했지만 다음 SQL을 보내지 않는다. 실제 DB는 idle in transaction 상태다.
                var during=source.marker(false);awaitMarker(during,1);assertTrue(websocket.forwardedBytes()>wsBytes);
                await().atMost(Duration.ofSeconds(10)).untilAsserted(()->{if(backendSamples.size()<100)backendSamples.add(backendStates());assertEquals(2L,epoch());});
                double takeoverSeconds=(System.nanoTime()-began)/1e9;assertTrue(a.isAlive());
                assertEquals(0,jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE pid=?",Integer.class,lockedPid));
                awaitConnected(1);var after=source.marker(false);awaitMarker(after,1);
                assertTrue(eventsAt(after).stream().noneMatch(e->e.collectionSessionId().equals(oldSession)));
                assertOldOwnerCannotChangeCurrent(original,1);
                boundaryEvidence.put("beforeBlackhole",lease);boundaryEvidence.put("takeoverSeconds",takeoverSeconds);
                boundaryEvidence.put("previousProcessAliveAtTakeover",true);boundaryEvidence.put("planLockBackendEndedBeforeTakeover",true);
                boundaryEvidence.put("sourceForwardedBytesDuringBlackhole",websocket.forwardedBytes()-wsBytes);
                boundaryEvidence.put("oldSessionPostTakeoverMarker",0);
            } finally {Files.writeString(Path.of(gate+".plan-lock-release"),"RELEASE");}
            database.mode(ControlledTcpProxy.Mode.CUT);database.mode(ControlledTcpProxy.Mode.FORWARD);
            assertTrue(a.waitFor(30,TimeUnit.SECONDS));assertFalse(report(0).path("complete").asBoolean());
            assertEquals(2L,epoch());stop(b);boundaryEvidence.put("databaseDiscardedBytes",database.discardedBytes());assertReconciled();
        }
    }
    @Test void unassignedWorkerRetriesRolledBackPlanLockAndCollectsAfterUnlock() throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));Process child;
        try(var blocker=holdPlanLock()) {
            child=start(15432,1);ready(0);awaitWorkerLockWait();
            long began=System.nanoTime();Thread.sleep(2300);assertTrue(child.isAlive());
            assertTrue(source.accepted.isEmpty());assertEquals(0,((Number)leases.status().get("assigned")).intValue());
            blocker.rollback();boundaryEvidence.put("observedLockHoldSeconds",(System.nanoTime()-began)/1e9);
        }
        awaitConnected(1);var marker=source.marker(false);awaitMarker(marker,1);stop(child);
        var result=report(0);assertTrue(result.path("complete").asBoolean());assertTrue(result.path("leaseContentionRetries").asInt()>0);
        assertEquals(1L,epoch());assertReconciled();
    }
    @Test void persistentPlanLockExhaustsUnassignedRetriesWithoutSourceAdmission() throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));
        try(var blocker=holdPlanLock()) {
            var child=start(15432,1);ready(0);awaitWorkerLockWait();assertTrue(child.waitFor(18,TimeUnit.SECONDS));
            var result=report(0);assertFalse(result.path("complete").asBoolean());assertEquals(5,result.path("leaseContentionRetries").asInt());
            assertEquals("COLLECTION_LEASE_LOST",result.path("failureCode").asText());
            assertTrue(result.path("leases").isEmpty());assertEquals(0,result.path("delivery").path("accepted").asInt());
            assertTrue(source.accepted.isEmpty());assertTrue(readBrokerEvents().isEmpty());
            assertEquals(0,((Number)leases.status().get("assigned")).intValue());boundaryEvidence.put("workerResult",JSON.readValue(Files.readString(reports.getFirst()),Map.class));
            blocker.rollback();
        }
    }
    @Test void ownedLeasePlanLockFailureDoesNotRetryOrReacquireInSameProcess() throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));var child=start(15432,1);var owner=ready(0);awaitConnected(1);
        var marker=source.marker(false);awaitMarker(marker,1);
        try(var blocker=holdPlanLock()) {
            awaitWorkerLockWait();assertTrue(child.waitFor(15,TimeUnit.SECONDS));
            assertFalse(report(0).path("complete").asBoolean());assertEquals(0,report(0).path("leaseContentionRetries").asInt());
            assertEquals("COLLECTION_LEASE_LOST",report(0).path("failureCode").asText());blocker.rollback();
        }
        var replacement=UUID.randomUUID();await().atMost(Duration.ofSeconds(10)).until(()->!tick(replacement,1,Map.of()).leases().isEmpty());
        assertEquals(2L,epoch());assertOldOwnerCannotChangeCurrent(owner,1);assertReconciled();
    }
    private java.sql.Connection holdPlanLock() throws Exception {
        var connection=ds.getConnection();
        try {
            connection.setAutoCommit(false);
            try(var statement=connection.createStatement();var rows=statement.executeQuery("SELECT singleton FROM collector_plan WHERE singleton=1 FOR UPDATE")){assertTrue(rows.next());}
            return connection;
        }catch(Exception|Error failure){connection.close();throw failure;}
    }
    private void awaitWorkerLockWait() {
        await().atMost(Duration.ofSeconds(5)).until(()->jdbc.queryForObject(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE ? AND wait_event_type='Lock'",Integer.class,"fleet_"+schema+"_%")>0);
    }
    @Test void websocketOnlyDisconnectionKeepsLeaseAndReconnectsWithoutRecoveringSourceGap() throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));
        try(var websocket=new ControlledTcpProxy(source.getPort())) {
            var a=start(15432,websocket.port(),1,"NORMAL",0);ready(0);awaitConnected(1);
            var before=source.marker(false);awaitMarker(before,1);var oldSession=eventsAt(before).getFirst().collectionSessionId();
            var b=start(15432,source.getPort(),1,"NORMAL",0);ready(1);var leasesBefore=leaseViews();
            websocket.mode(ControlledTcpProxy.Mode.CUT);
            await().atMost(Duration.ofSeconds(5)).until(()->source.accepted.isEmpty());
            var missing=source.marker(false);assertEquals(0,missing.sockets());
            await().atMost(Duration.ofSeconds(5)).untilAsserted(()->assertRenewedWithoutHandoff(leasesBefore,leaseViews()));
            var leasesDuring=leaseViews();websocket.mode(ControlledTcpProxy.Mode.FORWARD);
            awaitConnected(1);await().atMost(Duration.ofSeconds(10)).until(()->source.accepted.size()==1);
            var after=source.marker(false);awaitMarker(after,1);assertNotEquals(oldSession,eventsAt(after).getFirst().collectionSessionId());
            assertEquals(1L,epoch());stop(a);stop(b);
            assertTrue(eventsAt(missing).isEmpty());assertTrue(report(0).path("leases").get(0).path("stats").path("connections").asLong()>=2);
            boundaryEvidence.put("leasesBefore",leasesBefore);boundaryEvidence.put("leasesDuringSourceCut",leasesDuring);
            boundaryEvidence.put("syntheticSourceGap",1);boundaryEvidence.put("replayedGap",0);assertReconciled();
        }
    }
    @Test void stoppedSkewedOwnerCannotAdmitPostTakeoverSocketMarkerWhenResumed() throws Exception {
        source.automatic=false;recoverFuture=true;plan(List.of(CHANNEL));
        var a=start(15432,source.getPort(),1,"CLOCK",300);var oldOwner=ready(0);awaitConnected(1);
        var before=source.marker(false);awaitMarker(before,1);var oldSession=eventsAt(before).getFirst().collectionSessionId();
        var b=start(15432,source.getPort(),1,"CLOCK",-300);ready(1);
        signal(a,"STOP");await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertEquals(2L,epoch()));
        awaitConnected(1);await().atMost(Duration.ofSeconds(10)).until(()->source.accepted.size()==2);
        var takeover=leaseViews().getFirst();var after=source.marker(false);assertEquals(2,after.sockets());awaitMarker(after,1);
        signal(a,"CONT");assertTrue(a.waitFor(30,TimeUnit.SECONDS));assertOldOwnerCannotChangeCurrent(oldOwner,1);stop(b);
        assertEquals(1,eventsAt(after).size());assertTrue(eventsAt(after).stream().noneMatch(e->e.collectionSessionId().equals(oldSession)));
        boundaryEvidence.put("takeoverLease",takeover);boundaryEvidence.put("physicalSocketsAtMarker",after.sockets());
        boundaryEvidence.put("markerBrokerCount",1);boundaryEvidence.put("oldSessionPostTakeoverMarker",0);assertReconciled();
    }
    private UUID ready(int index) throws Exception {
        Path path=Path.of(reports.get(index)+".ready.json");
        await().atMost(Duration.ofSeconds(25)).until(()->{if(!children.get(index).isAlive())throw new IllegalStateException("FLEET_CHILD_START_FAILED");return Files.exists(path);});
        return UUID.fromString(JSON.readTree(Files.readString(path)).path("workerId").asText());
    }
    enum PublishBoundary { QUEUED_BEFORE_FINAL_PERMIT, AFTER_PERMIT_BEFORE_SEND, BROKER_ACK_BEFORE_CALLBACK }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.EnumSource(PublishBoundary.class)
    void leaseTakeoverDistinguishesQueuedAdmissionSendRaceAndAlreadyAcknowledgedRecord(PublishBoundary boundary) throws Exception {
        source.automatic=false;plan(List.of(CHANNEL));
        var owner=UUID.randomUUID();var nextOwner=UUID.randomUUID();var authority=new LeaseAuthority();
        var probe=new FleetProducerProbe(switch(boundary) {
            case QUEUED_BEFORE_FINAL_PERMIT -> FleetProducerProbe.Boundary.NONE;
            case AFTER_PERMIT_BEFORE_SEND -> FleetProducerProbe.Boundary.BEFORE_SEND;
            case BROKER_ACK_BEFORE_CALLBACK -> FleetProducerProbe.Boundary.AFTER_BROKER_ACK;
        });
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        PublishingAuthority guarded=channel->{var permit=authority.acquire(channel);return ()->{
            if(boundary==PublishBoundary.QUEUED_BEFORE_FINAL_PERMIT) {
                entered.countDown();try {if(!release.await(15,TimeUnit.SECONDS))throw new IllegalStateException("PERMIT_BARRIER_TIMEOUT");}
                catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IllegalStateException("PERMIT_BARRIER_INTERRUPTED");}
            }
            permit.check();
        };};
        var factory=KafkaSupport.producerFactory("localhost:19092");factory.addPostProcessor(probe::wrap);
        var kafka=new org.springframework.kafka.core.KafkaTemplate<>(factory);kafka.setProducerListener(null);
        try {
            assertEquals(3,kafka.partitionsFor(topic).size());long requested=System.nanoTime();var grant=tick(owner,1,Map.of());authority.update(grant,requested);
            assertTrue(tick(nextOwner,1,Map.of()).leases().isEmpty());
            var now=Instant.now();var event=new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic",UUID.randomUUID(),null,"CHAT",now,now,false,"test");
            try(var publisher=new BufferedKafkaPublisher(kafka,topic,new EventCodec(),new BufferedKafkaPublisher.Limits(4,4,1),guarded)) {
                publisher.accept(List.of(event));
                if(boundary==PublishBoundary.QUEUED_BEFORE_FINAL_PERMIT)assertTrue(entered.await(5,TimeUnit.SECONDS));
                else assertTrue(probe.awaitBoundary(Duration.ofSeconds(5)));
                int brokerBefore=readBrokerEvents().size();
                assertEquals(boundary==PublishBoundary.BROKER_ACK_BEFORE_CALLBACK?1:0,brokerBefore);
                assertEquals(0,publisher.channelSnapshot().get(CHANNEL).acknowledged());
                var acquired=new java.util.concurrent.atomic.AtomicReference<CollectorLeases.Grant>();
                await().atMost(Duration.ofSeconds(10)).until(()->{var candidate=tick(nextOwner,1,Map.of());if(candidate.leases().isEmpty())return false;acquired.set(candidate);return true;});
                var takeover=acquired.get();assertEquals(2,takeover.leases().getFirst().epoch());
                assertFalse(takeover.databaseNow().isBefore(grant.leases().getFirst().expiresAt()));
                assertThrows(io.chatpulse.poc.SourceFailure.class,authority::checkAll);
                release.countDown();probe.release();
                var delivery=publisher.finish();long expected=boundary==PublishBoundary.QUEUED_BEFORE_FINAL_PERMIT?0:1;
                assertEquals(1L,delivery.get("accepted"));assertEquals(expected,delivery.get("acknowledged"));assertEquals(1-expected,delivery.get("unconfirmed"));
                if(expected==0)assertEquals("PUBLISH_AUTHORITY_LOST",delivery.get("failureCode"));
                var records=readBrokerEvents();assertEquals(expected,records.size());
                var ids=records.stream().map(ChatEvent::eventId).collect(java.util.stream.Collectors.toSet());
                assertEquals(probe.sent(),ids);assertEquals(probe.acknowledged(),ids);
                var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
                var store=new ProjectionStore(jdbc,tx,new EventCodec(),"fleet-boundary",schema);
                try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory("localhost:19092",group,"earliest"),topic,store)) {
                    consumer.start();await().atMost(Duration.ofSeconds(15)).untilAsserted(()->{
                        assertEquals(3,consumer.status().get("assignedPartitions"));assertEquals(expected,store.totals().get("bucketTotal"));
                    });
                }
                assertEquals(ids,new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection='fleet-boundary'",(rs,n)->rs.getObject(1,UUID.class))));
                var evidence=new LinkedHashMap<String,Object>();evidence.put("boundary",boundary.name());evidence.put("recordedAt",Instant.now().toString());
                evidence.put("oldLeaseExpiresAt",grant.leases().getFirst().expiresAt());evidence.put("takeoverDatabaseNow",takeover.databaseNow());evidence.put("takeoverEpoch",2);
                evidence.put("brokerBeforeTakeover",brokerBefore);evidence.put("delivery",delivery);evidence.put("producerProbe",probe.snapshot());
                evidence.put("brokerUnique",ids.size());evidence.put("databaseUnique",ids.size());evidence.put("setDifference",0);
                Files.writeString(Path.of("build","fleet-boundary-"+boundary.name()+".json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence)+"\n");
            } finally {release.countDown();probe.release();}
        } finally {factory.destroy();}
    }
    private JsonNode report(int index) throws Exception{return JSON.readTree(Files.readString(reports.get(index)));}
    private List<Map<String,Object>> backendStates(){return jdbc.query("SELECT application_name,pid,state,xact_start IS NOT NULL AS transaction_open,wait_event_type,wait_event,pg_blocking_pids(pid)::text AS blocking_pids,clock_timestamp() AS observed FROM pg_stat_activity WHERE application_name LIKE ? ORDER BY pid",(rs,n)->{
        var row=new LinkedHashMap<String,Object>();row.put("worker",rs.getString(1));row.put("pid",rs.getInt(2));row.put("state",rs.getString(3));row.put("transactionOpen",rs.getBoolean(4));row.put("waitType",rs.getString(5));row.put("waitEvent",rs.getString(6));row.put("blockingPids",rs.getString(7));row.put("databaseNow",rs.getTimestamp(8).toInstant().toString());return row;
    },"fleet_"+schema+"_%");}
    private long epoch(){return jdbc.queryForObject("SELECT epoch FROM collector_lease WHERE channel_id=?",Long.class,CHANNEL);}
    private List<Map<String,Object>> leaseViews(){return jdbc.query("SELECT channel_id,owner,epoch,valid_until,clock_timestamp() AS observed FROM collector_lease WHERE selected ORDER BY channel_id",(rs,n)->Map.of("channel",rs.getString(1),"owner",rs.getObject(2,UUID.class).toString(),"epoch",rs.getLong(3),"expiresAt",rs.getTimestamp(4).toInstant().toString(),"databaseNow",rs.getTimestamp(5).toInstant().toString()));}
    private void assertRenewedWithoutHandoff(List<Map<String,Object>> before,List<Map<String,Object>> after) {
        assertEquals(before.size(),after.size());for(int i=0;i<before.size();i++) {
            assertEquals(before.get(i).get("owner"),after.get(i).get("owner"));assertEquals(before.get(i).get("epoch"),after.get(i).get("epoch"));
            assertTrue(Instant.parse((String)after.get(i).get("expiresAt")).isAfter(Instant.parse((String)before.get(i).get("expiresAt")).plusSeconds(2)));
        }
    }
    private void changeClock(int index,int sequence,long seconds) throws Exception {
        Path command=Path.of(reports.get(index)+".clock-command.json"),temporary=Path.of(command+".tmp");
        Files.writeString(temporary,JSON.writeValueAsString(Map.of("sequence",sequence,"offsetSeconds",seconds)));Files.move(temporary,command,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        Path applied=Path.of(reports.get(index)+".clock-applied.json");await().atMost(Duration.ofSeconds(5)).until(()->Files.exists(applied)&&JSON.readTree(Files.readString(applied)).path("sequence").asInt()==sequence);
        var response=JSON.readTree(Files.readString(applied));assertEquals(seconds,response.path("offsetSeconds").asLong());
        long actual=Duration.between(Instant.parse(response.path("appliedAt").asText()),Instant.parse(response.path("workerInstant").asText())).toSeconds();assertEquals(seconds,actual);
        boundaryEvidence.put("worker"+index+"ClockApplied",JSON.readValue(Files.readString(applied),Map.class));
    }
    private void awaitMarker(Marker marker,int count){await().atMost(Duration.ofSeconds(10)).untilAsserted(()->assertEquals(count,eventsAt(marker).size()));}
    private List<ChatEvent> eventsAt(Marker marker) throws Exception{return readBrokerEvents().stream().filter(e->e.eventTime().equals(marker.eventTime())).toList();}
    private void assertReceiptOffsets(Marker marker,Map<UUID,Long> offsets) throws Exception {
        for(var event:eventsAt(marker)) {
            var owner=jdbc.queryForObject("SELECT owner FROM collector_lease WHERE channel_id=?",UUID.class,event.channelId());
            long difference=Duration.between(marker.sentAt(),event.receivedAt()).toMillis();assertTrue(Math.abs(difference-offsets.get(owner)*1000)<3000);
            boundaryEvidence.put(event.channelId()+"ReceiptOffsetMillis"+marker.eventTime().toEpochMilli(),difference);
        }
    }
    private void assertOldOwnerCannotChangeCurrent(UUID owner,long epoch) {
        var before=leaseViews().getFirst();assertThrows(IllegalStateException.class,()->tick(owner,1,Map.of(CHANNEL,epoch)));
        leases.release(owner,Map.of(CHANNEL,epoch),false);var after=leaseViews().getFirst();
        assertEquals(before.get("owner"),after.get("owner"));assertEquals(before.get("epoch"),after.get("epoch"));
        assertFalse(Instant.parse((String)after.get("expiresAt")).isBefore(Instant.parse((String)before.get("expiresAt"))));
    }
    @Test void initialKafkaMetadataTimeoutCanRecoverBeforeLeaseAndSourceAdmission() throws Exception {
        plan(List.of(CHANNEL));
        try(var bootstrap=new ControlledTcpProxy(19092)) {
            bootstrap.mode(ControlledTcpProxy.Mode.BLACKHOLE);
            var child=start(15432,1,"NORMAL","localhost:"+bootstrap.port());
            Path diagnostic=Path.of(reports.getFirst()+".diagnostic.json");
            await().atMost(Duration.ofSeconds(15)).until(()->Files.exists(diagnostic)&&JSON.readTree(Files.readString(diagnostic)).path("phases").valueStream().anyMatch(p->p.path("phase").asText().equals("METADATA_TIMEOUT")));
            assertTrue(bootstrap.discardedBytes()>0);assertEquals(0,((Number)leases.status().get("assigned")).intValue());assertTrue(source.accepted.isEmpty());
            bootstrap.mode(ControlledTcpProxy.Mode.CUT);bootstrap.mode(ControlledTcpProxy.Mode.FORWARD);
            awaitConnected(1);stop(child);
            assertTrue(JSON.readTree(Files.readString(reports.getFirst())).path("complete").asBoolean());assertReconciled();
        }
    }
    @Test void unavailableKafkaStartupExitsAfterBoundedMetadataAttemptsWithoutClaimingLease() throws Exception {
        plan(List.of(CHANNEL));
        try(var bootstrap=new ControlledTcpProxy(19092)) {
            bootstrap.mode(ControlledTcpProxy.Mode.BLACKHOLE);
            var child=start(15432,1,"NORMAL","localhost:"+bootstrap.port());
            assertTrue(child.waitFor(20,TimeUnit.SECONDS));assertNotEquals(0,child.exitValue());
            Path path=reports.getFirst();assertFalse(Files.exists(path));
            var diagnostic=JSON.readTree(Files.readString(Path.of(path+".diagnostic.json")));
            assertEquals(5,diagnostic.path("phases").valueStream().filter(p->p.path("phase").asText().equals("METADATA_TIMEOUT")).count());
            assertEquals("FLEET_KAFKA_NOT_READY",diagnostic.path("codes").get(0).asText());
            assertTrue(bootstrap.discardedBytes()>0);assertEquals(0,((Number)leases.status().get("assigned")).intValue());assertTrue(source.accepted.isEmpty());
        }
    }
    private Process start(int dbPort,int capacity) throws Exception {return start(dbPort,capacity,false);}
    private Process start(int dbPort,int capacity,boolean delayedReport) throws Exception {return start(dbPort,capacity,delayedReport?"DELAY_REPORT":null);}
    private Process start(int dbPort,int capacity,String fixtureMode) throws Exception {return start(dbPort,source.getPort(),capacity,fixtureMode,0,null);}
    private Process start(int dbPort,int capacity,String fixtureMode,String bootstrap) throws Exception {return start(dbPort,source.getPort(),capacity,fixtureMode,0,bootstrap);}
    private Process start(int dbPort,int sourcePort,int capacity,String fixtureMode,long clockOffset) throws Exception {return start(dbPort,sourcePort,capacity,fixtureMode,clockOffset,null);}
    private Process start(int dbPort,int sourcePort,int capacity,String fixtureMode,long clockOffset,String bootstrap) throws Exception {
        Path report=Path.of("build","fleet-"+schema+"-"+reports.size()+".json").toAbsolutePath();
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx128m","-cp",System.getProperty("integration.classpath"),FleetWorkerProcess.class.getName(),url(dbPort)+"&ApplicationName=fleet_"+schema+"_"+reports.size(),topic,Integer.toString(sourcePort),Integer.toString(capacity),report.toString()));
        // mode/clock/bootstrap의 위치를 모든 시작 경로에서 동일하게 유지한다.
        command.add(fixtureMode==null?"NORMAL":fixtureMode);command.add(Long.toString(clockOffset));
        if(bootstrap!=null)command.add(bootstrap);
        var child=new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        children.add(child);reports.add(report);return child;
    }
    private void stop(Process process) throws Exception {process.destroy();assertTrue(process.waitFor(35,TimeUnit.SECONDS));}
    private static void signal(Process process,String name) throws Exception {if(process.isAlive())assertEquals(0,new ProcessBuilder("kill","-"+name,Long.toString(process.pid())).start().waitFor());}
    private void awaitConnected(int count){await().atMost(Duration.ofSeconds(25)).untilAsserted(()->{
        for(int i=0;i<children.size();i++)if(!children.get(i).isAlive()&&!Files.exists(reports.get(i)))throw new IllegalStateException("FLEET_CHILD_EXIT_WITHOUT_REPORT_"+children.get(i).exitValue());
        assertEquals(count,((Number)leases.status().get("connected")).intValue());
    });}
    private void assertReconciled() throws Exception {
        long accepted=0,acked=0;
        for(Path path:reports){var d=JSON.readTree(Files.readString(path));var p=d.path("delivery");assertEquals(p.path("accepted").asLong(),p.path("acknowledged").asLong()+p.path("unconfirmed").asLong());accepted+=p.path("accepted").asLong();acked+=p.path("acknowledged").asLong();}
        assertTrue(acked>0);var records=readBrokerEvents();var brokerIds=new HashSet<UUID>();records.forEach(e->brokerIds.add(e.eventId()));long physical=records.size();
        assertTrue(physical>=acked&&physical<=accepted);assertEquals(physical,brokerIds.size());
        var valid=new HashSet<UUID>();var expectedFailures=new HashSet<String>();var codec=new EventCodec();
        for(var record:readBrokerRecords()) {
            var event=codec.decode(record.value(),record.key());
            if(recoverFuture&&event.receivedAt().isAfter(Instant.now().plusSeconds(5)))expectedFailures.add(record.partition()+":"+record.offset());
            else valid.add(event.eventId());
        }
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);var store=new ProjectionStore(jdbc,tx,codec,"fleet-test",schema);
        try(var recovery=recoverFuture?new DeadLetterRecovery("localhost:19092",topic,jdbc,"fleet-test",schema):null;
            var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory("localhost:19092",group,"earliest"),topic,store,recovery)) {
            consumer.start();await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{
                assertEquals((long)valid.size(),store.totals().get("bucketTotal"));
                assertEquals(expectedFailures.size(),jdbc.queryForObject("SELECT count(*) FROM projection_failure WHERE projection='fleet-test'",Integer.class));
            });
        }
        assertEquals(valid,new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection='fleet-test'",(rs,n)->rs.getObject(1,UUID.class))));
        var actualFailures=new HashSet<>(jdbc.query("SELECT source_partition,source_offset,error_code FROM projection_failure WHERE projection='fleet-test'",(rs,n)->{
            assertEquals("OUTSIDE_LIVE_RETENTION",rs.getString(3));return rs.getInt(1)+":"+rs.getLong(2);
        }));assertEquals(expectedFailures,actualFailures);assertEquals(brokerIds.size(),valid.size()+actualFailures.size());
        var sentIds=new HashSet<UUID>();var ackIds=new HashSet<UUID>();
        for(Path path:reports) {
            var probe=JSON.readTree(Files.readString(path)).path("producerProbe");
            for(var id:probe.path("sentEventIds"))sentIds.add(UUID.fromString(id.asText()));
            for(var id:probe.path("brokerAcknowledgedEventIds"))ackIds.add(UUID.fromString(id.asText()));
        }
        assertTrue(brokerIds.containsAll(ackIds));assertTrue(sentIds.containsAll(brokerIds));
        for(Path path:reports)assertFalse(Files.readString(path).contains("PRIVATE_CANARY"));
        var evidence=new LinkedHashMap<String,Object>();evidence.put("scenario",scenario);evidence.put("recordedAt",Instant.now().toString());evidence.put("accepted",accepted);evidence.put("acknowledged",acked);evidence.put("unconfirmed",accepted-acked);evidence.put("brokerPhysical",physical);evidence.put("brokerUnique",brokerIds.size());evidence.put("databaseUnique",valid.size());evidence.put("quarantinedSourceRecords",actualFailures.size());evidence.put("brokerDatabaseResolvedDifference",0);evidence.put("producerSentUnique",sentIds.size());evidence.put("producerAcknowledgedUnique",ackIds.size());evidence.put("acknowledgedMissingFromBroker",0);evidence.put("boundaries",boundaryEvidence);evidence.put("children",reports.stream().map(path->{try{return JSON.readValue(Files.readString(path),Map.class);}catch(Exception failure){throw new RuntimeException(failure);}}).toList());
        if(stalePostResumeAccepted!=null)evidence.put("stalePostResumeAccepted",stalePostResumeAccepted);
        Files.writeString(Path.of("build","fleet-evidence-"+scenario+".json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence)+"\n");
    }
    private List<ChatEvent> readBrokerEvents() throws Exception {return readBrokerRecords().stream().map(r->new EventCodec().decode(r.value(),r.key())).toList();}
    private List<org.apache.kafka.clients.consumer.ConsumerRecord<String,byte[]>> readBrokerRecords() throws Exception {
        var records=new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String,byte[]>>();
        try(var source=new ReplaySource("localhost:19092",topic,UUID.randomUUID())) {
            var manifest=source.capture(null,null);source.assign(manifest);var next=new HashMap<Integer,Long>();manifest.partitions().forEach(p->next.put(p.partition(),p.start()));long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(manifest.partitions().stream().anyMatch(p->next.get(p.partition())<p.end())) {assertTrue(System.nanoTime()<end);for(var r:source.poll()){records.add(r);next.put(r.partition(),r.offset()+1);}}
        }
        return List.copyOf(records);
    }
    private record Marker(Instant eventTime,Instant sentAt,int sockets) { }
    private static final class SyntheticServer extends WebSocketServer implements AutoCloseable {
        final CountDownLatch ready=new CountDownLatch(1);final ScheduledExecutorService sender=Executors.newSingleThreadScheduledExecutor();
        final Set<WebSocket> accepted=ConcurrentHashMap.newKeySet();
        volatile boolean automatic=true;private final long markerBase=System.currentTimeMillis()-3_600_000;private long markerSequence;
        synchronized Marker marker(boolean current) {Instant at=Instant.ofEpochMilli(current?System.currentTimeMillis():markerBase+(++markerSequence));Instant sent=Instant.now();int sockets=emit(at.toEpochMilli());return new Marker(at,sent,sockets);}
        private int emit(long at){int sent=0;for(var c:accepted)if(c.isOpen()){c.send("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":"+at+",\"msg\":\"PRIVATE_CANARY\"}]}");sent++;}return sent;}
        SyntheticServer() throws Exception {super(new InetSocketAddress("127.0.0.1",0),1);start();assertTrue(ready.await(5,TimeUnit.SECONDS));sender.scheduleWithFixedDelay(()->{if(automatic)emit(System.currentTimeMillis());},0,100,TimeUnit.MILLISECONDS);}
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
