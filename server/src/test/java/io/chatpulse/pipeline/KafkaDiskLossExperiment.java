package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.*;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 소유 replica 디스크 교체와 ACK tail의 마지막 두 copy 손실을 서로 다른 cluster에서 검증한다. */
public final class KafkaDiskLossExperiment {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        if(args.length!=2||!Set.of("recover","last-copy").contains(args[0]))throw new IllegalArgumentException("DISK_ARGUMENTS");
        Path output=Path.of(args[1]).toAbsolutePath();Files.createDirectories(output.getParent());Files.createFile(output);
        var result=new LinkedHashMap<String,Object>();result.put("scenario",args[0]);result.put("startedAt",Instant.now().toString());
        result.put("revision",LocalKafkaCluster.command(List.of("git","rev-parse","HEAD"),Map.of(),Duration.ofSeconds(5)).strip());
        result.put("workingTreeDirty",!LocalKafkaCluster.command(List.of("git","status","--porcelain"),Map.of(),Duration.ofSeconds(5)).isBlank());
        result.put("scope","LOCAL_OWNED_VOLUME_REMOVAL_NO_AZ_POWER_LOSS_OR_BIT_CORRUPTION_CLAIM");
        var session=new Session(output,result);var hook=new Thread(session::close,"disk-owned-cleanup");Runtime.getRuntime().addShutdownHook(hook);
        try {session.open();if(args[0].equals("recover"))session.recover();else session.lastCopy();result.put("correct",true);}
        catch(Exception failure){result.put("correct",false);result.put("failureTypes",types(failure));result.put("failedPhase",session.phase);if(failure.getMessage()!=null&&failure.getMessage().matches("(DISK|HA|KAFKA)_[A-Z_]+"))result.put("failureCode",failure.getMessage());}
        finally {session.close();Runtime.getRuntime().removeShutdownHook(hook);result.put("cleanupComplete",session.cleaned);if(!session.cleaned)result.put("correct",false);result.put("finishedAt",Instant.now().toString());session.save();}
        System.out.println(JSON.writeValueAsString(Map.of("correct",result.get("correct"),"cleanupComplete",session.cleaned)));
        if(!Boolean.TRUE.equals(result.get("correct")))System.exit(2);
    }
    private static final class Session implements AutoCloseable {
        final String id=UUID.randomUUID().toString().replace("-",""),topic="disk."+id,schema="disk_"+id,group="disk-group-"+id;
        final EventCodec codec=new EventCodec();final LocalKafkaCluster cluster;final Path output;final Map<String,Object> result;
        final List<Map<String,Object>> phases=new ArrayList<>(),deliveries=new ArrayList<>();
        final Map<UUID,ChatEvent> attempted=new LinkedHashMap<>();final Set<UUID> acknowledged=new HashSet<>();
        final Map<String,String> acknowledgedDigest=new TreeMap<>();
        Admin admin;HikariDataSource ds;JdbcTemplate jdbc;DefaultKafkaProducerFactory<String,byte[]> factory;KafkaTemplate<String,byte[]> kafka;ProjectionConsumer consumer;ProjectionStore store;
        boolean schemaCreated,closed,cleaned;String phase="OPEN";
        Session(Path output,Map<String,Object> result) throws Exception {this.output=output;this.result=result;cluster=new LocalKafkaCluster(Path.of(""));result.put("cluster",cluster.identity());result.put("phases",phases);result.put("deliveries",deliveries);}
        void save() throws Exception {Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");}
        void open() throws Exception {
            result.put("frozenProbe",cluster.freezeDiskProbe(Path.of(output+".probe-classes")));save();
            cluster.start();admin=cluster.admin();
            try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)){new JdbcTemplate(root).execute("CREATE SCHEMA "+schema);schemaCreated=true;}
            ds=PostgresSupport.openMigrated("jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema,"chatpulse","local-chatpulse");jdbc=new JdbcTemplate(ds);
            KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());fullIsr();
            factory=KafkaSupport.producerFactory(cluster.bootstrap());kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
            var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);store=new ProjectionStore(jdbc,tx,codec,"disk",id);
            consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(cluster.bootstrap(),group,"earliest"),topic,store);consumer.start();
            until(30,()->((Number)consumer.status().get("assignedPartitions")).intValue()==3);
            result.put("runtime",Map.of("java",System.getProperty("java.version"),"kafkaClient",org.apache.kafka.common.utils.AppInfoParser.getVersion(),"imageId",LocalKafkaCluster.command(List.of("docker","image","inspect","apache/kafka:4.1.2","--format","{{.Id}}"),Map.of(),Duration.ofSeconds(5)).strip(),"features",admin.describeFeatures().featureMetadata().get(8,TimeUnit.SECONDS).finalizedFeatures().toString()));
            result.put("initialTopicId",description().topicId().toString());save();
        }
        void recover() throws Exception {
            measure("BASELINE_RF3",()->{publish(10);drain();return snapshot();});
            int replaced=description().partitions().getFirst().isr().stream().map(n->n.id()).filter(n->n!=leader()).findFirst().orElseThrow();
            var old=new HashMap<String,Object>();
            measure("REMOVE_ONE_REPLICA_DISK",()->{
                cluster.stopBroker(replaced);until(30,()->description().partitions().getFirst().isr().size()==2);
                old.putAll(cluster.diskSnapshot(replaced,topic));require(diskDigests(old).equals(acknowledgedDigest),"DISK_BASELINE_COPY_MISMATCH");
                return Map.of("before",old,"erasure",cluster.eraseBrokerDisk(replaced,topic));
            });
            measure("CONTINUE_WITH_TWO_REPLICAS",()->{publish(10);drain();return snapshot();});
            measure("REBUILD_EMPTY_REPLICA",()->{cluster.recreateBroker(replaced);fullIsr();publish(10);drain();return snapshot();});
            measure("READ_REBUILT_DISK",()->{
                cluster.stopBroker(replaced);var rebuilt=cluster.diskSnapshot(replaced,topic);
                require(diskDigests(rebuilt).equals(acknowledgedDigest),"DISK_REBUILT_CONTENT_MISMATCH");
                var before=(Map<?,?>)old.get("metadata");var after=(Map<?,?>)rebuilt.get("metadata");
                require(Objects.equals(before.get("node.id"),after.get("node.id"))&&Objects.equals(before.get("cluster.id"),after.get("cluster.id"))&&!Objects.equals(before.get("directory.id"),after.get("directory.id")),"DISK_IDENTITY_MISMATCH");
                cluster.startBroker(replaced);fullIsr();return Map.of("disk",rebuilt,"directoryIdChanged",true,"offsetDigestDifferences",0);
            });
            int survivor=leader();var followers=List.of(1,2,3).stream().filter(n->n!=survivor).toList();
            measure("ISR_ONE_REJECTS_ACK_ALL",()->{
                for(int node:followers)cluster.stopBroker(node);
                until(30,()->description().partitions().getFirst().isr().size()==1);
                var resource=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,topic);
                var actualConfig=admin.describeConfigs(List.of(resource)).all().get(8,TimeUnit.SECONDS).get(resource);
                boolean policyNotReady=false;try{KafkaTopicPolicy.ha().verify(description(),actualConfig);}catch(KafkaTopicPolicy.NotReady expected){policyNotReady=true;}
                require(policyNotReady,"DISK_ISR_POLICY_NOT_REJECTED");
                Throwable startupFailure=null;try{KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());}catch(IllegalStateException expected){startupFailure=expected;}
                boolean deadlineObserved=false;for(Throwable cause=startupFailure;cause!=null;cause=cause.getCause())if("TOPIC_VISIBILITY_DEADLINE".equals(cause.getMessage()))deadlineObserved=true;
                require(startupFailure!=null&&deadlineObserved,"DISK_ISR_STARTUP_REJECTION_UNCONFIRMED");
                var control=event();var settings=new HashMap<String,Object>(factory.getConfigurationProperties());settings.put("retries",0);settings.put("enable.idempotence",false);
                Throwable direct=null;
                try(var producer=new KafkaProducer<String,byte[]>(settings,new StringSerializer(),new ByteArraySerializer())) {
                    try{producer.send(record(control)).get(12,TimeUnit.SECONDS);}catch(Exception failure){direct=failure;}
                }
                require(direct!=null&&(types(direct).contains("NotEnoughReplicasException")||types(direct).contains("NotEnoughReplicasAfterAppendException")),"DISK_DIRECT_REJECTION_MISSING");
                var product=publishOne(false);require(!Boolean.TRUE.equals(product.get("acknowledged")),"DISK_ISR_ONE_ACKNOWLEDGED");
                return Map.of("startupRejected",true,"startupFailureTypes",types(startupFailure),"startupCode","TOPIC_VISIBILITY_DEADLINE","policyNotReadyObserved",policyNotReady,"controlProducerRetries",0,"controlFailureTypes",types(direct),"productDelivery",product,"snapshot",snapshot());
            });
            measure("ISR_RESTORED_RECONCILIATION",()->{
                for(int node:followers)cluster.startBroker(node);fullIsr();publish(2);drain();consumer.close();consumer=null;
                return reconcile();
            });
        }
        void lastCopy() throws Exception {
            measure("BASELINE_ON_THREE_DISKS",()->{publish(10);drain();return snapshot();});
            int latest=leader();var followers=List.of(1,2,3).stream().filter(n->n!=latest).toList();int stale=followers.get(0),fresh=followers.get(1);
            var baseline=new TreeMap<>(acknowledgedDigest);var diskStale=new HashMap<String,Object>();
            measure("KEEP_STALE_BASELINE_DISK_OFFLINE",()->{
                cluster.stopBroker(stale);until(30,()->description().partitions().getFirst().isr().size()==2);
                diskStale.putAll(cluster.diskSnapshot(stale,topic));require(diskDigests(diskStale).equals(baseline),"DISK_STALE_BASELINE_MISMATCH");return diskStale;
            });
            measure("ACK_TAIL_ON_TWO_COPIES",()->{publish(10);drain();consumer.close();consumer=null;return snapshot();});
            ReplayManifest manifest;try(var source=new ReplaySource(cluster.bootstrap(),topic,UUID.randomUUID())){manifest=source.capture(null,null);}
            result.put("beforeLossManifest",manifest);result.put("beforeLossAcknowledgedDigests",new TreeMap<>(acknowledgedDigest));save();
            var tail=new TreeMap<>(acknowledgedDigest);baseline.keySet().forEach(tail::remove);require(tail.size()==10,"DISK_TAIL_SIZE");
            measure("REMOVE_FIRST_LATEST_COPY",()->{
                cluster.stopBroker(fresh);var disk=cluster.diskSnapshot(fresh,topic);require(diskDigests(disk).entrySet().containsAll(acknowledgedDigest.entrySet()),"DISK_FIRST_TAIL_COPY_MISSING");
                return Map.of("disk",disk,"erasure",cluster.eraseBrokerDisk(fresh,topic));
            });
            measure("REMOVE_LAST_LATEST_COPY",()->{
                cluster.stopBroker(latest);var disk=cluster.diskSnapshot(latest,topic);require(diskDigests(disk).entrySet().containsAll(acknowledgedDigest.entrySet()),"DISK_LAST_TAIL_COPY_MISSING");
                var erased=cluster.eraseBrokerDisk(latest,topic);var remaining=cluster.diskSnapshot(stale,topic);
                require(diskDigests(remaining).equals(baseline),"DISK_REMAINING_STALE_CHANGED");
                return Map.of("lastLatestDisk",disk,"erasure",erased,"remainingStaleDisk",remaining,"tailCopiesRemaining",0,"tailRecordsPermanentlyRemoved",10);
            });
            measure("RETURN_STALE_AND_EMPTY_DISKS",()->{
                cluster.startBroker(stale);var onlyStale=observeSource(manifest);
                cluster.recreateBroker(fresh);var afterFirstEmpty=observeSource(manifest);cluster.recreateBroker(latest);
                until(60,()->new HashSet<>(admin.describeCluster().nodes().get(8,TimeUnit.SECONDS).stream().map(n->n.id()).toList()).equals(Set.of(1,2,3)));
                var returned=cluster.brokerStates();require(returned.stream().allMatch(state->Boolean.TRUE.equals(state.get("running"))&&!Boolean.TRUE.equals(state.get("oomKilled"))),"DISK_RETURNED_BROKER_NOT_RUNNING");
                // ELR/lastKnownElr 의 실제 결과를 보존한다. unclean election·minISR 변경은 하지 않는다.
                boolean ready=waitOptional(60,()->description().partitions().getFirst().leader()!=null&&description().partitions().getFirst().isr().size()==3);
                var restored=observeSource(manifest);var row=new LinkedHashMap<String,Object>();row.put("onlyStale",onlyStale);row.put("afterFirstEmptyDisk",afterFirstEmpty);row.put("allNodesReturned",true);row.put("returnedBrokerStates",returned);row.put("registeredBrokerIds",admin.describeCluster().nodes().get(8,TimeUnit.SECONDS).stream().map(n->n.id()).sorted().toList());row.put("fullIsrObserved",ready);row.put("source",restored);
                if(restored.get("metadataTopicId")!=null)require(restored.get("metadataTopicId").equals(manifest.topicId()),"DISK_TOPIC_ID_CHANGED");
                row.put("topicIdVerified",Objects.equals(restored.get("metadataTopicId"),manifest.topicId()));
                var database=dbIds();require(database.equals(acknowledged),"DISK_DATABASE_BASELINE_CHANGED");row.put("databaseUniqueRetained",database.size());
                if(Boolean.TRUE.equals(restored.get("available"))) {
                    var records=readBroker();var digests=digests(records);long retainedTail=tail.entrySet().stream().filter(e->Objects.equals(digests.get(e.getKey()),e.getValue())).count();
                    require(retainedTail==0,"DISK_REMOVED_TAIL_REAPPEARED");row.put("brokerPhysical",records.size());row.put("oldAcknowledgedTailMissing",10);row.put("brokerDigest",digests);
                } else {row.put("brokerPhysical",null);row.put("oldAcknowledgedTailMissing",null);}
                if(!Boolean.TRUE.equals(restored.get("available")))require(Set.of("SOURCE_UNAVAILABLE","OFFSET_TRUNCATED").contains(restored.get("captureCode")),"DISK_UNEXPECTED_SOURCE_FAILURE");
                require(!Boolean.TRUE.equals(restored.get("oldManifestAccepted"))&&Set.of("SOURCE_UNAVAILABLE","OFFSET_TRUNCATED").contains(restored.getOrDefault("validationCode",restored.get("captureCode"))),"DISK_OLD_MANIFEST_REJECTION_MISSING");
                row.put("lossBoundaryVerified",true);row.put("sourceRecoverySucceeded",false);row.put("physicalTailCopiesRemaining",0);return row;
            });
        }
        Map<String,Object> observeSource(ReplayManifest original) throws Exception {
            var row=new LinkedHashMap<String,Object>();var errors=new ArrayList<List<String>>();
            long began=System.nanoTime(),deadline=began+Duration.ofSeconds(30).toNanos();TopicDescription metadata=null;
            while(metadata==null&&System.nanoTime()<deadline) {
                try{metadata=description();}catch(ExecutionException|TimeoutException pending){errors.add(types(pending));Thread.sleep(200);}
            }
            row.put("metadataWaitSeconds",(System.nanoTime()-began)/1e9);row.put("metadataFailureTypes",errors);
            row.put("metadataAvailable",metadata!=null);row.put("metadataTopicId",metadata==null?null:metadata.topicId().toString());
            row.put("partitions",metadata==null?null:partitions(metadata));
            try(var source=new ReplaySource(cluster.bootstrap(),topic,UUID.randomUUID())) {
                try{var current=source.capture(null,null);row.put("available",true);row.put("manifest",current);}
                catch(ReplayFailure failure){row.put("available",false);row.put("captureCode",failure.getMessage());}
                try{source.validate(original);row.put("oldManifestAccepted",true);}
                catch(ReplayFailure failure){row.put("oldManifestAccepted",false);row.put("validationCode",failure.getMessage());}
            } catch(ReplayFailure unavailable){row.put("available",false);row.put("captureCode",unavailable.getMessage());row.put("oldManifestAccepted",null);}
            return row;
        }
        ChatEvent event(){var now=Instant.now();var event=new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL","a".repeat(32),"synthetic",UUID.randomUUID(),null,"CHAT",now,now,false,"disk-test");attempted.put(event.eventId(),event);return event;}
        ProducerRecord<String,byte[]> record(ChatEvent event){return new ProducerRecord<>(topic,0,event.receivedAt().toEpochMilli(),event.channelId(),codec.encode(event));}
        Map<String,Object> publishOne(boolean required) throws Exception {
            var event=event();var row=new LinkedHashMap<String,Object>();row.put("eventId",event.eventId());row.put("startedAt",Instant.now().toString());deliveries.add(row);
            try {var submitted=record(event);var metadata=kafka.send(submitted).get(12,TimeUnit.SECONDS).getRecordMetadata();acknowledged.add(event.eventId());String digest=KafkaDiskProbe.digest(ByteBuffer.wrap(submitted.key().getBytes(StandardCharsets.UTF_8)),ByteBuffer.wrap(submitted.value()));
                acknowledgedDigest.put(metadata.partition()+":"+metadata.offset(),digest);row.put("acknowledged",true);row.put("partition",metadata.partition());row.put("offset",metadata.offset());row.put("sha256",digest);
            }catch(Exception failure){row.put("acknowledged",false);row.put("failureTypes",types(failure));if(required)throw failure;}
            finally {row.put("finishedAt",Instant.now().toString());save();}
            return row;
        }
        void publish(int count) throws Exception {for(int i=0;i<count;i++)publishOne(true);}
        void drain() throws Exception {until(30,()->{
            var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS);var end=ends();
            return end.entrySet().stream().allMatch(e->committed.containsKey(e.getKey())&&committed.get(e.getKey()).offset()>=e.getValue());
        });}
        TopicDescription description() throws Exception{return admin.describeTopics(List.of(topic)).allTopicNames().get(8,TimeUnit.SECONDS).get(topic);}
        int leader(){try{return description().partitions().getFirst().leader().id();}catch(Exception failure){throw new IllegalStateException("DISK_LEADER_UNAVAILABLE",failure);}}
        void fullIsr() throws Exception{until(60,()->description().partitions().stream().allMatch(p->p.leader()!=null&&p.isr().size()==3));}
        Map<TopicPartition,Long> ends() throws Exception{var request=new HashMap<TopicPartition,OffsetSpec>();for(int p=0;p<3;p++)request.put(new TopicPartition(topic,p),OffsetSpec.latest());var result=new HashMap<TopicPartition,Long>();admin.listOffsets(request).all().get(8,TimeUnit.SECONDS).forEach((p,v)->result.put(p,v.offset()));return result;}
        List<Map<String,Object>> partitions() throws Exception{return partitions(description());}
        List<Map<String,Object>> partitions(TopicDescription metadata){return metadata.partitions().stream().map(p->{var row=new LinkedHashMap<String,Object>();row.put("partition",p.partition());row.put("leader",p.leader()==null?null:p.leader().id());row.put("replicas",p.replicas().stream().map(n->n.id()).toList());row.put("isr",p.isr().stream().map(n->n.id()).toList());row.put("elr",p.elr()==null?null:p.elr().stream().map(n->n.id()).toList());row.put("lastKnownElr",p.lastKnownElr()==null?null:p.lastKnownElr().stream().map(n->n.id()).toList());return (Map<String,Object>)row;}).toList();}
        Map<String,Object> snapshot() throws Exception {
            var row=new LinkedHashMap<String,Object>();row.put("partitions",partitions());row.put("database",store.totals());
            try{row.put("consumableEnd",ends().entrySet().stream().map(e->Map.of("partition",e.getKey().partition(),"end",e.getValue())).toList());row.put("endAvailable",true);}
            catch(ExecutionException|TimeoutException failure){row.put("endAvailable",false);row.put("endFailureTypes",types(failure));}
            row.put("quorumLeader",admin.describeMetadataQuorum().quorumInfo().get(8,TimeUnit.SECONDS).leaderId());return row;
        }
        List<ConsumerRecord<String,byte[]>> readBroker() throws Exception {
            var records=new ArrayList<ConsumerRecord<String,byte[]>>();
            try(var source=new ReplaySource(cluster.bootstrap(),topic,UUID.randomUUID())) {
                var manifest=source.capture(null,null);source.assign(manifest);var next=new HashMap<Integer,Long>();manifest.partitions().forEach(p->next.put(p.partition(),p.start()));long deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
                while(manifest.partitions().stream().anyMatch(p->next.get(p.partition())<p.end())){require(System.nanoTime()<deadline,"DISK_READ_TIMEOUT");for(var record:source.poll()){records.add(record);next.put(record.partition(),record.offset()+1);}}
                source.validate(manifest);
            }return records;
        }
        Set<UUID> dbIds(){return new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection='disk' AND run_id=?",(rs,n)->rs.getObject(1,UUID.class),id));}
        Map<String,Object> reconcile() throws Exception {
            var records=readBroker();var broker=new HashSet<UUID>();records.forEach(r->broker.add(codec.decode(r.value(),r.key()).eventId()));
            require(broker.containsAll(acknowledged)&&attempted.keySet().containsAll(broker)&&dbIds().equals(broker),"DISK_FINAL_SET_MISMATCH");
            require(((Number)store.totals().get("bucketTotal")).longValue()==broker.size(),"DISK_BUCKET_MISMATCH");
            var read=digests(records);require(read.entrySet().containsAll(acknowledgedDigest.entrySet()),"DISK_ACK_DIGEST_MISMATCH");
            return Map.of("attempted",attempted.size(),"acknowledged",acknowledged.size(),"brokerPhysical",records.size(),"brokerUnique",broker.size(),"databaseUnique",dbIds().size(),"missingAcknowledged",0,"setDifference",0,"unconfirmedPresent",broker.stream().filter(id->!acknowledged.contains(id)).count(),"offsetDigests",read);
        }
        Map<String,String> digests(List<ConsumerRecord<String,byte[]>> records) throws Exception {var result=new TreeMap<String,String>();for(var r:records)result.put(r.partition()+":"+r.offset(),KafkaDiskProbe.digest(ByteBuffer.wrap(r.key().getBytes(StandardCharsets.UTF_8)),ByteBuffer.wrap(r.value())));return result;}
        @SuppressWarnings("unchecked") Map<String,String> diskDigests(Map<String,Object> disk){var result=new TreeMap<String,String>();for(var r:(List<Map<String,Object>>)disk.get("records")){require(!Boolean.TRUE.equals(r.get("control")),"DISK_UNEXPECTED_CONTROL_RECORD");result.put(r.get("partition")+":"+r.get("offset"),(String)r.get("sha256"));}return result;}
        void measure(String name,Scenario scenario) throws Exception {phase=name;var row=new LinkedHashMap<String,Object>();row.put("name",name);row.put("startedAt",Instant.now().toString());phases.add(row);save();long began=System.nanoTime();try{row.put("result",scenario.run());row.put("passed",true);}catch(Exception failure){row.put("passed",false);throw failure;}finally{row.put("seconds",(System.nanoTime()-began)/1e9);save();}}
        public synchronized void close(){if(closed)return;closed=true;boolean ok=true;if(consumer!=null)try{consumer.close();}catch(Exception failure){ok=false;}if(factory!=null)try{factory.destroy();}catch(Exception failure){ok=false;}if(admin!=null)try{admin.close(Duration.ofSeconds(1));}catch(Exception failure){ok=false;}if(ds!=null)try{ds.close();}catch(Exception failure){ok=false;}if(schemaCreated)try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");}catch(Exception failure){ok=false;}cluster.close();cleaned=ok&&cluster.cleaned();}
    }
    @FunctionalInterface interface Scenario{Map<String,Object> run() throws Exception;}
    @FunctionalInterface interface Condition{boolean ready() throws Exception;}
    static void until(int seconds,Condition condition) throws Exception {require(waitOptional(seconds,condition),"DISK_WAIT_TIMEOUT");}
    static boolean waitOptional(int seconds,Condition condition) throws Exception {long deadline=System.nanoTime()+Duration.ofSeconds(seconds).toNanos();while(System.nanoTime()<deadline){try{if(condition.ready())return true;}catch(ExecutionException|TimeoutException pending){}Thread.sleep(200);}return false;}
    static void require(boolean value,String code){if(!value)throw new IllegalStateException(code);}
    static List<String> types(Throwable failure){var result=new ArrayList<String>();for(Throwable item=failure;item!=null&&result.size()<8;item=item.getCause())result.add(item.getClass().getSimpleName());return result;}
}
