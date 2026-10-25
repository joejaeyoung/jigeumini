package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.ReplaySource;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.*;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 단일 호스트의 실제3 controller/3 broker에서 복제·quorum·RF 이행을 구분한다. */
public final class KafkaHaExperiment {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public static void main(String[] args) {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)
            context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        try {
            if(args.length!=1)throw new IllegalArgumentException("HA_ARGUMENTS");
            var result=run(Path.of(args[0]));System.out.println(JSON.writeValueAsString(Map.of("correct",result.get("correct"),"cleanupComplete",result.get("cleanupComplete"))));
            if(!Boolean.TRUE.equals(result.get("correct")))System.exit(2);
        } catch(Exception failure) {System.out.println("{\"error\":\"HA_EXPERIMENT_FAILED\"}");System.exit(2);}
    }
    static Map<String,Object> run(Path output) throws Exception {
        output=output.toAbsolutePath();Files.createDirectories(output.getParent());Files.createFile(output);
        var result=new LinkedHashMap<String,Object>();result.put("startedAt",Instant.now().toString());
        result.put("codeCommit",LocalKafkaCluster.command(List.of("git","rev-parse","HEAD"),Map.of(),Duration.ofSeconds(10)).strip());
        result.put("scope","LOCAL_SIX_NODES_RF3_MINISR2_ELR1_NO_AZ_OR_DISK_CORRUPTION_CLAIM");
        var session=new Session(output,result);result.put("cluster",session.cluster.identity());result.put("schema",session.schema);result.put("topic",session.topic);
        result.put("policy",KafkaTopicPolicy.ha());result.put("phases",session.phases);Files.writeString(output,JSON.writeValueAsString(result));
        var hook=new Thread(session::close,"ha-experiment-cleanup");Runtime.getRuntime().addShutdownHook(hook);
        try {session.open();session.exercise();result.put("correct",true);}
        catch(Exception failure) {result.put("correct",false);result.put("failureCode","HA_SCENARIO_FAILED");result.put("failureType",failure.getClass().getSimpleName());result.put("failedPhase",session.phase);result.put("failedOperation",session.operation);if(failure.getCause()!=null)result.put("causeType",failure.getCause().getClass().getSimpleName());if(failure.getMessage()!=null&&failure.getMessage().matches("HA_[A-Z_]+"))result.put("failureDetailCode",failure.getMessage());}
        finally {
            session.close();Runtime.getRuntime().removeShutdownHook(hook);result.put("cleanupComplete",session.cleaned);
            if(!session.cleaned)result.put("correct",false);result.put("finishedAt",Instant.now().toString());
            Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");
        }
        return result;
    }
    private static final class Session implements AutoCloseable {
        final String id=UUID.randomUUID().toString().replace("-",""),schema="ha_"+id,topic="ha.chat."+id,migrationTopic="ha.migrate."+id;
        final LocalKafkaCluster cluster;final EventCodec codec=new EventCodec();
        final List<Map<String,Object>> phases=new ArrayList<>();final List<ProjectionConsumer> consumers=new ArrayList<>();
        final Map<String,Set<UUID>> attempted=new HashMap<>(),acked=new HashMap<>();
        Admin admin;HikariDataSource ds;JdbcTemplate jdbc;DefaultKafkaProducerFactory<String,byte[]> factory;KafkaTemplate<String,byte[]> kafka;
        boolean schemaCreated,closed,cleaned;String phase="OPEN",operation="OPEN";
        final Path output;final Map<String,Object> result;
        Session(Path output,Map<String,Object> result) throws Exception {this.output=output;this.result=result;cluster=new LocalKafkaCluster(Path.of(""));}
        void checkpoint() throws Exception {Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");}
        void open() throws Exception {
            cluster.start();admin=cluster.admin();
            try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)) {
                new JdbcTemplate(root).execute("CREATE SCHEMA "+schema);schemaCreated=true;
            }
            ds=PostgresSupport.openMigrated("jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema,"chatpulse","local-chatpulse");jdbc=new JdbcTemplate(ds);
            KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());
            factory=KafkaSupport.producerFactory(cluster.bootstrap());kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
            startConsumer(topic,"ha-main");
        }
        void exercise() throws Exception {
            measure("HEALTHY_RF3",()->{var delivery=publish(topic,100);require(delivery.get("acknowledged").equals(100L));drain(topic,"ha-main");return snapshot(topic,delivery);});
            int stopped=description(topic).partitions().getFirst().leader().id();
            measure("ONE_BROKER_DOWN",()->{
                cluster.stopBroker(stopped);until(40,()->description(topic).partitions().stream().allMatch(p->p.leader()!=null&&p.leader().id()!=stopped&&p.isr().size()==2));
                KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());var delivery=publish(topic,20);require(delivery.get("acknowledged").equals(20L));drain(topic,"ha-main");return snapshot(topic,delivery);
            });
            measure("BROKER_REJOIN",()->{cluster.startBroker(stopped);fullIsr(topic);return snapshot(topic,Map.of());});
            int survivor=description(topic).partitions().getFirst().leader().id();
            var followers=List.of(1,2,3).stream().filter(i->i!=survivor).toList();
            measure("ISR_ONE_WITH_CONTROLLER_QUORUM",()->{
                for(int node:followers)cluster.stopBroker(node);
                until(40,()->description(topic).partitions().stream().allMatch(p->p.isr().size()==1&&p.leader()!=null&&p.leader().id()==survivor));
                boolean rejected=false;try{KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());}catch(IllegalStateException expected){rejected=true;}
                require(rejected);var delivery=publish(topic,1);require(delivery.get("acknowledged").equals(0L));
                var row=snapshot(topic,delivery);row.put("startupRejected",true);row.put("quorumLeader",admin.describeMetadataQuorum().quorumInfo().get(8,TimeUnit.SECONDS).leaderId());return row;
            });
            measure("ISR_RECOVERY",()->{for(int node:followers)cluster.startBroker(node);fullIsr(topic);var delivery=publish(topic,20);require(delivery.get("acknowledged").equals(20L));drain(topic,"ha-main");return snapshot(topic,delivery);});
            int firstController=admin.describeMetadataQuorum().quorumInfo().get(8,TimeUnit.SECONDS).leaderId()-1000;
            measure("ONE_CONTROLLER_DOWN",()->{
                cluster.stopController(firstController);
                until(30,()->{int leader=admin.describeMetadataQuorum().quorumInfo().get(3,TimeUnit.SECONDS).leaderId();return leader>0&&leader!=firstController+1000;});
                KafkaSupport.ensureTopic(cluster.bootstrap(),"ha.metadata.one."+id,KafkaTopicPolicy.ha());
                var delivery=publish(topic,10);require(delivery.get("acknowledged").equals(10L));return snapshot(topic,delivery);
            });
            int secondController=admin.describeMetadataQuorum().quorumInfo().get(8,TimeUnit.SECONDS).leaderId()-1000;
            measure("CONTROLLER_QUORUM_LOST",()->{
                cluster.stopController(secondController);boolean failed=false;
                var probe=cluster.admin();try {
                    try{probe.createTopics(List.of(new NewTopic("ha.metadata.quorum."+id,3,(short)3)),new CreateTopicsOptions().timeoutMs(5000)).all().get(7,TimeUnit.SECONDS);}
                    catch(ExecutionException|TimeoutException expected){failed=true;}
                } finally {probe.close(Duration.ofSeconds(1));}
                require(failed);var row=new LinkedHashMap<String,Object>();row.put("metadataWriteAcknowledged",false);
                row.put("existingTopicDeliveryObserved",publish(topic,1));return row;
            });
            measure("QUORUM_RECOVERY",()->{
                cluster.startController(firstController);cluster.startController(secondController);
                until(40,()->admin.describeMetadataQuorum().quorumInfo().get(3,TimeUnit.SECONDS).leaderId()>0);
                KafkaSupport.ensureTopic(cluster.bootstrap(),"ha.metadata.restored."+id,KafkaTopicPolicy.ha());fullIsr(topic);
                var delivery=publish(topic,20);require(delivery.get("acknowledged").equals(20L));drain(topic,"ha-main");return snapshot(topic,delivery);
            });
            measure("RF1_TO_RF3_MIGRATION",()->{
                KafkaSupport.ensureTopic(cluster.bootstrap(),migrationTopic,KafkaTopicPolicy.local());startConsumer(migrationTopic,"ha-migration");
                var original=description(migrationTopic).topicId();var before=publish(migrationTopic,10);require(before.get("acknowledged").equals(10L));drain(migrationTopic,"ha-migration");
                boolean rejected=false;try{KafkaSupport.ensureTopic(cluster.bootstrap(),migrationTopic,KafkaTopicPolicy.ha());}catch(IllegalStateException expected){rejected=true;}require(rejected);
                var changes=new HashMap<TopicPartition,Optional<NewPartitionReassignment>>();
                for(var partition:description(migrationTopic).partitions()) {
                    var replicas=new ArrayList<Integer>();replicas.add(partition.replicas().getFirst().id());for(int node:List.of(1,2,3))if(!replicas.contains(node))replicas.add(node);
                    changes.put(new TopicPartition(migrationTopic,partition.partition()),Optional.of(new NewPartitionReassignment(replicas)));
                }
                admin.alterPartitionReassignments(changes).all().get(10,TimeUnit.SECONDS);fullIsr(migrationTopic);
                until(30,()->admin.listPartitionReassignments(changes.keySet()).reassignments().get(8,TimeUnit.SECONDS).isEmpty());
                var config=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,migrationTopic);
                var updates=KafkaTopicPolicy.ha().configurations().entrySet().stream().map(e->new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET)).toList();
                admin.incrementalAlterConfigs(Map.of(config,updates)).all().get(10,TimeUnit.SECONDS);KafkaSupport.ensureTopic(cluster.bootstrap(),migrationTopic,KafkaTopicPolicy.ha());
                require(original.equals(description(migrationTopic).topicId()));var after=publish(migrationTopic,10);require(after.get("acknowledged").equals(10L));drain(migrationTopic,"ha-migration");
                var row=snapshot(migrationTopic,after);row.put("before",before);row.put("topicIdPreserved",true);row.put("haStartupRejectedBeforeMigration",true);return row;
            });
            measure("FINAL_BROKER_AND_DATABASE_RECONCILIATION",()->{
                for(var consumer:consumers)consumer.close();
                return Map.of("main",reconcile(topic,"ha-main"),"migration",reconcile(migrationTopic,"ha-migration"));
            });
        }
        void measure(String name,Scenario action) throws Exception {
            phase=name;operation="SCENARIO";long begin=System.nanoTime();var row=new LinkedHashMap<String,Object>();row.put("name",name);row.put("startedAt",Instant.now().toString());phases.add(row);checkpoint();
            try{row.putAll(action.run());row.put("passed",true);}catch(Exception failure){row.put("passed",false);throw failure;}
            finally{row.put("seconds",(System.nanoTime()-begin)/1e9);checkpoint();}
        }
        void startConsumer(String input,String projection) throws Exception {
            var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(cluster.bootstrap(),"ha-group-"+projection+"-"+id,"earliest"),input,store(projection));
            consumers.add(consumer);consumer.start();until(30,()->((Number)consumer.status().get("assignedPartitions")).intValue()==3);
        }
        ProjectionStore store(String projection) {var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return new ProjectionStore(jdbc,tx,codec,projection,id);}
        Map<String,Object> publish(String input,int count) {
            long acknowledged=0,submitted=0;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);var accepted=attempted.computeIfAbsent(input,ignored->new HashSet<>());var confirmed=acked.computeIfAbsent(input,ignored->new HashSet<>());
            for(int i=0;i<count&&System.nanoTime()<deadline;i++) {
                Instant now=Instant.now();String channel=String.format("%032x",i%20+1);UUID eventId=UUID.randomUUID();
                var event=new ChatEvent(1,eventId,"CHZZK_UNOFFICIAL",channel,"synthetic-room",UUID.randomUUID(),null,"CHAT",now,now,false,"ha-experiment");accepted.add(eventId);submitted++;
                try{kafka.send(new ProducerRecord<>(input,null,now.toEpochMilli(),channel,codec.encode(event))).get(12,TimeUnit.SECONDS);confirmed.add(eventId);acknowledged++;}
                catch(Exception failure){if(failure instanceof InterruptedException)Thread.currentThread().interrupt();break;}
            }
            return Map.of("requested",(long)count,"attempted",submitted,"acknowledged",acknowledged,"unconfirmed",submitted-acknowledged,"ackMeaning","BROKER_ACK_NOT_DB_COMMIT");
        }
        TopicDescription description(String input) throws Exception {return admin.describeTopics(List.of(input)).allTopicNames().get(8,TimeUnit.SECONDS).get(input);}
        void fullIsr(String input) throws Exception {until(60,()->description(input).partitions().stream().allMatch(p->p.isr().size()==3&&p.leader()!=null&&p.leader().id()>0));}
        void drain(String input,String projection) throws Exception {
            until(30,()->{
                var committed=admin.listConsumerGroupOffsets("ha-group-"+projection+"-"+id).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS);
                var ends=ends(input);return ends.entrySet().stream().allMatch(e->committed.containsKey(e.getKey())&&committed.get(e.getKey()).offset()>=e.getValue());
            });
        }
        Map<TopicPartition,Long> ends(String input) throws Exception {
            var request=new HashMap<TopicPartition,OffsetSpec>();for(int p=0;p<3;p++)request.put(new TopicPartition(input,p),OffsetSpec.latest());
            var result=new HashMap<TopicPartition,Long>();admin.listOffsets(request).all().get(8,TimeUnit.SECONDS).forEach((p,o)->result.put(p,o.offset()));return result;
        }
        LinkedHashMap<String,Object> snapshot(String input,Map<String,Object> delivery) throws Exception {
            var row=new LinkedHashMap<String,Object>();row.put("delivery",delivery);
            row.put("partitions",description(input).partitions().stream().map(p->Map.of("partition",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),"replicas",p.replicas().stream().map(n->n.id()).toList(),"isr",p.isr().stream().map(n->n.id()).toList())).toList());
            String projection=input.equals(topic)?"ha-main":"ha-migration";
            row.put("database",store(projection).totals());
            operation="GROUP_OFFSETS_READ";
            try {
            var committed=admin.listConsumerGroupOffsets("ha-group-"+projection+"-"+id).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS);
            operation="END_OFFSETS_READ";var offsets=ends(input);row.put("offsets",offsets.entrySet().stream().sorted(Map.Entry.comparingByKey(Comparator.comparingInt(TopicPartition::partition)))
                    .map(e->{var offset=committed.get(e.getKey());var item=new LinkedHashMap<String,Object>();item.put("partition",e.getKey().partition());item.put("end",e.getValue());item.put("committed",offset==null?null:offset.offset());item.put("lag",offset==null?null:Math.max(0,e.getValue()-offset.offset()));return item;}).toList());row.put("offsetsAvailable",true);
            } catch(ExecutionException|TimeoutException unavailable) {
                row.put("offsetsAvailable",false);row.put("offsetFailureOperation",operation);row.put("offsetFailureType",unavailable.getClass().getSimpleName());
                if(unavailable.getCause()!=null)row.put("offsetFailureCauseType",unavailable.getCause().getClass().getSimpleName());
                // ISR 부족으로 __consumer_offsets 자체의 조회가 불가능하면 lag를0으로 만들지 않는다.
                if(!phase.equals("ISR_ONE_WITH_CONTROLLER_QUORUM"))throw unavailable;
            }
            return row;
        }
        Map<String,Object> reconcile(String input,String projection) throws Exception {
            var brokerIds=new HashSet<UUID>();long physical=0;
            try(var source=new ReplaySource(cluster.bootstrap(),input,UUID.randomUUID())) {
                var manifest=source.capture(null,null);source.assign(manifest);var next=new HashMap<Integer,Long>();manifest.partitions().forEach(p->next.put(p.partition(),p.start()));
                long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
                while(manifest.partitions().stream().anyMatch(p->next.get(p.partition())<p.end())) {
                    if(System.nanoTime()>end)throw new IllegalStateException("HA_READBACK_TIMEOUT");
                    for(var record:source.poll()) {brokerIds.add(codec.decode(record.value(),record.key()).eventId());physical++;next.put(record.partition(),record.offset()+1);}
                }
                source.validate(manifest);
            }
            var database=new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection=? AND run_id=?",(rs,n)->rs.getObject(1,UUID.class),projection,id));
            require(brokerIds.containsAll(acked.get(input))&&attempted.get(input).containsAll(brokerIds)&&database.equals(brokerIds));
            require(((Number)store(projection).totals().get("bucketTotal")).longValue()==database.size());
            return Map.of("attempted",attempted.get(input).size(),"acknowledged",acked.get(input).size(),"brokerUnique",brokerIds.size(),"brokerPhysical",physical,"databaseUnique",database.size(),"missingAcknowledged",0,"brokerDatabaseDifferences",0);
        }
        @Override public synchronized void close() {
            if(closed)return;closed=true;boolean ok=true;
            for(var consumer:consumers)try{consumer.close();}catch(Exception failure){ok=false;}
            if(factory!=null)try{factory.destroy();}catch(Exception failure){ok=false;}
            if(admin!=null)try{admin.close(Duration.ofSeconds(1));}catch(Exception failure){ok=false;}
            if(ds!=null)try{ds.close();}catch(Exception failure){ok=false;}
            if(schemaCreated)try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");}catch(Exception failure){ok=false;}
            cluster.close();cleaned=ok&&cluster.cleaned();
        }
    }
    @FunctionalInterface interface Scenario {Map<String,Object> run() throws Exception;}
    @FunctionalInterface interface Condition {boolean test() throws Exception;}
    private static void until(int seconds,Condition condition) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
        while(System.nanoTime()<end){try{if(condition.test())return;}catch(ExecutionException|TimeoutException pending){}Thread.sleep(200);}
        throw new IllegalStateException("HA_CONDITION_TIMEOUT");
    }
    private static void require(boolean value){if(!value)throw new IllegalStateException("HA_ASSERTION");}
}
