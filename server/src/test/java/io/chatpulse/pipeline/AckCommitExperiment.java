package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.ReplaySource;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** #31: 같은 합성 입력에서 commit 경계와 ACK 수준을 분리한다. 제품 기본값은 변경하지 않는다. */
public final class AckCommitExperiment {
    private static final Instant INPUT_BASE=Instant.now().minusSeconds(10);
    private static final JsonMapper JSON=JsonMapper.builder().build();private static final EventCodec CODEC=new EventCodec();
    public static void main(String[] args) {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        try{var result=run(Path.of(args[0]));System.out.println(JSON.writeValueAsString(Map.of("correct",result.get("correct"),"cleanupComplete",result.get("cleanupComplete"))));if(!Boolean.TRUE.equals(result.get("correct")))System.exit(2);}
        catch(Exception failure){System.out.println("{\"error\":\"ACK_EXPERIMENT_FAILED\"}");System.exit(2);}
    }
    static Map<String,Object> run(Path output) throws Exception {
        output=output.toAbsolutePath();Files.createDirectories(output.getParent());Files.createFile(output);
        var result=new LinkedHashMap<String,Object>();result.put("codeCommit",LocalKafkaCluster.command(List.of("git","rev-parse","HEAD"),Map.of(),Duration.ofSeconds(10)).strip());result.put("startedAt",Instant.now());result.put("seed",20261026);result.put("eventTimeBase",INPUT_BASE);result.put("phase","OPEN");
        try(var session=new Session(output,result)) {
            result.put("cluster",session.cluster.identity());session.open();session.commitCases(session.cluster.bootstrap());session.ackCases();result.put("correct",true);
        } catch(Exception failure){result.put("correct",false);result.put("failureType",failure.getClass().getSimpleName());if(failure.getCause()!=null)result.put("causeType",failure.getCause().getClass().getSimpleName());}
        if(!Boolean.TRUE.equals(result.get("cleanupComplete")))result.put("correct",false);result.put("finishedAt",Instant.now());Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");return result;
    }
    private static final class Session implements AutoCloseable {
        final String id=UUID.randomUUID().toString().replace("-",""),schema="ack_"+id;final LocalKafkaCluster cluster;
        final Path output;final Map<String,Object> result;final List<Process> children=new ArrayList<>();
        HikariDataSource ds;JdbcTemplate jdbc;Admin admin;boolean created,closed;
        final Thread hook=new Thread(this::close,"ack-owned-cleanup");
        Session(Path output,Map<String,Object> result) throws Exception{this.output=output;this.result=result;cluster=new LocalKafkaCluster(Path.of(""));Runtime.getRuntime().addShutdownHook(hook);}
        String url(){return "jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema;}
        void open() throws Exception {
            cluster.start();admin=cluster.admin();
            try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)){new JdbcTemplate(root).execute("CREATE SCHEMA "+schema);created=true;}
            ds=PostgresSupport.openMigrated(url(),"chatpulse","local-chatpulse");jdbc=new JdbcTemplate(ds);result.put("schema",schema);result.put("policy",KafkaTopicPolicy.ha());checkpoint();
        }
        void commitCases(String bootstrap) throws Exception {
            result.put("phase","COMMIT_BOUNDARIES");String topic="ack.commit."+id;KafkaSupport.ensureTopic(bootstrap,topic,KafkaTopicPolicy.ha());
            try(var producer=producer(bootstrap,"all")){producer.send(record(topic,0)).get(8,TimeUnit.SECONDS);}
            var rows=new ArrayList<Map<String,Object>>();result.put("commitCases",rows);
            for(String mode:List.of("AUTO","BATCH","MANUAL","AUTO_POLL_AHEAD"))for(String boundary:mode.equals("AUTO_POLL_AHEAD")?List.of("AFTER_OFFSET"):List.of("BEFORE_DB","AFTER_DB","AFTER_OFFSET")) {
                String key=(mode+"_"+boundary).toLowerCase(Locale.ROOT),group="ack-group-"+id+"-"+key;
                var zeros=new HashMap<TopicPartition,OffsetAndMetadata>();for(int p=0;p<3;p++)zeros.put(new TopicPartition(topic,p),new OffsetAndMetadata(0));admin.alterConsumerGroupOffsets(group,zeros).all().get(8,TimeUnit.SECONDS);
                Path trace=output.resolveSibling(output.getFileName()+"."+key+".trace.json");
                var row=new LinkedHashMap<String,Object>();row.put("mode",mode);row.put("boundary",boundary);row.put("group",group);rows.add(row);checkpoint();
                var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx128m","-cp",System.getProperty("java.class.path"),AckCommitCrashProcess.class.getName(),bootstrap,url(),topic,group,mode,boundary,key,trace.toString()).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();children.add(child);
                if(!child.waitFor(35,TimeUnit.SECONDS))throw new IllegalStateException("ACK_CHILD_TIMEOUT");
                int expected=switch(boundary){case "BEFORE_DB"->71;case "AFTER_DB"->72;default->73;};require(child.exitValue()==expected);
                row.put("exitCode",child.exitValue());row.put("trace",JSON.readValue(Files.readString(trace),Map.class));
                var store=store(key);long before=((Number)store.totals().get("bucketTotal")).longValue();long offset=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS).get(new TopicPartition(topic,0)).offset();
                row.put("databaseBeforeRestart",before);row.put("offsetBeforeRestart",offset);
                require(before==(boundary.equals("BEFORE_DB")||mode.equals("AUTO_POLL_AHEAD")?0:1));require(offset==(boundary.equals("AFTER_OFFSET")?1:0));
                long after;try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(bootstrap,group,"none"),topic,store)) {
                    consumer.start();until(20,()->((Number)consumer.status().get("assignedPartitions")).intValue()==3);
                    if(!mode.equals("AUTO_POLL_AHEAD"))until(20,()->((Number)store.totals().get("bucketTotal")).longValue()==1);
                    until(20,()->admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS).get(new TopicPartition(topic,0)).offset()==1);
                    after=((Number)store.totals().get("bucketTotal")).longValue();row.put("successfulBatchesAfterRestart",consumer.status().get("successfulBatches"));
                }
                row.put("databaseAfterRestart",after);row.put("missingAfterRestart",1-after);row.put("intentionalUnsafeOrdering",mode.equals("AUTO_POLL_AHEAD"));row.put("passed",true);require(after==(mode.equals("AUTO_POLL_AHEAD")?0:1));checkpoint();
            }
        }
        void ackCases() throws Exception {
            result.put("phase","ACK_LEVELS");var rows=new ArrayList<Map<String,Object>>();result.put("ackCases",rows);var topics=new LinkedHashMap<String,String>();
            for(String level:List.of("0","1","all")) {
                String topic="ack.level."+level+"."+id;topics.put(level,topic);
                admin.createTopics(List.of(new NewTopic(topic,Map.of(0,List.of(1,2,3),1,List.of(2,3,1),2,List.of(3,1,2))).configs(KafkaTopicPolicy.ha().configurations()))).all().get(8,TimeUnit.SECONDS);
                KafkaSupport.ensureTopic(cluster.bootstrap(),topic,KafkaTopicPolicy.ha());
                var row=new LinkedHashMap<String,Object>();row.put("acks",level);row.put("topic",topic);row.put("idempotence",false);row.put("retries",0);rows.add(row);
                try(var producer=producer(cluster.bootstrap(),level)){row.put("healthy",send(producer,topic,0,20,level));}
                until(15,()->read(topic).size()==20);row.put("healthyBrokerRecords",20);require(description(topic).partitions().getFirst().leader().id()==1);
            }
            cluster.stopBroker(2);cluster.stopBroker(3);
            for(var row:rows){String topic=(String)row.get("topic");until(30,()->description(topic).partitions().getFirst().isr().size()==1);row.put("beforeFaultPartitions",partitions(topic));
                try(var producer=producer(cluster.bootstrap(),(String)row.get("acks"))){row.put("isrOne",send(producer,topic,20,5,(String)row.get("acks")));}checkpoint();}
            cluster.stopBroker(1);cluster.startBroker(2);cluster.startBroker(3);
            for(String topic:topics.values())until(40,()->{var p=description(topic).partitions().getFirst();return p.leader()!=null&&p.leader().id()>0&&p.leader().id()!=1&&p.isr().size()>=2;});
            cluster.startBroker(1);for(String topic:topics.values())until(40,()->description(topic).partitions().stream().allMatch(p->p.isr().size()==3));
            for(var row:rows) {
                String topic=(String)row.get("topic"),level=(String)row.get("acks");Set<UUID> retained=read(topic);var healthy=new HashSet<UUID>();for(int i=0;i<20;i++)healthy.add(event(i).eventId());require(retained.containsAll(healthy));
                var store=store("acks-"+level);try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(cluster.bootstrap(),"ack-read-"+id+"-"+level,"earliest"),topic,store)){consumer.start();until(20,()->((Number)store.totals().get("bucketTotal")).longValue()==retained.size());}
                var database=new HashSet<>(jdbc.query("SELECT event_id FROM processed_event WHERE projection=?",(rs,n)->rs.getObject(1,UUID.class),"acks-"+level));require(database.equals(retained));
                long faultRetained=retained.stream().filter(e->!healthy.contains(e)).count();row.put("faultRecordsRetained",faultRetained);row.put("healthyRecordsLost",0);row.put("brokerUnique",retained.size());row.put("databaseUnique",database.size());row.put("brokerDatabaseDifferences",0);row.put("afterRecoveryPartitions",partitions(topic));
                @SuppressWarnings("unchecked") var fault=(Map<String,Object>)row.get("isrOne");long successes=((Number)fault.get("apiSuccess")).longValue();
                row.put("apiSuccessButAbsentLowerBound",Math.max(0,successes-faultRetained));if(level.equals("all"))require(successes==0);row.put("passed",true);checkpoint();
            }
        }
        ProjectionStore store(String name){var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return new ProjectionStore(jdbc,tx,CODEC,name,"comparison");}
        Map<String,Object> send(KafkaProducer<String,byte[]> producer,String topic,int start,int count,String level) {
            long success=0,failed=0;var offsets=new ArrayList<Long>();long begin=System.nanoTime();
            for(int i=start;i<start+count;i++)try{var metadata=producer.send(record(topic,i)).get(8,TimeUnit.SECONDS);success++;offsets.add(metadata.offset());}catch(Exception failure){failed++;}
            return Map.of("attempted",count,"apiSuccess",success,"apiFailureOrTimeout",failed,"brokerAcknowledged",level.equals("0")?0:success,"brokerConfirmationMeaning",level.equals("0")?"NONE":"BROKER_RESPONSE_NOT_REPLICA_LOSS_IMMUNITY","returnedOffsets",offsets,"seconds",(System.nanoTime()-begin)/1e9);
        }
        TopicDescription description(String topic) throws Exception{return admin.describeTopics(List.of(topic)).allTopicNames().get(8,TimeUnit.SECONDS).get(topic);}
        List<Map<String,Object>> partitions(String topic) throws Exception{return description(topic).partitions().stream().map(p->Map.<String,Object>of("partition",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),"isr",p.isr().stream().map(n->n.id()).toList(),"replicas",p.replicas().stream().map(n->n.id()).toList())).toList();}
        Set<UUID> read(String topic) throws Exception {
            var ids=new HashSet<UUID>();try(var source=new ReplaySource(cluster.bootstrap(),topic,UUID.randomUUID())) {
                var manifest=source.capture(null,null);source.assign(manifest);var positions=new HashMap<Integer,Long>();manifest.partitions().forEach(p->positions.put(p.partition(),p.start()));long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while(manifest.partitions().stream().anyMatch(p->positions.get(p.partition())<p.end())){require(System.nanoTime()<deadline);for(var r:source.poll()){ids.add(CODEC.decode(r.value(),r.key()).eventId());positions.put(r.partition(),r.offset()+1);}}
            }return ids;
        }
        void checkpoint() throws Exception{Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));}
        @Override public synchronized void close() {
            if(closed)return;closed=true;boolean ok=true;
            for(var child:children)if(child.isAlive()){child.destroyForcibly();try{ok&=child.waitFor(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();ok=false;}}
            if(admin!=null)admin.close(Duration.ofSeconds(1));if(ds!=null)ds.close();
            if(created)try(var root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false)){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");}catch(Exception failure){ok=false;}
            cluster.close();result.put("cleanupComplete",ok&&cluster.cleaned());try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){}
        }
    }
    private static KafkaProducer<String,byte[]> producer(String bootstrap,String acks) {
        var settings=new HashMap<String,Object>();settings.put("bootstrap.servers",bootstrap);settings.put("acks",acks);settings.put("enable.idempotence",false);settings.put("retries",0);settings.put("max.in.flight.requests.per.connection",1);settings.put("delivery.timeout.ms",5000);settings.put("request.timeout.ms",2000);settings.put("max.block.ms",2000);settings.put("linger.ms",0);settings.put("buffer.memory",1048576);
        return new KafkaProducer<>(settings,new org.apache.kafka.common.serialization.StringSerializer(),new org.apache.kafka.common.serialization.ByteArraySerializer());
    }
    private static ChatEvent event(int index){Instant at=INPUT_BASE.plusMillis(index);return new ChatEvent(1,UUID.nameUUIDFromBytes(("20261026/"+index).getBytes(StandardCharsets.UTF_8)),"CHZZK_UNOFFICIAL","a".repeat(32),"synthetic",UUID.nameUUIDFromBytes("20261026/session".getBytes(StandardCharsets.UTF_8)),null,"CHAT",at,at,false,"ack-experiment");}
    private static ProducerRecord<String,byte[]> record(String topic,int index){var event=event(index);return new ProducerRecord<>(topic,0,event.receivedAt().toEpochMilli(),event.channelId(),CODEC.encode(event));}
    @FunctionalInterface private interface Condition {boolean test() throws Exception;}
    private static void until(int seconds,Condition condition) throws Exception {long deadline=System.nanoTime()+Duration.ofSeconds(seconds).toNanos();while(System.nanoTime()<deadline){try{if(condition.test())return;}catch(ExecutionException|TimeoutException pending){}Thread.sleep(200);}throw new IllegalStateException("ACK_CONDITION_TIMEOUT");}
    private static void require(boolean condition){if(!condition)throw new IllegalStateException("ACK_ASSERTION");}
}
