package io.chatpulse.pipeline;

import io.chatpulse.replay.*;
import java.net.ServerSocket;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;
import tools.jackson.databind.json.JsonMapper;

/** 한 소유 broker에서 시간/크기 삭제와 실제 log 파일을 관측한다. 공개 원천·공유 broker는 사용하지 않는다. */
public final class KafkaRetentionExperiment implements AutoCloseable {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    private static final String LABEL="io.chatpulse.retention.owner",IMAGE="apache/kafka:4.1.2";
    private final String owner=UUID.randomUUID().toString().replace("-",""),name="jigeumini-retention-"+owner,volume=name+"-data";
    private final Map<String,Object> result=new LinkedHashMap<>();
    private final Path output;
    private String bootstrap,phase="OPEN";
    private boolean volumeCreated,containerCreated,closed,cleaned;
    private Admin admin;
    private KafkaRetentionExperiment(Path output){this.output=output;}
    public static void main(String[] args) throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("RETENTION_OUTPUT_REQUIRED");LoadExperiment.quiet();
        Path output=Path.of(args[0]).toAbsolutePath();Files.createDirectories(output.getParent());Files.createFile(output);
        var experiment=new KafkaRetentionExperiment(output);var hook=new Thread(experiment::close,"retention-owned-cleanup");
        Runtime.getRuntime().addShutdownHook(hook);
        experiment.result.put("startedAt",Instant.now().toString());experiment.result.put("scope","SINGLE_OWNED_BROKER_ACCELERATED_RETENTION_CHECK_NO_HA");
        experiment.result.put("limitations",List.of("BROKER_RETENTION_CHECK_1S_NOT_PRODUCTION_5M","FILE_DELETE_DELAY_1S","TIME_CASE_RETENTION_4S_NOT_24H","BYTES_CASE_PRODUCTION_256MIB_AND_32MIB_SEGMENTS_ACCELERATED_INPUT","LOG_FILE_BYTES_EXCLUDE_INDEX_AND_METADATA","NO_REAL_WORLD_24H_OR_PITR_RPO_GUARANTEE"));
        try{experiment.open();experiment.scenario(false);experiment.scenario(true);experiment.result.put("correct",true);}
        catch(Exception failure){experiment.result.put("correct",false);experiment.result.put("failedPhase",experiment.phase);experiment.result.put("failureType",failure.getClass().getSimpleName());
            if(failure.getCause()!=null)experiment.result.put("causeType",failure.getCause().getClass().getSimpleName());
            if(failure.getMessage()!=null&&failure.getMessage().matches("(RETENTION|HA)_[A-Z_]+"))experiment.result.put("failureCode",failure.getMessage());}
        finally{experiment.close();Runtime.getRuntime().removeShutdownHook(hook);experiment.result.put("cleanupComplete",experiment.cleaned);if(!experiment.cleaned)experiment.result.put("correct",false);experiment.result.put("finishedAt",Instant.now().toString());experiment.save();}
        System.out.println(JSON.writeValueAsString(Map.of("correct",experiment.result.get("correct"),"cleanupComplete",experiment.cleaned)));
        if(!Boolean.TRUE.equals(experiment.result.get("correct")))System.exit(2);
    }
    private void open() throws Exception {
        result.put("revision",command("git","rev-parse","HEAD").strip());result.put("workingTreeDirty",!command("git","status","--porcelain").isBlank());
        result.put("owner",owner);result.put("imageId",command("docker","image","inspect",IMAGE,"--format","{{.Id}}").strip());
        result.put("kafkaClient",org.apache.kafka.common.utils.AppInfoParser.getVersion());result.put("java",System.getProperty("java.version"));
        if(Files.getFileStore(output.getParent()).getUsableSpace()<2L*1024*1024*1024)throw new IllegalStateException("RETENTION_DISK_HEADROOM");
        int port;try(var socket=new ServerSocket(0)){port=socket.getLocalPort();}bootstrap="localhost:"+port;
        command("docker","volume","create","--label",LABEL+"="+owner,volume);volumeCreated=true;
        var args=new ArrayList<>(List.of("docker","create","--name",name,"--label",LABEL+"="+owner,"--cpus","1","--memory","768m","--publish","127.0.0.1:"+port+":9092","--mount","type=volume,source="+volume+",target=/var/lib/kafka/data"));
        var env=Map.ofEntries(Map.entry("KAFKA_NODE_ID","1"),Map.entry("KAFKA_PROCESS_ROLES","broker,controller"),
                Map.entry("KAFKA_LISTENERS","EXTERNAL://:9092,INTERNAL://:9094,CONTROLLER://:9093"),
                Map.entry("KAFKA_ADVERTISED_LISTENERS","EXTERNAL://localhost:"+port+",INTERNAL://localhost:9094"),
                Map.entry("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP","EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME","INTERNAL"),Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES","CONTROLLER"),
                Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS","1@localhost:9093"),Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","1"),
                Map.entry("KAFKA_AUTO_CREATE_TOPICS_ENABLE","false"),Map.entry("KAFKA_LOG_DIRS","/var/lib/kafka/data"),
                Map.entry("KAFKA_HEAP_OPTS","-Xms256m -Xmx512m"),Map.entry("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS","1000"),Map.entry("CLUSTER_ID",Uuid.randomUuid().toString()));
        env.forEach((k,v)->args.addAll(List.of("--env",k+"="+v)));args.add(IMAGE);command(args.toArray(String[]::new));containerCreated=true;command("docker","start",name);
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"default.api.timeout.ms",3000,"request.timeout.ms",2000));
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);boolean ready=false;
        while(System.nanoTime()<end&&!ready){try{ready=admin.describeCluster().nodes().get(3,TimeUnit.SECONDS).size()==1;}catch(ExecutionException|TimeoutException pending){Thread.sleep(100);}}
        require(ready,"RETENTION_BROKER_NOT_READY");
        result.put("container",Map.of("cpus",1,"memoryBytes",768L*1024*1024,"replicationFactor",1));
        var resource=new ConfigResource(ConfigResource.Type.BROKER,"1");
        result.put("retentionCheckMillis",admin.describeConfigs(List.of(resource)).all().get(5,TimeUnit.SECONDS).get(resource).get("log.retention.check.interval.ms").value());
        // 첫 하네스의 16KiB 설정 실패를 broker 오류 형으로 재확인한다. 4.1의 최소값은 1MiB다.
        boolean invalidRejected=false;
        try{admin.createTopics(List.of(new NewTopic("retention.invalid."+owner,1,(short)1).configs(Map.of("segment.bytes","16384")))).all().get(5,TimeUnit.SECONDS);}
        catch(ExecutionException failure){if(failure.getCause() instanceof org.apache.kafka.common.errors.InvalidConfigurationException){invalidRejected=true;result.put("minimumSegmentRejection",failure.getCause().getClass().getSimpleName());}else throw failure;}
        require(invalidRejected,"RETENTION_INVALID_CONFIG_ACCEPTED");save();
    }
    private void scenario(boolean bytes) throws Exception {
        phase=bytes?"BYTES_FIRST":"TIME_FIRST";String topic="retention."+(bytes?"bytes.":"time.")+owner;
        var config=new LinkedHashMap<String,String>();config.put("cleanup.policy","delete");config.put("message.timestamp.type","CreateTime");
        config.put("retention.ms",bytes?"86400000":"4000");config.put("retention.bytes",bytes?"268435456":"-1");
        config.put("segment.bytes",bytes?"33554432":"1048576");config.put("segment.ms",bytes?"604800000":"1000");config.put("file.delete.delay.ms","1000");
        admin.createTopics(List.of(new NewTopic(topic,3,(short)1).configs(config))).all().get(10,TimeUnit.SECONDS);
        var row=new LinkedHashMap<String,Object>();result.put(bytes?"bytesFirst":"timeFirst",row);row.put("startedAt",Instant.now().toString());
        var effective=KafkaSupport.awaitTopic(admin,topic,(description,settings)->{
            require(description.partitions().size()==3,"RETENTION_PARTITION_MISMATCH");
            for(var p:description.partitions())if(p.leader()==null||p.leader().id()<0||p.isr().size()!=1)throw new KafkaTopicPolicy.NotReady();
            for(var entry:config.entrySet())require(settings.get(entry.getKey())!=null&&settings.get(entry.getKey()).value().equals(entry.getValue()),"RETENTION_CONFIG_MISMATCH");
        },Duration.ofSeconds(10),System::nanoTime,Thread::sleep);
        var actual=new LinkedHashMap<String,String>();for(String key:config.keySet())actual.put(key,effective.get(key).value());require(actual.equals(config),"RETENTION_CONFIG_MISMATCH");row.put("effectiveConfig",actual);
        var producerConfig=Map.<String,Object>of("bootstrap.servers",bootstrap,"acks","all","enable.idempotence",true,"compression.type","none","linger.ms",5,"batch.size",65536,"delivery.timeout.ms",10000,"request.timeout.ms",5000);
        try(var producer=new KafkaProducer<String,byte[]>(producerConfig,new StringSerializer(),new ByteArraySerializer());var source=new ReplaySource(bootstrap,topic,UUID.randomUUID())) {
            byte[] payload=new byte[700];new Random(63).nextBytes(payload);
            var acknowledged=new java.util.concurrent.atomic.AtomicLongArray(3);var failures=new java.util.concurrent.atomic.AtomicLong();
            Callback receipt=(metadata,error)->{if(error==null)acknowledged.incrementAndGet(metadata.partition());else failures.incrementAndGet();};
            for(int p=0;p<3;p++)for(int i=0;i<10;i++)producer.send(new ProducerRecord<>(topic,p,System.currentTimeMillis(),"a".repeat(32),payload),receipt);producer.flush();
            require(failures.get()==0&&acknowledged.get(0)+acknowledged.get(1)+acknowledged.get(2)==30,"RETENTION_INITIAL_ACK_MISMATCH");
            var manifest=source.capture(null,null);row.put("captured",manifest.partitions());row.put("initial",snapshot(topic));
            int target=bytes?500000:90;long began=System.nanoTime(),firstTimestamp=System.currentTimeMillis();
            for(int i=0;i<target;i++) {
                int p=bytes?(i%10<8?0:i%10-7):i%3;
                producer.send(new ProducerRecord<>(topic,p,System.currentTimeMillis(),"a".repeat(32),payload),receipt);
                if(i%1000==999)producer.flush();
            }
            producer.flush();row.put("input",Map.of("additionalRecords",target,"initialRecords",30,"payloadBytes",700,"keyBytes",32,"compression","none","durationMillis",(System.nanoTime()-began)/1e6,"firstTimestamp",Instant.ofEpochMilli(firstTimestamp).toString()));
            row.put("acknowledged",List.of(acknowledged.get(0),acknowledged.get(1),acknowledged.get(2)));row.put("deliveryFailures",failures.get());
            require(failures.get()==0&&acknowledged.get(0)+acknowledged.get(1)+acknowledged.get(2)==target+30L,"RETENTION_ACK_MISMATCH");
            row.put("observationBudgetSeconds",45);row.put("afterInput",snapshot(topic));var samples=new ArrayList<Map<String,Object>>();row.put("samples",samples);save();
            boolean expired=false,filesSettled=false;int stable=0;List<?> previousBounds=List.of();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
            while(System.nanoTime()<deadline) {
                var sample=snapshot(topic);samples.add(sample);var bounds=(List<?>)sample.get("bounds");
                long earliest=((Number)((Map<?,?>)bounds.getFirst()).get("earliest")).longValue();
                expired=earliest>0;long deletedBytes=((Number)sample.get("deletedLogBytes")).longValue();
                boolean allTimeExpired=bytes||bounds.stream().allMatch(b->((Map<?,?>)b).get("earliest").equals(((Map<?,?>)b).get("end")));
                if(expired&&deletedBytes==0&&allTimeExpired&&Boolean.TRUE.equals(sample.get("filesMatchEarliest")))stable=bounds.equals(previousBounds)?stable+1:1;else stable=0;
                previousBounds=bounds;if(stable>=3){filesSettled=true;break;}Thread.sleep(500);
            }
            require(expired&&filesSettled,"RETENTION_DELETION_UNOBSERVED");
            String rejection="ACCEPTED";try{source.validate(manifest);}catch(ReplayFailure failure){rejection=failure.getMessage();}
            require(rejection.equals("RETENTION_EXPIRED"),"RETENTION_MANIFEST_NOT_REJECTED");row.put("oldManifestCode",rejection);row.put("retention",source.retention(manifest));row.put("final",snapshot(topic));
            row.put("retainedBounds",source.retention(source.capture(null,null)));
            if(bytes){var bounds=(List<?>)((Map<?,?>)row.get("final")).get("bounds");require(((Number)((Map<?,?>)bounds.get(1)).get("earliest")).longValue()==0&&((Number)((Map<?,?>)bounds.get(2)).get("earliest")).longValue()==0,"RETENTION_COLD_PARTITION_EXPIRED");}
            row.put("finishedAt",Instant.now().toString());row.put("correct",true);save();
        }
    }
    private Map<String,Object> snapshot(String topic) throws Exception {
        var starts=new LinkedHashMap<TopicPartition,OffsetSpec>();var ends=new LinkedHashMap<TopicPartition,OffsetSpec>();
        for(int p=0;p<3;p++){starts.put(new TopicPartition(topic,p),OffsetSpec.earliest());ends.put(new TopicPartition(topic,p),OffsetSpec.latest());}
        var earliest=admin.listOffsets(starts).all().get(5,TimeUnit.SECONDS);var latest=admin.listOffsets(ends).all().get(5,TimeUnit.SECONDS);
        var bounds=new ArrayList<Map<String,Object>>();for(var tp:starts.keySet())bounds.add(Map.of("partition",tp.partition(),"earliest",earliest.get(tp).offset(),"end",latest.get(tp).offset()));
        String listing=command("docker","exec",name,"find","/var/lib/kafka/data","-maxdepth","2","-type","f","-exec","stat","-c","%n %s","{}","+");
        var files=new ArrayList<Map<String,Object>>();long logBytes=0,deletedBytes=0;
        for(String line:listing.lines().toList()) {
            String[] parts=line.split(" ");String prefix="/var/lib/kafka/data/"+topic+"-";if(parts.length!=2||!parts[0].startsWith(prefix))continue;
            String file=parts[0].substring(prefix.length());long size=Long.parseLong(parts[1]);
            if(file.endsWith(".log"))logBytes+=size;else if(file.endsWith(".log.deleted"))deletedBytes+=size;
            files.add(Map.of("partitionFile",file,"bytes",size));
        }
        boolean filesMatch=true;
        for(var bound:bounds)for(var file:files) {
            String path=(String)file.get("partitionFile");
            if(path.startsWith(bound.get("partition")+"/")&&path.endsWith(".log")) {
                long base=Long.parseLong(path.substring(path.indexOf('/')+1,path.length()-4));
                if(base<((Number)bound.get("earliest")).longValue())filesMatch=false;
            }
        }
        return Map.of("at",Instant.now().toString(),"bounds",bounds,"logBytes",logBytes,"deletedLogBytes",deletedBytes,"files",files,"filesMatchEarliest",filesMatch);
    }
    private void save() throws Exception {Files.writeString(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result)+"\n");}
    private static String command(String... args) throws Exception {return LocalKafkaCluster.command(List.of(args),Map.of(),Duration.ofSeconds(30));}
    private static void require(boolean condition,String code){if(!condition)throw new IllegalStateException(code);}
    @Override public synchronized void close() {
        if(closed)return;closed=true;boolean okay=true;
        if(admin!=null)admin.close(Duration.ofSeconds(2));
        try {
            if(containerCreated){require(command("docker","inspect",name,"--format","{{index .Config.Labels \""+LABEL+"\"}}").strip().equals(owner),"RETENTION_CONTAINER_OWNER");command("docker","rm","--force",name);}
            if(volumeCreated){require(command("docker","volume","inspect",volume,"--format","{{index .Labels \""+LABEL+"\"}}").strip().equals(owner),"RETENTION_VOLUME_OWNER");command("docker","volume","rm",volume);}
        }catch(Exception failure){okay=false;}cleaned=okay;
    }
}
