package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.*;
import tools.jackson.databind.json.JsonMapper;

/** 공유 broker를 중지하지 않고 자신이 생성한 일회용 broker에만 장애를 주입한다. */
final class PublisherFaultExperiment {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    static Map<String,Object> run(Path output) throws Exception {
        output=output.toAbsolutePath();Files.createDirectories(output.getParent());
        if(Files.exists(output)||Files.exists(output.resolveSibling(output.getFileName()+".html")))throw new IllegalArgumentException("EXPERIMENT_OUTPUT_EXISTS");
        ExperimentResources.preflight(output.getParent());Files.createFile(output);
        String name="exp-kafka-"+UUID.randomUUID().toString().replace("-","");int port;
        try(var socket=new java.net.ServerSocket(0)){port=socket.getLocalPort();}
        var result=new LinkedHashMap<String,Object>();result.put("profile","broker-fault");result.put("startedAt",Instant.now().toString());
        result.put("codeCommit",command("git","rev-parse","HEAD").strip());result.put("environment",Map.of("image","apache/kafka:4.1.2","replicationFactor",1,"acks","all","minInSyncReplicas",1,"applicationCapacity",30,"containerMemoryBytes",536870912,"containerName",name,"bootstrap","localhost:"+port));
        result.put("scope","SYNTHETIC_OWNED_BROKER_NO_DATABASE_OR_HA_CLAIM");
        var rows=new ArrayList<Map<String,Object>>();result.put("cases",rows);
        var cleanup=new Thread(()->{try{command("docker","rm","-f",name);}catch(Exception ignored){}},"fault-broker-cleanup");Runtime.getRuntime().addShutdownHook(cleanup);
        try {
            var args=new ArrayList<>(List.of("docker","run","-d","--name",name,"--memory","512m","--label","io.chatpulse.experiment=publisher-fault","-p","127.0.0.1:"+port+":"+port));
            var env=new LinkedHashMap<String,String>();
            env.put("KAFKA_NODE_ID","1");env.put("KAFKA_PROCESS_ROLES","broker,controller");
            env.put("KAFKA_LISTENERS","INTERNAL://:9092,EXTERNAL://:"+port+",CONTROLLER://:9093");
            env.put("KAFKA_ADVERTISED_LISTENERS","INTERNAL://localhost:9092,EXTERNAL://localhost:"+port);
            env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP","INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");env.put("KAFKA_INTER_BROKER_LISTENER_NAME","INTERNAL");
            env.put("KAFKA_CONTROLLER_LISTENER_NAMES","CONTROLLER");env.put("KAFKA_CONTROLLER_QUORUM_VOTERS","1@localhost:9093");
            env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","1");env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR","1");env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR","1");
            env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE","false");env.put("KAFKA_HEAP_OPTS","-Xms128m -Xmx256m");env.put("CLUSTER_ID","MkU3OEVBNTcwNTJENDM2Qk");
            env.forEach((key,value)->{args.add("-e");args.add(key+"="+value);});args.add("apache/kafka:4.1.2");ExperimentResources.command(args);
            String bootstrap="localhost:"+port;ready(bootstrap);
            result.put("imageId",command("docker","inspect","--format","{{.Image}}",name).strip());
            for(boolean serial:List.of(true,false))for(boolean disconnect:List.of(false,true)) {
                String topic="exp.fault."+UUID.randomUUID().toString().replace("-","");KafkaSupport.ensureTopic(bootstrap,topic);
                var row=new LinkedHashMap<String,Object>();rows.add(row);scenario(bootstrap,name,topic,serial,disconnect,row);
            }
            result.put("correct",rows.stream().allMatch(row->Boolean.TRUE.equals(row.get("conserved"))&&Boolean.TRUE.equals(row.get("bounded"))&&Boolean.TRUE.equals(row.get("brokerCountWithinAckRange"))));
        } catch(Exception failure){result.put("correct",false);result.put("failureCode","EXPERIMENT_BROKER_FAULT_FAILED");result.put("failureType",failure.getClass().getSimpleName());if(failure.getCause()!=null)result.put("causeType",failure.getCause().getClass().getSimpleName());}
        finally {
            boolean cleaned=false;try{command("docker","rm","-f",name);cleaned=true;}catch(Exception ignored){}
            Runtime.getRuntime().removeShutdownHook(cleanup);result.put("cleanupComplete",cleaned);if(!cleaned)result.put("correct",false);
            result.put("finishedAt",Instant.now().toString());result.put("outcome",Boolean.TRUE.equals(result.get("correct"))?"RECONCILED":"EXPERIMENT_FAILED");
            Files.writeString(output,JSON.writeValueAsString(result)+"\n");LoadExperiment.writeHtml(output,result);
        }
        return result;
    }
    private static void scenario(String bootstrap,String container,String topic,boolean serialMode,boolean disconnect,Map<String,Object> result) throws Exception {
        var factory=KafkaSupport.producerFactory(bootstrap);var kafka=new KafkaTemplate<>(factory);var codec=new EventCodec();
        var serial=serialMode?new BufferedKafkaPublisher(kafka,topic,codec,30):null;var async=serialMode?null:new ExperimentPublisher(kafka,topic,30,false);
        result.put("publisher",serialMode?"SERIAL":"BOUNDED_ASYNC");result.put("fault",disconnect?"BROKER_STOP":"BROKER_PAUSE");
        boolean paused=false,stopped=false;long start=System.nanoTime();
        try {
            result.put("phase","WARMUP");var warm=events(1,0);accept(serial,async,warm);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while(pending(serial,async)>0&&System.nanoTime()<deadline)Thread.sleep(10);
            if(pending(serial,async)!=0)throw new IllegalStateException("EXPERIMENT_WARMUP");
            result.put("phase","INJECT_FAULT");if(disconnect){command("docker","stop","--time","0",container);stopped=true;}else{command("docker","pause",container);paused=true;}
            result.put("phase","FILL_BUFFER");accept(serial,async,events(20,0));accept(serial,async,events(10,1));
            result.put("pendingAtCapacity",pending(serial,async));
            boolean rejected=false;try{accept(serial,async,events(1,1));}catch(RuntimeException expected){rejected=true;}
            result.put("overflowFrameRejected",rejected);result.put("channelAdmission",List.of(Map.of("channel",0,"accepted",21,"rejected",0),Map.of("channel",1,"accepted",10,"rejected",rejected?1:0)));
            Thread.sleep(1500);result.put("oldestUnackedMillis",serialMode?oldest(serial):async.oldestAgeMillis());
            result.put("clientHeapUsedBytes",Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
            result.put("clientRssBytes",Long.parseLong(command("ps","-o","rss=","-p",Long.toString(ProcessHandle.current().pid())).strip())*1024);
            if(paused){command("docker","unpause",container);paused=false;}
            result.put("phase","CLOSE_PUBLISHER");if(serial!=null)serial.finish();else async.close();factory.destroy();
            var stats=serialMode?serial.finish():async.stats();result.put("producer",stats);
            long accepted=((Number)stats.get("accepted")).longValue(),acked=((Number)stats.get("acknowledged")).longValue(),unconfirmed=((Number)stats.get("unconfirmed")).longValue(),declined=((Number)stats.get("rejected")).longValue();
            result.put("conserved",accepted+declined==32&&accepted==acked+unconfirmed);
            result.put("bounded",((Number)result.get("pendingAtCapacity")).intValue()==30&&rejected);
            result.put("phase","RESTORE_BROKER");if(stopped){command("docker","start",container);stopped=false;ready(bootstrap);}
            result.put("phase","MEASURE_BROKER");try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap))) {
                var request=new HashMap<TopicPartition,OffsetSpec>();for(int p=0;p<3;p++)request.put(new TopicPartition(topic,p),OffsetSpec.latest());
                long broker=admin.listOffsets(request).all().get(10,TimeUnit.SECONDS).values().stream().mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset).sum();
                result.put("brokerOffsetSpan",broker);result.put("brokerCountWithinAckRange",broker>=acked&&broker<=accepted);
            }
            result.put("seconds",(System.nanoTime()-start)/1_000_000_000d);result.put("phase","COMPLETED");
        } finally {
            if(paused)command("docker","unpause",container);
            if(serial!=null)serial.close();if(async!=null)async.close();factory.destroy();
            if(stopped){command("docker","start",container);ready(bootstrap);}
        }
    }
    private static List<ChatEvent> events(int count,int channel){var events=new ArrayList<ChatEvent>();Instant at=Instant.now();for(int i=0;i<count;i++)events.add(new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",String.format("%032x",channel),"synthetic-room",UUID.randomUUID(),null,"CHAT",at,at,false,"experiment-fault"));return events;}
    private static void accept(BufferedKafkaPublisher serial,ExperimentPublisher async,List<ChatEvent> events){if(serial!=null)serial.accept(events);else async.accept(events,0);}
    private static int pending(BufferedKafkaPublisher serial,ExperimentPublisher async){return serial!=null?serial.bufferSnapshot().values().stream().mapToInt(BufferedKafkaPublisher.BufferState::count).sum():async.pending();}
    private static long oldest(BufferedKafkaPublisher serial){return serial.bufferSnapshot().values().stream().map(BufferedKafkaPublisher.BufferState::oldest).min(Comparator.naturalOrder()).map(at->Math.max(0,Duration.between(at,Instant.now()).toMillis())).orElse(0L);}
    private static String command(String...args) throws Exception{return ExperimentResources.command(List.of(args));}
    private static void ready(String bootstrap) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);
        try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"request.timeout.ms",1000,"default.api.timeout.ms",2000))) {
            while(true){try{if(admin.describeCluster().nodes().get(2,TimeUnit.SECONDS).size()==1)return;}catch(Exception ignored){}
                if(System.nanoTime()>deadline)throw new IllegalStateException("EXPERIMENT_BROKER_START_TIMEOUT");Thread.sleep(200);}
        }
    }
}
