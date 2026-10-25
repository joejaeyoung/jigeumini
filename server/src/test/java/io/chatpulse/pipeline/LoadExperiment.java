package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.*;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 별도 topic/schema에서만 실행하는 유한 합성 부하 실험. 결과는 목표 달성과 구분한다. */
public final class LoadExperiment {
    static final String BOOTSTRAP="localhost:19092",JDBC="jdbc:postgresql://localhost:15432/chatpulse",USER="chatpulse",PASSWORD="local-chatpulse";
    static final long SEED=20261025;
    private static final JsonMapper JSON=JsonMapper.builder().build();
    record Plan(String name,int rate,int seconds,int consumers,boolean hot,boolean sharded,boolean serial,boolean independent,int pauseSeconds,String fault,boolean membership,boolean pollStall,boolean duplicates,boolean twoProducers) {
        Plan { if(rate<1||rate>2000||seconds<1||seconds>600||rate*(long)seconds>400000||consumers<1||consumers>4||pauseSeconds<0||pauseSeconds>60||!Set.of("none","delay","failure").contains(fault))throw new IllegalArgumentException("EXPERIMENT_PLAN"); }
    }
    static Plan plan(String name) {
        if(name.matches("(uniform|hot)-[1-4]"))return new Plan(name,2000,15,Integer.parseInt(name.substring(name.length()-1)),name.startsWith("hot"),false,false,false,0,"none",false,false,false,false);
        return switch(name) {
            case "smoke" -> new Plan(name,100,2,1,false,false,false,true,0,"none",false,false,true,false);
            case "baseline" -> new Plan(name,500,30,1,false,false,true,false,0,"none",false,false,false,false);
            case "steady" -> new Plan(name,500,600,3,false,false,false,true,0,"none",false,false,false,false);
            case "burst" -> new Plan(name,2000,60,3,false,false,false,false,0,"none",false,false,false,false);
            case "recovery" -> new Plan(name,500,100,1,false,false,false,true,60,"none",false,false,false,false);
            case "db-delay","db-failure" -> new Plan(name,500,30,1,false,false,false,true,0,name.equals("db-delay")?"delay":"failure",false,false,false,false);
            case "membership" -> new Plan(name,500,40,1,false,false,false,true,0,"none",true,false,true,true);
            case "poll-stall" -> new Plan(name,500,30,2,false,false,false,true,0,"none",false,true,true,false);
            case "sharded-3" -> new Plan(name,2000,15,3,true,true,false,false,0,"none",false,false,true,true);
            case "hot-duplicates" -> new Plan(name,2000,15,3,true,false,false,false,0,"none",false,false,true,true);
            default -> throw new IllegalArgumentException("EXPERIMENT_PROFILE");
        };
    }
    public static void main(String[] args) {
        quiet();
        try {
            if(args.length<2||args.length>3)throw new IllegalArgumentException("EXPERIMENT_ARGUMENTS");
            var result=run(plan(args[0]),Path.of(args[1]),args.length==3?Instant.parse(args[2]):null,true);
            System.out.println(JSON.writeValueAsString(Map.of("profile",args[0],"outcome",result.get("outcome"),"report",args[1])));
            if(!Boolean.TRUE.equals(result.get("correct")))System.exit(2);
        } catch(Exception failure) {System.out.println("{\"error\":\"EXPERIMENT_FAILED\"}");System.exit(2);}
    }
    static void quiet() { if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF); }
    static Map<String,Object> run(Plan plan,Path output,Instant requestedBase,boolean resources) throws Exception {
        output=output.toAbsolutePath();Files.createDirectories(output.getParent());Files.createFile(output);
        var result=new LinkedHashMap<String,Object>();result.put("formatVersion",1);result.put("profile",plan);result.put("seed",SEED);result.put("startedAt",Instant.now().toString());
        result.put("codeCommit",ExperimentResources.command(List.of("git","rev-parse","HEAD")).strip());
        result.put("runtimeClassesSha256",runtimeHash());
        result.put("targets",Map.of("uniqueEvents",plan.rate()*(long)plan.seconds(),"eventsPerSecond",plan.rate(),"seconds",plan.seconds()));
        var environment=new LinkedHashMap<String,Object>(Map.of("java",System.getProperty("java.version"),"os",System.getProperty("os.name"),"architecture",System.getProperty("os.arch"),"processors",Runtime.getRuntime().availableProcessors(),"maxHeapBytes",Runtime.getRuntime().maxMemory(),"brokers",1,"replicationFactor",1,"kafka","4.1.2","postgresql","16.14"));
        environment.putAll(Map.of("acks","all","kafkaMaxInFlight",1,"poolMaximum",4));result.put("environment",environment);
        result.put("limits",Map.of("minimumFreeBytes",ExperimentResources.MIN_FREE,"maximumExperimentDataBytes",ExperimentResources.MAX_DATA,"maxGeneratorSeconds",600,"maxDrainSeconds",180,"maxReplaySeconds",300));
        var session=new Session(plan);session.progressFile=output.resolveSibling(output.getFileName()+".progress.json");Files.createFile(session.progressFile);
        result.put("resources",Map.of("schema",session.schema,"topic",session.topic,"groups",List.of(session.groupA,session.groupB)));
        Files.writeString(output,JSON.writeValueAsString(result));
        var hook=new Thread(session::close,"experiment-cleanup");Runtime.getRuntime().addShutdownHook(hook);
        try {
            if(resources)ExperimentResources.preflight(output.getParent());session.open();result.put("liveBefore",session.liveBefore);
            session.generate(output.getParent(),requestedBase,resources,result);
            var finalA=session.store("load-a").totals();var finalB=plan.independent()?session.store("load-b").totals():Map.of();
            result.put("databaseA",finalA);result.put("databaseB",finalB);result.put("metricsA",session.metricsA.snapshot());result.put("metricsB",session.metricsB.snapshot());
            result.put("samples",List.copyOf(session.samples));result.put("events",List.copyOf(session.events));
            boolean replay=session.replay(result);boolean expected=session.expectedMatches("load-a");result.put("expectedBucketsMatch",expected);
            long unique=((Number)finalA.get("processedEvents")).longValue(),upper=session.expected.values().stream().mapToLong(Long::longValue).sum(),lower=Math.max(0,session.acknowledged()-(session.accepted()-upper));
            result.put("acknowledgedUniqueLowerBound",lower);result.put("acceptedUniqueUpperBound",upper);
            var codes=session.events.stream().map(e->e.get("code")).toList();boolean scenarioComplete=(plan.pauseSeconds()==0||codes.containsAll(List.of("ANALYZER_STOPPED","ANALYZER_RESTARTED")))
                    &&(plan.fault().equals("none")||codes.containsAll(List.of("DB_FAULT_BEGIN","DB_FAULT_END")))&&(!plan.membership()||codes.containsAll(List.of("CONSUMERS_TWO","CONSUMERS_FOUR","CONSUMER_REMOVED")))
                    &&(!plan.pollStall()||codes.containsAll(List.of("POLL_STALL_BEGIN","POLL_STALL_END")));result.put("scenarioComplete",scenarioComplete);
            boolean correct=scenarioComplete&&replay&&unique>=lower&&unique<=upper&&unique==((Number)finalA.get("bucketTotal")).longValue()
                    &&(!plan.independent()||unique==((Number)finalB.get("processedEvents")).longValue())
                    &&(session.acknowledged()==session.accepted()?expected:true);
            result.put("correct",correct);result.put("outcome",correct?(session.generatedUnique==plan.rate()*(long)plan.seconds()&&!session.producerFailed()?"TARGET_INPUT_COMPLETED":"TARGET_INPUT_NOT_COMPLETED"):"RECONCILIATION_FAILED");
        } catch(Exception failure) {
            result.put("correct",false);result.put("outcome","EXPERIMENT_FAILED");String code=failure.getMessage();
            result.put("failureCode",code!=null&&code.matches("EXPERIMENT_[A-Z_]+")?code:"EXPERIMENT_UNAVAILABLE");
        } finally {
            result.putIfAbsent("samples",List.copyOf(session.samples));result.putIfAbsent("events",List.copyOf(session.events));result.putIfAbsent("metricsA",session.metricsA.snapshot());result.putIfAbsent("metricsB",session.metricsB.snapshot());
            session.close();Runtime.getRuntime().removeShutdownHook(hook);result.put("cleanupComplete",session.cleaned);result.put("finishedAt",Instant.now().toString());
            if(!session.cleaned){result.put("correct",false);result.put("outcome","CLEANUP_FAILED");}
            Files.writeString(output,JSON.writeValueAsString(result)+"\n");writeHtml(output,result);
        }
        return result;
    }
    private static void writeHtml(Path output,Map<String,Object> result) throws Exception {
        String pretty=JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
        String html="<!doctype html><html lang=ko><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>지금이니 합성 부하 실험</title><style>body{max-width:1100px;margin:40px auto;padding:0 20px;font:16px/1.7 system-ui;color:#173c30}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f0f7f4;padding:20px}h1{font-size:28px}summary{cursor:pointer}</style><h1>합성 부하 실험</h1><p>목표 수치와 실제 결과를 구분합니다. 단일 broker·RF1이며 외부 플랫폼에 요청하지 않았습니다.</p><h2>"+result.get("outcome")+"</h2><details open><summary>환경·입력·계수·지연·partition·자원·검증 결과</summary><pre>"+pretty+"</pre></details></html>";
        Files.writeString(output.resolveSibling(output.getFileName()+".html"),html,StandardOpenOption.CREATE_NEW);
    }
    private static String runtimeHash() throws Exception {
        var hash=java.security.MessageDigest.getInstance("SHA-256");Path classes=Path.of(LoadExperiment.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try(var files=Files.walk(classes)) {for(var file:files.filter(Files::isRegularFile).sorted().toList()) {hash.update(classes.relativize(file).toString().getBytes(StandardCharsets.UTF_8));hash.update((byte)0);hash.update(Files.readAllBytes(file));hash.update((byte)0);}}
        return HexFormat.of().formatHex(hash.digest());
    }
    private static final class Session implements AutoCloseable {
        final Plan plan;final String suffix=UUID.randomUUID().toString().replace("-",""),schema="exp_"+suffix,topic="exp.chat."+suffix,groupA="exp.a."+suffix,groupB="exp.b."+suffix;
        final EventCodec codec=new EventCodec();final ExperimentMetrics metricsA=new ExperimentMetrics(),metricsB=new ExperimentMetrics();
        final CopyOnWriteArrayList<ProjectionConsumer> consumersA=new CopyOnWriteArrayList<>(),consumersB=new CopyOnWriteArrayList<>();
        final List<Map<String,Object>> samples=new CopyOnWriteArrayList<>(),events=new CopyOnWriteArrayList<>();
        final Map<String,Long> expected=new HashMap<>();final List<Double> generatorLags=new ArrayList<>();
        final ScheduledExecutorService scheduler=Executors.newScheduledThreadPool(2);
        final AtomicBoolean closed=new AtomicBoolean(),abort=new AtomicBoolean(),stalled=new AtomicBoolean();
        HikariDataSource ds;JdbcTemplate jdbc;Admin admin;DefaultKafkaProducerFactory<String,byte[]> producerFactory,secondFactory;KafkaTemplate<String,byte[]> kafka,secondKafka;
        ExperimentPublisher async,secondAsync;BufferedKafkaPublisher serial;Map<String,Object> serialStats=Map.of(),liveBefore=Map.of();
        boolean schemaCreated,topicCreated,cleaned;long started,generatedUnique,attemptedRecords;Instant eventBase;int serialHighWater;double serialOldest;Path progressFile;
        Session(Plan plan){this.plan=plan;}
        void open() throws Exception {
            try(var root=PostgresSupport.open(JDBC,USER,PASSWORD,false)) {var rootJdbc=new JdbcTemplate(root);liveBefore=live(rootJdbc);rootJdbc.execute("CREATE SCHEMA "+schema);schemaCreated=true;}
            ds=PostgresSupport.openMigrated(JDBC+"?currentSchema="+schema,USER,PASSWORD);jdbc=new JdbcTemplate(ds);
            jdbc.execute("CREATE TABLE experiment_fault(enabled boolean NOT NULL,delay_seconds double precision NOT NULL)");jdbc.update("INSERT INTO experiment_fault VALUES(false,0)");
            jdbc.execute("CREATE FUNCTION experiment_fault() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE fault experiment_fault; BEGIN IF NEW.projection='load-a' THEN SELECT * INTO fault FROM experiment_fault; IF fault.enabled THEN RAISE EXCEPTION 'EXPERIMENT_DB_FAILURE'; END IF; IF fault.delay_seconds>0 THEN PERFORM pg_sleep(fault.delay_seconds); END IF; END IF; RETURN NEW; END $$");
            jdbc.execute("CREATE TRIGGER experiment_fault BEFORE INSERT ON projection_progress FOR EACH ROW EXECUTE FUNCTION experiment_fault()");
            admin=Admin.create(Map.of("bootstrap.servers",BOOTSTRAP,"default.api.timeout.ms",10000,"request.timeout.ms",5000));topicCreated=true;KafkaSupport.ensureTopic(BOOTSTRAP,topic);
            producerFactory=KafkaSupport.producerFactory(BOOTSTRAP);kafka=new KafkaTemplate<>(producerFactory);
            if(plan.serial())serial=new BufferedKafkaPublisher(kafka,topic,codec,1000);else async=new ExperimentPublisher(kafka,topic,1000,plan.sharded());
            if(plan.twoProducers()) {secondFactory=KafkaSupport.producerFactory(BOOTSTRAP);secondKafka=new KafkaTemplate<>(secondFactory);secondAsync=new ExperimentPublisher(secondKafka,topic,1000,plan.sharded());}
            for(int i=0;i<plan.consumers();i++)add(false);if(plan.independent())add(true);
            until(30,()->assigned(consumersA)==3&&(!plan.independent()||assigned(consumersB)==3));
        }
        ProjectionStore store(String projection) {var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return new ProjectionStore(jdbc,tx,codec,projection,"experiment");}
        void add(boolean independent) {
            var factory=KafkaSupport.consumerFactory(BOOTSTRAP,independent?groupB:groupA,"earliest");if(plan.pollStall())factory.updateConfigs(Map.of("max.poll.interval.ms",6000));
            if(plan.sharded())factory.addPostProcessor(consumer->normalizePoll(consumer));
            var consumer=new ProjectionConsumer(factory,topic,store(independent?"load-b":"load-a"),records->{
                (independent?metricsB:metricsA).committed(records);
                if(!independent&&plan.pollStall()&&started>0&&elapsed()>8&&stalled.compareAndSet(false,true)) {event("POLL_STALL_BEGIN");sleep(8000);event("POLL_STALL_END");}
            });
            (independent?consumersB:consumersA).add(consumer);consumer.start();
        }
        @SuppressWarnings("unchecked") private org.apache.kafka.clients.consumer.Consumer<String,byte[]> normalizePoll(org.apache.kafka.clients.consumer.Consumer<String,byte[]> consumer) {
            return (org.apache.kafka.clients.consumer.Consumer<String,byte[]>)java.lang.reflect.Proxy.newProxyInstance(org.apache.kafka.clients.consumer.Consumer.class.getClassLoader(),new Class<?>[]{org.apache.kafka.clients.consumer.Consumer.class},(proxy,method,args)->{
                try {Object value=method.invoke(consumer,args);if(method.getName().equals("poll")) {var records=(ConsumerRecords<String,byte[]>)value;var normalized=new HashMap<TopicPartition,List<ConsumerRecord<String,byte[]>>>();for(var p:records.partitions())normalized.put(p,records.records(p).stream().map(this::normalize).toList());return new ConsumerRecords<>(normalized,records.nextOffsets());}return value;}
                catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
            });
        }
        ConsumerRecord<String,byte[]> normalize(ConsumerRecord<String,byte[]> record) {
            if(!plan.sharded())return record;
            if(record.key()==null||!record.key().matches("[a-f0-9]{32}/[0-2]"))throw new IllegalArgumentException("EXPERIMENT_SHARD_KEY");
            return new ConsumerRecord<>(record.topic(),record.partition(),record.offset(),record.key().substring(0,32),record.value());
        }
        void generate(Path directory,Instant requestedBase,boolean resources,Map<String,Object> result) throws Exception {
            eventBase=requestedBase==null?Instant.now().minusSeconds(2).truncatedTo(java.time.temporal.ChronoUnit.SECONDS):requestedBase;
            if(eventBase.isBefore(Instant.now().minusSeconds(6*86400))||eventBase.isAfter(Instant.now()))throw new IllegalArgumentException("EXPERIMENT_EVENT_BASE");
            result.put("eventTimeBase",eventBase.toString());result.put("identityRule","UUID.nameUUIDFromBytes(seed/index); eventTime=base+index/rate, every100th minus120s; receivedAt=actual generation time");
            started=System.nanoTime();scheduleFaults();
            scheduler.scheduleWithFixedDelay(()->{try {sample(directory,resources);}catch(Exception failure){abort.set(true);event("RESOURCE_OR_SAMPLE_FAILED");}},0,5,TimeUnit.SECONDS);
            long target=plan.rate()*(long)plan.seconds();int frameSize=Math.max(1,plan.rate()/50);boolean rejected=false;
            for(long first=0;first<target&&!abort.get();first+=frameSize) {
                long scheduled=first*1_000_000_000L/plan.rate();while(System.nanoTime()-started<scheduled&&!abort.get())LockSupport.parkNanos(Math.min(5_000_000L,scheduled-(System.nanoTime()-started)));
                if(abort.get())break;generatorLags.add(Math.max(0,(System.nanoTime()-started-scheduled)/1_000_000d));var batch=new ArrayList<ChatEvent>();
                for(long i=first;i<Math.min(target,first+frameSize);i++) {var event=input(i);batch.add(event);if(plan.duplicates()&&i%100==0)batch.add(event);}
                generatedUnique+=Math.min(frameSize,target-first);attemptedRecords+=batch.size();
                try {
                    if(serial!=null)serial.accept(batch);else (secondAsync!=null&&(first/frameSize)%2==1?secondAsync:async).accept(batch,first);
                    var ids=new HashSet<UUID>();for(var input:batch)if(ids.add(input.eventId()))expected.merge(input.channelId()+"/"+Math.floorDiv(input.eventTime().getEpochSecond(),10)*10,1L,Long::sum);
                } catch(RuntimeException failure){rejected=true;event("PUBLISH_REJECTED");break;}
                if(producerFailed()){event("PUBLISH_FAILED");break;}
            }
            if(!rejected&&!producerFailed()&&!abort.get())while(elapsed()<plan.seconds())LockSupport.parkNanos(5_000_000L);
            result.put("generatorSeconds",elapsed());result.put("generatedUnique",generatedUnique);result.put("attemptedRecords",attemptedRecords);result.put("generatorScheduleLagMillis",ExperimentMetrics.percentiles(generatorLags));
            if(serial!=null)serialStats=serial.finish();if(async!=null)async.close();if(secondAsync!=null)secondAsync.close();result.put("producerMetrics",producerMetrics(kafka));if(secondKafka!=null)result.put("secondProducerMetrics",producerMetrics(secondKafka));
            producerFactory.destroy();producerFactory=null;if(secondFactory!=null){secondFactory.destroy();secondFactory=null;}if(serial!=null)serialStats=serial.finish();result.put("producer",producerStats());
            if(abort.get())throw new IllegalStateException("EXPERIMENT_RESOURCE_OR_SAMPLE_FAILED");
            // 멈춘 group의 예약 재개와 DB 장애 해제 뒤에도 입력 종료로부터180초만 기다린다.
            long drainStarted=System.nanoTime();until(180,()->caughtUp(groupA)&&(!plan.independent()||caughtUp(groupB)));
            result.put("postInputDrainSeconds",(System.nanoTime()-drainStarted)/1_000_000_000d);sample(directory,resources);
            scheduler.shutdownNow();scheduler.awaitTermination(15,TimeUnit.SECONDS);
            for(var consumer:consumersA)consumer.close();for(var consumer:consumersB)consumer.close();
        }
        ChatEvent input(long index) {
            int channel=plan.hot()?(index%5!=4?0:1+(int)(index/5%19)):(int)(index%20);
            String id=uuid("channel/"+SEED+"/"+channel).toString().replace("-","");Instant eventTime=eventBase.plusNanos(index*1_000_000_000L/plan.rate());if(index%100==0)eventTime=eventTime.minusSeconds(120);
            return new ChatEvent(1,uuid(SEED+"/"+index),"CHZZK_UNOFFICIAL",id,"synthetic-room",uuid("session/"+SEED),null,"CHAT",eventTime,Instant.now(),false,"experiment-v1");
        }
        void scheduleFaults() {
            if(plan.pauseSeconds()>0)scheduler.schedule(()->{for(var c:consumersA)c.close();consumersA.clear();event("ANALYZER_STOPPED");scheduler.schedule(()->{add(false);event("ANALYZER_RESTARTED");},plan.pauseSeconds(),TimeUnit.SECONDS);},10,TimeUnit.SECONDS);
            if(!plan.fault().equals("none")) {
                scheduler.schedule(()->{jdbc.update("UPDATE experiment_fault SET enabled=?,delay_seconds=?",plan.fault().equals("failure"),plan.fault().equals("delay")?.25:0);event("DB_FAULT_BEGIN");},8,TimeUnit.SECONDS);
                scheduler.schedule(()->{jdbc.update("UPDATE experiment_fault SET enabled=false,delay_seconds=0");event("DB_FAULT_END");},18,TimeUnit.SECONDS);
            }
            if(plan.membership()) {
                scheduler.schedule(()->{add(false);event("CONSUMERS_TWO");},8,TimeUnit.SECONDS);
                scheduler.schedule(()->{add(false);add(false);event("CONSUMERS_FOUR");},16,TimeUnit.SECONDS);
                scheduler.schedule(()->{consumersA.removeFirst().close();event("CONSUMER_REMOVED");},28,TimeUnit.SECONDS);
            }
        }
        synchronized void sample(Path directory,boolean resources) throws Exception {
            var sample=new LinkedHashMap<String,Object>();sample.put("seconds",elapsed());sample.put("sourceEnd",ends());
            sample.put("groupA",groupSample(groupA,consumersA,"load-a"));if(plan.independent())sample.put("groupB",groupSample(groupB,consumersB,"load-b"));
            if(serial!=null) {var buffers=serial.bufferSnapshot();int pending=buffers.values().stream().mapToInt(BufferedKafkaPublisher.BufferState::count).sum();serialHighWater=Math.max(serialHighWater,pending);Instant oldest=buffers.values().stream().map(BufferedKafkaPublisher.BufferState::oldest).min(Comparator.naturalOrder()).orElse(null);serialOldest=oldest==null?0:Math.max(0,Duration.between(oldest,Instant.now()).toMillis());sample.put("pending",pending);sample.put("oldestUnackedMillis",serialOldest);}
            else {sample.put("pending",async.pending()+(secondAsync==null?0:secondAsync.pending()));sample.put("oldestUnackedMillis",Math.max(async.oldestAgeMillis(),secondAsync==null?0:secondAsync.oldestAgeMillis()));}
            if(resources)sample.put("resourceUsage",ExperimentResources.sample(admin,topic,jdbc,schema,directory));sample.put("sampleFinishedSeconds",elapsed());samples.add(sample);
            if(progressFile!=null)Files.writeString(progressFile,JSON.writeValueAsString(sample)+"\n");
        }
        Map<String,Object> groupSample(String group,List<ProjectionConsumer> consumers,String projection) throws Exception {
            var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS);var ends=ends();var positions=new ArrayList<Long>();var lag=new ArrayList<Long>();
            for(int p=0;p<3;p++){var offset=committed.get(new TopicPartition(topic,p));long position=offset==null?0:offset.offset();positions.add(position);lag.add(ends.get(p)-position);}
            return Map.of("committed",positions,"lag",lag,"database",store(projection).totals(),"consumers",consumers.stream().map(ProjectionConsumer::status).toList(),"databaseProgress",store(projection).progress());
        }
        List<Long> ends() throws Exception {var request=new HashMap<TopicPartition,OffsetSpec>();for(int p=0;p<3;p++)request.put(new TopicPartition(topic,p),OffsetSpec.latest());var ends=admin.listOffsets(request).all().get(8,TimeUnit.SECONDS);return java.util.stream.IntStream.range(0,3).mapToObj(p->ends.get(new TopicPartition(topic,p)).offset()).toList();}
        boolean caughtUp(String group) throws Exception {var ends=ends();var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(8,TimeUnit.SECONDS);for(int p=0;p<3;p++){var offset=committed.get(new TopicPartition(topic,p));if(offset==null||offset.offset()<ends.get(p))return false;}return true;}
        void event(String code) {var event=new LinkedHashMap<String,Object>();event.put("code",code);event.put("seconds",elapsed());try{event.put("databaseA",store("load-a").totals());if(plan.independent())event.put("databaseB",store("load-b").totals());}catch(RuntimeException ignored){event.put("databaseUnavailable",true);}events.add(event);}
        double elapsed(){return started==0?0:(System.nanoTime()-started)/1_000_000_000d;}
        boolean producerFailed(){return serial!=null?serial.deliveryFailed():async.failed()||secondAsync!=null&&secondAsync.failed();}
        long accepted(){return ((Number)producerStats().get("accepted")).longValue();}
        long acknowledged(){return ((Number)producerStats().get("acknowledged")).longValue();}
        Map<String,Object> producerStats(){if(serial!=null){var result=new LinkedHashMap<>(serialStats);result.put("sampledHighWater",serialHighWater);return result;}var result=new LinkedHashMap<>(async.stats());if(secondAsync!=null){var second=secondAsync.stats();for(String key:List.of("accepted","acknowledged","rejected","unconfirmed","failedCallbacks"))result.put(key,((Number)result.get(key)).longValue()+((Number)second.get(key)).longValue());result.put("second",second);result.put("applicationTotalCapacity",2000);}return result;}
        boolean expectedMatches(String projection) {var actual=new HashMap<String,Long>();jdbc.query("SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection=? AND run_id='experiment'",rs->{actual.put(rs.getString(1)+"/"+rs.getTimestamp(2).toInstant().getEpochSecond(),rs.getLong(3));},projection);return actual.equals(expected);}
        boolean replay(Map<String,Object> result) throws Exception {
            UUID id=UUID.randomUUID();long begin=System.nanoTime();
            try(var source=new ReplaySource(BOOTSTRAP,topic,id)) {
                var manifest=source.capture(null,null);result.put("replayManifest",manifest);result.put("replayKeyNormalization",plan.sharded()?"EXPERIMENT_SHARD_PREFIX_REMOVAL":"NONE");source.assign(manifest);
                var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);var replay=new ProjectionStore(jdbc,tx,codec,"replay",id.toString());var next=new HashMap<Integer,Long>();for(var range:manifest.partitions())next.put(range.partition(),range.start());
                while(manifest.partitions().stream().anyMatch(r->next.get(r.partition())<r.end())) {
                    if((System.nanoTime()-begin)>TimeUnit.SECONDS.toNanos(300))throw new IllegalStateException("EXPERIMENT_REPLAY_TIMEOUT");source.validate(manifest);var batch=new ArrayList<ConsumerRecord<String,byte[]>>();
                    for(var record:source.poll()) {var range=manifest.partitions().get(record.partition());if(record.offset()>=range.end())continue;if(record.offset()!=next.get(record.partition()))throw new IllegalStateException("EXPERIMENT_OFFSET_GAP");batch.add(normalize(record));next.put(record.partition(),record.offset()+1);}
                    if(!batch.isEmpty())replay.applyReplay(batch);
                }
                source.validate(manifest);result.put("replaySeconds",(System.nanoTime()-begin)/1_000_000_000d);result.put("replayDatabase",replay.totals());
                long differences=jdbc.queryForObject("""
                        SELECT count(*) FROM (SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection='load-a' AND run_id='experiment') a
                        FULL JOIN (SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection='replay' AND run_id=?) b USING(channel_id,bucket_start)
                        WHERE a.chat_count IS DISTINCT FROM b.chat_count
                        """,Long.class,id.toString());result.put("replayBucketDifferences",differences);return differences==0;
            }
        }
        @Override public void close() {
            if(!closed.compareAndSet(false,true))return;
            try {scheduler.shutdownNow();scheduler.awaitTermination(15,TimeUnit.SECONDS);for(var c:consumersA)c.close();for(var c:consumersB)c.close();if(serial!=null)serial.close();if(async!=null)async.close();if(secondAsync!=null)secondAsync.close();if(producerFactory!=null)producerFactory.destroy();if(secondFactory!=null)secondFactory.destroy();
                if(admin!=null){var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().map(g->g.groupId()).filter(g->g.equals(groupA)||g.equals(groupB)).toList();if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);if(topicCreated)admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);admin.close(Duration.ofSeconds(1));}
                if(ds!=null)ds.close();if(schemaCreated)try(var root=PostgresSupport.open(JDBC,USER,PASSWORD,false)){var rootJdbc=new JdbcTemplate(root);rootJdbc.execute("DROP SCHEMA "+schema+" CASCADE");if(!live(rootJdbc).equals(liveBefore))throw new IllegalStateException("EXPERIMENT_LIVE_CHANGED");}cleaned=true;
            } catch(Exception ignored){cleaned=false;}
        }
    }
    private static int assigned(List<ProjectionConsumer> consumers){return consumers.stream().mapToInt(c->((Number)c.status().get("assignedPartitions")).intValue()).sum();}
    private static UUID uuid(String value){return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));}
    private static void sleep(long millis){try{Thread.sleep(millis);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}}
    @FunctionalInterface interface Condition{boolean check() throws Exception;}
    private static void until(int seconds,Condition condition) throws Exception {long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);while(!condition.check()){if(System.nanoTime()>deadline)throw new IllegalStateException("EXPERIMENT_DEADLINE");Thread.sleep(100);}}
    private static Map<String,Object> live(JdbcTemplate jdbc){return jdbc.queryForObject("SELECT (SELECT count(*) FROM processed_event),(SELECT coalesce(sum(chat_count),0)::bigint FROM chat_bucket_10s)",(rs,n)->Map.of("ledger",rs.getLong(1),"bucket",rs.getLong(2)));}
    private static Map<String,Object> producerMetrics(KafkaTemplate<String,byte[]> template){var values=new TreeMap<String,Object>();template.metrics().forEach((name,metric)->{if(Set.of("batch-size-avg","records-per-request-avg","request-rate","record-send-rate","record-error-total","record-retry-total","record-queue-time-max","record-queue-time-avg","buffer-available-bytes").contains(name.name()))values.put(name.name(),metric.metricValue());});return values;}
}
