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
    public static void main(String[] args) {
        quiet();
        try {
            if(args.length<2||args.length>3)throw new IllegalArgumentException("EXPERIMENT_ARGUMENTS");
            var result=args[0].equals("broker-fault")?PublisherFaultExperiment.run(Path.of(args[1])):run(ExperimentPlan.named(args[0]),Path.of(args[1]),args.length==3?Instant.parse(args[2]):null,true);
            System.out.println(JSON.writeValueAsString(Map.of("profile",args[0],"outcome",result.get("outcome"),"report",args[1])));
            if(!Boolean.TRUE.equals(result.get("correct")))System.exit(2);
        } catch(Exception failure) {System.out.println("{\"error\":\"EXPERIMENT_FAILED\"}");System.exit(2);}
    }
    static void quiet() { if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF); }
    static Map<String,Object> run(ExperimentPlan plan,Path output,Instant requestedBase,boolean resources) throws Exception {
        output=output.toAbsolutePath();Files.createDirectories(output.getParent());
        for(String suffix:List.of("",".progress.json",".html"))if(Files.exists(output.resolveSibling(output.getFileName()+suffix)))throw new IllegalArgumentException("EXPERIMENT_OUTPUT_EXISTS");
        Files.createFile(output);
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
            result.put("databaseA",finalA);result.put("databaseB",finalB);result.put("metricsA",session.metricsA.snapshot());result.put("metricsB",session.metricsB.snapshot());result.put("metricsAScope",plan.name().equals("heartbeat")?"PARENT_PROCESS_ONLY_CHILD_LATENCIES_NOT_CAPTURED":"ALL_GROUP_MEMBERS");
            result.put("samples",List.copyOf(session.samples));result.put("events",List.copyOf(session.events));
            boolean replay=session.replay(result);boolean expected=session.expectedMatches("load-a");result.put("expectedBucketsMatch",expected);
            long unique=((Number)finalA.get("processedEvents")).longValue(),upper=session.expected.values().stream().mapToLong(Long::longValue).sum(),lower=Math.max(0,session.acknowledged()-(session.accepted()-upper));
            result.put("acknowledgedUniqueLowerBound",lower);result.put("acceptedUniqueUpperBound",upper);
            var codes=session.events.stream().map(e->e.get("code")).toList();boolean scenarioComplete=(plan.pauseSeconds()==0||codes.containsAll(List.of("ANALYZER_STOPPED","ANALYZER_RESTARTED")))
                    &&(plan.fault().equals("none")||codes.containsAll(List.of("DB_FAULT_BEGIN","DB_FAULT_END")))&&(!plan.membership()||codes.containsAll(List.of("CONSUMERS_TWO","CONSUMERS_FOUR","CONSUMER_REMOVED")))
                    &&(!plan.pollStall()||codes.containsAll(List.of("POLL_STALL_BEGIN","POLL_STALL_END")))
                    &&(!plan.name().equals("heartbeat")||codes.containsAll(List.of("HEARTBEAT_STOPPED","HEARTBEAT_REASSIGNED","HEARTBEAT_RESUMED","HEARTBEAT_REJOINED")))&&codes.stream().noneMatch(code->code.toString().endsWith("_EXPERIMENT_FAILED"));result.put("scenarioComplete",scenarioComplete);
            long accepted=session.accepted(),acknowledged=session.acknowledged(),rejected=((Number)session.producerStats().get("rejected")).longValue(),unconfirmed=((Number)session.producerStats().get("unconfirmed")).longValue();
            boolean conserved=session.attemptedRecords==accepted+rejected&&accepted==acknowledged+unconfirmed;
            result.put("countConservation",conserved);
            boolean correct=conserved&&scenarioComplete&&replay&&unique>=lower&&unique<=upper&&unique==((Number)finalA.get("bucketTotal")).longValue()
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
            Files.writeString(output,JSON.writeValueAsString(result)+"\n");ExperimentReport.writeHtml(output,result);
        }
        return result;
    }
    private static String runtimeHash() throws Exception {
        var hash=java.security.MessageDigest.getInstance("SHA-256");Path classes=Path.of(LoadExperiment.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try(var files=Files.walk(classes)) {for(var file:files.filter(Files::isRegularFile).sorted().toList()) {hash.update(classes.relativize(file).toString().getBytes(StandardCharsets.UTF_8));hash.update((byte)0);hash.update(Files.readAllBytes(file));hash.update((byte)0);}}
        return HexFormat.of().formatHex(hash.digest());
    }
    private static final class Session implements AutoCloseable {
        final ExperimentPlan plan;final String suffix=UUID.randomUUID().toString().replace("-",""),schema="exp_"+suffix,topic="exp.chat."+suffix,groupA="exp.a."+suffix,groupB="exp.b."+suffix;
        final EventCodec codec=new EventCodec();final ExperimentMetrics metricsA=new ExperimentMetrics(),metricsB=new ExperimentMetrics();
        final CopyOnWriteArrayList<ProjectionConsumer> consumersA=new CopyOnWriteArrayList<>(),consumersB=new CopyOnWriteArrayList<>();
        final List<Map<String,Object>> samples=new CopyOnWriteArrayList<>(),events=new CopyOnWriteArrayList<>();
        final Map<String,Long> expected=new HashMap<>();final List<Double> generatorLags=new ArrayList<>();
        final ScheduledExecutorService scheduler=Executors.newScheduledThreadPool(2);
        final AtomicBoolean closed=new AtomicBoolean(),abort=new AtomicBoolean(),stalled=new AtomicBoolean();
        HikariDataSource ds;JdbcTemplate jdbc;Admin admin;DefaultKafkaProducerFactory<String,byte[]> producerFactory,secondFactory;KafkaTemplate<String,byte[]> kafka,secondKafka;
        ExperimentPublisher async,secondAsync;BufferedKafkaPublisher serial;Map<String,Object> serialStats=Map.of(),liveBefore=Map.of();
        Process child;boolean childStopped;
        boolean schemaCreated,topicCreated,cleaned;long started,generatedUnique,attemptedRecords;Instant eventBase;int serialHighWater;double serialOldest;Path progressFile;boolean guardResources;Path outputDirectory;
        Session(ExperimentPlan plan){this.plan=plan;}
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
            if(plan.name().equals("heartbeat")) {
                child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx128m","-cp",System.getProperty("integration.classpath",System.getProperty("java.class.path")),ExperimentPausedConsumer.class.getName(),schema,topic,groupA).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
                until(30,()->child.isAlive()&&assigned(consumersA)>0&&assigned(consumersA)<3);
            }
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
            guardResources=resources;outputDirectory=directory;
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
                } catch(RuntimeException failure){
                    rejected=true;event("PUBLISH_REJECTED");
                    result.put("pendingObservedAfterRejection",serial!=null?serial.bufferSnapshot().values().stream().mapToInt(BufferedKafkaPublisher.BufferState::count).sum():async.pending()+(secondAsync==null?0:secondAsync.pending()));
                    break;
                }
                if(producerFailed()){event("PUBLISH_FAILED");break;}
            }
            if(!rejected&&!producerFailed()&&!abort.get())while(elapsed()<plan.seconds())LockSupport.parkNanos(5_000_000L);
            result.put("generatorSeconds",elapsed());result.put("generatedUnique",generatedUnique);result.put("attemptedRecords",attemptedRecords);result.put("generatorScheduleLagMillis",ExperimentMetrics.percentiles(generatorLags));
            long publisherDrainStarted=System.nanoTime();
            if(serial!=null)serialStats=serial.finish();if(async!=null)async.close();if(secondAsync!=null)secondAsync.close();result.put("producerMetrics",producerMetrics(kafka));if(secondKafka!=null)result.put("secondProducerMetrics",producerMetrics(secondKafka));
            producerFactory.destroy();producerFactory=null;if(secondFactory!=null){secondFactory.destroy();secondFactory=null;}if(serial!=null)serialStats=serial.finish();result.put("producer",producerStats());result.put("publisherDrainSeconds",(System.nanoTime()-publisherDrainStarted)/1_000_000_000d);result.put("producerSettledSeconds",elapsed());result.put("acknowledgedPerSecondIncludingDrain",acknowledged()/elapsed());
            if(abort.get())throw new IllegalStateException("EXPERIMENT_RESOURCE_OR_SAMPLE_FAILED");
            // 멈춘 group의 예약 재개와 DB 장애 해제 뒤에도 입력 종료로부터180초만 기다린다.
            long drainStarted=System.nanoTime();until(180,()->caughtUp(groupA)&&(!plan.independent()||caughtUp(groupB)));
            result.put("postInputDrainSeconds",(System.nanoTime()-drainStarted)/1_000_000_000d);sample(directory,resources);
            scheduler.shutdownNow();scheduler.awaitTermination(15,TimeUnit.SECONDS);
            stopChild();for(var consumer:consumersA)consumer.close();for(var consumer:consumersB)consumer.close();
        }
        ChatEvent input(long index) {
            int channel=plan.hot()?(index%5!=4?0:1+(int)(index/5%19)):(int)(index%20);
            String id=uuid("channel/"+SEED+"/"+channel).toString().replace("-","");Instant eventTime=eventBase.plusNanos(index*1_000_000_000L/plan.rate());if(index%100==0)eventTime=eventTime.minusSeconds(120);
            return new ChatEvent(1,uuid(SEED+"/"+index),"CHZZK_UNOFFICIAL",id,"synthetic-room",uuid("session/"+SEED),null,"CHAT",eventTime,Instant.now(),false,"experiment-v1");
        }
        void scheduleFaults() {
            if(plan.name().equals("heartbeat"))scheduler.schedule(()->{
                try {
                    signalChild("-STOP");childStopped=true;event("HEARTBEAT_STOPPED");
                    until(18,()->assigned(consumersA)==3);event("HEARTBEAT_REASSIGNED");
                    signalChild("-CONT");childStopped=false;event("HEARTBEAT_RESUMED");
                    until(20,()->child.isAlive()&&assigned(consumersA)>0&&assigned(consumersA)<3);event("HEARTBEAT_REJOINED");
                } catch(Exception failed){abort.set(true);event("HEARTBEAT_EXPERIMENT_FAILED");}
            },8,TimeUnit.SECONDS);
            if(plan.pauseSeconds()>0)schedule("RECOVERY",10,()->{for(var c:consumersA)c.close();consumersA.clear();event("ANALYZER_STOPPED");schedule("RECOVERY",plan.pauseSeconds(),()->{add(false);event("ANALYZER_RESTARTED");});});
            if(!plan.fault().equals("none")) {
                schedule("DB_FAULT",8,()->{jdbc.update("UPDATE experiment_fault SET enabled=?,delay_seconds=?",plan.fault().equals("failure"),plan.fault().equals("delay")?.25:0);event("DB_FAULT_BEGIN");});
                schedule("DB_FAULT",18,()->{jdbc.update("UPDATE experiment_fault SET enabled=false,delay_seconds=0");event("DB_FAULT_END");});
            }
            if(plan.membership()) {
                schedule("MEMBERSHIP",8,()->{add(false);event("CONSUMERS_TWO");});
                schedule("MEMBERSHIP",16,()->{add(false);add(false);event("CONSUMERS_FOUR");});
                schedule("MEMBERSHIP",28,()->{consumersA.removeFirst().close();event("CONSUMER_REMOVED");});
            }
        }
        void schedule(String kind,long delay,Runnable action) {
            scheduler.schedule(()->{try{action.run();}catch(RuntimeException failure){abort.set(true);event(kind+"_EXPERIMENT_FAILED");}},delay,TimeUnit.SECONDS);
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
        Map<String,Object> producerStats(){if(serial!=null){var result=new LinkedHashMap<>(serialStats);result.put("sampledHighWater",serialHighWater);return result;}var result=new LinkedHashMap<>(async.stats());if(secondAsync!=null){var second=secondAsync.stats();for(String key:List.of("accepted","acknowledged","rejected","unconfirmed","failedCallbacks"))result.put(key,((Number)result.get(key)).longValue()+((Number)second.get(key)).longValue());result.put("firstPublisherHighWater",result.remove("highWater"));result.put("second",second);result.put("complete",Boolean.TRUE.equals(result.get("complete"))&&Boolean.TRUE.equals(second.get("complete")));result.put("applicationTotalCapacity",2000);}return result;}
        boolean expectedMatches(String projection) {var actual=new HashMap<String,Long>();jdbc.query("SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection=? AND run_id='experiment'",rs->{actual.put(rs.getString(1)+"/"+rs.getTimestamp(2).toInstant().getEpochSecond(),rs.getLong(3));},projection);return actual.equals(expected);}
        boolean replay(Map<String,Object> result) throws Exception {
            UUID id=UUID.randomUUID();long begin=System.nanoTime();
            var maximumEventTimes=new HashMap<String,Instant>();var keyPartitions=new TreeMap<String,Set<Integer>>();long regressions=0,recordsRead=0,nextResourceCheck=0;
            var replayResources=new ArrayList<Map<String,Object>>();
            try(var source=new ReplaySource(BOOTSTRAP,topic,id)) {
                var manifest=source.capture(null,null);result.put("replayManifest",manifest);result.put("replayKeyNormalization",plan.sharded()?"EXPERIMENT_SHARD_PREFIX_REMOVAL":"NONE");source.assign(manifest);
                var ranges=new HashMap<Integer,ReplayManifest.Range>();manifest.partitions().forEach(range->ranges.put(range.partition(),range));
                var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);var replay=new ProjectionStore(jdbc,tx,codec,"replay",id.toString());var next=new HashMap<Integer,Long>();for(var range:manifest.partitions())next.put(range.partition(),range.start());
                while(manifest.partitions().stream().anyMatch(r->next.get(r.partition())<r.end())) {
                    if((System.nanoTime()-begin)>TimeUnit.SECONDS.toNanos(300))throw new IllegalStateException("EXPERIMENT_REPLAY_TIMEOUT");if(guardResources&&System.nanoTime()>=nextResourceCheck){replayResources.add(ExperimentResources.sample(admin,topic,jdbc,schema,outputDirectory));nextResourceCheck=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);}
                    source.validate(manifest);var batch=new ArrayList<ConsumerRecord<String,byte[]>>();
                    for(var record:source.poll()) {var range=ranges.get(record.partition());if(range==null)throw new IllegalStateException("EXPERIMENT_PARTITION_CHANGED");if(record.offset()>=range.end())continue;if(record.offset()!=next.get(record.partition()))throw new IllegalStateException("EXPERIMENT_OFFSET_GAP");var normalized=normalize(record);var event=codec.decode(normalized.value(),normalized.key());
                        String sequenceKey=event.channelId()+"/"+record.partition();Instant previous=maximumEventTimes.get(sequenceKey);
                        if(previous!=null&&event.eventTime().isBefore(previous))regressions++;
                        maximumEventTimes.merge(sequenceKey,event.eventTime(),(a,b)->a.isAfter(b)?a:b);
                        keyPartitions.computeIfAbsent(event.channelId(),key->new TreeSet<>()).add(record.partition());recordsRead++;
                        batch.add(normalized);next.put(record.partition(),record.offset()+1);}
                    if(!batch.isEmpty())replay.applyReplay(batch);
                }
                source.validate(manifest);result.put("replaySeconds",(System.nanoTime()-begin)/1_000_000_000d);result.put("replayDatabase",replay.totals());result.put("replayResourceSamples",replayResources);result.put("appendOrderAudit",Map.of("records",recordsRead,"eventTimeBelowPriorMaximum",regressions,"channelPartitions",keyPartitions));
                long differences=jdbc.queryForObject("""
                        SELECT count(*) FROM (SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection='load-a' AND run_id='experiment') a
                        FULL JOIN (SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE projection='replay' AND run_id=?) b USING(channel_id,bucket_start)
                        WHERE a.chat_count IS DISTINCT FROM b.chat_count
                        """,Long.class,id.toString());result.put("replayBucketDifferences",differences);return differences==0;
            }
        }
        void signalChild(String signal) throws Exception {
            if(child==null||!child.isAlive())throw new IllegalStateException("EXPERIMENT_CHILD_EXITED");
            var command=new ProcessBuilder("kill",signal,Long.toString(child.pid())).start();
            if(!command.waitFor(5,TimeUnit.SECONDS)||command.exitValue()!=0)throw new IllegalStateException("EXPERIMENT_SIGNAL_FAILED");
        }
        void stopChild() throws Exception {
            if(child==null)return;
            if(child.isAlive()&&childStopped){signalChild("-CONT");childStopped=false;}
            child.destroy();if(!child.waitFor(10,TimeUnit.SECONDS)){child.destroyForcibly();if(!child.waitFor(5,TimeUnit.SECONDS))throw new IllegalStateException("EXPERIMENT_CHILD_STOP_FAILED");}
        }
        @Override public void close() {
            if(!closed.compareAndSet(false,true))return;
            var failures=new AtomicBoolean();
            cleanupStep(failures,()->{scheduler.shutdownNow();scheduler.awaitTermination(15,TimeUnit.SECONDS);});
            cleanupStep(failures,this::stopChild);
            for(var c:consumersA)cleanupStep(failures,c::close);for(var c:consumersB)cleanupStep(failures,c::close);
            if(serial!=null)cleanupStep(failures,serial::close);if(async!=null)cleanupStep(failures,async::close);if(secondAsync!=null)cleanupStep(failures,secondAsync::close);
            if(producerFactory!=null)cleanupStep(failures,producerFactory::destroy);if(secondFactory!=null)cleanupStep(failures,secondFactory::destroy);
            if(admin!=null) {
                cleanupStep(failures,()->{var groups=admin.listConsumerGroups().all().get(10,TimeUnit.SECONDS).stream().map(g->g.groupId()).filter(g->g.equals(groupA)||g.equals(groupB)).toList();if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(10,TimeUnit.SECONDS);});
                if(topicCreated)cleanupStep(failures,()->{try{admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);}catch(ExecutionException failure){if(!(failure.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException))throw failure;}});
                cleanupStep(failures,()->admin.close(Duration.ofSeconds(1)));
            }
            if(ds!=null)cleanupStep(failures,ds::close);
            if(schemaCreated)cleanupStep(failures,()->{try(var root=PostgresSupport.open(JDBC,USER,PASSWORD,false)){var rootJdbc=new JdbcTemplate(root);rootJdbc.execute("DROP SCHEMA "+schema+" CASCADE");if(!live(rootJdbc).equals(liveBefore))throw new IllegalStateException("EXPERIMENT_LIVE_CHANGED");}});
            cleaned=!failures.get();
        }
        @FunctionalInterface interface Cleanup {void run() throws Exception;}
        private static void cleanupStep(AtomicBoolean failures,Cleanup action){try{action.run();}catch(Exception failure){failures.set(true);}}

    }
    private static int assigned(List<ProjectionConsumer> consumers){return consumers.stream().mapToInt(c->((Number)c.status().get("assignedPartitions")).intValue()).sum();}
    private static UUID uuid(String value){return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));}
    private static void sleep(long millis){try{Thread.sleep(millis);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}}
    @FunctionalInterface interface Condition{boolean check() throws Exception;}
    private static void until(int seconds,Condition condition) throws Exception {long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);while(!condition.check()){if(System.nanoTime()>deadline)throw new IllegalStateException("EXPERIMENT_DEADLINE");Thread.sleep(100);}}
    private static Map<String,Object> live(JdbcTemplate jdbc){return jdbc.queryForObject("SELECT (SELECT count(*) FROM processed_event),(SELECT coalesce(sum(chat_count),0)::bigint FROM chat_bucket_10s)",(rs,n)->Map.of("ledger",rs.getLong(1),"bucket",rs.getLong(2)));}
    private static Map<String,Object> producerMetrics(KafkaTemplate<String,byte[]> template){var values=new TreeMap<String,Object>();template.metrics().forEach((name,metric)->{if(Set.of("batch-size-avg","records-per-request-avg","request-rate","record-send-rate","record-error-total","record-retry-total","record-queue-time-max","record-queue-time-avg","buffer-available-bytes").contains(name.name()))values.put(name.name(),metric.metricValue());});return values;}
}
