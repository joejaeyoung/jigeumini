package io.chatpulse.pipeline;

import io.chatpulse.categories.CategoryStore;
import io.chatpulse.reports.ReportStore;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.retention.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 합성 장기 이력을 별도 schema에 넣어 정리 처리율·잠금·공간 재사용을 측정한다. 운영 용량 시험은 아니다. */
public final class RetentionStorageExperiment {
    private static final int ROWS=60_000;
    private static final String RUN="retention-experiment",TOPIC="synthetic.retention";
    private final String schema="retention_"+UUID.randomUUID().toString().replace("-","");
    private final org.postgresql.ds.PGSimpleDataSource ds=IntegrationSupport.ds(schema);
    private final JdbcTemplate jdbc=new JdbcTemplate(ds);
    private final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(ds));
    private final Instant now=Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(),10)*10);
    private final Map<String,Object> result=new LinkedHashMap<>();
    public static void main(String[] args) throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("RETENTION_OUTPUT_REQUIRED");
        LoadExperiment.quiet();var experiment=new RetentionStorageExperiment();var file=Path.of(args[0]);
        if(Files.exists(file))throw new IllegalArgumentException("OUTPUT_EXISTS");Files.createDirectories(file.toAbsolutePath().getParent());
        boolean passed=experiment.run();Files.writeString(file,JsonMapper.builder().build().writeValueAsString(experiment.result)+"\n",StandardOpenOption.CREATE_NEW);
        if(!passed)throw new IllegalStateException("RETENTION_EXPERIMENT_FAILED");
    }
    private boolean run() {
        var root=new JdbcTemplate(IntegrationSupport.ds("public"));boolean passed=false;ExecutorService input=Executors.newSingleThreadExecutor();
        result.put("phase","SEED");result.put("startedAt",Instant.now().toString());result.put("scope","SYNTHETIC_SINGLE_SHARED_POSTGRES_SCHEMA");
        result.put("rowsPerProjectionPerCycle",ROWS);result.put("limitations",List.of("CLOCK_AGED_SYNTHETIC_HISTORY_NOT_MULTI_DAY_SOAK","CLUSTER_WAL_DELTA_INCLUDES_OTHER_DATABASE_WRITERS","NO_FULL_PRODUCT_API_OR_KAFKA_LOAD","DELETE_DOES_NOT_PROMISE_OS_FILE_SHRINK"));
        try {
            result.put("revision",LocalKafkaCluster.command(List.of("git","rev-parse","HEAD"),Map.of(),Duration.ofSeconds(5)).strip());
            result.put("workingTreeDirty",!LocalKafkaCluster.command(List.of("git","status","--porcelain"),Map.of(),Duration.ofSeconds(5)).isBlank());
            result.put("postgresVersion",root.queryForObject("SHOW server_version",String.class));
            root.execute("CREATE SCHEMA "+schema);tx.setTimeout(20);
            org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).target("13").load().migrate();seed(0);
            result.put("phase","MIGRATION");result.put("beforeMigration",space());String wal=lsn();long begin=System.nanoTime();
            org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
            result.put("migrationMillis",millis(begin));result.put("migrationWalBytes",walSince(wal));result.put("afterMigration",space());
            var ranking=new ProjectionRetention(jdbc,tx,"ranking",RUN,Clock.fixed(now,ZoneOffset.UTC));
            var trend=new ProjectionRetention(jdbc,tx,"ranking-trend",RUN,Clock.fixed(now,ZoneOffset.UTC));
            var categories=new CategoryStore(jdbc,tx,RUN,TOPIC,TOPIC+".observations",Clock.fixed(now,ZoneOffset.UTC));
            var reports=new ReportStore(jdbc,tx,RUN,TOPIC,TOPIC+".observations",Clock.fixed(now,ZoneOffset.UTC));
            // 열린 report의 오래된 사건은 실제 cleanup에서도 남는다.
            reports.cleanup();result.put("activeReportRowsProtected",jdbc.queryForObject("SELECT count(*) FROM report_event",Long.class));
            if(!Long.valueOf(ROWS).equals(result.get("activeReportRowsProtected")))throw new IllegalStateException("ACTIVE_REPORT_NOT_PROTECTED");
            result.put("phase","CLEANUP");var writer=input.submit(this::liveInput);
            var cycles=new ArrayList<Map<String,Object>>();result.put("cycles",cycles);
            for(int cycle=0;cycle<3;cycle++) {
                if(cycle>0)seed(cycle);
                jdbc.update("UPDATE report_session SET ended_at=?",Timestamp.from(now.minusSeconds(8*86400)));
                var phase=new LinkedHashMap<String,Object>();cycles.add(phase);phase.put("cycle",cycle);phase.put("before",space());phase.put("liveRankingBefore",liveRows());wal=lsn();begin=System.nanoTime();
                var batches=new ArrayList<Map<String,Object>>();phase.put("batches",batches);
                for(int batch=0;batch<8;batch++) {
                    long at=System.nanoTime();var a=ranking.cleanup();var b=trend.cleanup();int c=categories.cleanup();var r=reports.cleanup();
                    batches.add(Map.of("ranking",a,"trend",b,"categoryDeleted",c,"report",r.summary(),"durationMillis",millis(at)));
                    if(((Number)a.get("ledgerDeleted")).intValue()==0&&((Number)b.get("ledgerDeleted")).intValue()==0&&c==0&&r.deletedRows()==0)break;
                }
                phase.put("cleanupMillis",millis(begin));phase.put("cleanupWalBytes",walSince(wal));phase.put("batches",batches);phase.put("afterDelete",space());phase.put("liveRankingAfter",liveRows());
                for(String table:List.of("processed_event","chat_bucket_10s","category_event","category_bucket","report_event","report_bucket","report_session"))jdbc.execute("VACUUM (ANALYZE,TRUNCATE false) "+table);
                phase.put("afterVacuum",space());
            }
            result.put("cycles",cycles);result.put("phase","INPUT_RECONCILIATION");result.put("input",writer.get(60,TimeUnit.SECONDS));
            result.put("finalRanking",new ProjectionStore(jdbc,tx,new EventCodec(),"ranking",RUN).totals());
            result.put("finalTrend",new ProjectionStore(jdbc,tx,new EventCodec(),"ranking-trend",RUN).totals());
            result.put("remainingExpiredEvents",jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE received_at<?",Long.class,Timestamp.from(now.minusSeconds(72*3600))));
            result.put("remainingCategory",jdbc.queryForObject("SELECT count(*) FROM category_event",Long.class));
            result.put("remainingReport",jdbc.queryForObject("SELECT count(*) FROM report_event",Long.class));
            // 실제 row lock 대기 상한. 진행 중 writer가 하한 행을 공유 잠그면 정리는 재시도한다.
            try(var connection=ds.getConnection()) {
                connection.setAutoCommit(false);try(var s=connection.createStatement()){s.execute("SELECT * FROM retention_window WHERE scope='projection/ranking' FOR SHARE");}
                begin=System.nanoTime();boolean refused=false;try{ranking.cleanup();}catch(org.springframework.dao.DataAccessException expected){refused=true;}
                double waited=millis(begin);result.put("lockedFloor",Map.of("refused",refused,"elapsedMillis",waited));connection.rollback();if(!refused||waited>2000)throw new IllegalStateException("LOCK_GUARD_FAILED");
            }
            for(String name:List.of("finalRanking","finalTrend")) {
                var totals=(Map<?,?>)result.get(name);if(!Long.valueOf(10_000).equals(totals.get("processedEvents"))||!Long.valueOf(10_000).equals(totals.get("bucketTotal")))throw new IllegalStateException("LIVE_RECONCILIATION_FAILED");
            }
            if(!Long.valueOf(0).equals(result.get("remainingExpiredEvents"))||!Long.valueOf(0).equals(result.get("remainingCategory"))||!Long.valueOf(0).equals(result.get("remainingReport")))throw new IllegalStateException("CLEANUP_INCOMPLETE");
            passed=true;
        } catch(Exception failure) {result.put("failureCode","RETENTION_EXPERIMENT_FAILED");result.put("failureType",failure.getClass().getSimpleName());if(failure.getMessage()!=null&&failure.getMessage().matches("[A-Z_]+"))result.put("detailCode",failure.getMessage());if(failure.getCause()!=null)result.put("causeType",failure.getCause().getClass().getSimpleName());}
        finally {
            input.shutdownNow();try{if(!input.awaitTermination(12,TimeUnit.SECONDS))passed=false;}catch(InterruptedException e){Thread.currentThread().interrupt();passed=false;}
            try{root.execute("DROP SCHEMA IF EXISTS "+schema+" CASCADE");result.put("cleanupComplete",true);}catch(RuntimeException failure){result.put("cleanupComplete",false);passed=false;}
            result.put("finishedAt",Instant.now().toString());result.put("correct",passed);
        }
        return passed;
    }
    private Map<String,Object> liveInput() {
        var codec=new EventCodec();var ranking=new ProjectionStore(jdbc,tx,codec,"ranking",RUN);var trend=new ProjectionStore(jdbc,tx,codec,"ranking-trend",RUN);
        long start=System.nanoTime();var durations=new ArrayList<Double>();long maximumPacing=0;
        for(int batch=0;batch<100;batch++) {
            long due=start+TimeUnit.MILLISECONDS.toNanos(batch*200L),remaining;
            while((remaining=due-System.nanoTime())>0&&!Thread.currentThread().isInterrupted())LockSupport.parkNanos(remaining);
            if(Thread.currentThread().isInterrupted())throw new IllegalStateException("INPUT_INTERRUPTED");maximumPacing=Math.max(maximumPacing,System.nanoTime()-due);
            var records=new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String,byte[]>>();Instant at=Instant.now();
            for(int i=0;i<100;i++) {
                String channel=String.format("%032x",i);var event=new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",channel,"synthetic-room",UUID.randomUUID(),null,"CHAT",at,at,false,"retention-load");
                records.add(new org.apache.kafka.clients.consumer.ConsumerRecord<>(TOPIC,0,batch*100L+i,channel,codec.encode(event)));
            }
            long began=System.nanoTime();ranking.apply(records);trend.apply(records);durations.add(millis(began));
        }
        var sorted=durations.stream().sorted().toList();return Map.of("uniqueEvents",10_000,"targetEventsPerSecond",500,"elapsedMillis",millis(start),
                "twoProjectionBatchP95Millis",sorted.get(94),"twoProjectionBatchMaximumMillis",sorted.getLast(),"maximumPacingMillis",maximumPacing/1_000_000d);
    }
    private void seed(int cycle) {
        Timestamp old=Timestamp.from(now.minusSeconds(9*86400)),received=Timestamp.from(now.minusSeconds(4*86400));
        for(String projection:List.of("ranking","ranking-trend")) {
            jdbc.update("""
                    INSERT INTO processed_event(projection,run_id,event_id,channel_id,collection_session_id,event_time,received_at,source_topic,source_partition,source_offset,payload_hash)
                    SELECT ?,?,md5(?||i::text)::uuid,lpad(to_hex(i%100),32,'0'),md5('session')::uuid,?::timestamptz-(i/100)*interval '10 seconds',?::timestamptz,?,0,i,repeat('a',64) FROM generate_series(1,?) i
                    """,projection,RUN,"cycle"+cycle,old,received,TOPIC,ROWS);
            jdbc.update("INSERT INTO chat_bucket_10s SELECT ?,?,lpad(to_hex(i%100),32,'0'),?::timestamptz-(i/100)*interval '10 seconds',1 FROM generate_series(1,?) i",projection,RUN,old,ROWS);
        }
        jdbc.update("INSERT INTO category_event(run_id,event_id,channel_id,event_time,payload_hash,category_key) SELECT ?,md5(?||i::text)::uuid,lpad(to_hex(i%100),32,'0'),?::timestamptz-(i/100)*interval '10 seconds',repeat('a',64),'unknown' FROM generate_series(1,?) i",RUN,"cycle"+cycle,old,ROWS);
        jdbc.update("INSERT INTO category_bucket SELECT ?,'unknown',lpad(to_hex(i%100),32,'0'),?::timestamptz-(i/100)*interval '10 seconds',1 FROM generate_series(1,?) i",RUN,old,ROWS);
        UUID report=UUID.nameUUIDFromBytes(("report"+cycle).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        jdbc.update("INSERT INTO report_session(run_id,report_id,channel_id,observed_from,observed_to,payload,updated_at) VALUES (?,?,?,?::timestamptz-interval '1 day',?,'{}',?)",RUN,report,"a".repeat(32),old,old,old);
        jdbc.update("INSERT INTO report_event SELECT ?,md5(?||i::text)::uuid,repeat('a',32),?::timestamptz-(i/100)*interval '10 seconds',?,repeat('a',64),false,NULL,? FROM generate_series(1,?) i",RUN,"cycle"+cycle,old,received,report,ROWS);
        jdbc.update("INSERT INTO report_bucket SELECT run_id,report_id,date_trunc('second',event_time),count(*) FROM report_event WHERE report_id=? GROUP BY run_id,report_id,date_trunc('second',event_time)",report);
    }
    private List<Map<String,Object>> space(){return jdbc.queryForList("SELECT c.relname,pg_table_size(c.oid) table_bytes,pg_indexes_size(c.oid) index_bytes,pg_total_relation_size(c.oid) total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' ORDER BY c.relname");}
    private long liveRows(){return jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE projection='ranking' AND received_at>=?",Long.class,Timestamp.from(now));}
    private String lsn(){return jdbc.queryForObject("SELECT pg_current_wal_insert_lsn()::text",String.class);}
    private long walSince(String before){return jdbc.queryForObject("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),?::pg_lsn)::bigint",Long.class,before);}
    private static double millis(long began){return (System.nanoTime()-began)/1_000_000d;}
}
