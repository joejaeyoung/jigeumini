package io.chatpulse.replay;

import io.chatpulse.pipeline.*;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static io.chatpulse.replay.ReplayFailure.Code.*;

public final class ReplayStore {
    public record Job(UUID id,ReplayManifest manifest,String state,UUID owner,boolean cancelled) { }
    private final DataSource ds;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx, readTx;
    private final EventCodec codec=new EventCodec();
    public ReplayStore(DataSource ds) {
        this.ds=ds;jdbc=new JdbcTemplate(ds);jdbc.setQueryTimeout(5);
        var manager=new DataSourceTransactionManager(ds);
        tx=new TransactionTemplate(manager);tx.setTimeout(20);
        readTx=new TransactionTemplate(manager);readTx.setReadOnly(true);readTx.setTimeout(20);
        readTx.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }
    public UUID create(ReplayManifest manifest) {
        UUID id=UUID.randomUUID();
        tx.executeWithoutResult(status->{
            jdbc.update("INSERT INTO replay_run(run_id,manifest,state,created_build) VALUES (?,?::jsonb,'QUEUED',?)",id,manifest.json(),ReplayBuild.id());
            var projection=projection(id);
            manifest.partitions().forEach(r->projection.initializeProgress(manifest.topic(),r.partition(),r.start()));
        });return id;
    }
    private Job map(ResultSet rs,int index) throws SQLException {
        return new Job(rs.getObject("run_id",UUID.class),ReplayManifest.parse(rs.getString("manifest")),rs.getString("state"),rs.getObject("owner_id",UUID.class),rs.getBoolean("cancel_requested"));
    }
    public Job get(UUID id) {
        var jobs=jdbc.query("SELECT * FROM replay_run WHERE run_id=?",this::map,id);
        if(jobs.isEmpty()) throw new ReplayFailure(RUN_NOT_FOUND);return jobs.getFirst();
    }
    public Job claim(UUID owner) {
        return tx.execute(status->{
            var jobs=jdbc.query("SELECT * FROM replay_run WHERE state='QUEUED' ORDER BY created_at,run_id LIMIT 1 FOR UPDATE SKIP LOCKED",this::map);
            if(jobs.isEmpty()) return null;
            var job=jobs.getFirst();
            jdbc.update("UPDATE replay_run SET state='RUNNING',owner_id=?,started_at=now(),heartbeat_at=now(),worker_build=? WHERE run_id=?",owner,ReplayBuild.id(),job.id());
            return new Job(job.id(),job.manifest(),"RUNNING",owner,false);
        });
    }
    private Job locked(Job expected) {
        var current=jdbc.query("SELECT * FROM replay_run WHERE run_id=? FOR UPDATE",this::map,expected.id()).getFirst();
        if(!"RUNNING".equals(current.state()) || !Objects.equals(current.owner(),expected.owner())) throw new ReplayFailure(OWNER_LOST);
        if(current.cancelled()) throw new ReplayFailure(CANCELLED);
        return current;
    }
    public void heartbeat(Job expected) {
        int rows=jdbc.update("UPDATE replay_run SET heartbeat_at=now() WHERE run_id=? AND owner_id=? AND state='RUNNING' AND NOT cancel_requested",expected.id(),expected.owner());
        if(rows!=1) {var current=get(expected.id());throw new ReplayFailure(current.cancelled()?CANCELLED:OWNER_LOST);}
    }
    public void apply(Job job,List<ConsumerRecord<String,byte[]>> records,Map<Integer,Long> next) {
        var eligible=new ArrayList<ConsumerRecord<String,byte[]>>();
        for(var record:records) {
            var event=codec.decode(record.value(),record.key()); // 제외 입력도 계약 검증한다. 원본은 저장하지 않는다.
            if(job.manifest().includes(event.eventTime())) eligible.add(record);
        }
        tx.executeWithoutResult(status->{
            jdbc.execute("SET LOCAL lock_timeout='3s'");locked(job);
            long unique=projection(job.id()).applyReplay(eligible); // 같은 tx: ledger/bucket/진행/계수 모두 rollback 가능
            for(var entry:next.entrySet()) jdbc.update("UPDATE projection_progress SET next_resolved_offset=?,checked_at=now() WHERE projection='replay' AND run_id=? AND topic=? AND partition=?",
                    entry.getValue(),job.id().toString(),job.manifest().topic(),entry.getKey());
            jdbc.update("UPDATE replay_run SET read_count=read_count+?,filtered_count=filtered_count+?,unique_count=unique_count+?,duplicate_count=duplicate_count+?,heartbeat_at=now() WHERE run_id=?",
                    records.size(),records.size()-eligible.size(),unique,eligible.size()-unique,job.id());
        });
    }
    public void complete(Job job) {
        tx.executeWithoutResult(status->{
            jdbc.execute("SET LOCAL lock_timeout='3s'");locked(job);
            for(var range:job.manifest().partitions()) {
                Long position=jdbc.queryForObject("SELECT next_resolved_offset FROM projection_progress WHERE projection='replay' AND run_id=? AND topic=? AND partition=?",Long.class,job.id().toString(),job.manifest().topic(),range.partition());
                if(position==null || position!=range.end()) throw new ReplayFailure(OFFSET_GAP);
            }
            jdbc.update("UPDATE replay_run SET state='COMPLETED',finished_at=now(),heartbeat_at=now() WHERE run_id=?",job.id());
        });
    }
    public void fail(Job job,ReplayFailure.Code reason) {
        jdbc.update("""
                UPDATE replay_run SET state=CASE WHEN cancel_requested OR ?='CANCELLED' THEN 'CANCELLED' ELSE 'FAILED' END,
                    reason=CASE WHEN cancel_requested THEN 'CANCELLED' ELSE ? END,
                    error_count=CASE WHEN cancel_requested OR ?='CANCELLED' THEN 0 ELSE 1 END,finished_at=now()
                WHERE run_id=? AND owner_id=? AND state='RUNNING'
                """,reason.name(),reason.name(),reason.name(),job.id(),job.owner());
    }
    public void cancel(UUID id) {
        get(id);
        jdbc.update("""
                UPDATE replay_run SET cancel_requested=true,
                    state=CASE WHEN state='QUEUED' THEN 'CANCELLED' ELSE state END,
                    reason=CASE WHEN state='QUEUED' THEN 'CANCELLED' ELSE reason END,
                    finished_at=CASE WHEN state='QUEUED' THEN now() ELSE finished_at END
                WHERE run_id=? AND state IN ('QUEUED','RUNNING')
                """,id);
    }
    public final class WorkerLock implements AutoCloseable {
        private final Connection connection;
        private WorkerLock(Connection connection) {this.connection=connection;}
        public void check() {
            try {if(connection.isClosed() || !connection.isValid(2)) throw new ReplayFailure(OWNER_LOST);}
            catch(SQLException e) {throw new ReplayFailure(OWNER_LOST);}
        }
        @Override public void close() throws SQLException {
            try(var s=connection.createStatement()) {s.setQueryTimeout(3);s.execute("SELECT pg_advisory_unlock(hashtext(current_schema()),9371)");}
            catch(SQLException e) {try {connection.abort(Runnable::run);} catch(SQLException ignored) { } throw e;}
            finally {connection.close();}
        }
    }
    /** DB session lock: alive worker가 30초 동안 바빠도 watchdog이 잘못 실패시키지 않는다. */
    public WorkerLock tryWorkerLock() throws SQLException {
        var connection=ds.getConnection();boolean keep=false;
        try(var statement=connection.createStatement()) {
            statement.setQueryTimeout(3);
            try(var rows=statement.executeQuery("SELECT pg_try_advisory_lock(hashtext(current_schema()),9371)")) {
                rows.next();if(!rows.getBoolean(1)) return null;keep=true;return new WorkerLock(connection);
            }
        } finally {if(!keep) connection.close();}
    }
    public int reap() throws SQLException {
        try(var lock=tryWorkerLock()) {return lock==null?0:reapWhileLocked();}
    }
    int reapWhileLocked() {
        return jdbc.update("""
                UPDATE replay_run SET state=CASE WHEN cancel_requested THEN 'CANCELLED' ELSE 'FAILED' END,
                    reason=CASE WHEN cancel_requested THEN 'CANCELLED' ELSE 'WORKER_LOST' END,
                    error_count=CASE WHEN cancel_requested THEN 0 ELSE 1 END,finished_at=now()
                WHERE state='RUNNING' AND heartbeat_at<now()-interval '30 seconds'
                """);
    }
    private ProjectionStore projection(UUID id) {return new ProjectionStore(jdbc,tx,codec,"replay",id.toString());}
    public Map<String,Object> status(UUID id) {
        return readTx.execute(transaction->{
            var job=get(id);
            var result=new LinkedHashMap<String,Object>();result.put("runId",id.toString());result.put("state",job.state());
            result.put("manifest",job.manifest());result.put("consumerGroup","replay-"+id);result.put("projection","replay");
            result.put("metrics",jdbc.queryForMap("SELECT read_count,filtered_count,unique_count,duplicate_count,error_count,reason,cancel_requested,created_at,started_at,heartbeat_at,finished_at,created_build,worker_build FROM replay_run WHERE run_id=?",id));
            result.put("totals",projection(id).totals());result.put("progress",projection(id).progress());
            result.put("limitations",List.of("RETAINED_OFFSETS_ONLY","NO_FUTURE_LATE_INPUT","NO_LIVE_SNAPSHOT_OR_ALERT_REPLAY","DUPLICATES_COUNTED_WITHIN_TIME_FILTER"));
            return result;
        });
    }
    public Map<String,Object> compare(UUID left,UUID right) {
        if(left.equals(right))throw new ReplayFailure(INCOMPATIBLE_RUNS);
        return readTx.execute(transaction->{
            var a=get(left);var b=get(right);
            if(!a.state().equals("COMPLETED") || !b.state().equals("COMPLETED") || !a.manifest().sameInput(b.manifest())) throw new ReplayFailure(INCOMPATIBLE_RUNS);
            String build=jdbc.queryForObject("SELECT worker_build FROM replay_run WHERE run_id=?",String.class,left);
            if(build==null || !build.matches("[a-f0-9]{64}") || !build.equals(jdbc.queryForObject("SELECT worker_build FROM replay_run WHERE run_id=?",String.class,right)))throw new ReplayFailure(INCOMPATIBLE_RUNS);
            String paired="""
                    WITH paired AS (
                      SELECT coalesce(a.channel_id,b.channel_id) channel_id,coalesce(a.bucket_start,b.bucket_start) bucket_start,
                        coalesce(a.chat_count,0) left_count,coalesce(b.chat_count,0) right_count
                      FROM (SELECT * FROM chat_bucket_10s WHERE projection='replay' AND run_id=?) a
                      FULL OUTER JOIN (SELECT * FROM chat_bucket_10s WHERE projection='replay' AND run_id=?) b
                      ON a.channel_id=b.channel_id AND a.bucket_start=b.bucket_start)
                    """;
            var result=new LinkedHashMap<String,Object>();result.put("comparable",true);result.put("leftRun",left.toString());result.put("rightRun",right.toString());
            result.put("summary",jdbc.queryForMap(paired+"SELECT count(*) FILTER(WHERE left_count=right_count) matched_buckets,count(*) FILTER(WHERE left_count<>right_count) mismatched_buckets,coalesce(sum(left_count),0) left_total,coalesce(sum(right_count),0) right_total FROM paired",left.toString(),right.toString()));
            result.put("equal",((Number)((Map<?,?>)result.get("summary")).get("mismatched_buckets")).longValue()==0);
            result.put("differences",jdbc.queryForList(paired+"SELECT channel_id,bucket_start,left_count,right_count,right_count-left_count difference FROM paired WHERE left_count<>right_count ORDER BY bucket_start,channel_id LIMIT 100",left.toString(),right.toString()));
            result.put("differenceLimit",100);result.put("leftMetrics",status(left).get("metrics"));result.put("rightMetrics",status(right).get("metrics"));return result;
        });
    }
}
