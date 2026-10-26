package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** 최대20채널의 선정·용량·epoch 변경을 하나의 DB 행 잠금으로 직렬화한다. 네트워크 I/O를 잠금 안에서 수행하지 않는다. */
public final class CollectorLeases {
    public record Lease(String channelId,long epoch,Instant expiresAt,String policyVersion) { }
    public record Grant(Instant databaseNow,List<Lease> leases) { }
    /** rollback이 끝난 잠금 충돌만 worker의 미배정 재시도 정책에 전달한다. */
    static final class PlanContended extends RuntimeException {
        private PlanContended(DataAccessException cause){super("FLEET_PLAN_LOCK_CONTENDED",cause);}
    }
    private final JdbcTemplate jdbc;private final TransactionTemplate tx;
    public CollectorLeases(DataSource ds) {
        jdbc=new JdbcTemplate(ds);jdbc.setQueryTimeout(3);
        tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(4);
    }
    private void lock() {
        // SQL 사이 client 단절로 열린 transaction도 서버가 종료해 plan 잠금을 풀어야 한다.
        jdbc.execute("SET LOCAL idle_in_transaction_session_timeout='2s'");
        jdbc.execute("SET LOCAL lock_timeout='1s'");jdbc.execute("SET LOCAL statement_timeout='3s'");
        jdbc.queryForObject("SELECT singleton FROM collector_plan WHERE singleton=1 FOR UPDATE",Integer.class);
    }
    private Instant now(){return jdbc.queryForObject("SELECT clock_timestamp()",(rs,n)->rs.getTimestamp(1).toInstant());}
    public void apply(FleetPlan plan) {
        tx.executeWithoutResult(status->{
            lock();Instant now=now();
            long existing=jdbc.queryForObject("SELECT count(*) FROM collector_lease",Long.class);
            long fresh=plan.channels().stream().filter(c->jdbc.queryForObject("SELECT count(*) FROM collector_lease WHERE channel_id=?",Integer.class,c)==0).count();
            if(existing+fresh>20000)throw new IllegalStateException("FLEET_CHANNEL_REGISTRY_LIMIT");
            jdbc.update("UPDATE collector_lease SET selected=false");
            for(String channel:plan.channels())jdbc.update("""
                INSERT INTO collector_lease(channel_id,policy_version,selected) VALUES (?,?,true)
                ON CONFLICT(channel_id) DO UPDATE SET policy_version=excluded.policy_version,selected=true
                """,channel,plan.version());
            jdbc.update("UPDATE collector_plan SET version=?,source_mode=?,discovered=?,eligible=?,selected=?,valid_until=? WHERE singleton=1",
                    plan.version(),plan.sourceMode().name(),plan.discovered(),plan.eligible(),plan.channels().size(),Timestamp.from(now.plusSeconds(plan.validSeconds())));
            // 기존 유효 소유권을 강제로 삭제하지 않는다. 갱신 때 제외되고 정상 종료 또는 만료 후 인계한다.
            // epoch 이력을 보존한다. 등록 채널 총수20,000을 넘으면 명시적으로 중단한다.
        });
    }
    public Grant tick(UUID owner,int capacity,Duration ttl,SourceMode mode,Map<String,Long> held,Set<String> connected,boolean acquire) {
        if(owner==null||capacity<1||capacity>20||ttl.compareTo(Duration.ofSeconds(6))<0||ttl.compareTo(Duration.ofSeconds(60))>0
                ||held.size()>20||connected.size()>20||mode==SourceMode.OFFICIAL_AUTHORIZED)throw new IllegalArgumentException("FLEET_WORKER_LIMIT");
        try {return tx.execute(status->{
            lock();Instant now=now();var stamp=Timestamp.from(now);
            var plan=jdbc.queryForMap("SELECT * FROM collector_plan WHERE singleton=1");
            if(!mode.name().equals(plan.get("source_mode"))||!((Timestamp)plan.get("valid_until")).toInstant().isAfter(now))throw new IllegalStateException("FLEET_PLAN_UNAVAILABLE");
            Instant planEnd=((Timestamp)plan.get("valid_until")).toInstant();
            var expiry=Timestamp.from(now.plus(ttl).isBefore(planEnd)?now.plus(ttl):planEnd);
            jdbc.update("DELETE FROM collector_worker WHERE valid_until<=? AND worker_id<>?",stamp,owner);
            var workers=jdbc.queryForList("SELECT capacity FROM collector_worker WHERE worker_id=?",Integer.class,owner);
            if(workers.isEmpty()) {
                if(jdbc.queryForObject("SELECT count(*) FROM collector_worker WHERE valid_until>?",Integer.class,stamp)>=20)throw new IllegalStateException("FLEET_WORKER_CAPACITY");
                jdbc.update("INSERT INTO collector_worker VALUES (?,?,?,'ACTIVE')",owner,capacity,expiry);
            } else {
                if(workers.getFirst()!=capacity)throw new IllegalStateException("FLEET_CAPACITY_CHANGED");
                jdbc.update("UPDATE collector_worker SET valid_until=?,state='ACTIVE' WHERE worker_id=?",expiry,owner);
            }
            // 만료된 epoch는 갱신하지 않는다. held에는 이번 프로세스가 실제로 받은 epoch만 있다.
            for(var lease:held.entrySet())if(jdbc.update("""
                UPDATE collector_lease SET valid_until=?,connected=?
                WHERE channel_id=? AND owner=? AND epoch=? AND valid_until>? AND selected AND policy_version=?
                """,expiry,connected.contains(lease.getKey()),lease.getKey(),owner,lease.getValue(),stamp,plan.get("version"))!=1)throw new IllegalStateException("FLEET_LEASE_LOST");
            var leases=owned(owner,now);int globalLimit=mode==SourceMode.SYNTHETIC?20:3;
            int available=Math.max(0,globalLimit-jdbc.queryForObject("SELECT count(*) FROM collector_lease WHERE valid_until>? AND owner IS NOT NULL",Integer.class,stamp));
            if(acquire&&leases.size()<capacity&&available>0) {
                var candidates=jdbc.queryForList("""
                    SELECT channel_id FROM collector_lease WHERE selected AND policy_version=? AND valid_until<=? AND retry_after<=?
                    ORDER BY channel_id LIMIT ?
                    """,String.class,plan.get("version"),stamp,stamp,Math.min(capacity-leases.size(),available));
                for(String channel:candidates)jdbc.update("UPDATE collector_lease SET owner=?,epoch=epoch+1,valid_until=?,connected=false WHERE channel_id=?",owner,expiry,channel);
                leases=owned(owner,now);
            }
            return new Grant(now,List.copyOf(leases));
        });}catch(DataAccessException failure) {
            // TransactionSystemException(commit/rollback 실패)은 이 분류에 들어오지 않는다.
            if(failure.getMostSpecificCause() instanceof SQLException sql&&"55P03".equals(sql.getSQLState()))throw new PlanContended(failure);
            throw failure;
        }
    }
    private List<Lease> owned(UUID owner,Instant now) {
        return jdbc.query("SELECT channel_id,epoch,valid_until,policy_version FROM collector_lease WHERE owner=? AND valid_until>? AND selected ORDER BY channel_id",
                (rs,n)->new Lease(rs.getString(1),rs.getLong(2),rs.getTimestamp(3).toInstant(),rs.getString(4)),owner,Timestamp.from(now));
    }
    public void release(UUID owner,Map<String,Long> held,boolean retryDelay) {
        tx.executeWithoutResult(status->{lock();Instant now=now();
            for(var lease:held.entrySet())jdbc.update("UPDATE collector_lease SET owner=null,valid_until=?,connected=false,retry_after=? WHERE channel_id=? AND owner=? AND epoch=?",
                    Timestamp.from(now),Timestamp.from(retryDelay?now.plusSeconds(30):now),lease.getKey(),owner,lease.getValue());
            jdbc.update("UPDATE collector_worker SET state='STOPPED',valid_until=? WHERE worker_id=?",Timestamp.from(now),owner);
        });
    }
    public Map<String,Object> status() {
        // 하나의 SQL snapshot에서 분모·배정·연결을 함께 관측한다. healthyWindow는 품질 API의 별도 판정이다.
        return jdbc.queryForObject("""
            SELECT p.*,clock_timestamp() AS observed_at,
              (SELECT count(*) FROM collector_lease l WHERE l.selected AND l.valid_until>clock_timestamp() AND l.owner IS NOT NULL) AS assigned,
              (SELECT count(*) FROM collector_lease l WHERE l.selected AND l.valid_until>clock_timestamp() AND l.connected) AS connected,
              (SELECT count(*) FROM collector_worker w WHERE w.valid_until>clock_timestamp() AND w.state='ACTIVE') AS workers
            FROM collector_plan p WHERE singleton=1
            """,(rs,n)->{var r=new LinkedHashMap<String,Object>();for(String key:List.of("version","source_mode","discovered","eligible","selected","assigned","connected","workers"))r.put(key,rs.getObject(key));
                r.put("validUntil",rs.getTimestamp("valid_until").toInstant());r.put("observedAt",rs.getTimestamp("observed_at").toInstant());r.put("healthyWindow",null);
                r.put("pending",rs.getInt("selected")-rs.getInt("assigned"));r.put("pendingReason",rs.getTimestamp("valid_until").toInstant().isAfter(rs.getTimestamp("observed_at").toInstant())?"CAPACITY_OR_RETRY_OR_HANDOFF":"PLAN_EXPIRED");return r;});
    }
}
