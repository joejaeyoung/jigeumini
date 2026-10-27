package io.chatpulse.replay;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import static io.chatpulse.replay.ReplayFailure.Code.*;

/** terminal 결과를 먼저 만료 표시하고 유한 batch로 지운다. active run은 수명 제한만으로 지우지 않는다. */
public final class ReplayRetention {
    private static final int BATCH_SIZE=10_000;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;
    public ReplayRetention(JdbcTemplate jdbc,TransactionTemplate source,Clock clock) {
        this.jdbc=jdbc;this.clock=clock;tx=new TransactionTemplate(source.getTransactionManager());tx.setTimeout(2);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    public static void requireWritable(JdbcTemplate jdbc,UUID id) {
        requireActive(jdbc,id,false);
    }
    public static void requireInitializable(JdbcTemplate jdbc,UUID id) {
        requireActive(jdbc,id,true);
    }
    private static void requireActive(JdbcTemplate jdbc,UUID id,boolean queuedAllowed) {
        if(!TransactionSynchronizationManager.isActualTransactionActive())throw new IllegalStateException("REPLAY_TRANSACTION_REQUIRED");
        var rows=jdbc.query("SELECT state,result_expired_at FROM replay_run WHERE run_id=? FOR SHARE",
                (rs,n)->Map.of("state",rs.getString(1),"expired",rs.getTimestamp(2)!=null),id);
        if(rows.isEmpty())throw new ReplayFailure(RUN_NOT_FOUND);
        if(Boolean.TRUE.equals(rows.getFirst().get("expired")))throw new ReplayFailure(RESULT_EXPIRED);
        if(!rows.getFirst().get("state").equals("RUNNING") && !(queuedAllowed&&rows.getFirst().get("state").equals("QUEUED")))throw new ReplayFailure(OWNER_LOST);
    }
    public Map<String,Object> cleanup() {
        var now=clock.instant();
        UUID id=tx.execute(status->{
            limits();
            var ids=jdbc.query("""
                    SELECT run_id FROM replay_run WHERE state IN ('COMPLETED','FAILED','CANCELLED')
                      AND (result_expired_at IS NOT NULL OR finished_at<?) AND result_purged_at IS NULL
                    ORDER BY finished_at,run_id LIMIT 1 FOR UPDATE SKIP LOCKED
                    """,(rs,n)->rs.getObject(1,UUID.class),Timestamp.from(now.minusSeconds(7*86400)));
            if(ids.isEmpty())return null;
            jdbc.update("UPDATE replay_run SET result_expired_at=coalesce(result_expired_at,?) WHERE run_id=?",Timestamp.from(now),ids.getFirst());
            return ids.getFirst();
        });
        int deleted=0;boolean limited=false;
        if(id!=null) {
            for(String table:List.of("processed_event","chat_bucket_10s","projection_progress")) {
                int count=tx.execute(status->{
                    limits();
                    // 부모 행을 잠가 metadata 삭제/다른 정리와 같은 순서를 유지한다.
                    jdbc.queryForObject("SELECT run_id FROM replay_run WHERE run_id=? AND result_expired_at IS NOT NULL FOR UPDATE",UUID.class,id);
                    return jdbc.update("DELETE FROM "+table+" WHERE ctid IN (SELECT ctid FROM "+table+" WHERE projection='replay' AND run_id=? LIMIT ?)",id.toString(),BATCH_SIZE);
                });
                deleted+=count;limited|=count==BATCH_SIZE;
            }
            tx.executeWithoutResult(status->{limits();jdbc.update("""
                    UPDATE replay_run SET result_purged_at=? WHERE run_id=? AND result_expired_at IS NOT NULL
                      AND NOT EXISTS(SELECT 1 FROM processed_event WHERE projection='replay' AND run_id=?)
                      AND NOT EXISTS(SELECT 1 FROM chat_bucket_10s WHERE projection='replay' AND run_id=?)
                      AND NOT EXISTS(SELECT 1 FROM projection_progress WHERE projection='replay' AND run_id=?)
                    """,Timestamp.from(now),id,id.toString(),id.toString(),id.toString());});
        }
        int metadata=tx.execute(status->{limits();return jdbc.update("""
                DELETE FROM replay_run WHERE run_id IN (
                  SELECT run_id FROM replay_run WHERE result_purged_at IS NOT NULL AND finished_at<?
                  ORDER BY finished_at,run_id LIMIT 100 FOR UPDATE SKIP LOCKED)
                """,Timestamp.from(now.minusSeconds(30*86400)));});
        return Map.of("deletedRows",deleted+metadata,"limited",limited||metadata==100,"resultSelected",id!=null,"metadataDeleted",metadata);
    }
    private void limits() { jdbc.execute("SET LOCAL lock_timeout='100ms'");jdbc.execute("SET LOCAL statement_timeout='1s'"); }
}
