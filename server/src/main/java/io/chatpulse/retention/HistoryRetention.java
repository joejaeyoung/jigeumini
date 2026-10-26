package io.chatpulse.retention;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** 보고서가 참조하는 과거 품질은 보존한다. 오래 열린 보고서 때문에 보존량이 늘 수 있다. */
public final class HistoryRetention {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final String run;
    public HistoryRetention(JdbcTemplate jdbc,TransactionTemplate source,String run,Clock clock) {
        this.jdbc=jdbc;this.clock=clock;this.run=run;tx=new TransactionTemplate(source.getTransactionManager());tx.setTimeout(8);
    }
    public Map<String,Object> cleanup() {
        return tx.execute(status->{
            jdbc.execute("SET LOCAL lock_timeout='100ms'");jdbc.execute("SET LOCAL statement_timeout='1s'");
            var ordinary=Timestamp.from(clock.instant().minusSeconds(8*86400));
            var floor=jdbc.queryForObject("""
                    SELECT least(?::timestamptz,coalesce(min(date_trunc('minute',observed_from)),?::timestamptz))
                    FROM report_session WHERE ended_at IS NULL OR ended_at>=?
                    """,Timestamp.class,ordinary,ordinary,Timestamp.from(clock.instant().minusSeconds(7*86400)));
            int deleted=0;boolean limited=false;
            var ids=jdbc.query("SELECT snapshot_id FROM ranking_snapshot WHERE run_id=? AND evaluated_at<? ORDER BY evaluated_at,snapshot_id LIMIT 100",
                    (rs,n)->rs.getObject(1,UUID.class),run,floor);
            if(!ids.isEmpty()) {
                String slots=String.join(",",Collections.nCopies(ids.size(),"?"));
                for(String table:List.of("snapshot_quality_revision","ranking_entry")) {
                    int count=jdbc.update("DELETE FROM "+table+" WHERE ctid IN (SELECT ctid FROM "+table+" WHERE snapshot_id IN ("+slots+")"+(table.equals("ranking_entry")?" AND NOT EXISTS(SELECT 1 FROM snapshot_quality_revision r WHERE r.snapshot_id=ranking_entry.snapshot_id AND r.channel_id=ranking_entry.channel_id)":"")+" LIMIT 10000)",ids.toArray());
                    deleted+=count;limited|=count==10000;
                }
                deleted+=jdbc.update("DELETE FROM ranking_snapshot s WHERE snapshot_id IN ("+slots+") AND NOT EXISTS(SELECT 1 FROM ranking_entry e WHERE e.snapshot_id=s.snapshot_id)",ids.toArray());
            }
            for(String sql:List.of(
                    "DELETE FROM collection_session WHERE ctid IN (SELECT ctid FROM collection_session WHERE ended_at<? ORDER BY ended_at LIMIT 10000)",
                    "DELETE FROM collection_loss WHERE ctid IN (SELECT ctid FROM collection_loss WHERE end_at<? ORDER BY end_at LIMIT 10000)")) {
                int count=jdbc.update(sql,floor);deleted+=count;limited|=count==10000;
            }
            int failures=jdbc.update("DELETE FROM projection_failure WHERE ctid IN (SELECT ctid FROM projection_failure WHERE run_id=? AND affected_to<? AND quarantined_at<? LIMIT 10000)",run,floor,ordinary);
            int spikes=jdbc.update("DELETE FROM spike_event s WHERE ctid IN (SELECT s.ctid FROM spike_event s WHERE run_id=? AND evaluated_at<? AND NOT EXISTS(SELECT 1 FROM spike_outbox o WHERE o.spike_id=s.spike_id) LIMIT 10000)",run,ordinary);
            return Map.of("deletedRows",deleted+failures+spikes,"limited",limited||ids.size()==100||failures==10000||spikes==10000,
                    "qualityFloor",floor.toInstant().toString(),"protectedByReports",floor.before(ordinary));
        });
    }
}
