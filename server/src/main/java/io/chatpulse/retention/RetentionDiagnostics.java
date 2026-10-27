package io.chatpulse.retention;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;

/** 비싼 전체 COUNT 대신 관계 크기·추정 행수와 정리 진행을 읽는다. OS 여유 공간은 별도 관측한다. */
public final class RetentionDiagnostics {
    private final JdbcTemplate jdbc;
    public RetentionDiagnostics(JdbcTemplate jdbc) {this.jdbc=jdbc;}
    public Map<String,Object> inspect(String run) {
        var result=new LinkedHashMap<String,Object>();
        result.put("tasks",jdbc.queryForList("SELECT task,attempted_at,succeeded_at,state,deleted_rows,duration_millis,limited,failures FROM retention_task WHERE run_id=? ORDER BY task",run));
        result.put("windows",jdbc.queryForList("SELECT scope,run_id,received_floor,event_floor,updated_at FROM retention_window WHERE run_id IN (?,'global') ORDER BY scope,run_id",run));
        result.put("relations",jdbc.queryForList("""
                SELECT s.relname relation,pg_table_size(s.relid) table_bytes,pg_indexes_size(s.relid) index_bytes,
                  pg_total_relation_size(s.relid) total_bytes,s.n_live_tup estimated_live_rows,s.n_dead_tup estimated_dead_rows,
                  s.last_autovacuum,s.last_vacuum FROM pg_stat_user_tables s WHERE schemaname=current_schema()
                ORDER BY s.relname
                """));
        result.put("databaseBytes",jdbc.queryForObject("SELECT pg_database_size(current_database())",Long.class));
        result.put("wal",jdbc.queryForMap("SELECT wal_bytes,stats_reset FROM pg_stat_wal"));
        result.put("protected",jdbc.queryForMap("""
                SELECT (SELECT count(*) FROM replay_run WHERE state IN ('QUEUED','RUNNING')) active_replays,
                  (SELECT min(observed_from) FROM report_session WHERE ended_at IS NULL) oldest_open_report,
                  (SELECT min(created_at) FROM spike_outbox WHERE run_id=? AND published_at IS NULL) oldest_pending_outbox,
                  (SELECT min(o.created_at) FROM spike_outbox o WHERE o.run_id=? AND o.published_at IS NOT NULL
                    AND NOT EXISTS(SELECT 1 FROM notification_event n WHERE n.run_id=o.run_id AND n.spike_id=o.spike_id)) oldest_unconfirmed_outbox
                """,run,run));
        result.put("limitations",List.of("RELATION_BYTES_ARE_NOT_FREE_DISK","ROW_COUNTS_ARE_ESTIMATES","WAL_BYTES_ARE_CLUSTER_CUMULATIVE",
                "PROTECTED_HISTORY_CAN_EXCEED_TTL","DELETED_ROWS_ARE_LAST_SUCCESSFUL_TASK_NOT_LIFETIME_TOTAL","LIMITED_IS_POSSIBLE_BACKLOG_NOT_COUNT"));
        return result;
    }
}
