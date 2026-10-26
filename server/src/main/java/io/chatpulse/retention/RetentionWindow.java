package io.chatpulse.retention;

import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** 삭제한 범위를 다시 수락하지 않는다. 잠금은 호출자의 집계/정리 트랜잭션까지 유지한다. */
public final class RetentionWindow {
    public record Floors(Instant received, Instant event) {
        public Instant eventFrom(Instant proposed) { return event != null && event.isAfter(proposed) ? event : proposed; }
        public boolean excludes(Instant receivedAt, Instant eventTime) {
            return received != null && receivedAt.isBefore(received) || event != null && eventTime.isBefore(event);
        }
    }
    private final JdbcTemplate jdbc;
    private final String scope, run;
    public RetentionWindow(JdbcTemplate jdbc, String scope, String run) {
        this.jdbc=jdbc;this.scope=scope;this.run=run;
        jdbc.update("INSERT INTO retention_window(scope,run_id) VALUES (?,?) ON CONFLICT DO NOTHING",scope,run);
    }
    public Floors lock() {
        requireTransaction();
        return jdbc.queryForObject("SELECT received_floor,event_floor FROM retention_window WHERE scope=? AND run_id=? FOR SHARE",
                (rs,n)->new Floors(instant(rs.getTimestamp(1)),instant(rs.getTimestamp(2))),scope,run);
    }
    public Floors advance(Instant received, Instant event) {
        requireTransaction();
        return jdbc.queryForObject("""
                UPDATE retention_window SET received_floor=greatest(received_floor,?),event_floor=greatest(event_floor,?),updated_at=clock_timestamp()
                WHERE scope=? AND run_id=? RETURNING received_floor,event_floor
                """,(rs,n)->new Floors(instant(rs.getTimestamp(1)),instant(rs.getTimestamp(2))),timestamp(received),timestamp(event),scope,run);
    }
    private static void requireTransaction() {
        if(!TransactionSynchronizationManager.isActualTransactionActive())throw new IllegalStateException("RETENTION_TRANSACTION_REQUIRED");
    }
    private static Instant instant(Timestamp value) { return value==null?null:value.toInstant(); }
    private static Timestamp timestamp(Instant value) { return value==null?null:Timestamp.from(value); }
}
