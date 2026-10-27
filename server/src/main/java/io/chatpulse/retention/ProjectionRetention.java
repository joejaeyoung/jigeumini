package io.chatpulse.retention;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** live ledger 72시간, event-time bucket 8일. 진행 offset/원천 identity/입력 하한은 삭제하지 않는다. */
public final class ProjectionRetention {
    public static final int BATCH_SIZE=10_000;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final RetentionWindow window;
    private final String projection,run;
    private final Clock clock;
    public ProjectionRetention(JdbcTemplate jdbc,TransactionTemplate source,String projection,String run,Clock clock) {
        if(projection.equals("replay"))throw new IllegalArgumentException("LIVE_NAMESPACE_REQUIRED");
        this.jdbc=jdbc;this.projection=projection;this.run=run;this.clock=clock;
        window=new RetentionWindow(jdbc,"projection/"+projection,run);
        tx=new TransactionTemplate(source.getTransactionManager());tx.setTimeout(2);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    public Map<String,Object> cleanup() {
        Instant now=clock.instant();
        var floors=tx.execute(status->{limits();return window.advance(now.minusSeconds(72*3600).truncatedTo(ChronoUnit.SECONDS),
                Instant.ofEpochSecond(Math.floorDiv(now.minusSeconds(8*86400).getEpochSecond(),10)*10));});
        int ledger=tx.execute(status->{limits();return jdbc.update("""
                DELETE FROM processed_event WHERE ctid IN (
                  SELECT ctid FROM processed_event WHERE projection=? AND run_id=? AND received_at<?
                  ORDER BY received_at,event_id LIMIT ?)
                """,projection,run,Timestamp.from(floors.received()),BATCH_SIZE);});
        int buckets=tx.execute(status->{limits();return jdbc.update("""
                DELETE FROM chat_bucket_10s WHERE ctid IN (
                  SELECT ctid FROM chat_bucket_10s WHERE projection=? AND run_id=? AND bucket_start<?
                  ORDER BY bucket_start,channel_id LIMIT ?)
                """,projection,run,Timestamp.from(floors.event()),BATCH_SIZE);});
        return Map.of("deletedRows",ledger+buckets,"ledgerDeleted",ledger,"bucketsDeleted",buckets,
                "limited",ledger==BATCH_SIZE||buckets==BATCH_SIZE,"receivedFloor",floors.received().toString(),"eventFloor",floors.event().toString());
    }
    private void limits() { jdbc.execute("SET LOCAL lock_timeout='100ms'");jdbc.execute("SET LOCAL statement_timeout='1s'"); }
}
