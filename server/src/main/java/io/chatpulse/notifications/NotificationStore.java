package io.chatpulse.notifications;

import java.math.BigDecimal;
import java.sql.Timestamp;
import io.chatpulse.retention.RetentionWindow;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class NotificationStore {
    public record Entry(String spikeId,String channelId,String evaluatedAt,String receivedAt,long count,BigDecimal baseline,
            BigDecimal ratio,String algorithmVersion,boolean delayed,boolean valid,String qualityWarning) { }
    public record Page(String asOf,List<Entry> events,String nextCursor) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String run;
    private final TransactionTemplate cleanupTx;
    private final Clock clock;
    private final RetentionWindow retention;
    public NotificationStore(JdbcTemplate jdbc,TransactionTemplate tx,String run,Clock clock){this.jdbc=jdbc;this.tx=tx;cleanupTx=new TransactionTemplate(tx.getTransactionManager());cleanupTx.setTimeout(8);this.run=run;this.clock=clock;retention=new RetentionWindow(jdbc,"notification",run);}
    public void apply(List<ConsumerRecord<String,byte[]>> records) {
        tx.executeWithoutResult(status -> {
            jdbc.execute("SET LOCAL statement_timeout='5s'");
            Instant oldest=retention.lock().eventFrom(clock.instant().minusSeconds(7*86400));
            for(var record:records) {
                var e=SpikeDetected.decode(record.key(),record.value());
                if(e.runId().equals(run)&&!e.evaluatedAt().isBefore(oldest)) {
                    String canonical=new String(e.encode(),java.nio.charset.StandardCharsets.UTF_8);
                    int inserted=jdbc.update("""
                            INSERT INTO notification_event(run_id,spike_id,channel_id,evaluated_at,received_at,payload)
                            VALUES (?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING
                            """,run,e.spikeId(),e.channelId(),Timestamp.from(e.evaluatedAt()),Timestamp.from(clock.instant()),canonical);
                    if(inserted==0&&!Boolean.TRUE.equals(jdbc.queryForObject("SELECT payload=?::jsonb FROM notification_event WHERE run_id=? AND spike_id=?",Boolean.class,canonical,run,e.spikeId())))
                        throw new IllegalArgumentException("SPIKE_ID_CONFLICT");
                }
                jdbc.update("""
                        INSERT INTO notification_progress(run_id,topic,partition,next_offset,updated_at) VALUES (?,?,?,?,clock_timestamp())
                        ON CONFLICT(run_id,topic,partition) DO UPDATE SET next_offset=greatest(notification_progress.next_offset,excluded.next_offset),updated_at=excluded.updated_at
                        """,run,record.topic(),record.partition(),record.offset()+1);
            }
        });
    }
    public Page page(List<String> channels,Instant before,UUID beforeId,int limit) {
        Instant now=clock.instant();
        if(channels.isEmpty())return new Page(now.toString(),List.of(),null);
        var args=new ArrayList<Object>();args.add(run);args.add(Timestamp.from(now.minusSeconds(7*86400)));args.add(Timestamp.from(before));args.add(beforeId);args.addAll(channels);args.add(limit+1);
        String slots=String.join(",",Collections.nCopies(channels.size(),"?"));
        var rows=jdbc.query("""
                SELECT n.*,s.spike_id source_id,s.quality_warning FROM notification_event n
                LEFT JOIN spike_event s ON s.run_id=n.run_id AND s.spike_id=n.spike_id
                WHERE n.run_id=? AND n.evaluated_at>=? AND (n.evaluated_at,n.spike_id)<(?,?) AND n.channel_id IN (
                """+slots+") ORDER BY n.evaluated_at DESC,n.spike_id DESC LIMIT ?",(rs,i)->{
            var e=SpikeDetected.decode(rs.getString("channel_id"),rs.getString("payload").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Instant received=rs.getTimestamp("received_at").toInstant();
            String warning=rs.getObject("source_id")==null?"SOURCE_UNAVAILABLE":rs.getString("quality_warning");
            return new Entry(e.spikeId().toString(),e.channelId(),e.evaluatedAt().toString(),received.toString(),e.count(),e.baseline(),e.ratio(),e.algorithmVersion(),
                    received.isAfter(e.evaluatedAt().plusSeconds(600)),warning==null,warning);
        },args.toArray());
        var events=rows.stream().limit(limit).toList();var last=events.isEmpty()?null:events.getLast();
        return new Page(now.toString(),events,rows.size()>limit?last.evaluatedAt()+"|"+last.spikeId():null);
    }
    public int cleanup() {
        return cleanupTx.execute(status->{
            jdbc.execute("SET LOCAL lock_timeout='100ms'");jdbc.execute("SET LOCAL statement_timeout='1s'");
            var floor=retention.advance(null,clock.instant().minusSeconds(7*86400).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)).event();
            return jdbc.update("DELETE FROM notification_event WHERE ctid IN (SELECT ctid FROM notification_event WHERE run_id=? AND evaluated_at<? AND NOT EXISTS(SELECT 1 FROM spike_outbox o WHERE o.run_id=notification_event.run_id AND o.spike_id=notification_event.spike_id) ORDER BY evaluated_at,spike_id LIMIT 10000)",run,Timestamp.from(floor));
        });
    }

}
