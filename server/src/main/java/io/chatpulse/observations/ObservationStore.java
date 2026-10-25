package io.chatpulse.observations;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class ObservationStore {
    public record History(List<BroadcastObserved> samples, boolean truncated) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String run;
    private final Clock clock;
    public ObservationStore(JdbcTemplate jdbc, TransactionTemplate tx, String run, Clock clock) {
        this.jdbc = jdbc; this.tx = tx; this.run = run; this.clock = clock;
    }
    public void apply(List<ConsumerRecord<String, byte[]>> records) {
        tx.executeWithoutResult(status -> {
            jdbc.execute("SET LOCAL statement_timeout='5s'");
            Instant now = clock.instant(), oldest = now.minusSeconds(7 * 86400);
            for (var record : records) {
                var event = BroadcastObserved.decode(record.key(), record.value());
                if (event.observedAt().isAfter(now.plusSeconds(5))) throw new IllegalArgumentException("OBSERVATION_FUTURE_TIME");
                if (event.runId().equals(run) && !event.observedAt().isBefore(oldest)) {
                    String payload = new String(event.encode(), StandardCharsets.UTF_8);
                    int inserted = jdbc.update("""
                            INSERT INTO broadcast_observation(run_id,event_id,channel_id,observed_at,received_at,payload)
                            VALUES (?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING
                            """, run, event.eventId(), event.channelId(), Timestamp.from(event.observedAt()), Timestamp.from(now), payload);
                    if (inserted == 0 && !Boolean.TRUE.equals(jdbc.queryForObject(
                            "SELECT payload=?::jsonb FROM broadcast_observation WHERE run_id=? AND event_id=?", Boolean.class, payload, run, event.eventId())))
                        throw new IllegalArgumentException("OBSERVATION_ID_CONFLICT");
                }
                jdbc.update("""
                        INSERT INTO observation_progress(run_id,topic,partition,next_offset,updated_at) VALUES (?,?,?,?,?)
                        ON CONFLICT(run_id,topic,partition) DO UPDATE SET next_offset=greatest(observation_progress.next_offset,excluded.next_offset),updated_at=excluded.updated_at
                        """, run, record.topic(), record.partition(), record.offset() + 1, Timestamp.from(now));
            }
        });
    }
    public History history(String channel, Instant from, Instant to) {
        var rows = jdbc.query("""
                SELECT payload FROM broadcast_observation WHERE run_id=? AND channel_id=? AND observed_at>=? AND observed_at<?
                ORDER BY observed_at DESC,event_id DESC LIMIT 1001
                """, (rs, i) -> BroadcastObserved.decode(channel, rs.getString(1).getBytes(StandardCharsets.UTF_8)),
                run, channel, Timestamp.from(from), Timestamp.from(to));
        var samples = new ArrayList<>(rows.stream().limit(1000).toList());
        Collections.reverse(samples);
        return new History(List.copyOf(samples), rows.size() > 1000);
    }
    public int cleanup() {
        // 정리된 입력을 재전달해도 apply의 보존 기준이 과거 이력을 다시 만들지 않는다.
        return jdbc.update("""
                DELETE FROM broadcast_observation WHERE (run_id,event_id) IN (
                    SELECT run_id,event_id FROM broadcast_observation WHERE observed_at<? ORDER BY observed_at LIMIT 10000)
                """, Timestamp.from(clock.instant().minusSeconds(7 * 86400)));
    }
}
