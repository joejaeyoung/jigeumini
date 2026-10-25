package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class ProjectionStore {
    private record Input(ChatEvent event, String hash, ConsumerRecord<String, byte[]> record) { }
    private record Bucket(String channel, Instant time) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventCodec codec;
    private final String projection;
    private final String runId;

    public ProjectionStore(JdbcTemplate jdbc, TransactionTemplate tx, EventCodec codec, String projection, String runId) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
        this.projection = projection;
        this.runId = runId;
    }
    public long apply(List<ConsumerRecord<String, byte[]>> records) {
        var unique = new LinkedHashMap<UUID, Input>();
        var offsets = new HashMap<TopicPartition, Long>();
        Instant now = Instant.now();
        for (var r : records) {
            var event = codec.decode(r.value(), r.key());
            codec.validateLiveAge(event, now);
            var input = new Input(event, codec.hash(event), r);
            var previous = unique.putIfAbsent(event.eventId(), input);
            if (previous != null && !previous.hash.equals(input.hash)) throw new EventContractException("EVENT_ID_CONFLICT");
            offsets.merge(new TopicPartition(r.topic(), r.partition()), r.offset() + 1, Math::max);
        }
        Long inserted = tx.execute(status -> {
            jdbc.execute("SET LOCAL lock_timeout = '3s'");
            jdbc.execute("SET LOCAL statement_timeout = '5s'");
            var buckets = new HashMap<Bucket, Long>();
            long count = 0;
            // 동일 배치 ID는 위에서 제거. DB conflict는 concurrent consumer/replay에도 원자적으로 검사한다.
            for (var input : unique.values()) {
                var e = input.event;
                var r = input.record;
                var ids = jdbc.query("""
                        INSERT INTO processed_event(projection, run_id, event_id, channel_id, collection_session_id,
                            event_time, received_at, source_topic, source_partition, source_offset, payload_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (projection, run_id, event_id) DO NOTHING RETURNING event_id
                        """, (rs, n) -> rs.getObject(1, UUID.class), projection, runId, e.eventId(), e.channelId(),
                        e.collectionSessionId(), Timestamp.from(e.eventTime()), Timestamp.from(e.receivedAt()),
                        r.topic(), r.partition(), r.offset(), input.hash);
                if (ids.isEmpty()) {
                    String old = jdbc.queryForObject("SELECT payload_hash FROM processed_event WHERE projection=? AND run_id=? AND event_id=?",
                            String.class, projection, runId, e.eventId());
                    if (!input.hash.equals(old)) throw new EventContractException("EVENT_ID_CONFLICT");
                } else {
                    count++;
                    var bucket = new Bucket(e.channelId(), Instant.ofEpochSecond(Math.floorDiv(e.eventTime().getEpochSecond(), 10) * 10));
                    buckets.merge(bucket, 1L, Long::sum);
                }
            }
            for (var b : buckets.entrySet()) {
                jdbc.update("""
                        INSERT INTO chat_bucket_10s(projection, run_id, channel_id, bucket_start, chat_count) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (projection, run_id, channel_id, bucket_start)
                        DO UPDATE SET chat_count = chat_bucket_10s.chat_count + EXCLUDED.chat_count
                        """, projection, runId, b.getKey().channel, Timestamp.from(b.getKey().time), b.getValue());
            }
            for (var offset : offsets.entrySet()) {
                jdbc.update("""
                        INSERT INTO projection_progress(projection, run_id, topic, partition, next_resolved_offset) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (projection, run_id, topic, partition) DO UPDATE
                        SET next_resolved_offset = greatest(projection_progress.next_resolved_offset, EXCLUDED.next_resolved_offset), checked_at=now()
                        """, projection, runId, offset.getKey().topic(), offset.getKey().partition(), offset.getValue());
            }
            return count;
        });
        return inserted == null ? 0 : inserted;
    }
    public Map<String, Object> totals() {
        return jdbc.queryForObject("""
                SELECT (SELECT count(*) FROM processed_event WHERE projection=? AND run_id=?),
                    (SELECT coalesce(sum(chat_count),0)::bigint FROM chat_bucket_10s WHERE projection=? AND run_id=?)
                """, (rs, n) -> Map.<String, Object>of("projection", projection, "runId", runId,
                        "processedEvents", rs.getLong(1), "bucketTotal", rs.getLong(2)), projection, runId, projection, runId);
    }
    public List<Map<String, Object>> buckets(String channel, int seconds) {
        Instant to = Instant.ofEpochSecond(Math.floorDiv(Instant.now().getEpochSecond(), 10) * 10 + 10);
        return jdbc.query("""
                SELECT channel_id, bucket_start, chat_count FROM chat_bucket_10s
                WHERE projection=? AND run_id=? AND (?='' OR channel_id=?) AND bucket_start>=? AND bucket_start<?
                ORDER BY bucket_start, channel_id
                """, (rs, i) -> Map.<String, Object>of("channelId", rs.getString(1), "bucketStart", rs.getTimestamp(2).toInstant().toString(),
                        "chatCount", rs.getLong(3)), projection, runId, channel, channel, Timestamp.from(to.minusSeconds(seconds)), Timestamp.from(to));
    }
    public List<Map<String, Object>> progress() {
        return jdbc.query("SELECT topic, partition, next_resolved_offset, checked_at FROM projection_progress WHERE projection=? AND run_id=? ORDER BY topic, partition",
                (rs, i) -> Map.<String, Object>of("topic", rs.getString(1), "partition", rs.getInt(2), "nextResolvedOffset", rs.getLong(3),
                        "checkedAt", rs.getTimestamp(4).toInstant().toString()), projection, runId);
    }
}
