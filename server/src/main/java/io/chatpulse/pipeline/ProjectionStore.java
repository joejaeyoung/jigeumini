package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.retention.RetentionWindow;
import io.chatpulse.replay.ReplayRetention;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class ProjectionStore {
    private record Input(ConsumerRecord<String,byte[]> record, ChatEvent event, String hash, EventContractException error) { }
    private record Bucket(String channel, Instant time) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventCodec codec;
    private final String projection;
    private final String runId;
    private final Clock clock;
    private final RetentionWindow retention;

    public ProjectionStore(JdbcTemplate jdbc, TransactionTemplate tx, EventCodec codec, String projection, String runId) {
        this(jdbc,tx,codec,projection,runId,Clock.systemUTC());
    }
    ProjectionStore(JdbcTemplate jdbc, TransactionTemplate tx, EventCodec codec, String projection, String runId, Clock clock) {
        this.clock = clock;
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
        this.projection = projection;
        this.runId = runId;
        retention="replay".equals(projection)?null:new RetentionWindow(jdbc,"projection/"+projection,runId);
    }
    public void initializeProgress(String topic, int partition, long position) {
        if(retention==null) {
            tx.executeWithoutResult(status->{ReplayRetention.requireInitializable(jdbc,UUID.fromString(runId));insertProgress(topic,partition,position);});
        } else insertProgress(topic,partition,position);
    }
    private void insertProgress(String topic,int partition,long position) {
        jdbc.update("INSERT INTO projection_progress(projection,run_id,topic,partition,next_resolved_offset) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                projection, runId, topic, partition, position);
    }
    public long apply(List<ConsumerRecord<String, byte[]>> records) {
        if(retention==null)throw new IllegalArgumentException("LIVE_NAMESPACE_REQUIRED");
        return apply(records,true,null);
    }
    public long applyRecovering(List<ConsumerRecord<String, byte[]>> records, DeadLetterRecovery recovery) {
        if ("replay".equals(projection)) throw new IllegalArgumentException("LIVE_NAMESPACE_REQUIRED");
        recovery.checkSource(records);
        return apply(records,true,recovery);
    }
    public long applyReplay(List<ConsumerRecord<String, byte[]>> records) {
        if (!"replay".equals(projection)) throw new IllegalArgumentException("REPLAY_NAMESPACE_REQUIRED");
        UUID.fromString(runId);
        return apply(records,false,null);
    }
    private long apply(List<ConsumerRecord<String, byte[]>> records, boolean live, DeadLetterRecovery recovery) {
        long ackDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        Long inserted = tx.execute(status -> {
            jdbc.execute("SET LOCAL lock_timeout = '3s'");
            jdbc.execute("SET LOCAL statement_timeout = '5s'");
            var floors=live?retention.lock():null;
            if(!live)ReplayRetention.requireWritable(jdbc,UUID.fromString(runId));
            if (recovery != null) recovery.bindSource();
            // 확정된 위치는 시각·배치 구성이 바뀌어도 다시 분류하지 않는다.
            var pending = recovery == null ? records : unresolved(records);
            Instant now = clock.instant();
            var inputs = new ArrayList<Input>();
            var hashes = new HashMap<UUID,String>();
            var conflicts = new HashSet<UUID>();
            for (var record : pending) {
                ChatEvent event = null;
                try {
                    event = codec.decode(record.value(),record.key());
                    if (live) {
                        codec.validateLiveAge(event,now);
                        if(floors.excludes(event.receivedAt(),event.eventTime()))throw new EventContractException("OUTSIDE_LIVE_RETENTION");
                    }
                    String hash = codec.hash(event);
                    String previous = hashes.putIfAbsent(event.eventId(),hash);
                    if (previous != null && !previous.equals(hash)) {
                        if (recovery == null) throw new EventContractException("EVENT_ID_CONFLICT");
                        conflicts.add(event.eventId());
                    }
                    inputs.add(new Input(record,event,hash,null));
                } catch (EventContractException error) {
                    if (recovery == null) throw error;
                    inputs.add(new Input(record,event,null,error));
                }
            }
            var buckets = new HashMap<Bucket, Long>();
            var offsets = new HashMap<TopicPartition, Long>();
            var unique = new HashMap<UUID, String>();
            long count = 0;
            for (var input : inputs) {
                var r = input.record;
                ChatEvent event = input.event;
                try {
                    if (input.error != null) throw input.error;
                    if (conflicts.contains(event.eventId())) throw new EventContractException("EVENT_ID_CONFLICT");
                    String hash = input.hash;
                    String previous = unique.putIfAbsent(event.eventId(), hash);
                    if (previous != null && !previous.equals(hash)) throw new EventContractException("EVENT_ID_CONFLICT");
                    if (previous == null && insert(event, hash, r)) {
                        count++;
                        var bucket = new Bucket(event.channelId(), Instant.ofEpochSecond(Math.floorDiv(event.eventTime().getEpochSecond(), 10) * 10));
                        buckets.merge(bucket, 1L, Long::sum);
                    }
                } catch (EventContractException error) {
                    if (recovery == null) throw error;
                    // DB/네트워크 예외는 여기서 잡지 않는다. ACK 실패도 전체 트랜잭션을 되돌린다.
                    if (event != null) unique.remove(event.eventId());
                    recovery.resolve(r, event, error, ackDeadline);
                }
                offsets.merge(new TopicPartition(r.topic(), r.partition()), r.offset() + 1, Math::max);
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
    private List<ConsumerRecord<String,byte[]>> unresolved(List<ConsumerRecord<String,byte[]>> records) {
        var starts = new HashMap<TopicPartition,Long>();
        for (var r : records) starts.merge(new TopicPartition(r.topic(),r.partition()),r.offset(),Math::min);
        var resolved = new HashMap<TopicPartition,Long>();
        var partitions = starts.keySet().stream().sorted(java.util.Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition)).toList();
        for (var tp : partitions) {
            initializeProgress(tp.topic(),tp.partition(),starts.get(tp));
            resolved.put(tp,jdbc.queryForObject("""
                    SELECT next_resolved_offset FROM projection_progress
                    WHERE projection=? AND run_id=? AND topic=? AND partition=? FOR UPDATE
                    """,Long.class,projection,runId,tp.topic(),tp.partition()));
        }
        return records.stream().filter(r -> r.offset() >= resolved.get(new TopicPartition(r.topic(),r.partition()))).toList();
    }
    private boolean insert(ChatEvent event, String hash, ConsumerRecord<String, byte[]> record) {
        var ids = jdbc.query("""
                INSERT INTO processed_event(projection, run_id, event_id, channel_id, collection_session_id,
                    event_time, received_at, source_topic, source_partition, source_offset, payload_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (projection, run_id, event_id) DO NOTHING RETURNING event_id
                """, (rs, n) -> rs.getObject(1, UUID.class), projection, runId, event.eventId(), event.channelId(),
                event.collectionSessionId(), Timestamp.from(event.eventTime()), Timestamp.from(event.receivedAt()),
                record.topic(), record.partition(), record.offset(), hash);
        if (!ids.isEmpty()) return true;
        String old = jdbc.queryForObject("SELECT payload_hash FROM processed_event WHERE projection=? AND run_id=? AND event_id=?",
                String.class, projection, runId, event.eventId());
        if (!hash.equals(old)) throw new EventContractException("EVENT_ID_CONFLICT");
        return false;
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
    public List<String> recentChannelIds(int limit) {
        if (limit < 1 || limit > 20) throw new IllegalArgumentException("CHANNEL_LIMIT");
        return jdbc.query("""
                SELECT channel_id FROM chat_bucket_10s WHERE projection=? AND run_id=?
                GROUP BY channel_id ORDER BY max(bucket_start) DESC, channel_id LIMIT ?
                """, (rs, i) -> rs.getString(1), projection, runId, limit);
    }
    public Map<String,Object> retention() {
        if(retention==null)throw new IllegalArgumentException("LIVE_NAMESPACE_REQUIRED");
        var result=new java.util.LinkedHashMap<String,Object>(jdbc.queryForMap("SELECT received_floor,event_floor FROM retention_window WHERE scope=? AND run_id=?","projection/"+projection,runId));
        result.put("ledgerHours",72);result.put("bucketDays",8);result.put("totalsScope","DIFFERENT_RETAINED_WINDOWS");
        return result;
    }
    public List<Map<String, Object>> progress() {
        return jdbc.query("SELECT topic, partition, next_resolved_offset, checked_at FROM projection_progress WHERE projection=? AND run_id=? ORDER BY topic, partition",
                (rs, i) -> Map.<String, Object>of("topic", rs.getString(1), "partition", rs.getInt(2), "nextResolvedOffset", rs.getLong(3),
                        "checkedAt", rs.getTimestamp(4).toInstant().toString()), projection, runId);
    }
}
