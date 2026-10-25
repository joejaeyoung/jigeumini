package io.chatpulse.quality;

import io.chatpulse.poc.CollectionObserver;
import io.chatpulse.pipeline.BufferedKafkaPublisher;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** 공개 ID·시각·진단 코드만 영속화한다. 조회 실패 시 heartbeat가 멈춰 UNKNOWN으로 내려간다. */
public final class CollectionTelemetry implements CollectionObserver, AutoCloseable {
    private record Channel(Instant since, String state, boolean enabled) { }
    private record Session(String channel, Instant start, Instant alive, Instant end, String reason) { }
    private record Loss(UUID id, String channel, Instant from, Instant to, String reason) { }
    private final Object flushLock = new Object();
    private final UUID collector = UUID.randomUUID();
    private final Map<String, Channel> channels = new HashMap<>();
    private final Map<UUID, Session> sessions = new HashMap<>();
    private final List<Loss> losses = new ArrayList<>();
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final BufferedKafkaPublisher publisher;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    public CollectionTelemetry(JdbcTemplate jdbc, TransactionTemplate tx, BufferedKafkaPublisher publisher) {
        this.jdbc = jdbc; this.tx = tx; this.publisher = publisher;
    }
    public void start() { worker.scheduleWithFixedDelay(this::safeFlush, 0, 5, TimeUnit.SECONDS); }
    @Override public synchronized void start(String channel) { channels.put(channel, new Channel(Instant.now(), "CONNECTING", true)); }
    @Override public synchronized void state(String channel, String state) {
        var old = channels.get(channel);
        if (old != null) channels.put(channel, new Channel(old.since, state, old.enabled));
    }
    @Override public synchronized void connected(String channel, UUID id, Instant at) {
        sessions.put(id, new Session(channel, at, at, null, null)); state(channel, "CONNECTED");
    }
    @Override public synchronized void alive(UUID id, Instant at) {
        var s = sessions.get(id);
        if (s != null && s.end == null) sessions.put(id, new Session(s.channel, s.start, at, null, null));
    }
    @Override public synchronized void ended(UUID id, Instant at, String reason) {
        var s = sessions.get(id);
        if (s != null && s.end == null) {
            // 비정상 종료는 마지막 원천 생존 이후를 정상 구간으로 확정하지 않는다.
            Instant end = "CLOSED".equals(reason) ? at : s.alive;
            sessions.put(id, new Session(s.channel, s.start, s.alive, end, reason));
        }
    }
    @Override public synchronized void loss(String channel, Instant from, Instant to, String reason) {
        if (!to.isAfter(from)) to = from.plusMillis(1);
        // 포화 시 같은 채널의 범위를 보수적으로 합쳐 누락 사실을 버리지 않는다.
        if (losses.size() >= 128) {
            var existing = losses.stream().filter(l -> l.channel.equals(channel)).findFirst();
            if (existing.isPresent()) {
                var old = existing.get(); losses.remove(old);
                losses.add(new Loss(old.id, channel, from.isBefore(old.from) ? from : old.from, to.isAfter(old.to) ? to : old.to, "MULTIPLE_LOSSES"));
                return;
            }
        }
        losses.add(new Loss(UUID.randomUUID(), channel, from, to, reason));
    }
    @Override public synchronized void stopped(String channel) {
        var old = channels.get(channel);
        if (old != null && !Set.of("OFFLINE", "UNAVAILABLE").contains(old.state)) channels.put(channel, new Channel(old.since, "STOPPED", false));
    }
    private void safeFlush() { try { flush(null); } catch (RuntimeException ignored) { /* heartbeat 만료가 품질을 하향한다. */ } }
    public void flush(Instant at) { synchronized (flushLock) { flushOnce(at); } }
    private void flushOnce(Instant at) {
        Map<String, Channel> cs; Map<UUID, Session> ss; List<Loss> ls;
        synchronized (this) { cs = Map.copyOf(channels); ss = Map.copyOf(sessions); ls = List.copyOf(losses); }
        var buffers = publisher.bufferSnapshot();
        final Instant observed = at == null ? Instant.now() : at;
        if (publisher.deliveryFailed()) {
            synchronized (this) {
                for (var item : buffers.entrySet()) loss(item.getKey(), item.getValue().oldest(), observed, "UNCONFIRMED_DELIVERY");
                ls = List.copyOf(losses);
            }
        }
        final List<Loss> writtenLosses = ls;
        tx.executeWithoutResult(transaction -> {
            for (var item : cs.entrySet()) {
                var c = item.getValue();
                jdbc.update("""
                        INSERT INTO collection_channel VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(channel_id) DO UPDATE SET collector_id=excluded.collector_id, enabled=excluded.enabled,
                        observed_since=excluded.observed_since, state=excluded.state, updated_at=excluded.updated_at
                        """, item.getKey(), collector, c.enabled, ts(c.since), c.state, ts(observed));
                var buffer = buffers.get(item.getKey());
                jdbc.update("""
                        INSERT INTO collector_channel_buffer VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(channel_id) DO UPDATE SET collector_id=excluded.collector_id, observed_at=excluded.observed_at,
                        oldest_unacked_at=excluded.oldest_unacked_at, buffer_count=excluded.buffer_count
                        """, item.getKey(), collector, ts(observed), buffer == null ? null : ts(buffer.oldest()), buffer == null ? 0 : buffer.count());
                jdbc.update("INSERT INTO collector_buffer_observation VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                        item.getKey(),collector,ts(observed),buffer == null ? null : ts(buffer.oldest()));
            }
            jdbc.update("DELETE FROM collector_buffer_observation WHERE observed_at<?",ts(observed.minusSeconds(120)));
            for (var item : ss.entrySet()) {
                var s = item.getValue();
                jdbc.update("""
                        INSERT INTO collection_session VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(session_id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at, source_alive_at=excluded.source_alive_at,
                        ended_at=excluded.ended_at, end_reason=excluded.end_reason
                        """, item.getKey(), s.channel, collector, ts(s.start), ts(observed), ts(s.alive), ts(s.end), s.reason);
            }
            for (var l : writtenLosses) jdbc.update("INSERT INTO collection_loss VALUES (?, ?, ?, ?, ?) ON CONFLICT(loss_id) DO UPDATE SET start_at=least(collection_loss.start_at,excluded.start_at), end_at=greatest(collection_loss.end_at,excluded.end_at), reason=excluded.reason",
                    l.id, l.channel, ts(l.from), ts(l.to), l.reason);
        });
        synchronized (this) {
            losses.removeAll(writtenLosses);
            ss.forEach((id, s) -> { if (s.end != null && s.equals(sessions.get(id))) sessions.remove(id); });
        }
    }
    static Timestamp ts(Instant value) { return value == null ? null : Timestamp.from(value); }
    @Override public void close() { worker.shutdownNow(); safeFlush(); }
}
