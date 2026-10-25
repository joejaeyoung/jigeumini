package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.poc.EventSink;
import io.chatpulse.poc.SourceFailure;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/** F14: frame 원자 수락, 채널별 순환 배분, ACK 미확인 입력까지 포함한 유한 비동기 발행. */
public final class BufferedKafkaPublisher implements EventSink, AutoCloseable {
    public record Limits(int capacity, int channelCapacity, int maxInFlight) {
        public Limits {
            if (capacity < 1 || capacity > 1000 || channelCapacity < 1 || channelCapacity > capacity
                    || maxInFlight < 1 || maxInFlight > capacity) throw new IllegalArgumentException("publisher limits");
        }
    }
    public record BufferState(int count, Instant oldest) { }
    public record ChannelState(long accepted, long acknowledged, long rejected, int pending, Instant oldest,
            boolean stopped, String reason) { }
    private record Pending(long sequence, ChatEvent event, byte[] bytes) { }
    private static final int MAX_TRACKED_CHANNELS = 1000;
    private static final class Channel {
        final ArrayDeque<Pending> queue = new ArrayDeque<>();
        long accepted, acknowledged, rejected;
        int pending;
        boolean stopped;
        String reason = "NONE";
    }
    private final Object lock = new Object(), finishLock = new Object();
    private final Map<String, Channel> channels = new LinkedHashMap<>();
    private final ArrayDeque<String> ready = new ArrayDeque<>();
    private final Map<Long, Pending> unacked = new LinkedHashMap<>();
    private final Map<Long, Pending> sending = new HashMap<>();
    private final KafkaTemplate<String, byte[]> kafka;
    private final String topic;
    private final EventCodec codec;
    private final Limits limits;
    private final Duration drainTimeout;
    private final Thread worker;
    private long sequence, accepted, acknowledged, rejected, failedCallbacks, untrackedRejected;
    private int peakPending, peakInFlight;
    private boolean closing, failed, finished;
    private String failureCode = "NONE";
    private Map<String, Object> finalReport;

    public BufferedKafkaPublisher(KafkaTemplate<String, byte[]> kafka, String topic, EventCodec codec, int capacity) {
        this(kafka, topic, codec, new Limits(capacity, Math.min(capacity, 500), Math.min(capacity, 64)));
    }
    public BufferedKafkaPublisher(KafkaTemplate<String, byte[]> kafka, String topic, EventCodec codec, Limits limits) {
        this(kafka, topic, codec, limits, Duration.ofSeconds(10));
    }
    BufferedKafkaPublisher(KafkaTemplate<String, byte[]> kafka, String topic, EventCodec codec, Limits limits, Duration drainTimeout) {
        if (drainTimeout.compareTo(Duration.ofMillis(1)) < 0 || drainTimeout.compareTo(Duration.ofSeconds(10)) > 0)
            throw new IllegalArgumentException("publisher drain timeout");
        this.kafka = kafka; this.topic = topic; this.codec = codec; this.limits = limits; this.drainTimeout = drainTimeout;
        worker = Thread.ofPlatform().daemon(true).name("kafka-publisher").start(this::publish);
    }
    @Override public void accept(List<ChatEvent> events) {
        synchronized (lock) {
            // finish 결과의 관측 경계 이후 호출은 계수를 바꾸지 않는다.
            if (finished) throw new SourceFailure("PUBLISH_CLOSED", true);
            if (events.isEmpty()) return;
            if (closing || failed) reject(events, closing ? "PUBLISH_CLOSED" : failureCode, false);
            if (events.size() > limits.capacity() - unacked.size()) reject(events, "PUBLISH_BUFFER_REJECTED", true);
            List<Pending> frame;
            try { frame = events.stream().map(e -> new Pending(++sequence, e, codec.encode(e))).toList(); }
            catch (RuntimeException invalid) { reject(events, "PUBLISH_INVALID_EVENT", true); return; }
            var counts = new HashMap<String, Integer>();
            frame.forEach(p -> counts.merge(p.event.channelId(), 1, Integer::sum));
            long newChannels = counts.keySet().stream().filter(id -> !channels.containsKey(id)).count();
            if (newChannels + channels.size() > MAX_TRACKED_CHANNELS) reject(events, "PUBLISH_CHANNELS_LIMIT", true);
            for (var item : counts.entrySet()) {
                Channel c = channels.computeIfAbsent(item.getKey(), ignored -> new Channel());
                if (c.stopped || item.getValue() > limits.channelCapacity() - c.pending) {
                    c.stopped = true; c.reason = "PUBLISH_CHANNEL_LIMIT";
                    reject(events, "PUBLISH_CHANNEL_LIMIT", false);
                }
            }
            // 모든 인코딩과 한도 검사가 끝난 뒤 한 frame을 전량 수락한다.
            for (var p : frame) {
                Channel c = channels.get(p.event.channelId());
                if (c.queue.isEmpty()) ready.addLast(p.event.channelId());
                c.queue.addLast(p); c.pending++; c.accepted++;
                unacked.put(p.sequence, p);
            }
            accepted += frame.size(); peakPending = Math.max(peakPending, unacked.size());
            lock.notifyAll();
        }
    }
    private void reject(List<ChatEvent> events, String code, boolean globalFailure) {
        rejected += events.size();
        for (var event : events) {
            if (event != null && event.channelId() != null && event.channelId().matches("[0-9a-f]{32}")
                    && (channels.containsKey(event.channelId()) || channels.size() < MAX_TRACKED_CHANNELS)) {
                Channel c = channels.computeIfAbsent(event.channelId(), ignored -> new Channel()); c.rejected++;
            } else untrackedRejected++;
        }
        if (globalFailure) fail(code);
        throw new SourceFailure(code, true);
    }
    private void fail(String code) {
        if (!failed) failureCode = code;
        failed = true; lock.notifyAll();
    }
    private void publish() {
        try {
            while (true) {
                Pending p;
                synchronized (lock) {
                    while (true) {
                        if (finished || ((closing || failed) && sending.isEmpty() && (failed || ready.isEmpty()))) return;
                        if (!failed && !ready.isEmpty() && sending.size() < limits.maxInFlight()) break;
                        lock.wait();
                    }
                    String id = ready.removeFirst(); Channel c = channels.get(id);
                    p = c.queue.removeFirst();
                    if (!c.queue.isEmpty()) ready.addLast(id);
                    sending.put(p.sequence, p); peakInFlight = Math.max(peakInFlight, sending.size());
                }
                // send 자체도 metadata/buffer를 기다릴 수 있어 WebSocket callback 밖에서 호출한다.
                try {
                    kafka.send(new ProducerRecord<>(topic, null, p.event.receivedAt().toEpochMilli(),
                            p.event.channelId(), p.bytes)).whenComplete((result, error) -> delivered(p, error));
                } catch (RuntimeException error) { delivered(p, error); }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            synchronized (lock) { if (!finished) fail("PUBLISH_INTERRUPTED"); }
        }
    }
    private void delivered(Pending p, Throwable error) {
        synchronized (lock) {
            if (finished || sending.remove(p.sequence) == null) return;
            if (error == null) {
                unacked.remove(p.sequence); acknowledged++;
                Channel c = channels.get(p.event.channelId()); c.acknowledged++; c.pending--;
            } else {
                failedCallbacks++; fail("KAFKA_DELIVERY_FAILED");
            }
            lock.notifyAll();
        }
    }
    public boolean deliveryFailed() { synchronized (lock) { return failed; } }
    @Override public void checkHealthy() {
        synchronized (lock) { if (failed) throw new SourceFailure(failureCode, true); }
    }
    public Map<String, BufferState> bufferSnapshot() {
        synchronized (lock) { return buffers(); }
    }
    private Map<String, BufferState> buffers() {
        var result = new HashMap<String, BufferState>();
        for (var p : unacked.values()) result.compute(p.event.channelId(), (id, previous) -> new BufferState(
                previous == null ? 1 : previous.count + 1,
                previous == null || p.event.receivedAt().isBefore(previous.oldest) ? p.event.receivedAt() : previous.oldest));
        return Map.copyOf(result);
    }
    public Map<String, ChannelState> channelSnapshot() {
        synchronized (lock) {
            var buffers = buffers(); var result = new LinkedHashMap<String, ChannelState>();
            channels.forEach((id, c) -> result.put(id, new ChannelState(c.accepted, c.acknowledged, c.rejected, c.pending,
                    buffers.containsKey(id) ? buffers.get(id).oldest() : null, c.stopped, c.reason)));
            return Map.copyOf(result);
        }
    }
    @Override public Map<String, Object> finish() {
        synchronized (finishLock) {
            synchronized (lock) {
                if (finished) return finalReport;
                closing = true; lock.notifyAll();
            }
            try { worker.join(drainTimeout.toMillis()); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); synchronized (lock) { fail("PUBLISH_INTERRUPTED"); }
            }
            synchronized (lock) {
                if (worker.isAlive()) fail("PUBLISH_DRAIN_TIMEOUT");
                finished = true; lock.notifyAll(); worker.interrupt();
                var report = new LinkedHashMap<String, Object>();
                report.put("mode", "KAFKA"); report.put("strategy", "BOUNDED_ASYNC_ROUND_ROBIN");
                report.put("attempted", accepted + rejected); report.put("accepted", accepted);
                report.put("acknowledged", acknowledged); report.put("rejected", rejected);
                report.put("unconfirmed", accepted - acknowledged); report.put("failedCallbacks", failedCallbacks);
                int queued = channels.values().stream().mapToInt(c -> c.queue.size()).sum();
                report.put("queued", queued); report.put("inFlight", sending.size());
                report.put("failedUnconfirmed", unacked.size() - queued - sending.size());
                report.put("untrackedRejected", untrackedRejected); report.put("failureCode", failureCode);
                report.put("limits", limits); report.put("peakPending", peakPending); report.put("peakInFlight", peakInFlight);
                report.put("channels", channelSnapshot());
                report.put("complete", !failed && accepted == acknowledged && rejected == 0);
                finalReport = Map.copyOf(report); return finalReport;
            }
        }
    }
    @Override public void close() { finish(); }
}
