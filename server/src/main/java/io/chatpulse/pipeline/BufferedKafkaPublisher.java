package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.poc.EventSink;
import io.chatpulse.poc.SourceFailure;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

public final class BufferedKafkaPublisher implements EventSink, AutoCloseable {
    private record Pending(ChatEvent event, byte[] bytes) { }
    private final ArrayBlockingQueue<Pending> queue;
    // 발행 중인 한 건까지 포함한 한도. 메모리 상한은 capacity * 16KiB + 객체 overhead.
    private final Semaphore slots;
    private final KafkaTemplate<String, byte[]> kafka;
    private final String topic;
    private final EventCodec codec;
    private final Thread worker;
    private final AtomicLong accepted = new AtomicLong(), acknowledged = new AtomicLong(), rejected = new AtomicLong();
    public record BufferState(int count, java.time.Instant oldest) { }
    private final java.util.concurrent.ConcurrentHashMap<Pending, ChatEvent> unacked = new java.util.concurrent.ConcurrentHashMap<>();
    public boolean deliveryFailed() { return failed; }
    public Map<String, BufferState> bufferSnapshot() {
        var result = new java.util.HashMap<String, BufferState>();
        for (var event : unacked.values()) result.compute(event.channelId(), (id, previous) -> new BufferState(
                previous == null ? 1 : previous.count + 1,
                previous == null || event.receivedAt().isBefore(previous.oldest) ? event.receivedAt() : previous.oldest));
        return Map.copyOf(result);
    }
    private volatile boolean closing;
    private volatile boolean failed;
    private volatile boolean finished;

    public BufferedKafkaPublisher(KafkaTemplate<String, byte[]> kafka, String topic, EventCodec codec, int capacity) {
        this.kafka = kafka;
        this.topic = topic;
        this.codec = codec;
        queue = new ArrayBlockingQueue<>(capacity);
        slots = new Semaphore(capacity);
        worker = Thread.ofPlatform().name("kafka-publisher").start(this::publish);
    }
    @Override
    public synchronized void accept(List<ChatEvent> events) {
        // 한 frame의 일부만 enqueue하지 않는다. 초과한 frame은 전부 거절하고 수집을 중단한다.
        if (closing || failed || !slots.tryAcquire(events.size())) {
            rejected.addAndGet(events.size());
            failed = true;
            throw new SourceFailure("PUBLISH_BUFFER_REJECTED", true);
        }
        try {
            var pending = events.stream().map(e -> new Pending(e, codec.encode(e))).toList();
            accepted.addAndGet(pending.size());
            pending.forEach(p -> { unacked.put(p, p.event); queue.add(p); });
        } catch (RuntimeException e) {
            slots.release(events.size());
            rejected.addAndGet(events.size());
            failed = true;
            throw new SourceFailure("PUBLISH_INVALID_EVENT", true);
        }
    }
    private void publish() {
        try {
            while (!closing || !queue.isEmpty()) {
                Pending p = queue.poll(100, TimeUnit.MILLISECONDS);
                if (p == null) continue;
                // Kafka client가 동일 record를 delivery timeout까지 재시도한다. 새 UUID를 만들지 않는다.
                kafka.send(new ProducerRecord<>(topic, null, p.event.receivedAt().toEpochMilli(),
                        p.event.channelId(), p.bytes)).get(12, TimeUnit.SECONDS);
                unacked.remove(p);
                acknowledged.incrementAndGet();
                slots.release();
            }
        } catch (Exception e) {
            failed = true;
            // 상세 broker 오류나 입력 bytes를 로그에 남기지 않는다. 미확인 건수는 결과에 보존한다.
        }
    }
    @Override public void checkHealthy() {
        if (failed) throw new SourceFailure("KAFKA_DELIVERY_FAILED", true);
    }
    @Override public synchronized Map<String, Object> finish() {
        if (!finished) {
            closing = true;
            try { worker.join(10000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); failed = true; }
            if (worker.isAlive()) { failed = true; worker.interrupt(); }
            finished = true;
        }
        return Map.of("mode", "KAFKA", "accepted", accepted.get(), "acknowledged", acknowledged.get(),
                "rejected", rejected.get(), "unconfirmed", accepted.get() - acknowledged.get(),
                "complete", !failed && accepted.get() == acknowledged.get() && rejected.get() == 0);
    }
    @Override public void close() { finish(); }
}
