package io.chatpulse.quality;

import io.chatpulse.pipeline.ProjectionConsumer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import static io.chatpulse.quality.CollectionTelemetry.ts;

/** 과거 end-offset 표본을 따라잡은 시각을 보존한다. 조용한 partition도 관측한다. */
public final class ProjectionObserver implements AutoCloseable {
    private record Sample(Instant at, long end) { }
    private final Map<TopicPartition, Deque<Sample>> samples = new HashMap<>();
    private final Admin admin;
    private final JdbcTemplate jdbc;
    private final ProjectionConsumer consumer;
    private final String projection, run;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    public ProjectionObserver(String bootstrap, JdbcTemplate jdbc, ProjectionConsumer consumer, String projection, String run) {
        this.admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "request.timeout.ms", 3000, "default.api.timeout.ms", 4000));
        this.jdbc = jdbc; this.consumer = consumer; this.projection = projection; this.run = run;
    }
    public void start() { worker.scheduleWithFixedDelay(() -> { try { observe(); } catch (Exception ignored) { /* 미관측은 정상으로 갱신하지 않는다. */ } }, 0, 1, TimeUnit.SECONDS); }
    public void observe() throws Exception {
        var poll = consumer.pollState();
        if (poll.partitions().isEmpty()) return;
        var request = new HashMap<TopicPartition, OffsetSpec>();
        poll.partitions().forEach(tp -> request.put(tp, OffsetSpec.latest()));
        Instant sampledAt = Instant.now(); // RPC 응답 시각을 표본 시각으로 쓰면 경계 이전 요청이 이후 표본으로 보인다.
        var ends = admin.listOffsets(request).all().get(4, TimeUnit.SECONDS);
        Instant now = Instant.now();
        samples.keySet().retainAll(poll.partitions());
        boolean blocked = !Boolean.TRUE.equals(consumer.status().get("running")) || !Set.of("RUNNING", "WAITING_FOR_EVENTS").contains(consumer.status().get("state"));
        for (var tp : poll.partitions()) {
            var history = samples.computeIfAbsent(tp, key -> new ArrayDeque<>());
            history.addLast(new Sample(sampledAt, ends.get(tp).offset()));
            while (history.size() > 15) history.removeFirst();
            long resolved = jdbc.queryForObject("SELECT coalesce(max(next_resolved_offset),-1) FROM projection_progress WHERE projection=? AND run_id=? AND topic=? AND partition=?",
                    Long.class, projection, run, tp.topic(), tp.partition());
            Instant caught = history.stream().filter(sample -> resolved >= sample.end).map(Sample::at).max(Instant::compareTo).orElse(null);
            jdbc.update("""
                    INSERT INTO projection_observation VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(projection,run_id,topic,partition) DO UPDATE SET observed_at=excluded.observed_at,
                    end_offset=excluded.end_offset, last_caught_up_at=greatest(projection_observation.last_caught_up_at,excluded.last_caught_up_at),
                    poll_heartbeat_at=excluded.poll_heartbeat_at, blocked=excluded.blocked
                    """, projection, run, tp.topic(), tp.partition(), ts(now), ends.get(tp).offset(), ts(caught), ts(poll.at()), blocked);
        }
    }
    @Override public void close() { worker.shutdownNow(); admin.close(java.time.Duration.ofSeconds(3)); }
}
