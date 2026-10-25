package io.chatpulse.observations;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.metadata.PublicBroadcastObservation;
import io.chatpulse.metadata.PublicBroadcastObservation.Quality;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ObservationCollectorTest {
    private static final String CHANNEL = "a".repeat(32);
    private static final PublicBroadcastObservation LIMITED = new PublicBroadcastObservation(LiveStatus.UNKNOWN, null, null, null, null, Quality.RATE_LIMITED);
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-10-25T12:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
        void advance(long seconds) { now = now.plusSeconds(seconds); }
    }
    static final class QueuedExecutor extends AbstractExecutorService {
        final Queue<Runnable> queue = new ArrayDeque<>(); boolean closed;
        @Override public void execute(Runnable task) { if (closed) throw new RejectedExecutionException(); queue.add(task); }
        void run() { queue.remove().run(); }
        @Override public void shutdown() { closed = true; }
        @Override public List<Runnable> shutdownNow() { closed = true; var remaining = List.copyOf(queue); queue.clear(); return remaining; }
        @Override public boolean isShutdown() { return closed; }
        @Override public boolean isTerminated() { return closed; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return closed; }
    }
    @Test void pendingRequestsAreNotDuplicatedAndTimestampIsFreshResponseTime() {
        var clock = new MutableClock(); var executor = new QueuedExecutor(); var sent = new ArrayList<BroadcastObserved>();
        try (var collector = new ObservationCollector(List.of(CHANNEL), "test-v1", 60, 120,
                id -> new PublicBroadcastObservation(LiveStatus.LIVE, 0L, null, null, null, Quality.OBSERVED), sent::add, clock, executor)) {
            collector.pollDue(); assertTrue(executor.queue.isEmpty()); collector.start(CHANNEL);
            collector.pollDue(); clock.advance(70); collector.pollDue(); assertEquals(1, executor.queue.size());
            executor.run(); assertEquals(clock.instant(), sent.getFirst().observedAt()); assertEquals(0L, sent.getFirst().viewerCount());
            clock.advance(59); collector.pollDue(); assertTrue(executor.queue.isEmpty());
            clock.advance(1); collector.pollDue(); assertEquals(1, executor.queue.size());
            collector.stopped(CHANNEL); executor.run(); assertEquals(1, sent.size());
            clock.advance(60); collector.pollDue(); assertTrue(executor.queue.isEmpty());
        }
        assertTrue(executor.closed); assertTrue(executor.queue.isEmpty());
    }
    @Test void rateLimitBackoffCapsAt900AndResetsAfterFreshSuccess() {
        var clock = new MutableClock(); var executor = new QueuedExecutor(); var sent = new ArrayList<BroadcastObserved>();
        var attempts = new AtomicInteger();
        try (var collector = new ObservationCollector(List.of(CHANNEL), "test-v1", 60, 120,
                id -> attempts.incrementAndGet() <= 6 ? LIMITED : new PublicBroadcastObservation(LiveStatus.OFFLINE, null, null, null, null, Quality.OFFLINE), sent::add, clock, executor)) {
            collector.start(CHANNEL);
            for (int delay : new int[]{60,120,240,480,900,900,60}) {
                collector.start(CHANNEL); collector.pollDue(); executor.run(); clock.advance(delay - 1); collector.pollDue(); assertTrue(executor.queue.isEmpty());
                clock.advance(1);
            }
            assertEquals(7, sent.size()); assertEquals(Quality.OFFLINE, sent.getLast().quality());
        }
    }
    @Test void localRequestSkipDoesNotPublishUpstreamErrorAndWaitsUntilNextInterval() {
        var clock=new MutableClock();var executor=new QueuedExecutor();var sent=new ArrayList<BroadcastObserved>();
        var attempts=new AtomicInteger();
        try(var collector=new ObservationCollector(List.of(CHANNEL),"test-v1",60,120,id->{
            if(attempts.incrementAndGet()==1)throw new io.chatpulse.metadata.PublicObservationSource.Skipped();return LIMITED;
        },sent::add,clock,executor)) {
            collector.start(CHANNEL);collector.pollDue();executor.run();assertTrue(sent.isEmpty());assertEquals(1,collector.skippedRequests());
            clock.advance(59);collector.pollDue();assertTrue(executor.queue.isEmpty());
            clock.advance(1);collector.pollDue();executor.run();assertEquals(1,sent.size());assertEquals(Quality.RATE_LIMITED,sent.getFirst().quality());
        }
    }
    @Test void failuresBecomeMissingObservationsAndCloseCancelsPendingWork() {
        var clock = new MutableClock(); var executor = new QueuedExecutor(); var sent = new ArrayList<BroadcastObserved>();
        var collector = new ObservationCollector(List.of(CHANNEL), "test-v1", 60, 120, id -> null, event -> {
            sent.add(event); throw new IllegalStateException("SYNTHETIC_PRIVATE_FAILURE");
        }, clock, executor);
        collector.start(CHANNEL); collector.pollDue(); executor.run(); assertEquals(Quality.UPSTREAM_ERROR, sent.getFirst().quality());
        clock.advance(60); collector.pollDue(); assertEquals(1, executor.queue.size()); collector.close();
        collector.pollDue(); assertTrue(executor.queue.isEmpty()); assertEquals(1, sent.size());
        assertThrows(IllegalArgumentException.class, () -> new ObservationCollector(List.of(CHANNEL), "test-v1", 59, 120, id -> LIMITED, sent::add, clock, executor));
    }
}
