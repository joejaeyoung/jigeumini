package io.chatpulse.metadata;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.metadata.BroadcastMetadata.*;

class MetadataServiceTest {
    private final String id = "a".repeat(32);
    private final MutableClock clock = new MutableClock();
    private final ManualExecutor executor = new ManualExecutor();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<BroadcastMetadata> next = new AtomicReference<>(new BroadcastMetadata(id, "합성 채널", "합성 제목", "game", "게임",
            "https://livecloud.pstatic.net/a.jpg", LiveStatus.LIVE, Freshness.FRESH, Reason.NONE, null, null));
    private final MetadataService service = new MetadataService(channel -> { calls.incrementAndGet(); return next.get(); }, clock, executor, true);
    @AfterEach void close() { service.close(); }
    private void warm() { service.get(id); executor.run(); }
    @Test void coldRequestIsNonblockingAndConcurrentReadsShareOneRefresh() {
        for (int i = 0; i < 20; i++) assertThat(service.get(id).reason()).isEqualTo(Reason.PENDING);
        assertThat(executor.queue).hasSize(1);
        executor.run();
        assertThat(service.get(id).liveStatus()).isEqualTo(LiveStatus.LIVE);
        assertThat(calls).hasValue(1);
        clock.advance(59);
        assertThat(service.get(id).freshness()).isEqualTo(Freshness.FRESH);
        assertThat(executor.queue).isEmpty();
    }
    @Test void expiresLiveThenServesBoundedStaleOnTransientFailure() {
        warm(); clock.advance(60);
        var stale = service.get(id);
        assertThat(stale.liveStatus()).isEqualTo(LiveStatus.UNKNOWN);
        assertThat(stale.thumbnailUrl()).isNull();
        next.set(empty(id, LiveStatus.UNKNOWN, Reason.RATE_LIMITED)); executor.run();
        assertThat(service.get(id).freshness()).isEqualTo(Freshness.STALE);
        assertThat(service.get(id).title()).isEqualTo("합성 제목");
        assertThat(service.get(id).reason()).isEqualTo(Reason.RATE_LIMITED);
        clock.advance(15); service.get(id); executor.run();
        clock.advance(225);
        assertThat(service.get(id).title()).isNull();
        assertThat(service.get(id).reason()).isEqualTo(Reason.PENDING);
    }
    @Test void restrictionNotFoundAndInvalidResponseInvalidateCachedPublicData() {
        for (Reason reason : List.of(Reason.RESTRICTED, Reason.NOT_FOUND, Reason.INVALID_RESPONSE)) {
            next.set(new BroadcastMetadata(id, "합성 채널", "합성 제목", null, null, null, LiveStatus.LIVE, Freshness.FRESH, Reason.NONE, null, null));
            clock.advance(301); warm(); clock.advance(60); service.get(id);
            next.set(empty(id, reason == Reason.RESTRICTED ? LiveStatus.RESTRICTED : reason == Reason.NOT_FOUND ? LiveStatus.NOT_FOUND : LiveStatus.UNKNOWN, reason));
            executor.run();
            assertThat(service.get(id).channelName()).isNull();
            assertThat(service.get(id).title()).isNull();
            assertThat(service.get(id).reason()).isEqualTo(reason);
        }
    }
    @Test void failureHasRetryBackoffAndRecovers() {
        next.set(empty(id, LiveStatus.UNKNOWN, Reason.UPSTREAM_ERROR)); warm();
        clock.advance(14); service.get(id); assertThat(executor.queue).isEmpty();
        clock.advance(1); service.get(id); assertThat(executor.queue).hasSize(1);
        next.set(new BroadcastMetadata(id, "합성 채널", null, null, null, null, LiveStatus.OFFLINE, Freshness.FRESH, Reason.NONE, null, null));
        executor.run(); assertThat(service.get(id).liveStatus()).isEqualTo(LiveStatus.OFFLINE);
    }
    @Test void cacheIsBoundedAndRejectedWorkCanRetry() {
        warm();
        for (int i = 1; i <= 256; i++) { service.get(String.format("%032x", i)); executor.run(); }
        assertThat(service.get(id).reason()).isEqualTo(Reason.PENDING);
        executor.run();
        executor.shutdown(); clock.advance(60);
        assertThatCode(() -> service.get(id)).doesNotThrowAnyException();
        assertThat(service.get(id).liveStatus()).isEqualTo(LiveStatus.UNKNOWN);
    }
    @Test void disabledAndInvalidIdsNeverCallUpstream() {
        try (var disabled = new MetadataService(channel -> { throw new AssertionError(); }, clock, new ManualExecutor(), false)) {
            assertThat(disabled.get(id).reason()).isEqualTo(Reason.DISABLED);
            assertThatThrownBy(() -> disabled.get("invalid")).isInstanceOf(IllegalArgumentException.class);
        }
    }
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-10-25T00:00:00Z");
        void advance(long seconds) { now = now.plusSeconds(seconds); }
        public Instant instant() { return now; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
    }
    static final class ManualExecutor extends AbstractExecutorService {
        final Queue<Runnable> queue = new ArrayDeque<>(); boolean stopped;
        void run() { queue.remove().run(); }
        public void execute(Runnable command) { if (stopped) throw new RejectedExecutionException(); queue.add(command); }
        public void shutdown() { stopped = true; }
        public List<Runnable> shutdownNow() { stopped = true; return List.copyOf(queue); }
        public boolean isShutdown() { return stopped; }
        public boolean isTerminated() { return stopped; }
        public boolean awaitTermination(long timeout, TimeUnit unit) { return stopped; }
    }
}
