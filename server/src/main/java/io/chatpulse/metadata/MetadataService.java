package io.chatpulse.metadata;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import static io.chatpulse.metadata.BroadcastMetadata.*;

/** HTTP 요청을 막지 않는 메모리 캐시. 동시 갱신 4개, 대기 32개, 캐시 256개로 제한한다. */
public final class MetadataService implements AutoCloseable {
    private record Entry(BroadcastMetadata data, Instant retryAt, Instant discardAt) { }
    private final LinkedHashMap<String, Entry> cache = new LinkedHashMap<>(32, .75f, true);
    private final Set<String> pending = new HashSet<>();
    private final MetadataSource source;
    private final Clock clock;
    private final ExecutorService executor;
    private final boolean enabled;
    public MetadataService(boolean enabled) {
        this(new ChzzkMetadataSource(), Clock.systemUTC(), new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(32), new ThreadPoolExecutor.AbortPolicy()), enabled);
    }
    MetadataService(MetadataSource source, Clock clock, ExecutorService executor, boolean enabled) {
        this.source = source; this.clock = clock; this.executor = executor; this.enabled = enabled;
    }
    public synchronized BroadcastMetadata get(String id) {
        if (!id.matches("[a-f0-9]{32}")) throw new IllegalArgumentException("INVALID_CHANNEL_ID");
        if (!enabled) return empty(id, LiveStatus.UNKNOWN, Reason.DISABLED);
        Instant now = clock.instant();
        cache.entrySet().removeIf(e -> !now.isBefore(e.getValue().discardAt));
        Entry entry = cache.get(id);
        if ((entry == null || !now.isBefore(entry.retryAt)) && pending.add(id)) {
            try { executor.execute(() -> refresh(id)); }
            catch (RejectedExecutionException ignored) { pending.remove(id); }
        }
        if (entry == null) return empty(id, LiveStatus.UNKNOWN, Reason.PENDING);
        if (entry.data.freshness() == Freshness.FRESH && !now.isBefore(Instant.parse(entry.data.expiresAt()))) {
            // 제한 상태도 만료 후에는 재확인 전 UNKNOWN. 이전 공개 데이터만 STALE로 돌려준다.
            return entry.data.channelName() == null ? empty(id, LiveStatus.UNKNOWN, Reason.PENDING) : entry.data.stale(Reason.PENDING);
        }
        return entry.data;
    }
    private void refresh(String id) {
        BroadcastMetadata loaded;
        try { loaded = source.fetch(id); }
        catch (Exception ignored) { loaded = empty(id, LiveStatus.UNKNOWN, Reason.UPSTREAM_ERROR); }
        synchronized (this) {
            Instant now = clock.instant();
            Entry old = cache.get(id);
            boolean transientFailure = loaded.reason() == Reason.UPSTREAM_ERROR || loaded.reason() == Reason.RATE_LIMITED;
            Instant retry = now.plusSeconds(loaded.liveStatus() == LiveStatus.UNKNOWN ? 15 : 60);
            Instant discard = now.plus(Duration.ofMinutes(5));
            BroadcastMetadata result = loaded.checked(now, retry);
            if (transientFailure && old != null && old.data.channelName() != null && now.isBefore(old.discardAt)) {
                result = old.data.stale(loaded.reason());
                discard = old.discardAt; // 실패가 반복되어도 공개 정보의 보관 기한을 늘리지 않는다.
            }
            cache.put(id, new Entry(result, retry, discard));
            while (cache.size() > 256) cache.remove(cache.keySet().iterator().next());
            pending.remove(id);
        }
    }
    @Override public void close() { executor.shutdownNow(); }
}
