package io.chatpulse.observations;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.metadata.PublicBroadcastObservation;
import io.chatpulse.metadata.PublicBroadcastObservation.Quality;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** 선정한 채널의 새 HTTP 관측만 발행한다. 대기 중인 채널은 중복 요청하지 않는다. */
public final class ObservationCollector implements AutoCloseable, io.chatpulse.poc.CollectionObserver {
    private final List<String> channels;
    private final String run;
    private final int interval, ttl;
    private final Function<String, PublicBroadcastObservation> source;
    private final Consumer<BroadcastObserved> publisher;
    private final Clock clock;
    private final ExecutorService requests;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Set<String> pending = new HashSet<>();
    private final Set<String> active = ConcurrentHashMap.newKeySet();
    private final Map<String, Instant> next = new HashMap<>();
    private final Map<String, Integer> rateLimits = new HashMap<>();
    private volatile boolean closed;
    public ObservationCollector(List<String> channels, String run, int interval, int ttl,
            Function<String, PublicBroadcastObservation> source, Consumer<BroadcastObserved> publisher) {
        this(channels, run, interval, ttl, source, publisher, Clock.systemUTC(),
                new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20), new ThreadPoolExecutor.AbortPolicy()));
    }
    ObservationCollector(List<String> channels, String run, int interval, int ttl,
            Function<String, PublicBroadcastObservation> source, Consumer<BroadcastObserved> publisher, Clock clock, ExecutorService requests) {
        if (channels.isEmpty() || channels.size() > 20 || channels.stream().distinct().count() != channels.size()
                || run == null || !run.matches("[a-zA-Z0-9_-]{1,80}")
                || channels.stream().anyMatch(id -> id == null || !id.matches("[a-f0-9]{32}")) || interval < 60 || interval > 900 || ttl < interval || ttl > 1800)
            throw new IllegalArgumentException("OBSERVATION_CONFIGURATION");
        this.channels = List.copyOf(channels); this.run = run; this.interval = interval; this.ttl = ttl;
        this.source = source; this.publisher = publisher; this.clock = clock; this.requests = requests;
    }
    public void start() { scheduler.scheduleWithFixedDelay(this::pollDue, 0, 1, TimeUnit.SECONDS); }
    @Override public void start(String channel) { if (channels.contains(channel) && !closed) active.add(channel); }
    @Override public void stopped(String channel) { active.remove(channel); }
    synchronized void pollDue() {
        if (closed) return;
        Instant now = clock.instant();
        for (String channel : channels) {
            if (!active.contains(channel) || now.isBefore(next.getOrDefault(channel, Instant.MIN)) || !pending.add(channel)) continue;
            try { requests.execute(() -> collect(channel)); }
            catch (RejectedExecutionException ignored) { pending.remove(channel); }
        }
    }
    private void collect(String channel) {
        if (closed || !active.contains(channel)) { synchronized (this) { pending.remove(channel); } return; }
        PublicBroadcastObservation data;
        try { data = Objects.requireNonNull(source.apply(channel)); }
        catch (RuntimeException ignored) { data = new PublicBroadcastObservation(LiveStatus.UNKNOWN, null, null, null, null, Quality.UPSTREAM_ERROR); }
        Instant at = clock.instant();
        try {
            if (!closed && active.contains(channel)) publisher.accept(BroadcastObserved.observed(run, channel, at, ttl, data));
        } catch (IllegalArgumentException invalid) {
            org.slf4j.LoggerFactory.getLogger(ObservationCollector.class).warn("OBSERVATION_CONTRACT_INVALID");
        } catch (RuntimeException ignored) {
            // ACK를 확인하지 못한 관측을 완료로 간주하거나 과거로 채우지 않는다. 이력 공백은 TTL로 드러난다.
            org.slf4j.LoggerFactory.getLogger(ObservationCollector.class).warn("OBSERVATION_PUBLISH_UNCONFIRMED");
        } finally {
            synchronized (this) {
                int failures = data.quality() == Quality.RATE_LIMITED ? Math.min(5, rateLimits.getOrDefault(channel, 0) + 1) : 0;
                rateLimits.put(channel, failures);
                long delay = failures == 0 ? interval : Math.max(interval, Math.min(900L, 60L << (failures - 1)));
                next.put(channel, at.plusSeconds(delay)); pending.remove(channel);
            }
        }
    }
    @Override public void close() {
        closed = true; scheduler.shutdownNow(); requests.shutdownNow();
        try { requests.awaitTermination(15, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
