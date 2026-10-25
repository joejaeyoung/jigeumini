package io.chatpulse.poc;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.websocket.ContainerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

public final class ChannelProbe {
    private static final Logger log = LoggerFactory.getLogger(ChannelProbe.class);
    private final ChatSource source;
    private EventSink sink = EventSink.DISCARD;
    private CollectionObserver observer = CollectionObserver.NONE;
    private final Duration pingInterval;
    private final Duration pongTimeout;
    private final Duration roomPollInterval;

    public ChannelProbe(ChatSource source) {
        this(source, Duration.ofSeconds(20), Duration.ofSeconds(10), Duration.ofSeconds(30));
    }

    public ChannelProbe(ChatSource source, EventSink sink) {
        this(source);
        this.sink = sink;
    }

    public ChannelProbe(ChatSource source, EventSink sink, CollectionObserver observer) {
        this(source, sink); this.observer = observer;
    }

    ChannelProbe(ChatSource source, Duration pingInterval, Duration pongTimeout, Duration roomPollInterval) {
        this.source = source;
        this.pingInterval = pingInterval;
        this.pongTimeout = pongTimeout;
        this.roomPollInterval = roomPollInterval;
    }

    ChannelProbe(ChatSource source, Duration pingInterval, Duration pongTimeout, Duration roomPollInterval, CollectionObserver observer) {
        this(source, pingInterval, pongTimeout, roomPollInterval); this.observer = observer;
    }

    public void run(String channel, Duration duration, Duration reconnectAfter, ProbeStats stats, AtomicBoolean stop) {
        observer.start(channel);
        try { runObserved(channel, duration, reconnectAfter, stats, stop); }
        finally { observer.stopped(channel); }
    }

    private void runObserved(String channel, Duration duration, Duration reconnectAfter, ProbeStats stats, AtomicBoolean stop) {
        long start = System.nanoTime();
        long deadline = start + duration.toNanos();
        boolean forced = false;
        var container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxTextMessageBufferSize(ChatProtocol.MAX_FRAME_BYTES);
        container.setDefaultMaxSessionIdleTimeout(60_000);
        container.setAsyncSendTimeout(5_000);
        var client = new StandardWebSocketClient(container);
        client.setUserProperties(java.util.Map.of("org.apache.tomcat.websocket.BLOCKING_SEND_TIMEOUT", 5_000L));
        try (var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            for (int attempt = 1; attempt <= 6 && !stop.get() && System.nanoTime() < deadline; attempt++) {
                stats.attempt();
                try {
                    String room = source.resolveRoom(channel);
                    String token = source.readToken(room);
                    try (var connection = new ChatConnection(client, scheduler, source.socketUri(room), channel,
                            room, token, stats, pingInterval, pongTimeout, sink, observer)) {
                        log.info("channel={} connected attempt={}", channel, attempt);
                        long nextRoomPoll = System.nanoTime() + roomPollInterval.toNanos();
                        while (!stop.get() && System.nanoTime() < deadline) {
                            sink.checkHealthy();
                            SourceFailure failure = connection.failure();
                            if (failure != null) throw failure;
                            if (!forced && !reconnectAfter.isZero() && System.nanoTime() - start >= reconnectAfter.toNanos()) {
                                forced = true;
                                stats.reason("FORCED_RECONNECT");
                                break;
                            }
                            if (System.nanoTime() >= nextRoomPoll) {
                                String currentRoom = source.resolveRoom(channel);
                                nextRoomPoll = System.nanoTime() + roomPollInterval.toNanos();
                                if (!room.equals(currentRoom)) {
                                    stats.reason("ROOM_CHANGED");
                                    break;
                                }
                            }
                            Thread.sleep(100);
                        }
                    }
                } catch (SourceFailure e) {
                    stats.reason(e.getMessage());
                    observer.state(channel, "OFFLINE".equals(e.getMessage()) ? "OFFLINE" : e.terminal() ? "UNAVAILABLE" : "BACKOFF");
                    log.info("channel={} attempt={} reason={}", channel, attempt, e.getMessage());
                    if (e.terminal()) return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stats.reason("INTERRUPTED");
                    return;
                } catch (Exception e) {
                    stats.reason("CONNECTION_FAILURE");
                    log.info("channel={} attempt={} reason=CONNECTION_FAILURE", channel, attempt);
                }
                if (!stop.get() && System.nanoTime() < deadline) {
                    long waitUntil = Math.min(deadline, System.nanoTime()
                            + Duration.ofSeconds(Math.min(30, 1L << (attempt - 1))).toNanos());
                    while (!stop.get() && System.nanoTime() < waitUntil) {
                        try { Thread.sleep(100); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                    }
                }
            }
        }
        if (!stop.get() && System.nanoTime() < deadline) stats.reason("ATTEMPTS_EXHAUSTED");
        else stats.reason(stop.get() ? "CANCELLED" : "DURATION_COMPLETE");
    }
}
