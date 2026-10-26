package io.chatpulse.poc;

import java.net.URI;
import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

final class ChatConnection implements AutoCloseable {
    private final ChatProtocol protocol = new ChatProtocol();
    private final IncomingText incoming = new IncomingText();
    private final ProbeStats stats;
    private final CollectionObserver observer;
    private final String channel;
    private final UUID sessionId = UUID.randomUUID();
    private final Clock clock;
    private volatile Instant lastAlive;
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final CompletableFuture<SourceFailure> ended = new CompletableFuture<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private volatile WebSocketSession socket;
    private final Object sendLock = new Object();
    private long lastPingNano = System.nanoTime();
    private long awaitingPongNano;
    private final Duration pingInterval;
    private final Duration pongTimeout;
    private final java.util.concurrent.ScheduledFuture<?> heartbeat;

    ChatConnection(ChatSocketClient client, ScheduledExecutorService scheduler,
            URI uri, String channel, String room, String token, ProbeStats stats,
            Duration pingInterval, Duration pongTimeout, EventSink sink, CollectionObserver observer, Clock clock) throws Exception {
        this.clock=clock;this.lastAlive=clock.instant();
        this.stats = stats; this.observer = observer; this.channel = channel;
        this.pingInterval = pingInterval;
        this.pongTimeout = pongTimeout;
        var handler = new TextWebSocketHandler() {
            @Override public boolean supportsPartialMessages() { return true; }
            @Override
            public void afterConnectionEstablished(WebSocketSession session) throws Exception {
                if (!active.get()) { session.close(); return; }
                socket = session;
                session.setTextMessageSizeLimit(ChatSocketClient.TEXT_CHUNK_CHARACTERS);
                send(protocol.connect(room, token));
            }
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                if (!active.get()) return;
                try {
                    var complete = incoming.accept(message.getPayload(), message.isLast());
                    if (complete == null) return;
                    int byteCount = complete.bytes();
                    var frame = protocol.decode(complete, channel, room, sessionId, clock.instant());
                    if (frame.command() == 10100 && !ready.isDone()) {
                        stats.connected();
                        observer.connected(channel, sessionId, clock.instant());
                        ready.complete(null);
                    }
                    if (frame.command() == 0) send(ChatProtocol.pong());
                    if (frame.command() == 10000) {
                        synchronized (sendLock) { awaitingPongNano = 0; }
                        stats.pong();
                    }
                    if (!frame.events().isEmpty() && !ready.isDone()) {
                        throw new SourceFailure("CHAT_BEFORE_HANDSHAKE", false);
                    }
                    lastAlive = clock.instant();
                    observer.alive(sessionId, lastAlive);
                    if (frame.skipped().keySet().stream().anyMatch(key -> key.startsWith("INVALID_"))) {
                        observer.loss(channel, lastAlive.minusMillis(1), lastAlive, "INVALID_MESSAGE");
                    }
                    stats.accept(frame, byteCount);
                    if (!frame.events().isEmpty()) sink.accept(frame.events());
                } catch (SourceFailure e) { fail(e); }
                catch (Exception e) { fail(new SourceFailure("FRAME_PROCESSING_OR_SEND", false)); }
            }
            @Override
            public void handleTransportError(WebSocketSession session, Throwable error) {
                fail(new SourceFailure("WS_TRANSPORT", false));
            }
            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                fail(new SourceFailure("WS_CLOSED", false));
            }
        };
        var connecting = client.connect(handler, uri);
        try {
            socket = connecting.get(10, TimeUnit.SECONDS);
            ready.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            active.set(false);
            incoming.clear();
            stats.disconnected();
            connecting.thenAccept(session -> { try { session.close(); } catch (Exception ignored) { } });
            closeSocket();
            SourceFailure failure = ended.getNow(null);
            if (failure != null) throw failure;
            throw new SourceFailure("HANDSHAKE_TIMEOUT_OR_CONNECT", false);
        }
        heartbeat = scheduler.scheduleAtFixedRate(this::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (!active.get()) return;
        try {
            synchronized (sendLock) {
                long now = System.nanoTime();
                if (awaitingPongNano != 0 && now - awaitingPongNano > pongTimeout.toNanos()) {
                    fail(new SourceFailure("HEARTBEAT_TIMEOUT", false));
                } else if (awaitingPongNano == 0 && now - lastPingNano >= pingInterval.toNanos()) {
                    awaitingPongNano = now;
                    lastPingNano = now;
                    send(ChatProtocol.ping());
                    stats.ping();
                }
            }
        } catch (Exception e) { fail(new SourceFailure("HEARTBEAT_SEND", false)); }
    }

    private void send(String text) throws Exception {
        synchronized (sendLock) {
            if (active.get() && socket != null && socket.isOpen()) socket.sendMessage(new TextMessage(text));
        }
    }

    private void fail(SourceFailure failure) {
        active.set(false);
        incoming.clear();
        if (ended.complete(failure)) {
            observer.ended(sessionId, clock.instant(), failure.getMessage());
            if (failure.getMessage().contains("FRAME") || failure.getMessage().startsWith("INVALID_") || failure.getMessage().contains("PUBLISH") || failure.getMessage().contains("KAFKA")) {
                observer.loss(channel, lastAlive, clock.instant(), failure.getMessage());
            }
        }
        ready.completeExceptionally(failure);
    }

    SourceFailure failure() { return ended.getNow(null); }

    @Override
    public void close() {
        active.set(false);
        incoming.clear();
        stats.disconnected();
        observer.ended(sessionId, clock.instant(), "CLOSED");
        heartbeat.cancel(false);
        closeSocket();
    }

    private void closeSocket() {
        try { if (socket != null && socket.isOpen()) socket.close(CloseStatus.NORMAL); }
        catch (Exception ignored) { }
    }
}
