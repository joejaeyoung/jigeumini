package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class ChannelProbeTest {
    private FixtureServer server;

    @AfterEach
    void stop() throws Exception { if (server != null) server.stop(1_000); }

    private ChatSource source(boolean changeRoom) {
        var calls = new AtomicInteger();
        return new ChatSource() {
            public String resolveRoom(String channel) { return changeRoom && calls.incrementAndGet() > 1 ? "room2" : "room1"; }
            public String readToken(String room) { return "TEST_ONLY_TOKEN"; }
            public URI socketUri(String room) { return URI.create("ws://127.0.0.1:" + server.getPort()); }
        };
    }

    private Map<String, Object> run(Duration duration, Duration reconnect, boolean changeRoom) {
        var stats = new ProbeStats("fixture-channel");
        new ChannelProbe(source(changeRoom), Duration.ofMillis(100), Duration.ofMillis(300), Duration.ofMillis(200))
                .run("fixture-channel", duration, reconnect, stats, new AtomicBoolean());
        return stats.snapshot(!reconnect.isZero());
    }

    @Test
    void realWebSocketHandshakeHeartbeatsAndForcedReconnectStayReadOnly() throws Exception {
        server = new FixtureServer(true, false);
        var result = run(Duration.ofMillis(3000), Duration.ofMillis(800), false);
        assertThat(result).containsEntry("verdict", "PASS").containsEntry("connections", 2L)
                .containsEntry("chatCount", 2L).containsEntry("chatAfterReconnect", 1L);
        assertThat((Map<?, ?>) result.get("skipped")).hasSize(2);
        assertThat(server.commands).contains(100, 0, 10000).doesNotContain(3101, 5101);
        assertThat(server.anonymousOnly.get()).isTrue();
    }

    @Test
    void missingPongTimesOutEvenWhenChatWasReceived() throws Exception {
        server = new FixtureServer(false, false);
        var result = run(Duration.ofMillis(1000), Duration.ZERO, false);
        assertThat(((Map<?, ?>) result.get("reasons")).containsKey("HEARTBEAT_TIMEOUT")).isTrue();
        assertThat(result).containsEntry("verdict", "FAIL");
    }

    @Test
    void changedRoomTriggersFreshHandshakeAndContinuesCollection() throws Exception {
        server = new FixtureServer(true, false);
        var result = run(Duration.ofMillis(2500), Duration.ZERO, true);
        assertThat(((Map<?, ?>) result.get("reasons")).containsKey("ROOM_CHANGED")).isTrue();
        assertThat(result).containsEntry("connections", 2L).containsEntry("verdict", "PASS");
        assertThat(server.rooms).containsExactly("room1", "room2");
    }

    @Test
    void protocolRejectionStopsWithoutRetryingWithCredentials() throws Exception {
        server = new FixtureServer(true, true);
        var result = run(Duration.ofMillis(1000), Duration.ZERO, false);
        assertThat(result).containsEntry("connections", 0L).containsEntry("attempts", 1L).containsEntry("verdict", "FAIL");
        assertThat(((Map<?, ?>) result.get("reasons")).containsKey("PROTOCOL_REJECTED")).isTrue();
    }

    @Test
    void remoteSocketCloseReconnectsWithoutLosingReadOnlyMode() throws Exception {
        server = new FixtureServer(true, false);
        server.closeFirst.set(true);
        var result = run(Duration.ofMillis(2500), Duration.ZERO, false);
        assertThat(result).containsEntry("connections", 2L).containsEntry("verdict", "PASS");
        var reasons = (Map<?, ?>) result.get("reasons");
        assertThat(reasons.containsKey("WS_CLOSED") || reasons.containsKey("WS_TRANSPORT")).isTrue();
        assertThat(server.anonymousOnly.get()).isTrue();
    }

    @Test
    void cancellationClosesConnectionAndDoesNotReportPass() throws Exception {
        server = new FixtureServer(true, false);
        var stats = new ProbeStats("fixture-channel");
        var stop = new AtomicBoolean();
        try (var worker = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var task = worker.submit(() -> new ChannelProbe(source(false), Duration.ofMillis(100),
                    Duration.ofMillis(300), Duration.ofSeconds(30))
                    .run("fixture-channel", Duration.ofSeconds(30), Duration.ZERO, stats, stop));
            try {
                assertThat(server.handshakeObserved.await(3, TimeUnit.SECONDS)).isTrue();
            } finally { stop.set(true); }
            task.get(3, TimeUnit.SECONDS);
        }
        assertThat(server.closedObserved.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(stats.snapshot(false)).containsEntry("state", "CANCELLED");
        assertThat(stats.snapshot(false).get("verdict")).isNotEqualTo("PASS");
    }

    private static final class FixtureServer extends WebSocketServer {
        private final JsonMapper json = JsonMapper.builder().build();
        private final CountDownLatch started = new CountDownLatch(1);
        private final boolean answerPings;
        private final boolean reject;
        final List<Integer> commands = new CopyOnWriteArrayList<>();
        final List<String> rooms = new CopyOnWriteArrayList<>();
        final AtomicBoolean anonymousOnly = new AtomicBoolean(true);
        final AtomicBoolean closeFirst = new AtomicBoolean();
        final CountDownLatch handshakeObserved = new CountDownLatch(1);
        final CountDownLatch closedObserved = new CountDownLatch(1);

        FixtureServer(boolean answerPings, boolean reject) throws Exception {
            super(new InetSocketAddress("127.0.0.1", 0), 1);
            this.answerPings = answerPings;
            this.reject = reject;
            start();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        }
        @Override public void onStart() { started.countDown(); }
        @Override public void onOpen(WebSocket conn, ClientHandshake handshake) { }
        @Override public void onClose(WebSocket conn, int code, String reason, boolean remote) { closedObserved.countDown(); }
        @Override public void onError(WebSocket conn, Exception exception) { }
        @Override public void onMessage(WebSocket conn, String text) {
            var node = json.readTree(text);
            int command = node.path("cmd").asInt();
            commands.add(command);
            if (command == 100) {
                rooms.add(node.path("cid").asText());
                if (!"READ".equals(node.path("bdy").path("auth").asText())
                        || !node.path("bdy").path("uid").isNull()) anonymousOnly.set(false);
                if (reject) { conn.send("{\"cmd\":10100,\"retCode\":403}"); return; }
                conn.send("{\"cmd\":10100,\"retCode\":0,\"bdy\":{\"sid\":\"fixture-session\"}}");
                conn.send("{\"cmd\":0,\"ver\":\"2\"}");
                long time = System.currentTimeMillis();
                conn.send("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":" + time
                        + ",\"msg\":\"PRIVATE_MESSAGE_CANARY\",\"profile\":\"PRIVATE_PROFILE_CANARY\"},"
                        + "{\"msgTypeCode\":30,\"msgTime\":" + time + "}]}");
                conn.send("{\"cmd\":15101,\"bdy\":{\"messageList\":[{\"msgTypeCode\":1,\"msgTime\":" + time + "}]}}");
                handshakeObserved.countDown();
                if (closeFirst.compareAndSet(true, false)) conn.close();
            } else if (command == 0 && answerPings) conn.send("{\"cmd\":10000,\"ver\":\"2\"}");
        }
    }
}
