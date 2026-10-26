package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.java_websocket.WebSocket;
import org.java_websocket.enums.Opcode;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class ChannelProbeTextTest {
    private Fixture server;
    private final ProbeStats stats = new ProbeStats("fixture-channel");
    private final List<ChatEvent> events = new CopyOnWriteArrayList<>();
    private final AtomicInteger losses = new AtomicInteger();

    @AfterEach void stopSource() throws Exception { if (server != null) server.stop(1_000); }

    @Test void fullOneMiBUtf8MessagePassesThroughSmallTransportChunksAndHeartbeatsContinue() throws Exception {
        String text = chat(ChatProtocol.MAX_FRAME_BYTES);
        server = new Fixture((socket, attempt) -> socket.send(text));
        runUntil(() -> events.size() == 1 && number("serverPongs") > 0);
        assertThat(number("maxFrameBytes")).isEqualTo(ChatProtocol.MAX_FRAME_BYTES);
        assertThat(commandCount(93101)).isEqualTo(1);
        assertThat(events).hasSize(1);
        assertThat(losses.get()).isZero();
        assertThat(server.commands).contains(100, 0).allMatch(c -> c == 100 || c == 0 || c == 10000);
    }

    @Test void fragmentedWireUtf8AndPartialJsonBecomeExactlyOneProtocolMessage() throws Exception {
        byte[] bytes = chat(32768).getBytes(StandardCharsets.UTF_8);
        int unicode = -1;
        for (int n = 0; n < bytes.length; n++) if ((bytes[n] & 0x80) != 0) { unicode = n; break; }
        int split = unicode + 1;
        server = new Fixture((socket, attempt) -> {
            socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap(bytes, 0, split), false);
            socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap(bytes, split, 10003), false);
            socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap(bytes, split + 10003, bytes.length - split - 10003), true);
            socket.send("{\"cmd\":0}");
        });
        runUntil(() -> events.size() == 1 && server.commands.contains(10000));
        assertThat(number("maxFrameBytes")).isEqualTo(bytes.length);
        assertThat(commandCount(93101)).isEqualTo(1);
        assertThat(events).hasSize(1);
        assertThat(losses.get()).isZero();
    }

    @Test void oversizedUtf8AcrossFragmentsIsARecordedLossWithoutPartialEvents() throws Exception {
        byte[] bytes = chat(ChatProtocol.MAX_FRAME_BYTES + 1).getBytes(StandardCharsets.UTF_8);
        server = new Fixture((socket, attempt) -> {
            int half = bytes.length / 2;
            socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap(bytes, 0, half), false);
            socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap(bytes, half, bytes.length - half), true);
        });
        runUntil(() -> ((Map<?, ?>) stats.snapshot(false).get("reasons")).containsKey("FRAME_TOO_LARGE"));
        assertThat(events).isEmpty();
        assertThat(commandCount(93101)).isZero();
        assertThat(losses.get()).isPositive();
    }

    @Test void disconnectDuringPartialJsonDoesNotContaminateTheNextConnection() throws Exception {
        String text = chat(20000);
        server = new Fixture((socket, attempt) -> {
            if (attempt == 1) {
                socket.sendFragmentedFrame(Opcode.TEXT, ByteBuffer.wrap("{\"cmd\":".getBytes(StandardCharsets.UTF_8)), false);
                socket.close();
            } else socket.send(text);
        });
        runUntil(() -> events.size() == 1);
        assertThat(number("connections")).isEqualTo(2);
        assertThat(events).hasSize(1);
        assertThat(((Map<?, ?>) stats.snapshot(false).get("reasons")).containsKey("INVALID_JSON")).isFalse();
        assertThat(commandCount(93101)).isEqualTo(1);
    }

    private void runUntil(BooleanSupplier reached) throws Exception {
        ChatSource source = new ChatSource() {
            public String resolveRoom(String channel) { return "fixture-room"; }
            public String readToken(String room) { return "TEST_ONLY_TOKEN"; }
            public URI socketUri(String room) { return URI.create("ws://127.0.0.1:" + server.getPort()); }
        };
        var stop = new AtomicBoolean();
        CollectionObserver observer = new CollectionObserver() {
            public void loss(String channel, java.time.Instant from, java.time.Instant to, String reason) { losses.incrementAndGet(); }
        };
        try (var worker = Executors.newSingleThreadExecutor()) {
            EventSink sink = events::addAll;
            var probe = new ChannelProbe(source, Duration.ofMillis(100), Duration.ofSeconds(2),
                    Duration.ofSeconds(30), sink, observer, Clock.systemUTC());
            var task = worker.submit(() -> probe.run("fixture-channel", Duration.ofSeconds(30), Duration.ZERO, stats, stop));
            try { org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(12)).until(reached::getAsBoolean); }
            finally { stop.set(true); task.get(15, TimeUnit.SECONDS); }
        }
    }

    private long number(String key) { return ((Number) stats.snapshot(false).get(key)).longValue(); }
    private long commandCount(int command) {
        Object count = ((Map<?, ?>) stats.snapshot(false).get("commands")).get(Integer.toString(command));
        return count instanceof Number value ? value.longValue() : 0;
    }
    private static String chat(int bytes) {
        String prefix = "{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":" + System.currentTimeMillis() + ",\"msg\":\"";
        String suffix = "\"}]}";
        int payload = bytes - prefix.length() - suffix.length();
        return prefix + "😀한a".repeat(payload / 8) + "a".repeat(payload % 8) + suffix;
    }

    private static final class Fixture extends WebSocketServer {
        private final JsonMapper json = JsonMapper.builder().build();
        final List<Integer> commands = new CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch started = new CountDownLatch(1);
        private final BiConsumer<WebSocket, Integer> script;
        Fixture(BiConsumer<WebSocket, Integer> script) throws Exception {
            super(new InetSocketAddress("127.0.0.1", 0), 2); this.script = script; start();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        }
        public void onStart() { started.countDown(); }
        public void onOpen(WebSocket socket, ClientHandshake handshake) { }
        public void onClose(WebSocket socket, int code, String reason, boolean remote) { }
        public void onError(WebSocket socket, Exception error) { }
        public void onMessage(WebSocket socket, String message) {
            int command = json.readTree(message).path("cmd").asInt(-1); commands.add(command);
            if (command == 100) {
                socket.send("{\"cmd\":10100,\"retCode\":0,\"bdy\":{\"sid\":\"fixture\"}}");
                script.accept(socket, attempts.incrementAndGet());
            } else if (command == 0) socket.send("{\"cmd\":10000}");
        }
    }
}
