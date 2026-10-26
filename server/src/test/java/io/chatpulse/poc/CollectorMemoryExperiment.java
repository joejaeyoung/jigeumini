package io.chatpulse.poc;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.tomcat.websocket.AsyncChannelGroupUtil;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import tools.jackson.databind.json.JsonMapper;

/** 합성 loopback READ 연결만 사용한다. Kafka/DB·API 부하·기대 이벤트 저장소는 없다. */
public final class CollectorMemoryExperiment {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !List.of("connections", "lifecycle").contains(args[0]))
            throw new IllegalArgumentException("COLLECTOR_MEMORY_ARGUMENTS");
        int channels = args[0].equals("connections") ? 32 : 8;
        int rounds = args[0].equals("connections") ? 2 : 3;
        Path output = Path.of(args[1]);
        if (Files.exists(output)) throw new IllegalArgumentException("COLLECTOR_MEMORY_OUTPUT_EXISTS");
        Files.createDirectories(output.toAbsolutePath().getParent());
        var report = new LinkedHashMap<String, Object>();
        var samples = new ArrayList<Map<String, Object>>();
        report.put("scope", "LOCAL_WEBSOCKET_ONLY_NO_HTTP_KAFKA_DB_OR_ORACLE");
        report.put("profile", args[0]); report.put("startedAt", Instant.now().toString());
        report.put("channels", channels); report.put("rounds", rounds);
        report.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("tomcat", AsyncChannelGroupUtil.class.getPackage().getImplementationVersion());
        report.put("java", System.getProperty("java.version")); report.put("os", System.getProperty("os.name"));
        report.put("processors", Runtime.getRuntime().availableProcessors());
        report.put("garbageCollectors", ManagementFactory.getGarbageCollectorMXBeans().stream().map(b -> b.getName()).toList());
        report.put("samples", samples); report.put("correct", false);
        save(output, report);
        boolean correct = true;
        var server = new Fixture();
        try {
            server.start(); require(server.started.await(5, TimeUnit.SECONDS), "SOURCE_START");
            Map<String, Object> baseline = sample("BASELINE", 0, server);
            samples.add(baseline); save(output, report); int originalUsers = groupUsers();
            for (int round = 1; round <= rounds; round++) {
                var stop = new AtomicBoolean();
                var ready = new AtomicInteger();
                var stats = new ArrayList<ProbeStats>();
                var tasks = new ArrayList<Future<?>>();
                ChatSource source = new ChatSource() {
                    public String resolveRoom(String channel) { return "fixture-room"; }
                    public String readToken(String room) { return "TEST_ONLY_TOKEN"; }
                    public URI socketUri(String room) { return URI.create("ws://127.0.0.1:" + server.getPort()); }
                };
                CollectionObserver observer = new CollectionObserver() {
                    public void connected(String channel, java.util.UUID session, Instant at) { ready.incrementAndGet(); }
                };
                try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                    try {
                        for (int n = 0; n < channels; n++) {
                            String channel = "fixture-" + n;
                            var stat = new ProbeStats(channel); stats.add(stat);
                            tasks.add(workers.submit(() -> new ChannelProbe(source, Duration.ofMillis(100),
                                    Duration.ofSeconds(2), Duration.ofSeconds(30), observer)
                                    .run(channel, Duration.ofSeconds(30), Duration.ZERO, stat, stop)));
                        }
                        require(await(() -> ready.get() == channels && server.connections.get() == channels
                                && stats.stream().allMatch(s -> ((Number) s.snapshot(false).get("serverPongs")).longValue() > 0),
                                Duration.ofSeconds(15)), "CONNECTIONS_OR_HEARTBEATS");
                        samples.add(sample("CONNECTED", round, server)); save(output, report);
                    } finally {
                        stop.set(true);
                        for (Future<?> task : tasks) task.get(15, TimeUnit.SECONDS);
                    }
                }
                require(await(() -> server.connections.get() == 0, Duration.ofSeconds(5)), "SOCKETS_REMAIN");
                // destroy 직후 비동기 IO thread 종료까지 유한 대기하되 숫자만 기록한다.
                await(() -> ioThreads() == 0, Duration.ofSeconds(3));
                samples.add(sample("CLOSED", round, server)); save(output, report);
                correct &= groupUsers() == originalUsers && server.protocolErrors.get() == 0;
                correct &= stats.stream().allMatch(s -> ((Number) s.snapshot(false).get("connections")).longValue() == 1);
            }
            report.put("correct", correct);
            if (!correct) report.put("failureCode", "OWNED_IO_GROUP_NOT_RELEASED_OR_PROTOCOL");
        } catch (Exception failure) {
            report.put("failureCode", failure instanceof CheckFailure ? failure.getMessage() : "EXPERIMENT_FAILED");
        } finally {
            server.stop(1_000);
            report.put("finishedAt", Instant.now().toString()); save(output, report);
        }
        if (!Boolean.TRUE.equals(report.get("correct"))) System.exit(2);
    }

    private static Map<String, Object> sample(String phase, int round, Fixture server) throws Exception {
        long gcBefore = gcCount(); System.gc();
        boolean gcObserved = await(() -> gcCount() > gcBefore, Duration.ofSeconds(2));
        var os = ManagementFactory.getOperatingSystemMXBean();
        long fd = os instanceof com.sun.management.UnixOperatingSystemMXBean unix ? unix.getOpenFileDescriptorCount() : -1;
        return Map.of("phase", phase, "round", round, "at", Instant.now().toString(), "gcObserved", gcObserved,
                "heapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(),
                "liveThreads", ManagementFactory.getThreadMXBean().getThreadCount(), "ioThreads", ioThreads(),
                "openFileDescriptors", fd, "sourceConnections", server.connections.get(), "ioGroupUsers", groupUsers());
    }

    private static long gcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(b -> Math.max(0, b.getCollectionCount())).sum();
    }
    private static long ioThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(t -> t.isAlive() && t.getName().startsWith("WebSocketClient-AsyncIO-")).count();
    }
    private static int groupUsers() throws Exception {
        // 시험 전용: 고정 Tomcat 버전의 공유 group 등록 해제를 실제 연결 수명에서 확인한다.
        var field = AsyncChannelGroupUtil.class.getDeclaredField("usageCount"); field.setAccessible(true);
        return field.getInt(null);
    }
    private static boolean await(BooleanSupplier ready, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!ready.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        return ready.getAsBoolean();
    }
    private static void require(boolean condition, String code) { if (!condition) throw new CheckFailure(code); }
    private static void save(Path output, Map<String, Object> report) throws Exception { Files.writeString(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n"); }
    private static final class CheckFailure extends RuntimeException { CheckFailure(String code) { super(code); } }

    private static final class Fixture extends WebSocketServer {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger connections = new AtomicInteger();
        final AtomicInteger protocolErrors = new AtomicInteger();
        Fixture() { super(new InetSocketAddress("127.0.0.1", 0), 2); }
        public void onStart() { started.countDown(); }
        public void onOpen(WebSocket socket, ClientHandshake handshake) { connections.incrementAndGet(); }
        public void onClose(WebSocket socket, int code, String reason, boolean remote) { connections.decrementAndGet(); }
        public void onError(WebSocket socket, Exception error) { protocolErrors.incrementAndGet(); }
        public void onMessage(WebSocket socket, String message) {
            var frame = JSON.readTree(message); int command = frame.path("cmd").asInt(-1);
            if (command == 100 && frame.path("bdy").path("uid").isNull() && "READ".equals(frame.path("bdy").path("auth").asText()))
                socket.send("{\"cmd\":10100,\"retCode\":0,\"bdy\":{\"sid\":\"fixture\"}}");
            else if (command == 0) socket.send("{\"cmd\":10000}");
            else protocolErrors.incrementAndGet();
        }
    }
}
