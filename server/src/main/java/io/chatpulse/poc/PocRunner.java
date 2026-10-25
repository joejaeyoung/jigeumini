package io.chatpulse.poc;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public final class PocRunner {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PocRunner.class);
    private final PocProperties properties;
    private final ChatSource source;
    private final EventSink sink;

    public PocRunner(PocProperties properties, ChatSource source, org.springframework.beans.factory.ObjectProvider<EventSink> sinks) {
        this.properties = properties;
        this.source = source;
        this.sink = sinks.getIfAvailable(() -> EventSink.DISCARD);
    }

    public int run() {
        properties.validate();
        var started = Instant.now();
        var stop = new AtomicBoolean();
        var finished = new java.util.concurrent.CountDownLatch(1);
        var shutdown = new Thread(() -> {
            stop.set(true);
            try { finished.await(25, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        var stats = properties.channels().stream().map(ProbeStats::new).toList();
        Path output = properties.output().toAbsolutePath();
        try {
            Files.createDirectories(output.getParent());
            Files.createDirectories(Path.of("work"));
            // 같은 checkout에서 실수로 중복 수집하는 실행을 막는다.
            try (var lockChannel = FileChannel.open(Path.of("work/poc.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    var lock = lockChannel.tryLock()) {
                if (lock == null) {
                    System.err.println("PoC 실행 거부: 같은 checkout의 수집기가 이미 실행 중입니다.");
                    return 2;
                }
                Runtime.getRuntime().addShutdownHook(shutdown);
                try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                    var tasks = new ArrayList<java.util.concurrent.Future<?>>();
                    for (int i = 0; i < properties.channels().size(); i++) {
                        String channel = properties.channels().get(i);
                        ProbeStats channelStats = stats.get(i);
                        tasks.add(workers.submit(() -> new ChannelProbe(source, sink).run(channel,
                                properties.duration(), properties.reconnectAfter(), channelStats, stop)));
                    }
                    try {
                        for (var task : tasks) {
                            boolean done = false;
                            while (!done) {
                                try {
                                    task.get(30, java.util.concurrent.TimeUnit.SECONDS);
                                    done = true;
                                } catch (java.util.concurrent.TimeoutException ignored) {
                                    for (var channelStats : stats) {
                                        var progress = channelStats.snapshot(false);
                                        log.info("progress channel={} chat={} connections={} state={}",
                                                progress.get("channelId"), progress.get("chatCount"),
                                                progress.get("connections"), progress.get("state"));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        stop.set(true);
                        tasks.forEach(task -> task.cancel(true));
                        throw e;
                    }
                }
                var delivery = sink.finish();
                var channels = stats.stream().map(s -> s.snapshot(!properties.reconnectAfter().isZero())).toList();
                boolean passed = Boolean.TRUE.equals(delivery.get("complete")) && !stop.get() && channels.stream().allMatch(s -> "PASS".equals(s.get("verdict")));
                var report = new LinkedHashMap<String, Object>();
                report.put("reportVersion", 1);
                report.put("mode", "LIVE_CHZZK_ANONYMOUS_READ");
                report.put("startedAt", started.toString());
                report.put("completedAt", Instant.now().toString());
                report.put("requestedDurationSeconds", properties.duration().toSeconds());
                report.put("forcedReconnectAfterSeconds", properties.reconnectAfter().toSeconds());
                report.put("verdict", passed ? "PASS" : "FAIL_OR_INCONCLUSIVE");
                report.put("channels", channels);
                report.put("delivery", delivery);
                report.put("limitations", java.util.List.of("No source completeness or upstream dedup guarantee",
                        "DB projection verified separately from collection report", "No raw chat, nickname, user ID or token stored"));
                Files.writeString(output, JsonMapper.builder().build().writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
                System.out.println("PoC " + report.get("verdict") + " report=" + output);
                return passed ? 0 : 2;
            }
        } catch (Exception e) {
            System.err.println("PoC 실행 실패: " + e.getClass().getSimpleName());
            return 2;
        } finally {
            finished.countDown();
            try { Runtime.getRuntime().removeShutdownHook(shutdown); } catch (IllegalStateException ignored) { }
        }
    }
}
