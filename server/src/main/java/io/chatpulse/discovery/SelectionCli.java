package io.chatpulse.discovery;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** 선정 미리보기만 수행한다. 결과가 수집기 시작이나 채팅 접근 승인이 되지 않는다. */
public final class SelectionCli {
    public record Configuration(SelectionPolicy policy, BroadcastCatalog.Limits limits) {
        public Configuration {
            if (policy == null || limits == null) throw new IllegalArgumentException("INVALID_CONFIGURATION");
        }
    }
    private SelectionCli() { }
    public static int run(String[] args) {
        if (org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext context)
            context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        return run(args, System.getenv());
    }
    static int run(String[] args, Map<String,String> environment) {
        var json = FixtureBroadcastCatalog.JSON;
        try {
            boolean fixture = args.length == 4 && args[0].equals("fixture");
            boolean official = args.length == 3 && args[0].equals("official");
            if (!fixture && !official) throw new IllegalArgumentException("INVALID_COMMAND");
            Path output = Path.of(args[args.length - 1]);
            if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) throw new IllegalArgumentException("OUTPUT_EXISTS");
            Path parent = output.toAbsolutePath().getParent();
            if (!Files.isDirectory(parent) || !Files.isWritable(parent)) throw new IllegalArgumentException("OUTPUT_PARENT_UNAVAILABLE");
            var config = json.readValue(FixtureBroadcastCatalog.read(Path.of(args[1]), 1_048_576), Configuration.class);
            BroadcastCatalog catalog;
            if (fixture) {
                if (config.policy().sourceMode() != SelectionPolicy.SourceMode.SYNTHETIC)
                    throw new IllegalArgumentException("SYNTHETIC_POLICY_REQUIRED");
                catalog = new FixtureBroadcastCatalog(Path.of(args[2]));
            } else {
                if (!"true".equals(environment.get("SELECTION_OFFICIAL_ENABLED")))
                    throw new IllegalArgumentException("OFFICIAL_DISCOVERY_DISABLED");
                if (config.policy().sourceMode() == SelectionPolicy.SourceMode.SYNTHETIC)
                    throw new IllegalArgumentException("REAL_SOURCE_POLICY_REQUIRED");
                catalog = new OfficialBroadcastCatalog(environment.get("CHZZK_CLIENT_ID"), environment.get("CHZZK_CLIENT_SECRET"));
            }
            var snapshot = catalog.scan(config.limits());
            // 합성 fixture는 저장된 종료 시각을 평가 시계로 사용해 같은 입력의 결과를 재현한다.
            var report = config.policy().evaluate(snapshot, fixture ? snapshot.finishedAt() : Instant.now());
            String document = json.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n";
            Files.writeString(output, document, StandardOpenOption.CREATE_NEW);
            System.out.println(json.writeValueAsString(Map.of("scope",report.scope(),"scanComplete",report.scanComplete(),
                    "counts",report.counts(),"activationAllowed",false)));
            return snapshot.complete() ? 0 : 2;
        } catch (Exception failure) {
            // 요청 헤더, 원문 body, 자유 형식 경로/예외를 출력하지 않는다.
            System.out.println("{\"error\":\"SELECTION_COMMAND_FAILED\"}"); return 2;
        }
    }
}
