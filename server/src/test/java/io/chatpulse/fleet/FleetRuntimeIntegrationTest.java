package io.chatpulse.fleet;

import static org.junit.jupiter.api.Assertions.*;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 실제 패키징 CLI의 조립/종료를 검증한다. 빈 계획이므로 외부 원천 요청은 없다. */
@Tag("integration") @Timeout(120)
class FleetRuntimeIntegrationTest {
    @TempDir Path directory;
    private String schema, topic, url;

    @BeforeEach void prepare() {
        String id = UUID.randomUUID().toString().replace("-", "");
        schema = "fleet_cli_" + id;
        topic = "fleet.cli." + id;
        url = "jdbc:postgresql://localhost:15432/chatpulse?currentSchema=" + schema;
        try (var root = PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse", "chatpulse", "local-chatpulse", false)) {
            new JdbcTemplate(root).execute("CREATE SCHEMA " + schema);
        }
        try (var database = PostgresSupport.openMigrated(url, "chatpulse", "local-chatpulse")) {
            new CollectorLeases(database).apply(new FleetPlan("a".repeat(64), SourceMode.UNOFFICIAL_POC, 0, 0, List.of(), 120));
        }
    }

    @AfterEach void cleanup() throws Exception {
        try (var root = PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse", "chatpulse", "local-chatpulse", false)) {
            new JdbcTemplate(root).execute("DROP SCHEMA " + schema + " CASCADE");
        }
        try (var admin = Admin.create(Map.of("bootstrap.servers", "localhost:19092"))) {
            var existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            var owned = List.of(topic, topic + ".observed").stream().filter(existing::contains).toList();
            if (!owned.isEmpty()) admin.deleteTopics(owned).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Test void packagedWorkerWritesFinalDiagnosticsAndExitsWithObserversEnabledOrDisabled() throws Exception {
        for (boolean enabled : List.of(false, true)) {
            Path output = directory.resolve("result-" + enabled + ".json");
            assertEquals(0, run(output, enabled, topic + ".observed"));
            var report = JsonMapper.builder().build().readTree(Files.readString(output));
            assertTrue(report.path("complete").asBoolean());
            assertTrue(report.path("released").asBoolean());
            assertEquals(0, report.path("httpRequests").asInt());
            assertEquals(0, report.path("delivery").path("accepted").asLong());
            assertEquals(enabled, report.has("observations"));
            if (enabled) {
                assertEquals(0, report.path("observations").path("activeChannels").asInt());
                assertEquals(0, report.path("observations").path("skippedRequests").asLong());
            }
        }
    }

    @Test void observerSetupFailureClosesAlreadyStartedResourcesAndKeepsReservedOutputEmpty() throws Exception {
        Path output = directory.resolve("failed.json");
        assertEquals(2, run(output, true, topic));
        assertEquals(0, Files.size(output));
        try (var database = PostgresSupport.open(url, "chatpulse", "local-chatpulse", true)) {
            assertEquals(0, new JdbcTemplate(database).queryForObject("SELECT count(*) FROM collector_worker", Integer.class));
        }
    }

    @Test void runtimeReturnsWithoutSystemExitOrLeakedNonDaemonExecutors() throws Exception {
        for (boolean failure : List.of(false, true)) {
            Path output = directory.resolve("natural-" + failure + ".json");
            assertEquals(0, run(output, true, failure ? topic : topic + ".observed", false));
            assertEquals(failure ? "2" : "0", Files.readString(Path.of(output + ".returned")));
            if (failure) assertEquals(0, Files.size(output));
            else assertTrue(JsonMapper.builder().build().readTree(Files.readString(output)).path("complete").asBoolean());
        }
    }

    private int run(Path output, boolean observations, String observedTopic) throws Exception {
        return run(output, observations, observedTopic, true);
    }

    private int run(Path output, boolean observations, String observedTopic, boolean packaged) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m"));
        if (packaged) command.addAll(List.of("-jar", "build/libs/chat-pulse-poc.jar", "fleet"));
        else command.addAll(List.of("-cp", System.getProperty("integration.classpath"), FleetRuntimeProcess.class.getName()));
        command.addAll(List.of("worker", "1", "1", output.toString()));
        var builder = new ProcessBuilder(command);
        var env = builder.environment();
        env.putAll(Map.of("PIPELINE_JDBC_URL", url, "PIPELINE_DB_USER", "chatpulse", "PIPELINE_DB_PASSWORD", "local-chatpulse",
                "PIPELINE_BOOTSTRAP_SERVERS", "localhost:19092", "PIPELINE_TOPIC", topic, "PIPELINE_RUN_ID", "test-cli",
                "FLEET_LIVE_ENABLED", "true", "OBSERVATIONS_ENABLED", Boolean.toString(observations), "OBSERVATIONS_TOPIC", observedTopic));
        env.remove("PIPELINE_DB_PASSWORD_FILE");
        env.remove("PIPELINE_SCHEMA_MODE");
        env.put("DEPLOYMENT_ENABLED", "false");
        env.put("PIPELINE_KAFKA_DURABILITY", "LOCAL");
        env.put("PIPELINE_KAFKA_RETENTION_MS", "86400000");
        env.put("PIPELINE_KAFKA_RETENTION_BYTES", "268435456");
        env.put("PIPELINE_KAFKA_SEGMENT_BYTES", "33554432");
        env.put("OBSERVATIONS_INTERVAL_SECONDS", "60");
        env.put("OBSERVATIONS_TTL_SECONDS", "120");
        env.put("FLEET_TEST_RETURNED", output + ".returned");
        var child = builder.redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        try {
            assertTrue(child.waitFor(40, TimeUnit.SECONDS), "CLI가 소유 자원을 종료하지 못함");
            return child.exitValue();
        } finally {
            if (child.isAlive()) child.destroyForcibly().waitFor();
        }
    }
}
