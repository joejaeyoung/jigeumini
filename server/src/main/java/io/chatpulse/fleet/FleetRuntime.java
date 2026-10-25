package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy;
import io.chatpulse.metadata.ChzzkMetadataSource;
import io.chatpulse.observations.ObservationPublisher;
import io.chatpulse.operations.RuntimeEnvironment;
import io.chatpulse.pipeline.BufferedKafkaPublisher;
import io.chatpulse.pipeline.KafkaSupport;
import io.chatpulse.pipeline.KafkaTopicPolicy;
import io.chatpulse.poc.ChzzkHttpSource;
import io.chatpulse.poc.CollectionObserver;
import io.chatpulse.quality.CollectionTelemetry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** worker와 함께 살아 있는 발행·품질·시청자 관측 자원 및 종료 결과를 소유한다. */
final class FleetRuntime implements AutoCloseable {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final DataSource dataSource;
    private final Map<String, String> environment;
    private final String bootstrap, topic;
    private final KafkaTopicPolicy policy;
    private final DefaultKafkaProducerFactory<String, byte[]> factory;
    private volatile CollectionTelemetry telemetry;
    private volatile ObservationPublisher observationPublisher;
    private volatile LeasedObservations observations;

    private FleetRuntime(DataSource dataSource, Map<String, String> environment) {
        this.dataSource = dataSource;
        this.environment = environment;
        bootstrap = environment.getOrDefault("PIPELINE_BOOTSTRAP_SERVERS", "localhost:19092");
        topic = environment.getOrDefault("PIPELINE_TOPIC", "chat.received.v1");
        policy = RuntimeEnvironment.policy(environment);
        KafkaSupport.ensureTopic(bootstrap, topic, policy);
        factory = KafkaSupport.producerFactory(bootstrap);
    }

    static int run(CollectorLeases leases, DataSource dataSource, CollectorWorker.Limits limits,
            Map<String, String> environment, Path output) {
        var runtime = new FleetRuntime(dataSource, environment);
        try {
            var kafka = new KafkaTemplate<>(runtime.factory);
            kafka.setProducerListener(null);
            try (var worker = new CollectorWorker(leases, SelectionPolicy.SourceMode.UNOFFICIAL_POC,
                    limits, new ChzzkHttpSource(), kafka, runtime.topic, runtime::observe)) {
                var report = worker.run(result -> runtime.persist(output, result));
                System.out.println(JSON.writeValueAsString(Map.of(
                        "complete", report.get("complete"), "workerId", report.get("workerId"))));
                return Boolean.TRUE.equals(report.get("complete")) ? 0 : 2;
            }
        } finally {
            runtime.close();
        }
    }

    private CollectionObserver observe(BufferedKafkaPublisher publisher, BudgetedChatSource budget) {
        var transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transaction.setTimeout(20);
        telemetry = new CollectionTelemetry(new JdbcTemplate(dataSource), transaction, publisher);
        telemetry.start();
        if (!"true".equals(environment.get("OBSERVATIONS_ENABLED"))) return telemetry;

        String observedTopic = environment.getOrDefault("OBSERVATIONS_TOPIC", "broadcast.observed.v1");
        if (observedTopic.equals(topic) || observedTopic.equals(topic + ".dlq") || observedTopic.equals(topic + ".spikes"))
            throw new IllegalArgumentException("OBSERVATION_TOPIC");
        KafkaSupport.ensureTopic(bootstrap, observedTopic, policy);
        observationPublisher = new ObservationPublisher(bootstrap, observedTopic);
        observations = new LeasedObservations(budget, environment.getOrDefault("PIPELINE_RUN_ID", "live-v1"),
                Integer.parseInt(environment.getOrDefault("OBSERVATIONS_INTERVAL_SECONDS", "60")),
                Integer.parseInt(environment.getOrDefault("OBSERVATIONS_TTL_SECONDS", "120")),
                new ChzzkMetadataSource(budget::reserveFor)::observe, observationPublisher);
        return CollectionObserver.combine(List.of(telemetry, observations));
    }

    // CollectorWorker가 수신·drain·임대 해제를 끝낸 뒤 호출한다. hook의 저장 대기 계약도 worker가 소유한다.
    private void persist(Path output, Map<String, Object> result) {
        try {
            close();
            var diagnostic = new LinkedHashMap<>(result);
            if (observations != null) diagnostic.put("observations", observations.snapshot());
            Files.writeString(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(diagnostic) + "\n");
        } catch (Exception failure) {
            throw new IllegalStateException("FLEET_OUTPUT_FAILED");
        }
    }

    @Override public void close() {
        if (observations != null) observations.close();
        if (observationPublisher != null) observationPublisher.close();
        if (telemetry != null) telemetry.close();
        factory.destroy();
    }
}
