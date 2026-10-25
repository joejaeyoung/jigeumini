package io.chatpulse.observations;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.metadata.ChzzkMetadataSource;
import io.chatpulse.metadata.PublicObservationSource;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.PocProperties;
import java.time.Clock;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("pipeline")
class ObservationConfiguration {
    @Bean ObservationTopic observationTopic(PipelineProperties pipeline, KafkaTopicPolicy policy,
            @Value("${observations.topic:broadcast.observed.v1}") String topic,
            @Value("${notifications.topic:}") String notificationTopic) {
        if (!topic.matches("[a-zA-Z0-9._-]{1,240}") || topic.equals(pipeline.topic()) || topic.equals(pipeline.topic()+".spikes")
                || topic.equals(pipeline.topic()+".dlq") || topic.equals(notificationTopic)) throw new IllegalArgumentException("OBSERVATION_TOPIC");
        KafkaSupport.ensureTopic(pipeline.bootstrapServers(), topic, policy);
        return new ObservationTopic(topic);
    }
    @Configuration
    @Profile("pipeline")
    @ConditionalOnProperty(name="pipeline.role", havingValue="collector")
    @ConditionalOnProperty(name="observations.enabled", havingValue="true", matchIfMissing=true)
    static class CollectorConfiguration {
        @Bean @ConditionalOnMissingBean(PublicObservationSource.class)
        PublicObservationSource publicObservationSource() { return new ChzzkMetadataSource(); }
        @Bean(destroyMethod="close") ObservationPublisher observationPublisher(PipelineProperties p, ObservationTopic topic) {
            return new ObservationPublisher(p.bootstrapServers(), topic.name());
        }
        @Bean(initMethod="start", destroyMethod="close")
        ObservationCollector observationCollector(PocProperties selected, PipelineProperties pipeline, ObservationPublisher publisher, PublicObservationSource source,
                @Value("${observations.interval-seconds:60}") int interval, @Value("${observations.ttl-seconds:120}") int ttl) {
            selected.validate();
            return new ObservationCollector(selected.channels(), pipeline.runId(), interval, ttl, source::observe, publisher);
        }
    }
    @Bean
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    ObservationStore observationStore(HikariDataSource ds, PipelineProperties pipeline) {
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        return new ObservationStore(new JdbcTemplate(ds), tx, pipeline.runId(), Clock.systemUTC());
    }
    @Bean(initMethod="start", destroyMethod="close")
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    ObservationConsumer observationConsumer(PipelineProperties pipeline, ObservationTopic topic, ObservationStore store,
            @Value("${observations.offset-reset:${pipeline.offset-reset:none}}") String reset) {
        return new ObservationConsumer(pipeline.bootstrapServers(), topic.name(), pipeline.group()+"-observations", reset, store);
    }
    @Bean(destroyMethod="shutdownNow")
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    ScheduledExecutorService observationRetention(ObservationStore store) {
        var worker = Executors.newSingleThreadScheduledExecutor();
        worker.scheduleWithFixedDelay(() -> {
            try { store.cleanup(); }
            catch (RuntimeException ignored) { org.slf4j.LoggerFactory.getLogger(ObservationStore.class).warn("OBSERVATION_CLEANUP_UNAVAILABLE"); }
        }, 60, 60, TimeUnit.SECONDS);
        return worker;
    }
}
