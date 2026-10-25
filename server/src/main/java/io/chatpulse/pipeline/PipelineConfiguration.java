package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("pipeline")
@EnableConfigurationProperties({PipelineProperties.class, KafkaTopicPolicy.class})
public class PipelineConfiguration {
    @Bean EventCodec eventCodec(PipelineProperties p) { p.validate(); return new EventCodec(); }
    @Bean String readyTopic(PipelineProperties p, KafkaTopicPolicy policy) {
        p.validate();
        KafkaSupport.ensureTopic(p.bootstrapServers(), p.topic(), policy);
        return p.topic();
    }
    @Bean(destroyMethod = "destroy")
    @ConditionalOnProperty(name="pipeline.role", havingValue="collector")
    DefaultKafkaProducerFactory<String, byte[]> producerFactory(PipelineProperties p) {
        return KafkaSupport.producerFactory(p.bootstrapServers());
    }
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name="pipeline.role", havingValue="collector")
    BufferedKafkaPublisher publisher(DefaultKafkaProducerFactory<String, byte[]> factory,
            PipelineProperties p, EventCodec codec, String readyTopic) {
        return new BufferedKafkaPublisher(new KafkaTemplate<>(factory), readyTopic, codec, p.bufferCapacity());
    }
    @Bean(destroyMethod = "close")
    HikariDataSource dataSource(PipelineProperties p) {
        return PostgresSupport.openMigrated(p.jdbcUrl(), p.dbUser(), p.dbPassword());
    }
    @Bean
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    ProjectionStore projectionStore(HikariDataSource ds, EventCodec codec, PipelineProperties p) {
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        tx.setTimeout(20);
        return new ProjectionStore(new JdbcTemplate(ds), tx, codec, p.projection(), p.runId());
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    ProjectionConsumer projectionConsumer(PipelineProperties p, ProjectionStore store, String readyTopic, DeadLetterRecovery recovery) {
        return new ProjectionConsumer(KafkaSupport.consumerFactory(p.bootstrapServers(), p.group(), p.offsetReset()), readyTopic, store, recovery);
    }
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
    DeadLetterRecovery deadLetterRecovery(PipelineProperties p, HikariDataSource ds, String readyTopic, KafkaTopicPolicy policy) {
        return new DeadLetterRecovery(p.bootstrapServers(), readyTopic, new JdbcTemplate(ds), p.projection(), p.runId(), policy);
    }
}
