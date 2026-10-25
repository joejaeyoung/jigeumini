package io.chatpulse.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.db.PostgresSupport;
import io.chatpulse.operations.*;
import org.springframework.beans.factory.annotation.Value;
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
        return new BufferedKafkaPublisher(new KafkaTemplate<>(factory), readyTopic, codec, p.publisherLimits());
    }
    @Bean(destroyMethod = "close")
    HikariDataSource dataSource(PipelineProperties p,KafkaTopicPolicy policy,
            @Value("${pipeline.schema-mode:MIGRATE}") String schemaMode,
            @Value("${pipeline.db-password-file:}") String secretFile,
            @Value("${deployment.recovery-guard:false}") boolean recoveryGuard,
            @Value("${deployment.enabled:false}") boolean deploymentEnabled,
            @Value("${observations.topic:broadcast.observed.v1}") String observationTopic,
            @Value("${notifications.topic:}") String notificationTopic) {
        String password=RuntimeEnvironment.secret(p.dbPassword(),secretFile,deploymentEnabled||schemaMode.equals("VALIDATE"));
        if(!java.util.Set.of("MIGRATE","VALIDATE").contains(schemaMode))throw new IllegalArgumentException("SCHEMA_MODE");
        var ds=schemaMode.equals("VALIDATE")?PostgresSupport.openValidated(p.jdbcUrl(),p.dbUser(),password):PostgresSupport.openMigrated(p.jdbcUrl(),p.dbUser(),password);
        try {
            if(recoveryGuard && p.role().equals("analyzer")) {
                var config=new RuntimeEnvironment(p.jdbcUrl(),p.dbUser(),password,p.bootstrapServers(),p.topic(),observationTopic,
                        notificationTopic.isEmpty()?p.topic()+".spikes":notificationTopic,p.group(),p.projection(),p.runId(),policy);
                try(var planner=new RecoveryPlan(new JdbcTemplate(ds),config)) {planner.requireSafeStartup();}
            }
            return ds;
        } catch(RuntimeException|Error failure){ds.close();throw failure;}
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
