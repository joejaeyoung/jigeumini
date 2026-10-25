package io.chatpulse.quality;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.pipeline.*;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("pipeline")
class QualityConfiguration {
    private TransactionTemplate transaction(HikariDataSource ds) { var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return tx; }
    @Bean(initMethod="start",destroyMethod="close")
    @ConditionalOnProperty(name="pipeline.role",havingValue="collector")
    CollectionTelemetry collectionTelemetry(HikariDataSource ds,BufferedKafkaPublisher publisher) {
        return new CollectionTelemetry(new JdbcTemplate(ds),transaction(ds),publisher);
    }
    @Bean
    @ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
    RankingStore rankingStore(HikariDataSource ds,PipelineProperties p) {
        return new RankingStore(new JdbcTemplate(ds),transaction(ds),p.projection(),p.projection()+"-trend",p.runId(),p.topic(),Clock.systemUTC());
    }
    @Bean(initMethod="start",destroyMethod="close")
    @ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
    QualityRuntime qualityRuntime(HikariDataSource ds,PipelineProperties p,ProjectionConsumer consumer,RankingStore ranking,
            @Value("${quality.trend-offset-reset:${pipeline.offset-reset:none}}") String reset) {
        return new QualityRuntime(p,new JdbcTemplate(ds),transaction(ds),consumer,ranking,reset);
    }
}
