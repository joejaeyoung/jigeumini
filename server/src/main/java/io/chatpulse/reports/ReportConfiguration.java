package io.chatpulse.reports;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.observations.ObservationTopic;
import io.chatpulse.pipeline.PipelineProperties;
import io.chatpulse.quality.QualityHistory;
import java.time.Clock;
import java.util.List;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
class ReportConfiguration {
    private static TransactionTemplate tx(HikariDataSource ds) { var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return tx; }
    @Bean ReportStore reportStore(HikariDataSource ds,PipelineProperties p,@Qualifier("readyTopic") String chatTopic,ObservationTopic observations) {
        return new ReportStore(new JdbcTemplate(ds),tx(ds),p.runId(),chatTopic,observations.name(),Clock.systemUTC());
    }
    @Bean(initMethod="start",destroyMethod="close")
    ReportConsumer reportConsumer(PipelineProperties p,@Qualifier("readyTopic") String chatTopic,ObservationTopic observations,ReportStore store,
            @Value("${reports.offset-reset:${pipeline.offset-reset:none}}") String reset) {
        return new ReportConsumer(p.bootstrapServers(),List.of(chatTopic,observations.name()),p.group()+"-reports",reset,store);
    }
    @Bean ReportQueries reportQueries(HikariDataSource ds,PipelineProperties p,@Qualifier("readyTopic") String topic,ReportStore store) {
        return new ReportQueries(store,new QualityHistory(new JdbcTemplate(ds),tx(ds),p.runId(),topic,p.projection(),Clock.systemUTC()),Clock.systemUTC());
    }
}
