package io.chatpulse.categories;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.observations.ObservationTopic;
import io.chatpulse.pipeline.PipelineProperties;
import io.chatpulse.quality.RankingStore;
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
class CategoryConfiguration {
    @Bean CategoryStore categoryStore(HikariDataSource ds,PipelineProperties p,@Qualifier("readyTopic") String chatTopic,ObservationTopic observationTopic) {
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        return new CategoryStore(new JdbcTemplate(ds),tx,p.runId(),chatTopic,observationTopic.name(),Clock.systemUTC());
    }
    @Bean(initMethod="start",destroyMethod="close")
    CategoryConsumer categoryConsumer(PipelineProperties p, @Qualifier("readyTopic") String chatTopic, ObservationTopic observationTopic, CategoryStore store,
            @Value("${categories.offset-reset:${pipeline.offset-reset:none}}") String reset) {
        return new CategoryConsumer(p.bootstrapServers(),List.of(chatTopic,observationTopic.name()),p.group()+"-categories",reset,store);
    }
    @Bean CategoryQueries categoryQueries(CategoryStore store,RankingStore ranking) { return new CategoryQueries(store,ranking,Clock.systemUTC()); }
}
