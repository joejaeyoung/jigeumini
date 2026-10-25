package io.chatpulse.notifications;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.pipeline.*;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
class NotificationConfiguration {
    private TransactionTemplate tx(HikariDataSource ds){var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);return tx;}
    @Bean String notificationTopic(PipelineProperties p,@Value("${notifications.topic:}") String configured){
        String topic=configured.isEmpty()?p.topic()+".spikes":configured;
        if(!topic.matches("[a-zA-Z0-9._-]{1,240}")||topic.equals(p.topic()))throw new IllegalArgumentException("NOTIFICATION_TOPIC");
        KafkaSupport.ensureTopic(p.bootstrapServers(),topic);return topic;
    }
    @Bean NotificationStore notificationStore(HikariDataSource ds,PipelineProperties p){return new NotificationStore(new JdbcTemplate(ds),tx(ds),p.runId(),Clock.systemUTC());}
    @Bean(initMethod="start",destroyMethod="close")
    SpikeOutbox spikeOutbox(HikariDataSource ds,PipelineProperties p,@Qualifier("notificationTopic") String notificationTopic){return new SpikeOutbox(new JdbcTemplate(ds),tx(ds),p.bootstrapServers(),notificationTopic,p.runId());}
    @Bean(initMethod="start",destroyMethod="close")
    NotificationConsumer notificationConsumer(PipelineProperties p,@Qualifier("notificationTopic") String notificationTopic,NotificationStore store,
            @Value("${notifications.offset-reset:${pipeline.offset-reset:none}}") String reset){
        return new NotificationConsumer(p.bootstrapServers(),notificationTopic,p.group()+"-notifications",reset,store);
    }
}
