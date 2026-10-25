package io.chatpulse.operations;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.pipeline.*;
import io.chatpulse.quality.QualityRuntime;
import io.chatpulse.observations.*;
import io.chatpulse.notifications.NotificationConsumer;
import io.chatpulse.categories.CategoryConsumer;
import io.chatpulse.reports.ReportConsumer;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;

@Configuration
@Profile("pipeline")
@ConditionalOnProperty(name="deployment.enabled",havingValue="true")
public class DeploymentConfiguration {
    @Bean(initMethod="start",destroyMethod="close")
    DeploymentHealth deploymentHealth(HikariDataSource ds,PipelineProperties p,KafkaTopicPolicy policy,
            ObservationTopic observations,@Qualifier("notificationTopic") String notifications,ProjectionConsumer ranking,
            QualityRuntime quality,ObservationConsumer observation,NotificationConsumer notification,CategoryConsumer category,ReportConsumer report) {
        return new DeploymentHealth(ds,p.bootstrapServers(),policy,List.of(p.topic(),p.topic()+".dlq",observations.name(),notifications),
                Map.of("ranking",ranking::status,"trend",quality.trendConsumer()::status,"observations",observation::status,"notifications",notification::status,"categories",category::status,"reports",report::status),
                ()->fresh(ranking.pollState().at())&&fresh(quality.trendConsumer().pollState().at()));
    }
    private static boolean fresh(Instant at){return at!=null&&!at.isBefore(Instant.now().minusSeconds(15));}
}
