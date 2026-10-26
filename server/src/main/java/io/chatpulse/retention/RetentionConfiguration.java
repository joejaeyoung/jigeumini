package io.chatpulse.retention;

import com.zaxxer.hikari.HikariDataSource;
import io.chatpulse.categories.CategoryStore;
import io.chatpulse.reports.ReportStore;
import io.chatpulse.observations.ObservationStore;
import io.chatpulse.notifications.NotificationStore;
import io.chatpulse.notifications.SpikeOutbox;
import io.chatpulse.pipeline.PipelineProperties;
import io.chatpulse.replay.ReplayRetention;
import java.time.Clock;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static io.chatpulse.retention.RetentionMaintenance.counted;

@Configuration
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
public class RetentionConfiguration {
    @Bean(initMethod="start",destroyMethod="close")
    RetentionMaintenance retentionMaintenance(HikariDataSource ds,PipelineProperties p,CategoryStore categories,ReportStore reports,
            ObservationStore observations,NotificationStore notifications,SpikeOutbox outbox) {
        var jdbc=new JdbcTemplate(ds);jdbc.setQueryTimeout(3);
        var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(8);
        var clock=Clock.systemUTC();
        var ranking=new ProjectionRetention(jdbc,tx,p.projection(),p.runId(),clock);
        var trend=new ProjectionRetention(jdbc,tx,p.projection()+"-trend",p.runId(),clock);
        var replay=new ReplayRetention(jdbc,tx,clock);
        var history=new HistoryRetention(jdbc,tx,p.runId(),clock);
        return new RetentionMaintenance(jdbc,p.runId(),List.of(
                new RetentionMaintenance.Task("ranking",ranking::cleanup),new RetentionMaintenance.Task("trend",trend::cleanup),
                new RetentionMaintenance.Task("replay",replay::cleanup),new RetentionMaintenance.Task("observations",()->counted(observations.cleanup())),
                new RetentionMaintenance.Task("categories",()->counted(categories.cleanup())),new RetentionMaintenance.Task("reports",()->counted(reports.cleanup())),
                new RetentionMaintenance.Task("outbox",()->counted(outbox.cleanup())),new RetentionMaintenance.Task("notifications",()->counted(notifications.cleanup())),
                new RetentionMaintenance.Task("history",history::cleanup)));
    }
}
