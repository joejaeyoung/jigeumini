package io.chatpulse.quality;

import io.chatpulse.pipeline.*;
import java.util.concurrent.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class QualityRuntime implements AutoCloseable {
    private final ProjectionConsumer trendConsumer;
    private final ProjectionObserver rankingObserver, trendObserver;
    private final RankingStore ranking;
    private final ScheduledExecutorService evaluator = Executors.newSingleThreadScheduledExecutor();
    public QualityRuntime(PipelineProperties p, JdbcTemplate jdbc, TransactionTemplate tx, ProjectionConsumer rankingConsumer,
            RankingStore ranking, String trendReset) {
        String trend = p.projection() + "-trend", group = p.group() + "-trend";
        if (trend.length()>40 || group.length()>100 || !java.util.Set.of("none","earliest").contains(trendReset)) throw new IllegalArgumentException("TREND_CONFIGURATION");
        var store = new ProjectionStore(jdbc,tx,new EventCodec(),trend,p.runId());
        trendConsumer = new ProjectionConsumer(KafkaSupport.consumerFactory(p.bootstrapServers(),group,trendReset),p.topic(),store);
        rankingObserver = new ProjectionObserver(p.bootstrapServers(),jdbc,rankingConsumer,p.projection(),p.runId());
        trendObserver = new ProjectionObserver(p.bootstrapServers(),jdbc,trendConsumer,trend,p.runId());
        this.ranking = ranking;
    }
    public void start() {
        trendConsumer.start(); rankingObserver.start(); trendObserver.start();
        evaluator.scheduleWithFixedDelay(() -> { try { ranking.evaluate(); } catch (RuntimeException ignored) { /* 저장 실패 시 기존 snapshot의 age가 STALE로 내려간다. */ } },1,10,TimeUnit.SECONDS);
    }
    @Override public void close() {
        evaluator.shutdownNow(); rankingObserver.close(); trendObserver.close(); trendConsumer.close();
    }
}
