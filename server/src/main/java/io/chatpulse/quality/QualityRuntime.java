package io.chatpulse.quality;

import io.chatpulse.pipeline.*;
import java.util.concurrent.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class QualityRuntime implements AutoCloseable {
    private final ProjectionConsumer trendConsumer;
    private final DeadLetterRecovery trendRecovery;
    private final ProjectionObserver rankingObserver, trendObserver;
    private final RankingStore ranking;
    private final ScheduledExecutorService evaluator = Executors.newSingleThreadScheduledExecutor();
    public QualityRuntime(PipelineProperties p, JdbcTemplate jdbc, TransactionTemplate tx, ProjectionConsumer rankingConsumer,
            RankingStore ranking, String trendReset) {
        String trend = p.projection() + "-trend", group = p.group() + "-trend";
        if (trend.length()>40 || group.length()>100 || !java.util.Set.of("none","earliest").contains(trendReset)) throw new IllegalArgumentException("TREND_CONFIGURATION");
        var store = new ProjectionStore(jdbc,tx,new EventCodec(),trend,p.runId());
        trendRecovery = new DeadLetterRecovery(p.bootstrapServers(),p.topic(),jdbc,trend,p.runId());
        trendConsumer = new ProjectionConsumer(KafkaSupport.consumerFactory(p.bootstrapServers(),group,trendReset),p.topic(),store,trendRecovery);
        rankingObserver = new ProjectionObserver(p.bootstrapServers(),jdbc,rankingConsumer,p.projection(),p.runId());
        trendObserver = new ProjectionObserver(p.bootstrapServers(),jdbc,trendConsumer,trend,p.runId());
        this.ranking = ranking;
    }
    public ProjectionConsumer trendConsumer() { return trendConsumer; }
    public void start() {
        trendConsumer.start(); rankingObserver.start(); trendObserver.start();
        scheduleEvaluation(evaluator, java.time.Clock.systemUTC(), () -> { try { ranking.evaluate(); } catch (RuntimeException ignored) { /* 저장 실패 시 기존 snapshot의 age가 STALE로 내려간다. */ } });
    }
    static void scheduleEvaluation(ScheduledExecutorService scheduler, java.time.Clock clock, Runnable task) {
        class Tick implements Runnable {
            long due;
            void plan() {
                if (scheduler.isShutdown()) return;
                long now = clock.millis();
                due = (Math.floorDiv(now - 8000, 10000) + 1) * 10000 + 8000;
                scheduler.schedule(this, due - now, TimeUnit.MILLISECONDS);
            }
            public void run() {
                if (scheduler.isShutdown()) return;
                long remaining = due - clock.millis();
                if (remaining > 0) { // 예약 후 벽시계가 뒤로 이동했다면 예정 경계를 다시 기다린다.
                    scheduler.schedule(this, remaining, TimeUnit.MILLISECONDS); return;
                }
                try { task.run(); } finally { plan(); }
            }
        }
        new Tick().plan(); // 실행 비용과 벽시계 보정이 다음 회차의 위상에 누적되지 않는다.
    }
    @Override public void close() {
        evaluator.shutdownNow(); rankingObserver.close(); trendObserver.close(); trendConsumer.close(); trendRecovery.close();
    }
}
