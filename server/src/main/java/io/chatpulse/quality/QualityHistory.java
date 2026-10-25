package io.chatpulse.quality;

import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** 보고서의 긴 관측 범위를 일정한 SQL 수로 읽는다. 집계나 실시간 알림을 수정하지 않는다. */
public final class QualityHistory {
    public record Minute(Instant from,Instant to,double coverage,boolean healthy,List<String> reasons) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final CollectionQuality quality;
    private final String run,topic,projection;
    public QualityHistory(JdbcTemplate jdbc,TransactionTemplate tx,String run,String topic,String projection,Clock clock) {
        this.jdbc=jdbc;this.tx=tx;this.run=run;this.topic=topic;this.projection=projection;this.quality=new CollectionQuality(jdbc,run,topic,clock);
        tx.setReadOnly(true);tx.setTimeout(20);tx.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }
    public List<Minute> minutes(String channel,Instant from,Instant to) {
        if(from.getNano()!=0||to.getNano()!=0||from.getEpochSecond()%60!=0||to.getEpochSecond()%60!=0||to.isBefore(from))throw new IllegalArgumentException("QUALITY_WINDOW");
        return tx.execute(status->{
            jdbc.execute("SET LOCAL statement_timeout='5s'");
            var coverage=QualityPolicy.minuteCoverage(from,to,quality.intervals(channel,from,to));
            var losses=jdbc.query("""
                    SELECT start_at,end_at FROM collection_loss WHERE channel_id=? AND start_at<? AND end_at>?
                    UNION ALL SELECT affected_from,affected_to FROM projection_failure WHERE run_id=? AND source_topic=?
                      AND (channel_id IS NULL OR channel_id=?) AND affected_from<? AND affected_to>?
                    """,(rs,n)->new QualityPolicy.Interval(rs.getTimestamp(1).toInstant(),rs.getTimestamp(2).toInstant()),channel,Timestamp.from(to),Timestamp.from(from),run,topic,channel,Timestamp.from(to),Timestamp.from(from));
            var lossCoverage=QualityPolicy.minuteCoverage(from,to,losses);
            var healthy=new HashSet<Instant>();
            jdbc.query("""
                    SELECT s.evaluated_at FROM ranking_snapshot s JOIN ranking_entry e USING(snapshot_id)
                    LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                    WHERE s.projection=? AND s.run_id=? AND s.window_sec=60 AND s.algorithm_version=? AND e.channel_id=?
                      AND s.evaluated_at>? AND s.evaluated_at<=? AND e.status='HEALTHY' AND r.snapshot_id IS NULL
                    """,rs->{healthy.add(rs.getTimestamp(1).toInstant().minusSeconds(60));},projection,run,RankingStore.VERSION,channel,Timestamp.from(from),Timestamp.from(to));
            var result=new ArrayList<Minute>();
            for(Instant at=from;at.isBefore(to);at=at.plusSeconds(60)) {
                double observed=coverage.getOrDefault(at,0d);boolean loss=lossCoverage.getOrDefault(at,0d)>0,complete=observed>=.95&&!loss&&healthy.contains(at);
                result.add(new Minute(at,at.plusSeconds(60),observed,complete,complete?List.of():List.of(loss?"KNOWN_LOSS":observed<.95?"LOW_COVERAGE":"QUALITY_UNCONFIRMED")));
            }
            return List.copyOf(result);
        });
    }
}
