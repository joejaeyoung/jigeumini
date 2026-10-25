package io.chatpulse.quality;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.math.BigDecimal;
import io.chatpulse.quality.CollectionQuality.Channel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.quality.CollectionTelemetry.ts;

/** 품질 판정·순위·연속 급증 상태를 동일 DB snapshot 안에서 확정한다. */
public final class RankingStore {
    public static final String VERSION = "quality-ranking-v1";
    public record Entry(String channelId, Integer rank, Long count, long referenceCount, double coverage,
            String status, List<String> reasons, Integer previousRank, String previousReason) { }
    public record RankingResponse(String asOf, String evaluatedAt, String snapshotId, int windowSeconds,
            String algorithmVersion, String status, List<Entry> entries, List<Entry> excludedChannels) { }
    public record Spike(String spikeId, String channelId, String evaluatedAt, long count, BigDecimal baseline,
            BigDecimal ratio, String algorithmVersion, boolean valid, String qualityWarning) { }
    public record SpikeResponse(String asOf, String status, List<Spike> events, String nextCursor) { }
    public record ActivityPoint(String from, String to, Long count, long referenceCount, double coverage, String status, List<String> reasons) { }
    public record ActivityResponse(String channelId, String asOf, String from, String to, List<ActivityPoint> points) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String ranking, trend, run, topic;
    private final CollectionQuality quality;
    private final Clock clock;
    private final JsonMapper json = JsonMapper.builder().build();
    public RankingStore(JdbcTemplate jdbc, TransactionTemplate tx, String ranking, String trend, String run, String topic, Clock clock) {
        this.jdbc = jdbc; this.tx = tx; this.ranking = ranking; this.trend = trend; this.run = run; this.topic = topic; this.clock = clock;
        this.quality = new CollectionQuality(jdbc, run, topic, clock);
        tx.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
        tx.setTimeout(20);
    }
    public void evaluate() { evaluate(Instant.ofEpochSecond(Math.floorDiv(clock.instant().minusSeconds(7).getEpochSecond(), 10) * 10)); }
    public void evaluate(Instant at) {
        if (at.getNano() != 0 || at.getEpochSecond() % 10 != 0 || at.isAfter(clock.instant())) throw new IllegalArgumentException("EVALUATION_TIME");
        tx.executeWithoutResult(transaction -> {
            Boolean locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(hashtext(?),hashtext(?))", Boolean.class, ranking, run);
            if (!Boolean.TRUE.equals(locked)) return;
            jdbc.execute("SET LOCAL statement_timeout = '5s'");
            var channels = quality.channels();
            reviseQuality(channels);
            for (int window : List.of(60, 300)) snapshot(channels, at, window);
            // trend 소비자는 ranking과 별도 group/ledger/bucket을 사용한다.
            for (var channel : channels) evaluateTrend(channel, at);
        });
    }
    private long count(String projection,String channel,Instant from,Instant to) {
        return jdbc.queryForObject("SELECT coalesce(sum(chat_count),0)::bigint FROM chat_bucket_10s WHERE projection=? AND run_id=? AND channel_id=? AND bucket_start>=? AND bucket_start<?",
                Long.class,projection,run,channel,ts(from),ts(to));
    }
    private void snapshot(List<Channel> channels,Instant at,int window) {
        UUID id = UUID.randomUUID();
        int inserted = jdbc.update("INSERT INTO ranking_snapshot(snapshot_id,projection,run_id,window_sec,evaluated_at,algorithm_version) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",id,ranking,run,window,ts(at),VERSION);
        if (inserted == 0) return;
        var entries = new ArrayList<Entry>();
        for (var channel : channels) {
            var health = quality.evaluate(channel,at.minusSeconds(window),at,ranking);
            long count = count(ranking,channel.id(),at.minusSeconds(window),at);
            entries.add(new Entry(channel.id(),null,health.healthy()?count:null,count,health.coverage(),health.status(),health.reasons(),null,null));
        }
        entries.sort(Comparator.comparingLong(Entry::referenceCount).reversed().thenComparing(Entry::channelId));
        int ordinal = 0, rank = 0; Long previousCount = null;
        for (var entry : entries) {
            Integer assigned = null, previousRank = null; String previousReason = "CURRENT_INCOMPLETE";
            if (entry.status.equals("HEALTHY")) {
                ordinal++;
                if (previousCount == null || previousCount != entry.referenceCount) rank = ordinal;
                previousCount = entry.referenceCount; assigned = rank;
                var previous = jdbc.query("""
                        SELECT e.rank,e.status,r.reason FROM ranking_snapshot s JOIN ranking_entry e USING(snapshot_id)
                        LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                        WHERE s.projection=? AND s.run_id=? AND s.window_sec=? AND s.evaluated_at=? AND s.algorithm_version=? AND e.channel_id=?
                        """, (rs,n) -> new Previous((Integer)rs.getObject(1),rs.getString(2),rs.getString(3)),ranking,run,window,ts(at.minusSeconds(60)),VERSION,entry.channelId);
                if (previous.isEmpty()) previousReason = "NO_PREVIOUS_SNAPSHOT";
                else if (!previous.getFirst().status.equals("HEALTHY") || previous.getFirst().revision != null) previousReason = "PREVIOUS_INCOMPLETE";
                else { previousRank=previous.getFirst().rank; previousReason=null; }
            }
            jdbc.update("INSERT INTO ranking_entry VALUES (?,?,?,?,?,?,?,?::jsonb,?,?)",id,entry.channelId,assigned,entry.count,entry.referenceCount,entry.coverage,entry.status,
                    json.writeValueAsString(entry.reasons),previousRank,previousReason);
        }
    }
    private record Previous(Integer rank,String status,String revision) { }
    private void evaluateTrend(Channel channel,Instant at) {
        var states = jdbc.query("SELECT last_evaluated_at,consecutive_hits,last_alert_at FROM channel_alert_state WHERE run_id=? AND channel_id=? AND algorithm_version=? FOR UPDATE",
                (rs,n) -> new TrendPolicy.State(instant(rs,1),rs.getInt(2),instant(rs,3)),run,channel.id(),TrendPolicy.VERSION);
        var previous = states.isEmpty()?null:states.getFirst();
        // 재시작 시 누락 tick을 소급 평가하지 않는다.
        if (previous != null && !at.isAfter(previous.evaluatedAt())) return;
        boolean healthy = quality.evaluate(channel,at.minusSeconds(360),at,trend).healthy();
        long current = count(trend,channel.id(),at.minusSeconds(60),at);
        long baseline = count(trend,channel.id(),at.minusSeconds(360),at.minusSeconds(60));
        var result = TrendPolicy.evaluate(previous,at,current,baseline,healthy);
        var state = result.state();
        jdbc.update("""
                INSERT INTO channel_alert_state VALUES (?,?,?,?,?,?)
                ON CONFLICT(run_id,channel_id,algorithm_version) DO UPDATE SET last_evaluated_at=excluded.last_evaluated_at,
                consecutive_hits=excluded.consecutive_hits,last_alert_at=excluded.last_alert_at
                """,run,channel.id(),TrendPolicy.VERSION,ts(at),state.hits(),ts(state.lastAlertAt()));
        if (result.alert()) jdbc.update("INSERT INTO spike_event VALUES (?,?,?,?,?,?,?,?,NULL) ON CONFLICT DO NOTHING",
                UUID.randomUUID(),run,channel.id(),ts(at),TrendPolicy.VERSION,current,result.baseline(),result.ratio());
    }
    private void reviseQuality(List<Channel> channels) {
        Instant now=clock.instant();
        // 뒤늦게 격리한 입력도 이미 공개된 순위·급상승의 완전성을 무효화한다. 건수는 재계산하지 않는다.
        jdbc.update("""
                INSERT INTO snapshot_quality_revision(snapshot_id,channel_id,reason,revised_at)
                SELECT s.snapshot_id,e.channel_id,'COLLECTION_REVISED',? FROM ranking_snapshot s JOIN ranking_entry e USING(snapshot_id)
                LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                WHERE s.run_id=? AND s.projection=? AND e.status='HEALTHY' AND r.snapshot_id IS NULL AND s.evaluated_at>=?
                  AND EXISTS(SELECT 1 FROM projection_failure f WHERE f.run_id=s.run_id AND f.source_topic=?
                    AND (f.channel_id IS NULL OR f.channel_id=e.channel_id) AND f.affected_from<s.evaluated_at
                    AND f.affected_to>s.evaluated_at-make_interval(secs=>s.window_sec))
                ON CONFLICT DO NOTHING
                """,ts(now),run,ranking,ts(now.minusSeconds(7*86400)),topic);
        jdbc.update("""
                UPDATE spike_event s SET quality_warning='COLLECTION_REVISED'
                WHERE s.run_id=? AND s.quality_warning IS NULL AND s.evaluated_at>=?
                  AND EXISTS(SELECT 1 FROM projection_failure f WHERE f.run_id=s.run_id AND f.source_topic=?
                    AND (f.channel_id IS NULL OR f.channel_id=s.channel_id) AND f.affected_from<s.evaluated_at
                    AND f.affected_to>s.evaluated_at-interval '360 seconds')
                """,run,ts(now.minusSeconds(7*86400)),topic);
        for (var c:channels) {
            var candidates=jdbc.query("""
                    SELECT s.snapshot_id,s.evaluated_at,s.window_sec FROM ranking_snapshot s JOIN ranking_entry e USING(snapshot_id)
                    LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                    WHERE s.run_id=? AND s.projection=? AND e.channel_id=? AND e.status='HEALTHY' AND r.snapshot_id IS NULL AND s.evaluated_at>=?
                    AND (EXISTS (SELECT 1 FROM collection_loss l WHERE l.channel_id=e.channel_id AND l.start_at<s.evaluated_at
                        AND l.end_at>s.evaluated_at-make_interval(secs=>s.window_sec))
                      OR EXISTS (SELECT 1 FROM collection_session x WHERE x.channel_id=e.channel_id AND x.connected_at<s.evaluated_at
                        AND s.evaluated_at<=x.heartbeat_at+interval '30 seconds'
                        AND s.evaluated_at>least(coalesce(x.ended_at,x.heartbeat_at),x.source_alive_at)
                        AND ((x.ended_at IS NOT NULL AND x.end_reason<>'CLOSED') OR (x.ended_at IS NULL AND (x.heartbeat_at<=? OR x.source_alive_at<=?)))))
                    """,(rs,n)->new Candidate(rs.getObject(1,UUID.class),instant(rs,2),rs.getInt(3)),run,ranking,c.id(),ts(now.minusSeconds(7*86400)),ts(now.minusSeconds(15)),ts(now.minusSeconds(30)));
            for(var s:candidates) {
                Instant from=s.at.minusSeconds(s.window);
                if(quality.hasLoss(c.id(),from,s.at) || QualityPolicy.coverage(from,s.at,quality.intervals(c.id(),from,s.at))<.95)
                    jdbc.update("INSERT INTO snapshot_quality_revision VALUES (?,?,?,?) ON CONFLICT DO NOTHING",s.id,c.id(),"COLLECTION_REVISED",ts(now));
            }
            var spikes=jdbc.query("""
                    SELECT spike_id,evaluated_at FROM spike_event s WHERE run_id=? AND channel_id=? AND quality_warning IS NULL AND evaluated_at>=?
                    AND (EXISTS (SELECT 1 FROM collection_loss l WHERE l.channel_id=s.channel_id AND l.start_at<s.evaluated_at AND l.end_at>s.evaluated_at-interval '360 seconds')
                      OR EXISTS (SELECT 1 FROM collection_session x WHERE x.channel_id=s.channel_id AND x.connected_at<s.evaluated_at
                        AND s.evaluated_at<=x.heartbeat_at+interval '30 seconds'
                        AND s.evaluated_at>least(coalesce(x.ended_at,x.heartbeat_at),x.source_alive_at)
                        AND ((x.ended_at IS NOT NULL AND x.end_reason<>'CLOSED') OR (x.ended_at IS NULL AND (x.heartbeat_at<=? OR x.source_alive_at<=?)))))
                    """, (rs,n)->new Candidate(rs.getObject(1,UUID.class),instant(rs,2),360),run,c.id(),ts(now.minusSeconds(7*86400)),ts(now.minusSeconds(15)),ts(now.minusSeconds(30)));
            for(var s:spikes) {
                Instant from=s.at.minusSeconds(360);
                if(quality.hasLoss(c.id(),from,s.at)||QualityPolicy.coverage(from,s.at,quality.intervals(c.id(),from,s.at))<.95)
                    jdbc.update("UPDATE spike_event SET quality_warning='COLLECTION_REVISED' WHERE spike_id=?",s.id);
            }
        }
    }
    private record Candidate(UUID id,Instant at,int window) { }
    public RankingResponse rankings(int window,int limit) {
        return tx.execute(transaction -> {
            var headers=jdbc.query("SELECT snapshot_id,evaluated_at FROM ranking_snapshot WHERE projection=? AND run_id=? AND window_sec=? AND algorithm_version=? ORDER BY evaluated_at DESC LIMIT 1",
                    (rs,n)->new Candidate(rs.getObject(1,UUID.class),instant(rs,2),window),ranking,run,window,VERSION);
            Instant now=clock.instant();
            if(headers.isEmpty())return new RankingResponse(now.toString(),null,null,window,VERSION,"WARMING_UP",List.of(),List.of());
            var header=headers.getFirst();
            var rows=jdbc.query("""
                    SELECT e.*,r.reason revision,c.enabled currently_enabled,c.state current_state,pr.reason previous_revision
                    FROM ranking_entry e JOIN ranking_snapshot s USING(snapshot_id) LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                    LEFT JOIN collection_channel c ON c.channel_id=e.channel_id
                    LEFT JOIN ranking_snapshot ps ON ps.projection=s.projection AND ps.run_id=s.run_id AND ps.window_sec=s.window_sec
                        AND ps.algorithm_version=s.algorithm_version AND ps.evaluated_at=s.evaluated_at-interval '60 seconds'
                    LEFT JOIN snapshot_quality_revision pr ON pr.snapshot_id=ps.snapshot_id AND pr.channel_id=e.channel_id
                    WHERE e.snapshot_id=? ORDER BY e.rank NULLS LAST,e.channel_id
                    """,(rs,n)->entry(rs),header.id);
            var healthy=rows.stream().filter(e->e.status.equals("HEALTHY")).limit(limit).toList();
            var excluded=rows.stream().filter(e->!e.status.equals("HEALTHY")).limit(limit).toList();
            return new RankingResponse(now.toString(),header.at.toString(),header.id.toString(),window,VERSION,
                    !now.isBefore(header.at.plusSeconds(30))?"STALE":rows.stream().anyMatch(e->e.status.equals("HEALTHY"))?"READY":"WARMING_UP",healthy,excluded);
        });
    }
    private Entry entry(ResultSet rs) throws SQLException {
        List<String> reasons=new ArrayList<>(); json.readTree(rs.getString("reasons")).forEach(node->reasons.add(node.asText()));
        String revision=rs.getString("revision");
        String override = !rs.getBoolean("currently_enabled") ? "DISABLED" : "OFFLINE".equals(rs.getString("current_state")) ? "OFFLINE" : null;
        if(override!=null) reasons.add(override);
        if(revision!=null)reasons.add(revision);
        Integer previous=(Integer)rs.getObject("previous_rank"); String previousReason=rs.getString("previous_reason");
        if(rs.getString("previous_revision")!=null) {previous=null;previousReason="PREVIOUS_INCOMPLETE";}
        return new Entry(rs.getString("channel_id"),revision==null&&override==null?(Integer)rs.getObject("rank"):null,
                revision==null&&override==null?(Long)rs.getObject("count"):null,rs.getLong("reference_count"),rs.getDouble("coverage"),override!=null?override:revision==null?rs.getString("status"):"INCOMPLETE",reasons,
                revision==null&&override==null?previous:null,revision==null&&override==null?previousReason:"CURRENT_INCOMPLETE");
    }
    public SpikeResponse spikes(Instant cursorTime,UUID cursorId,int limit) {
        var rows=jdbc.query("""
                SELECT * FROM spike_event WHERE run_id=? AND (evaluated_at,spike_id)<(?,?)
                ORDER BY evaluated_at DESC,spike_id DESC LIMIT ?
                """,(rs,n)->new Spike(rs.getString("spike_id"),rs.getString("channel_id"),instant(rs,"evaluated_at").toString(),rs.getLong("count"),
                        rs.getBigDecimal("baseline"),rs.getBigDecimal("ratio"),rs.getString("algorithm_version"),rs.getString("quality_warning")==null,rs.getString("quality_warning")),
                run,ts(cursorTime),cursorId,limit+1);
        boolean more=rows.size()>limit;
        var page=rows.stream().limit(limit).toList();
        var last=page.isEmpty()?null:page.getLast();
        String cursor=more?last.evaluatedAt+"|"+last.spikeId:null;
        var lastTick=jdbc.queryForObject("SELECT max(last_evaluated_at) FROM channel_alert_state WHERE run_id=? AND algorithm_version=?", java.sql.Timestamp.class,run,TrendPolicy.VERSION);
        String status=lastTick==null?"WARMING_UP":!clock.instant().isBefore(lastTick.toInstant().plusSeconds(30))?"STALE":"READY";
        return new SpikeResponse(clock.instant().toString(),status,page,cursor);
    }
    public ActivityResponse activity(String channel,Instant from,Instant to) {
        return tx.execute(transaction->{
            var points=new ArrayList<ActivityPoint>();
            for(Instant end=from.plusSeconds(60);!end.isAfter(to);end=end.plusSeconds(60)) {
                Instant start=end.minusSeconds(60); var sessions=quality.intervals(channel,start,end);
                double coverage=QualityPolicy.coverage(start,end,sessions); boolean loss=quality.hasLoss(channel,start,end);
                boolean complete=coverage>=.95 && !loss;
                long count=count(ranking,channel,start,end);
                // 최근 완료 구간은 현재 분석 freshness도 필요하다. 과거 구간은 당시 snapshot 품질을 확인한다.
                var snapshot=jdbc.query("""
                        SELECT e.status,r.reason FROM ranking_snapshot s JOIN ranking_entry e USING(snapshot_id)
                        LEFT JOIN snapshot_quality_revision r USING(snapshot_id,channel_id)
                        WHERE s.projection=? AND s.run_id=? AND s.window_sec=60 AND s.evaluated_at=? AND e.channel_id=? AND s.algorithm_version=?
                        """,(rs,n)->rs.getString(1).equals("HEALTHY")&&rs.getString(2)==null,ranking,run,ts(end),channel,VERSION);
                complete=complete&&!snapshot.isEmpty()&&snapshot.getFirst();
                points.add(new ActivityPoint(start.toString(),end.toString(),complete?count:null,count,coverage,complete?"HEALTHY":"INCOMPLETE",
                        complete?List.of():List.of(loss?"KNOWN_LOSS":coverage<.95?"LOW_COVERAGE":"QUALITY_UNCONFIRMED")));
            }
            return new ActivityResponse(channel,clock.instant().toString(),from.toString(),to.toString(),points);
        });
    }
    private static Instant instant(ResultSet rs,int column) throws SQLException { var value=rs.getTimestamp(column);return value==null?null:value.toInstant(); }
    private static Instant instant(ResultSet rs,String column) throws SQLException { var value=rs.getTimestamp(column);return value==null?null:value.toInstant(); }
}
