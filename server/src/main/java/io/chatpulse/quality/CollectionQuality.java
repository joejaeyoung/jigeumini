package io.chatpulse.quality;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.springframework.jdbc.core.JdbcTemplate;
import static io.chatpulse.quality.CollectionTelemetry.ts;

/** 수집·버퍼·소비 관측을 읽어 품질을 판정한다. 호출자의 DB 트랜잭션에 참여하며 쓰지 않는다. */
final class CollectionQuality {
    record Channel(String id, boolean enabled, String state, Instant since, Instant updated, UUID collector) { }
    private final JdbcTemplate jdbc;
    private final String run, topic;
    private final Clock clock;

    CollectionQuality(JdbcTemplate jdbc, String run, String topic, Clock clock) {
        this.jdbc = jdbc;
        this.run = run;
        this.topic = topic;
        this.clock = clock;
    }
    List<Channel> channels() {
        return jdbc.query("SELECT channel_id,enabled,state,observed_since,updated_at,collector_id FROM collection_channel ORDER BY channel_id",
                (rs,n) -> new Channel(rs.getString(1),rs.getBoolean(2),rs.getString(3),instant(rs,4),instant(rs,5),rs.getObject(6, UUID.class)));
    }
    List<QualityPolicy.Interval> intervals(String channel, Instant from, Instant to) {
        Instant now = clock.instant();
        return jdbc.query("SELECT connected_at,heartbeat_at,source_alive_at,ended_at FROM collection_session WHERE channel_id=? AND connected_at<? AND (ended_at IS NULL OR ended_at>?)",
                (rs,n) -> {
                    Instant end = instant(rs,4);
                    if (end == null) {
                        end = to;
                        if (!fresh(instant(rs,2),15,now)) end = min(end,instant(rs,2));
                        if (!fresh(instant(rs,3),30,now)) end = min(end,instant(rs,3));
                    }
                    return new QualityPolicy.Interval(instant(rs,1), end);
                }, channel, ts(to), ts(from));
    }
    boolean hasLoss(String channel, Instant from, Instant to) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS(SELECT 1 FROM collection_loss WHERE channel_id=? AND start_at<? AND end_at>?)
                    OR EXISTS(SELECT 1 FROM projection_failure WHERE run_id=? AND source_topic=?
                        AND (channel_id IS NULL OR channel_id=?) AND affected_from<? AND affected_to>?)
                """, Boolean.class, channel,ts(to),ts(from),run,topic,channel,ts(to),ts(from)));
    }
    QualityPolicy.Result evaluate(Channel c, Instant from, Instant to, String projection) {
        Instant now = clock.instant();
        var intervals = intervals(c.id,from,to); boolean loss = hasLoss(c.id,from,to);
        var live = jdbc.query("SELECT heartbeat_at,source_alive_at FROM collection_session WHERE channel_id=? AND collector_id=? AND ended_at IS NULL",
                (rs,n) -> List.of(instant(rs,1),instant(rs,2)), c.id,c.collector);
        boolean sourceFresh = live.stream().anyMatch(s -> fresh(s.get(0),15,now) && fresh(s.get(1),30,now));
        var buffers = jdbc.query("SELECT observed_at,oldest_unacked_at FROM collector_channel_buffer WHERE channel_id=? AND collector_id=?",
                (rs,n) -> new Instant[]{instant(rs,1),instant(rs,2)},c.id,c.collector);
        boolean bufferFresh = !buffers.isEmpty() && fresh(buffers.getFirst()[0],15,now)
                && !buffers.getFirst()[0].isBefore(to);
        boolean bufferLag = bufferFresh && buffers.getFirst()[1] != null && (buffers.getFirst()[1].isBefore(to) || now.isAfter(buffers.getFirst()[1].plusSeconds(10)));
        // 최신 표본을 계속 쫓으면 정상 연속 입력도 끝없이 제외된다. 창 뒤의 첫 안전한 drain을 고정한다.
        var drains = jdbc.query("""
                SELECT observed_at FROM collector_buffer_observation
                WHERE channel_id=? AND collector_id=? AND observed_at>=? AND observed_at<=?
                  AND (oldest_unacked_at IS NULL OR oldest_unacked_at>=?)
                ORDER BY observed_at LIMIT 1
                """,(rs,n)->instant(rs,1),c.id,c.collector,ts(to),ts(now),ts(to));
        bufferFresh = bufferFresh && !drains.isEmpty();
        int partition = Utils.toPositive(Utils.murmur2(c.id.getBytes(java.nio.charset.StandardCharsets.UTF_8))) % 3;
        var observations = jdbc.query("SELECT observed_at,last_caught_up_at,poll_heartbeat_at,blocked FROM projection_observation WHERE projection=? AND run_id=? AND topic=? AND partition=?",
                (rs,n) -> new Observation(instant(rs,1),instant(rs,2),instant(rs,3),rs.getBoolean(4)),projection,run,topic,partition);
        var observation = observations.isEmpty() ? null : observations.getFirst();
        boolean known = observation != null && fresh(observation.observed,15,now) && fresh(observation.poll,15,now);
        // 창 끝 이후 수집 버퍼를 관측한 시점까지의 broker 표본이 DB에 반영돼야 한다.
        // 버퍼 ACK 이후에 DB에 미반영된 이벤트를 오래된 caught-up 관측으로 정상 확정하지 않는다.
        boolean analysisFresh = known && bufferFresh && observation.caught != null
                && !observation.caught.isBefore(drains.getFirst())
                && !now.isAfter(observation.caught.plusSeconds(10));
        return QualityPolicy.evaluate(c.enabled,c.state,c.since,from,to,intervals,loss,fresh(c.updated,15,now),sourceFresh,
                bufferFresh,bufferLag,known,analysisFresh,observation != null && observation.blocked);
    }
    private record Observation(Instant observed, Instant caught, Instant poll, boolean blocked) { }
    private static boolean fresh(Instant value,long seconds,Instant now) { return value!=null&&!value.isAfter(now)&&now.isBefore(value.plusSeconds(seconds)); }
    private static Instant min(Instant a,Instant b) { return a.isBefore(b)?a:b; }
    private static Instant instant(ResultSet rs,int column) throws SQLException { var value=rs.getTimestamp(column);return value==null?null:value.toInstant(); }
}
