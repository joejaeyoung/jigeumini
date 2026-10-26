package io.chatpulse.reports;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.observations.BroadcastObserved;
import io.chatpulse.pipeline.*;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.quality.QualityPolicy;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 보고서 전용 멱등 입력·관측 세션·귀속 집계. live 순위/알림을 쓰지 않는다. */
public final class ReportStore implements DualTopicProjection {
    record Stored(ReportSessions.Session session,long revision,Instant updatedAt) { }
    record Minute(Instant at,long count) { }
    record Detail(Stored stored,List<Minute> minutes,List<QualityPolicy.Interval> identityIntervals,long unassigned,long lateCount,boolean fresh) { }
    record Page(List<Stored> reports,boolean truncated,long unassigned,boolean fresh) { }
    private record Input(ConsumerRecord<String,byte[]> record,ChatEvent chat,BroadcastObserved observation) {
        String channel() { return chat!=null?chat.channelId():observation.channelId(); }
    }
    private record Delta(UUID report,Instant bucket) implements Comparable<Delta> {
        public int compareTo(Delta other) { int compared=report.toString().compareTo(other.report.toString());return compared!=0?compared:bucket.compareTo(other.bucket); }
    }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx,read;
    private final String run,chatTopic,observationTopic;
    private final Clock clock;
    private final EventCodec codec=new EventCodec();
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public ReportStore(JdbcTemplate jdbc,TransactionTemplate tx,String run,String chatTopic,String observationTopic,Clock clock) {
        this.jdbc=jdbc;this.tx=tx;this.run=run;this.chatTopic=chatTopic;this.observationTopic=observationTopic;this.clock=clock;
        read=new TransactionTemplate(tx.getTransactionManager());read.setReadOnly(true);read.setTimeout(20);read.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }
    @Override public void bindSources(Map<String,String> identities) {
        tx.executeWithoutResult(status->identities.forEach((topic,id)->{
            jdbc.update("INSERT INTO report_source VALUES (?,?,?) ON CONFLICT DO NOTHING",run,topic,id);
            if(!id.equals(jdbc.queryForObject("SELECT topic_id FROM report_source WHERE run_id=? AND topic=?",String.class,run,topic)))throw new IllegalStateException("REPORT_SOURCE_CHANGED");
        }));
    }
    @Override public void initialize(TopicPartition partition,long offset) {
        jdbc.update("INSERT INTO report_progress VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",run,partition.topic(),partition.partition(),offset,ts(clock.instant()));
    }
    @Override public void apply(List<ConsumerRecord<String,byte[]>> records) {
        var inputs=new ArrayList<Input>();
        for(var record:records) {
            if(record.topic().equals(chatTopic))inputs.add(new Input(record,codec.decode(record.value(),record.key()),null));
            else if(record.topic().equals(observationTopic))inputs.add(new Input(record,null,BroadcastObserved.decode(record.key(),record.value())));
            else throw new IllegalArgumentException("REPORT_INPUT_TOPIC");
        }
        tx.executeWithoutResult(status->{
            jdbc.execute("SET LOCAL statement_timeout='5s'");jdbc.execute("SET LOCAL lock_timeout='3s'");
            jdbc.queryForObject("SELECT pg_advisory_xact_lock_shared(hashtextextended(?,0))",Object.class,"report-cleanup/"+run);
            Instant now=clock.instant(),oldest=now.minusSeconds(7*86400);
            for(String channel:inputs.stream().map(Input::channel).distinct().sorted().toList())
                jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtextextended(?,0))",Object.class,"report/"+run+"/"+channel);
            var affected=new TreeMap<String,Instant[]>();
            // 새 관측의 세션/별칭을 먼저 확정한다. 새 채팅은 최종 관측 귀속으로 바로 삽입한다.
            for(var input:inputs) if(input.observation()!=null) {
                var sample=input.observation();if(!sample.runId().equals(run)||sample.observedAt().isBefore(oldest.minusSeconds(120)))continue;
                if(sample.observedAt().isAfter(now.plusSeconds(5)))throw new IllegalArgumentException("REPORT_FUTURE_TIME");
                String digest=hash(sample.encode());
                int inserted=jdbc.update("INSERT INTO report_observation VALUES (?,?,?,?,?,?,?::jsonb,?,NULL) ON CONFLICT DO NOTHING",run,sample.eventId(),sample.channelId(),ts(sample.observedAt()),Math.min(120,sample.ttlSeconds()),sample.liveStatus()==LiveStatus.LIVE,new String(sample.encode(),java.nio.charset.StandardCharsets.UTF_8),digest);
                if(inserted==0) {
                    if(!digest.equals(jdbc.queryForObject("SELECT payload_hash FROM report_observation WHERE run_id=? AND event_id=?",String.class,run,sample.eventId())))throw new IllegalArgumentException("REPORT_OBSERVATION_CONFLICT");
                } else affect(affected,sample.channelId(),sample.observedAt(),sample.observedAt().plusSeconds(120));
            }
            // 영구 입력 오류를 세션 재구성의 일시적 만료 ID 대기보다 먼저 판정한다.
            for(var input:inputs)if(input.chat()!=null && !input.chat().eventTime().isBefore(oldest)
                    && input.chat().receivedAt().isAfter(now.plusSeconds(5)))throw new IllegalArgumentException("REPORT_FUTURE_TIME");
            var changes=new TreeMap<Delta,Long>();
            for(var channel:affected.entrySet()) {
                var changedObservations=rebuildSessions(channel.getKey(),now,oldest);
                reassign(channel.getKey(),channel.getValue()[0],channel.getValue()[1],oldest,changedObservations,changes);
            }
            var chatChannels=new TreeSet<String>();
            for(var input:inputs) if(input.chat()!=null) {
                var event=input.chat();if(event.eventTime().isBefore(oldest))continue;
                Instant at=event.eventTime().truncatedTo(java.time.temporal.ChronoUnit.MICROS);String digest=codec.hash(event);
                var assigned=insertChat(event,at,oldest,digest);
                if(assigned.isEmpty()) {
                    if(!digest.equals(jdbc.queryForObject("SELECT payload_hash FROM report_event WHERE run_id=? AND event_id=?",String.class,run,event.eventId())))throw new IllegalArgumentException("REPORT_EVENT_CONFLICT");
                } else {delta(changes,assigned.getFirst(),at,1);chatChannels.add(event.channelId());}
            }
            for(String channel:chatChannels)if(!affected.containsKey(channel))refreshStates(channel,now,oldest);
            var changedReports=new HashSet<UUID>();
            for(var entry:changes.entrySet()) {
                if(entry.getValue()==0)continue;var delta=entry.getKey();changedReports.add(delta.report());
                if(entry.getValue()>0)jdbc.update("INSERT INTO report_bucket VALUES (?,?,?,?) ON CONFLICT(run_id,report_id,bucket_start) DO UPDATE SET message_count=report_bucket.message_count+excluded.message_count",run,delta.report(),ts(delta.bucket()),entry.getValue());
                else if(jdbc.update("UPDATE report_bucket SET message_count=message_count+? WHERE run_id=? AND report_id=? AND bucket_start=?",entry.getValue(),run,delta.report(),ts(delta.bucket()))!=1)throw new IllegalStateException("REPORT_BUCKET_MISSING");
            }
            for(UUID report:changedReports)jdbc.update("UPDATE report_session SET revision=revision+1,updated_at=? WHERE run_id=? AND report_id=?",ts(now),run,report);
            var progress=new HashMap<TopicPartition,Long>();for(var input:inputs)progress.merge(new TopicPartition(input.record().topic(),input.record().partition()),input.record().offset()+1,Math::max);
            progress.forEach((partition,offset)->jdbc.update("INSERT INTO report_progress VALUES (?,?,?,?,?) ON CONFLICT(run_id,topic,partition) DO UPDATE SET next_offset=greatest(report_progress.next_offset,excluded.next_offset),updated_at=excluded.updated_at",run,partition.topic(),partition.partition(),offset,ts(now)));
        });
    }
    private List<UUID> rebuildSessions(String channel,Instant now,Instant oldest) {
        var anchors=new HashMap<UUID,UUID>();jdbc.query("SELECT anchor_observation_id,report_id FROM report_session WHERE run_id=? AND channel_id=? AND anchor_observation_id IS NOT NULL",rs->{anchors.put(rs.getObject(1,UUID.class),rs.getObject(2,UUID.class));},run,channel);
        var previous=new HashMap<UUID,UUID>();
        var samples=jdbc.query("""
                SELECT event_id,payload::text,report_id FROM report_observation o WHERE run_id=? AND channel_id=?
                  AND NOT EXISTS (SELECT 1 FROM report_session s WHERE s.run_id=o.run_id AND s.report_id=o.report_id AND s.ended_at<?)
                ORDER BY observed_at,event_id
                """,(rs,n)->{
            previous.put(rs.getObject(1,UUID.class),rs.getObject(3,UUID.class));return BroadcastObserved.decode(channel,rs.getString(2).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        },run,channel,ts(oldest));
        var derived=ReportSessions.derive(samples,anchors,now);var changedObservations=new ArrayList<UUID>();
        for(var session:derived.sessions()) {
            // 부분 정리 중 동일 원천 ID가 다시 오면 정리 완료 뒤 같은 배치를 재시도한다.
            if(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM report_session WHERE run_id=? AND report_id=? AND ended_at<?)",Boolean.class,run,session.id(),ts(oldest))))throw new IllegalStateException("REPORT_EXPIRED_ID_REUSE");
            jdbc.update("""
                INSERT INTO report_session(run_id,report_id,channel_id,anchor_observation_id,observed_from,observed_to,ended_at,payload,updated_at)
                VALUES (?,?,?,?,?,?,?,?::jsonb,?) ON CONFLICT(run_id,report_id) DO UPDATE SET anchor_observation_id=excluded.anchor_observation_id,
                  observed_from=excluded.observed_from,observed_to=excluded.observed_to,ended_at=excluded.ended_at,payload=excluded.payload,
                  revision=report_session.revision+1,updated_at=excluded.updated_at WHERE report_session.payload<>excluded.payload
                """,run,session.id(),channel,session.anchorObservationId(),ts(session.observedFrom()),ts(session.observedTo()),session.endedAt()==null?null:ts(session.endedAt()),JSON.writeValueAsString(session),ts(now));
        }
        for(var sample:samples) {
            var report=derived.observationReports().get(sample.eventId());
            if(!Objects.equals(report,previous.get(sample.eventId()))) {
                jdbc.update("UPDATE report_observation SET report_id=? WHERE run_id=? AND event_id=?",report,run,sample.eventId());changedObservations.add(sample.eventId());
            }
        }
        derived.aliases().forEach((alias,canonical)->jdbc.update("INSERT INTO report_alias VALUES (?,?,?) ON CONFLICT(run_id,alias_id) DO UPDATE SET canonical_id=excluded.canonical_id",run,alias,canonical));
        return changedObservations;
    }
    private void reassign(String channel,Instant from,Instant to,Instant oldest,List<UUID> changedObservations,Map<Delta,Long> changes) {
        var affected=new ArrayList<Object>(List.of(ts(oldest),run,channel,ts(from.isBefore(oldest)?oldest:from),ts(to)));
        String identityChanges="";
        if(!changedObservations.isEmpty()) { identityChanges=" OR e.observation_id=ANY(CAST(? AS uuid[]))";affected.add("{"+String.join(",",changedObservations.stream().map(UUID::toString).toList())+"}"); }
        affected.add(run);
        jdbc.query("""
                WITH assigned AS (
                  SELECT e.event_id,e.report_id old_report,
                    CASE WHEN m.live AND e.event_time<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN m.report_id END new_report,
                    CASE WHEN m.live AND e.event_time<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN m.event_id END new_observation
                  FROM report_event e LEFT JOIN LATERAL (SELECT * FROM report_observation m WHERE m.run_id=e.run_id AND m.channel_id=e.channel_id
                    AND m.observed_at<=e.event_time AND NOT EXISTS(SELECT 1 FROM report_session s WHERE s.run_id=m.run_id AND s.report_id=m.report_id AND s.ended_at<?)
                    ORDER BY m.observed_at DESC,m.event_id DESC LIMIT 1) m ON true
                  WHERE e.run_id=? AND e.channel_id=? AND ((e.event_time>=? AND e.event_time<?)
                """+identityChanges+"""
                  )
                ), changed AS (
                  UPDATE report_event e SET report_id=a.new_report,observation_id=a.new_observation FROM assigned a
                  WHERE e.run_id=? AND e.event_id=a.event_id AND (e.report_id IS DISTINCT FROM a.new_report OR e.observation_id IS DISTINCT FROM a.new_observation)
                  RETURNING e.event_time,a.old_report,a.new_report
                ) SELECT event_time,old_report,new_report FROM changed
                """,rs->{var at=rs.getTimestamp(1).toInstant();delta(changes,rs.getObject(2,UUID.class),at,-1);delta(changes,rs.getObject(3,UUID.class),at,1);},affected.toArray());
    }
    private List<UUID> insertChat(ChatEvent event,Instant at,Instant oldest,String digest) {
        return jdbc.query("""
                INSERT INTO report_event(run_id,event_id,channel_id,event_time,received_at,payload_hash,late,observation_id,report_id)
                SELECT ?,?,?,?,?,?,?,
                  CASE WHEN m.live AND ?<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN m.event_id END,
                  CASE WHEN m.live AND ?<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN m.report_id END
                FROM (VALUES(1)) seed(dummy) LEFT JOIN LATERAL (
                  SELECT m.* FROM report_observation m WHERE m.run_id=? AND m.channel_id=? AND m.observed_at<=?
                    AND NOT EXISTS(SELECT 1 FROM report_session s WHERE s.run_id=m.run_id AND s.report_id=m.report_id AND s.ended_at<?)
                  ORDER BY m.observed_at DESC,m.event_id DESC LIMIT 1
                ) m ON true
                ON CONFLICT DO NOTHING RETURNING report_id
                """,(rs,n)->rs.getObject(1,UUID.class),run,event.eventId(),event.channelId(),ts(at),
                ts(event.receivedAt().truncatedTo(java.time.temporal.ChronoUnit.MICROS)),digest,!event.eventTime().isAfter(event.receivedAt().minusSeconds(120)),
                ts(at),ts(at),run,event.channelId(),ts(at),ts(oldest));
    }
    private void refreshStates(String channel,Instant now,Instant oldest) {
        // 관측이 그대로인 배치에서도 TTL에 따른 저장 상태·revision·updatedAt 계약을 유지한다.
        jdbc.update("""
                WITH latest AS (
                  SELECT s.report_id,CASE WHEN s.ended_at IS NOT NULL THEN 'OFFLINE'
                    WHEN m.live AND ?<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN 'LIVE' ELSE 'UNKNOWN' END state
                  FROM report_session s JOIN LATERAL (
                    SELECT observed_at,ttl_seconds,live FROM report_observation o WHERE o.run_id=s.run_id AND o.report_id=s.report_id
                    ORDER BY observed_at DESC,event_id DESC LIMIT 1
                  ) m ON true
                  WHERE s.run_id=? AND s.channel_id=? AND (s.ended_at IS NULL OR s.ended_at>=?)
                ) UPDATE report_session s SET payload=jsonb_set(s.payload,'{state}',to_jsonb(latest.state)),
                    revision=s.revision+1,updated_at=? FROM latest
                  WHERE s.run_id=? AND s.report_id=latest.report_id AND s.payload->>'state'<>latest.state
                """,ts(now),run,channel,ts(oldest),ts(now),run);
    }
    private static void affect(Map<String,Instant[]> affected,String channel,Instant from,Instant to) {
        affected.compute(channel,(id,range)->range==null?new Instant[]{from,to}:new Instant[]{range[0].isBefore(from)?range[0]:from,range[1].isAfter(to)?range[1]:to});
    }
    private static void delta(Map<Delta,Long> changes,UUID report,Instant at,long amount) { if(report!=null)changes.merge(new Delta(report,Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(),10)*10)),amount,Long::sum); }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("REPORT_HASH_UNAVAILABLE"); }
    }
    @Override public void observe(TopicPartition partition,long end,Instant observedAt,Instant pollAt,boolean blocked) {
        jdbc.update("""
                INSERT INTO report_partition VALUES (?,?,?,?,?,?,CASE WHEN NOT ? AND coalesce((SELECT next_offset FROM report_progress WHERE run_id=? AND topic=? AND partition=?),-1)>=? THEN CAST(? AS timestamptz) ELSE NULL END,?)
                ON CONFLICT(run_id,topic,partition) DO UPDATE SET observed_at=excluded.observed_at,poll_at=excluded.poll_at,source_end_offset=excluded.source_end_offset,
                caught_up_at=coalesce(excluded.caught_up_at,report_partition.caught_up_at),blocked=excluded.blocked
                """,run,partition.topic(),partition.partition(),ts(observedAt),pollAt==null?null:ts(pollAt),end,blocked,run,partition.topic(),partition.partition(),end,ts(observedAt),blocked);
    }
    @Override public void unavailable(Collection<TopicPartition> partitions) { for(var p:partitions)jdbc.update("UPDATE report_partition SET blocked=true WHERE run_id=? AND topic=? AND partition=?",run,p.topic(),p.partition()); }
    private boolean fresh() {
        Instant now=clock.instant();return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT count(*)=6 AND coalesce(bool_and(NOT blocked AND poll_at IS NOT NULL AND caught_up_at IS NOT NULL AND observed_at BETWEEN ? AND ? AND poll_at BETWEEN ? AND ? AND caught_up_at BETWEEN ? AND ? AND caught_up_at>=?
                  AND coalesce((SELECT next_offset FROM report_progress g WHERE g.run_id=p.run_id AND g.topic=p.topic AND g.partition=p.partition),-1)>=source_end_offset),false)
                FROM report_partition p WHERE run_id=? AND topic IN (?,?)
                """,Boolean.class,ts(now.minusSeconds(15)),ts(now),ts(now.minusSeconds(15)),ts(now),ts(now.minusSeconds(10)),ts(now),ts(Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond()-8,60)*60)),run,chatTopic,observationTopic));
    }
    private Stored stored(java.sql.ResultSet rs,int n) throws java.sql.SQLException {
        try { return new Stored(JSON.readValue(rs.getString("payload"),ReportSessions.Session.class),rs.getLong("revision"),rs.getTimestamp("updated_at").toInstant()); }
        catch(RuntimeException invalid) { throw new IllegalArgumentException("REPORT_STORED_CONTRACT"); }
    }
    <T> T readSnapshot(java.util.function.Supplier<T> query) { return read.execute(status->{jdbc.execute("SET LOCAL statement_timeout='5s'");return query.get();}); }
    public Page list(String channel) {
        return readSnapshot(()->{
            var reports=jdbc.query("SELECT * FROM report_session s WHERE run_id=? AND channel_id=? AND (greatest(observed_to,ended_at)>=? OR ended_at IS NULL) AND NOT EXISTS(SELECT 1 FROM report_alias a WHERE a.run_id=s.run_id AND a.alias_id=s.report_id) ORDER BY observed_from DESC,report_id LIMIT 101",this::stored,run,channel,ts(clock.instant().minusSeconds(7*86400)));
            return new Page(reports.stream().limit(100).toList(),reports.size()>100,unassigned(channel),fresh());
        });
    }
    private long unassigned(String channel) { return jdbc.queryForObject("SELECT count(*) FROM report_event WHERE run_id=? AND channel_id=? AND report_id IS NULL AND event_time>=?",Long.class,run,channel,ts(clock.instant().minusSeconds(7*86400))); }
    public Detail detail(UUID id) {
        return readSnapshot(()->{
            UUID canonical=id;var visited=new HashSet<UUID>();
            for(int i=0;i<100;i++) {
                if(!visited.add(canonical))throw new IllegalArgumentException("REPORT_ALIAS_CYCLE");
                var aliases=jdbc.queryForList("SELECT canonical_id FROM report_alias WHERE run_id=? AND alias_id=?",UUID.class,run,canonical);
                if(aliases.isEmpty())break;canonical=aliases.getFirst();if(i==99)throw new IllegalArgumentException("REPORT_ALIAS_LIMIT");
            }
            var rows=jdbc.query("SELECT * FROM report_session WHERE run_id=? AND report_id=? AND (greatest(observed_to,ended_at)>=? OR ended_at IS NULL)",this::stored,run,canonical,ts(clock.instant().minusSeconds(7*86400)));if(rows.isEmpty())return null;
            var stored=rows.getFirst();var minutes=jdbc.query("SELECT date_trunc('minute',bucket_start) at,sum(message_count)::bigint count FROM report_bucket WHERE run_id=? AND report_id=? GROUP BY at ORDER BY at",(rs,n)->new Minute(rs.getTimestamp(1).toInstant(),rs.getLong(2)),run,canonical);
            var intervals=jdbc.query("""
                    WITH ordered AS (SELECT *,lead(observed_at) OVER(ORDER BY observed_at,event_id) next_at FROM report_observation o WHERE run_id=? AND channel_id=? AND NOT EXISTS(SELECT 1 FROM report_session s WHERE s.run_id=o.run_id AND s.report_id=o.report_id AND s.ended_at<?))
                    SELECT observed_at,least(observed_at+make_interval(secs=>ttl_seconds),next_at) FROM ordered WHERE report_id=? AND live
                    """,(rs,n)->new QualityPolicy.Interval(rs.getTimestamp(1).toInstant(),rs.getTimestamp(2).toInstant()),run,stored.session().channelId(),ts(clock.instant().minusSeconds(7*86400)),canonical);
            long late=jdbc.queryForObject("SELECT count(*) FROM report_event WHERE run_id=? AND report_id=? AND late",Long.class,run,canonical);
            return new Detail(stored,minutes,intervals,unassigned(stored.session().channelId()),late,fresh());
        });
    }
    @Override public void cleanup() {
        tx.executeWithoutResult(status->{
            jdbc.execute("SET LOCAL statement_timeout='5s'");Instant oldest=clock.instant().minusSeconds(7*86400);
            if(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(hashtextextended(?,0))",Boolean.class,"report-cleanup/"+run)))return;
            // 미확정/활성 세션은 침묵만으로 삭제하지 않는다. 종료가 확인된 세션만 자식부터 한정 정리한다.
            for(String table:List.of("report_event","report_observation","report_bucket"))jdbc.update("DELETE FROM "+table+" WHERE ctid IN (SELECT c.ctid FROM "+table+" c JOIN report_session s ON s.run_id=c.run_id AND s.report_id=c.report_id WHERE s.run_id=? AND s.ended_at<? LIMIT 10000)",run,ts(oldest));
            jdbc.update("DELETE FROM report_event WHERE ctid IN (SELECT ctid FROM report_event WHERE run_id=? AND report_id IS NULL AND event_time<? LIMIT 10000)",run,ts(oldest));
            jdbc.update("DELETE FROM report_observation WHERE ctid IN (SELECT ctid FROM report_observation WHERE run_id=? AND report_id IS NULL AND observed_at<? LIMIT 10000)",run,ts(oldest.minusSeconds(120)));
            jdbc.update("DELETE FROM report_bucket WHERE ctid IN (SELECT ctid FROM report_bucket WHERE run_id=? AND message_count=0 LIMIT 10000)",run);
            jdbc.update("""
                    DELETE FROM report_session s WHERE run_id=? AND (ended_at<? OR EXISTS(SELECT 1 FROM report_alias a WHERE a.run_id=s.run_id AND a.alias_id=s.report_id)) AND NOT EXISTS(SELECT 1 FROM report_event e WHERE e.run_id=s.run_id AND e.report_id=s.report_id)
                    AND NOT EXISTS(SELECT 1 FROM report_observation o WHERE o.run_id=s.run_id AND o.report_id=s.report_id) AND NOT EXISTS(SELECT 1 FROM report_bucket b WHERE b.run_id=s.run_id AND b.report_id=s.report_id)
                    """,run,ts(oldest));
            jdbc.update("""
                    DELETE FROM report_alias a WHERE run_id=? AND NOT EXISTS(SELECT 1 FROM report_session s WHERE s.run_id=a.run_id AND s.report_id=a.canonical_id)
                      AND NOT EXISTS(SELECT 1 FROM report_alias next WHERE next.run_id=a.run_id AND next.alias_id=a.canonical_id)
                    """,run);
        });
    }
    private static Timestamp ts(Instant at) { return Timestamp.from(at); }
}
