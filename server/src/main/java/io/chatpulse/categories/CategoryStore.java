package io.chatpulse.categories;

import io.chatpulse.observations.BroadcastObserved;
import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.pipeline.EventCodec;
import io.chatpulse.poc.ChatEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** 시각 기준 카테고리 배정과 보정을 독립 트랜잭션에 저장한다. */
public final class CategoryStore implements io.chatpulse.pipeline.DualTopicProjection {
    public static final String UNKNOWN = "unknown";
    public record Bucket(String channelId, String categoryKey, Instant at, long count) { }
    public record Category(String key, String name) { }
    public record Membership(String channelId,String categoryKey,Instant from,Instant to) { }
    public record Window(List<String> channels, List<Bucket> buckets, Map<String,Category> labels,
                         List<Membership> memberships, long revision, boolean truncated, boolean fresh) { }
    private record Input(ConsumerRecord<String,byte[]> record, ChatEvent chat, BroadcastObserved observation) {
        String channel() { return chat != null ? chat.channelId() : observation.channelId(); }
    }
    private record Delta(String category, String channel, Instant bucket) implements Comparable<Delta> {
        public int compareTo(Delta other) { int c=category.compareTo(other.category); if(c==0)c=channel.compareTo(other.channel); return c==0?bucket.compareTo(other.bucket):c; }
    }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx, read;
    private final String run, chatTopic, observationTopic;
    private final Clock clock;
    private final EventCodec codec = new EventCodec();
    public CategoryStore(JdbcTemplate jdbc, TransactionTemplate tx, String run, String chatTopic, String observationTopic, Clock clock) {
        this.jdbc=jdbc; this.tx=tx; this.run=run; this.chatTopic=chatTopic; this.observationTopic=observationTopic; this.clock=clock;
        read=new TransactionTemplate(tx.getTransactionManager()); read.setReadOnly(true); read.setTimeout(20);
        read.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }
    public static String key(String sourceCategory) { return sourceCategory == null ? UNKNOWN : hash(sourceCategory.getBytes(StandardCharsets.UTF_8)); }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("CATEGORY_HASH_UNAVAILABLE"); }
    }
    public void bindSources(Map<String,String> identities) {
        tx.executeWithoutResult(status -> identities.forEach((topic,id) -> {
            jdbc.update("INSERT INTO category_source VALUES (?,?,?) ON CONFLICT DO NOTHING",run,topic,id);
            if (!id.equals(jdbc.queryForObject("SELECT topic_id FROM category_source WHERE run_id=? AND topic=?",String.class,run,topic)))
                throw new IllegalStateException("CATEGORY_SOURCE_CHANGED");
        }));
    }
    public void initialize(TopicPartition partition, long offset) {
        jdbc.update("INSERT INTO category_progress VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",run,partition.topic(),partition.partition(),offset,ts(clock.instant()));
    }
    public void apply(List<ConsumerRecord<String,byte[]>> records) {
        var inputs = new ArrayList<Input>();
        for(var record:records) {
            if(record.topic().equals(chatTopic)) inputs.add(new Input(record,codec.decode(record.value(),record.key()),null));
            else if(record.topic().equals(observationTopic)) inputs.add(new Input(record,null,BroadcastObserved.decode(record.key(),record.value())));
            else throw new IllegalArgumentException("CATEGORY_INPUT_TOPIC");
        }
        tx.executeWithoutResult(status -> {
            jdbc.execute("SET LOCAL statement_timeout='5s'"); jdbc.execute("SET LOCAL lock_timeout='3s'");
            Instant now=clock.instant(), oldest=now.minusSeconds(7*86400);
            // 같은 채널의 서로 다른 입력 토픽/consumer와 지연 보정을 직렬화한다.
            for(String channel:inputs.stream().map(Input::channel).distinct().sorted().toList())
                jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtextextended(?,0))",Object.class,"category/"+run+"/"+channel);
            var changes = new TreeMap<Delta,Long>(); var added = new ArrayList<BroadcastObserved>();
            boolean changed=false;
            for(var input:inputs) if(input.observation()!=null) {
                var sample=input.observation();
                if(sample.observedAt().isAfter(now.plusSeconds(5))) throw new IllegalArgumentException("CATEGORY_FUTURE_TIME");
                if(!sample.runId().equals(run) || sample.observedAt().isBefore(oldest.minusSeconds(120))) continue;
                String category=sample.liveStatus()==LiveStatus.LIVE?key(sample.categoryId()):UNKNOWN;
                String name=category.equals(UNKNOWN)?null:sample.categoryName()==null?sample.categoryId():sample.categoryName();
                String digest=hash(sample.encode());
                int inserted=jdbc.update("INSERT INTO category_observation VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        run,sample.eventId(),sample.channelId(),ts(sample.observedAt()),Math.min(120,sample.ttlSeconds()),category,name,digest);
                if(inserted==0) {
                    if(!digest.equals(jdbc.queryForObject("SELECT payload_hash FROM category_observation WHERE run_id=? AND event_id=?",String.class,run,sample.eventId())))
                        throw new IllegalArgumentException("CATEGORY_OBSERVATION_CONFLICT");
                } else { added.add(sample); changed=true; }
            }
            for(var input:inputs) if(input.chat()!=null) {
                var event=input.chat(); if(event.eventTime().isBefore(oldest)) continue;
                codec.validateLiveAge(event,now);
                Instant eventTime=event.eventTime().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                String category=categoryAt(event.channelId(),eventTime), digest=codec.hash(event);
                int inserted=jdbc.update("INSERT INTO category_event(run_id,event_id,channel_id,event_time,payload_hash,category_key) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        run,event.eventId(),event.channelId(),ts(eventTime),digest,category);
                if(inserted==0) {
                    if(!digest.equals(jdbc.queryForObject("SELECT payload_hash FROM category_event WHERE run_id=? AND event_id=?",String.class,run,event.eventId())))
                        throw new IllegalArgumentException("CATEGORY_EVENT_CONFLICT");
                } else { delta(changes,category,event.channelId(),eventTime,1); changed=true; }
            }
            for(var sample:added) {
                // 모든 새 관측을 먼저 넣어 입력 순서와 무관하게 최종 시각 배정을 계산한다.
                jdbc.query("""
                        WITH assigned AS (
                          SELECT e.event_id,e.category_key old_key,coalesce((
                            SELECT CASE WHEN e.event_time<m.observed_at+make_interval(secs=>m.ttl_seconds) THEN m.category_key ELSE 'unknown' END
                            FROM category_observation m WHERE m.run_id=e.run_id AND m.channel_id=e.channel_id AND m.observed_at<=e.event_time
                            ORDER BY m.observed_at DESC,m.event_id DESC LIMIT 1),'unknown') new_key
                          FROM category_event e WHERE e.run_id=? AND e.channel_id=? AND e.event_time>=? AND e.event_time<?
                        ), changed AS (
                          UPDATE category_event e SET category_key=a.new_key,revision=e.revision+1 FROM assigned a
                          WHERE e.run_id=? AND e.event_id=a.event_id AND a.old_key<>a.new_key
                          RETURNING e.channel_id,e.event_time,a.old_key,a.new_key
                        ) SELECT channel_id,event_time,old_key,new_key FROM changed
                        """, rs -> {
                    var at=rs.getTimestamp(2).toInstant();
                    delta(changes,rs.getString(3),rs.getString(1),at,-1); delta(changes,rs.getString(4),rs.getString(1),at,1);
                },run,sample.channelId(),ts(sample.observedAt().isBefore(oldest)?oldest:sample.observedAt()),
                        ts(sample.observedAt().plusSeconds(120)),run);
            }
            for(var change:changes.entrySet()) {
                if(change.getValue()==0)continue;
                var key=change.getKey();
                if(change.getValue()>0) jdbc.update("""
                        INSERT INTO category_bucket VALUES (?,?,?,?,?) ON CONFLICT(run_id,category_key,channel_id,bucket_start)
                        DO UPDATE SET message_count=category_bucket.message_count+excluded.message_count
                        """,run,key.category(),key.channel(),ts(key.bucket()),change.getValue());
                else if(jdbc.update("UPDATE category_bucket SET message_count=message_count+? WHERE run_id=? AND category_key=? AND channel_id=? AND bucket_start=?",
                        change.getValue(),run,key.category(),key.channel(),ts(key.bucket()))!=1) throw new IllegalStateException("CATEGORY_BUCKET_MISSING");
            }
            if(changed) jdbc.update("INSERT INTO category_revision VALUES (?,1) ON CONFLICT(run_id) DO UPDATE SET revision=category_revision.revision+1",run);
            for(var input:inputs) jdbc.update("""
                    INSERT INTO category_progress VALUES (?,?,?,?,?) ON CONFLICT(run_id,topic,partition)
                    DO UPDATE SET next_offset=greatest(category_progress.next_offset,excluded.next_offset),updated_at=excluded.updated_at
                    """,run,input.record().topic(),input.record().partition(),input.record().offset()+1,ts(now));
        });
    }
    private static void delta(Map<Delta,Long> changes,String category,String channel,Instant time,long amount) {
        changes.merge(new Delta(category,channel,Instant.ofEpochSecond(Math.floorDiv(time.getEpochSecond(),10)*10)),amount,Long::sum);
    }
    private String categoryAt(String channel, Instant at) {
        var values=jdbc.query("""
                SELECT CASE WHEN ?<observed_at+make_interval(secs=>ttl_seconds) THEN category_key ELSE 'unknown' END
                FROM category_observation WHERE run_id=? AND channel_id=? AND observed_at<=? ORDER BY observed_at DESC,event_id DESC LIMIT 1
                """,(rs,n)->rs.getString(1),ts(at),run,channel,ts(at));
        return values.isEmpty()?UNKNOWN:values.getFirst();
    }
    public void observe(TopicPartition partition,long end,Instant observedAt,Instant pollAt,boolean blocked) {
        jdbc.update("""
                INSERT INTO category_partition(run_id,topic,partition,observed_at,poll_at,source_end_offset,caught_up_at,blocked)
                VALUES (?,?,?,?,?,?,CASE WHEN NOT ? AND coalesce((SELECT next_offset FROM category_progress WHERE run_id=? AND topic=? AND partition=?),-1)>=? THEN CAST(? AS timestamptz) ELSE NULL END,?)
                ON CONFLICT(run_id,topic,partition) DO UPDATE SET observed_at=excluded.observed_at,poll_at=excluded.poll_at,
                  source_end_offset=excluded.source_end_offset,caught_up_at=coalesce(excluded.caught_up_at,category_partition.caught_up_at),blocked=excluded.blocked
                """,run,partition.topic(),partition.partition(),ts(observedAt),pollAt==null?null:ts(pollAt),end,blocked,
                run,partition.topic(),partition.partition(),end,ts(observedAt),blocked);
    }
    public void unavailable(Collection<TopicPartition> partitions) {
        for(var partition:partitions)jdbc.update("UPDATE category_partition SET blocked=true WHERE run_id=? AND topic=? AND partition=?",run,partition.topic(),partition.partition());
    }
    public Window window(Instant from,Instant to) { return read.execute(status -> readWindow(from,to)); }
    private Window readWindow(Instant from,Instant to) {
        var channels=jdbc.query("""
                SELECT channel_id FROM (SELECT channel_id,event_time at FROM category_event WHERE run_id=? AND event_time>=? AND event_time<?
                  UNION ALL SELECT channel_id,observed_at FROM category_observation WHERE run_id=? AND observed_at>=? AND observed_at<?) recent
                GROUP BY channel_id ORDER BY max(at) DESC,channel_id LIMIT 21
                """,(rs,n)->rs.getString(1),run,ts(from),ts(to),run,ts(from.minusSeconds(120)),ts(to));
        var selected=channels.stream().limit(20).toList();
        var buckets=jdbc.query("SELECT channel_id,category_key,bucket_start,message_count FROM category_bucket WHERE run_id=? AND bucket_start>=? AND bucket_start<? AND message_count>0 ORDER BY bucket_start,category_key,channel_id",
                (rs,n)->new Bucket(rs.getString(1),rs.getString(2),rs.getTimestamp(3).toInstant(),rs.getLong(4)),run,ts(from),ts(to))
                .stream().filter(b -> selected.contains(b.channelId())).toList();
        record Sample(String channel,String key,Instant at,int ttl) { }
        var samples=jdbc.query("SELECT channel_id,category_key,observed_at,ttl_seconds FROM category_observation WHERE run_id=? AND observed_at>=? AND observed_at<? ORDER BY channel_id,observed_at,event_id",
                (rs,n)->new Sample(rs.getString(1),rs.getString(2),rs.getTimestamp(3).toInstant(),rs.getInt(4)),run,ts(from.minusSeconds(120)),ts(to));
        var memberships=new ArrayList<Membership>();
        for(String channel:selected) {
            var history=samples.stream().filter(sample->sample.channel().equals(channel)).toList(); Instant cursor=from;
            for(int i=0;i<history.size();i++) {
                var sample=history.get(i); Instant begin=sample.at().isBefore(from)?from:sample.at();
                Instant end=sample.at().plusSeconds(sample.ttl());
                if(i+1<history.size() && history.get(i+1).at().isBefore(end))end=history.get(i+1).at();
                if(end.isAfter(to))end=to;
                if(!end.isAfter(begin))continue;
                if(begin.isAfter(cursor))memberships.add(new Membership(channel,UNKNOWN,cursor,begin));
                memberships.add(new Membership(channel,sample.key(),begin,end)); cursor=end;
            }
            if(cursor.isBefore(to))memberships.add(new Membership(channel,UNKNOWN,cursor,to));
        }
        var labels=new HashMap<String,Category>(); labels.put(UNKNOWN,new Category(UNKNOWN,"카테고리 미상"));
        jdbc.query("""
                SELECT DISTINCT ON(category_key) category_key,category_name FROM category_observation
                WHERE run_id=? AND observed_at<? AND category_key<>'unknown' ORDER BY category_key,observed_at DESC,event_id DESC
                """,rs -> { labels.put(rs.getString(1),new Category(rs.getString(1),rs.getString(2))); },run,ts(to));
        long revision=Objects.requireNonNull(jdbc.queryForObject("SELECT coalesce((SELECT revision FROM category_revision WHERE run_id=?),0)",Long.class,run));
        Instant now=clock.instant();
        boolean fresh=jdbc.queryForObject("""
                SELECT count(*)=6 AND coalesce(bool_and(poll_at IS NOT NULL AND caught_up_at IS NOT NULL AND observed_at>=? AND observed_at<=? AND poll_at>=? AND poll_at<=? AND caught_up_at>=? AND caught_up_at>=? AND caught_up_at<=? AND NOT blocked
                  AND coalesce((SELECT next_offset FROM category_progress g WHERE g.run_id=p.run_id AND g.topic=p.topic AND g.partition=p.partition),-1)>=source_end_offset),false)
                FROM category_partition p WHERE run_id=? AND topic IN (?,?)
                """,Boolean.class,ts(now.minusSeconds(15)),ts(now),ts(now.minusSeconds(15)),ts(now),ts(now.minusSeconds(10)),ts(to),ts(now),run,chatTopic,observationTopic);
        return new Window(selected,buckets,Map.copyOf(labels),List.copyOf(memberships),revision,channels.size()>20,fresh);
    }
    public void cleanup() {
        Instant oldest=clock.instant().minusSeconds(7*86400);
        tx.executeWithoutResult(status -> {
            jdbc.execute("SET LOCAL statement_timeout='5s'");
            jdbc.update("DELETE FROM category_event WHERE ctid IN (SELECT ctid FROM category_event WHERE run_id=? AND event_time<? LIMIT 10000)",run,ts(oldest));
            jdbc.update("DELETE FROM category_bucket WHERE ctid IN (SELECT ctid FROM category_bucket WHERE run_id=? AND bucket_start<? LIMIT 10000)",run,ts(Instant.ofEpochSecond(Math.floorDiv(oldest.getEpochSecond(),10)*10)));
            jdbc.update("DELETE FROM category_observation WHERE ctid IN (SELECT ctid FROM category_observation WHERE run_id=? AND observed_at<? LIMIT 10000)",run,ts(oldest.minusSeconds(120)));
        });
    }
    private static Timestamp ts(Instant at) { return Timestamp.from(at); }
}
