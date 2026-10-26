package io.chatpulse.operations;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.pipeline.EventCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;

/** 생성 입력에서 기대 UUID·내용·10초 합계를 계산한다. 제품 집계 SQL을 호출하지 않는다. */
final class ProductLoadFixture {
    static final long SEED = 20261026;
    final NavigableMap<String,String> events = new TreeMap<>();
    final NavigableMap<String,Long> buckets = new TreeMap<>();
    final NavigableMap<String,String> observations = new TreeMap<>();
    final Set<String> spikes = new TreeSet<>();
    final List<String> channels;
    final Map<String,String> categories = new TreeMap<>();
    private final EventCodec codec = new EventCodec();

    ProductLoadFixture(int count) {
        var values = new ArrayList<String>();
        for(int i=0;i<count;i++) values.add(String.format("%032x",i+1));
        Collections.shuffle(values,new Random(SEED));channels=List.copyOf(values);
        for(int i=0;i<count;i++) categories.put(channels.get(i),"synthetic-"+(i%3));
    }
    String channel(long ordinal, boolean hot) {
        int index = hot ? ordinal%5!=4 ? 0 : 1+(int)(ordinal/5%(channels.size()-1)) : (int)(ordinal%channels.size());
        return channels.get(index);
    }
    static UUID id(String name) { return UUID.nameUUIDFromBytes(("product-load/"+SEED+"/"+name).getBytes(StandardCharsets.UTF_8)); }
    ChatEvent event(long ordinal, boolean hot, Instant at) {
        String channel=channel(ordinal,hot);
        return new ChatEvent(1,id("chat/"+ordinal),"CHZZK_UNOFFICIAL",channel,"synthetic",id("session/"+channel),null,"CHAT",at,Instant.now(),false,"product-load-test");
    }
    synchronized void accepted(ChatEvent event) {
        check(events.put(event.eventId().toString(),event.channelId()+"|"+event.eventTime()+"|"+codec.hash(event))==null,"GENERATOR_DUPLICATE_ID");
        String key=event.channelId()+"|"+Math.floorDiv(event.eventTime().getEpochSecond(),10)*10;
        buckets.merge(key,1L,Long::sum);
    }
    synchronized int uniqueCount() {return events.size();}
    Map<String,Object> verify(JdbcTemplate jdbc, RuntimeEnvironment c) throws Exception {
        var checks=new LinkedHashMap<String,Object>();
        for(String projection:List.of(c.projection(),c.projection()+"-trend")) {
            var actual=eventRows(jdbc,"SELECT event_id,channel_id,event_time,payload_hash FROM processed_event WHERE run_id=? AND projection=?",c.runId(),projection);
            same(checks,projection+"Ledger",events,actual);
            same(checks,projection+"Buckets",buckets,bucketRows(jdbc,"SELECT channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE run_id=? AND projection=?",c.runId(),projection));
        }
        for(String table:List.of("category_event","report_event"))
            same(checks,table,events,eventRows(jdbc,"SELECT event_id,channel_id,event_time,payload_hash FROM "+table+" WHERE run_id=?",c.runId()));
        var expectedCategories=new TreeMap<String,Long>();
        for(var entry:buckets.entrySet()) {
            String channel=entry.getKey().substring(0,32);
            expectedCategories.put(hash(categories.get(channel).getBytes(StandardCharsets.UTF_8))+"|"+entry.getKey(),entry.getValue());
        }
        var categoryBuckets=new TreeMap<String,Long>();
        jdbc.query("SELECT category_key,channel_id,bucket_start,message_count FROM category_bucket WHERE run_id=? AND message_count>0",rs->{
            categoryBuckets.put(rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getTimestamp(3).toInstant().getEpochSecond(),rs.getLong(4));
        },c.runId());
        same(checks,"categoryBuckets",expectedCategories,categoryBuckets);
        for(var entry:categories.entrySet()) check(jdbc.queryForObject("SELECT count(*) FROM category_event WHERE run_id=? AND channel_id=? AND category_key<>?",Long.class,c.runId(),entry.getKey(),hash(entry.getValue().getBytes(StandardCharsets.UTF_8)))==0,"CATEGORY_ASSIGNMENT");
        same(checks,"reportBuckets",buckets,bucketRows(jdbc,"SELECT s.channel_id,b.bucket_start,sum(b.message_count) FROM report_bucket b JOIN report_session s USING(run_id,report_id) WHERE b.run_id=? GROUP BY s.channel_id,b.bucket_start HAVING sum(b.message_count)>0",c.runId()));
        check(jdbc.queryForObject("SELECT count(*) FROM report_event e LEFT JOIN report_session s USING(run_id,report_id) WHERE e.run_id=? AND (s.report_id IS NULL OR e.channel_id<>s.channel_id OR e.observation_id IS NULL)",Long.class,c.runId())==0,"REPORT_ASSIGNMENT");
        for(String table:List.of("broadcast_observation","category_observation","report_observation")) {
            var actual=new TreeMap<String,String>();
            jdbc.query("SELECT event_id,channel_id,observed_at FROM "+table+" WHERE run_id=?",rs->{actual.put(rs.getString(1),rs.getString(2)+"|"+rs.getTimestamp(3).toInstant());},c.runId());
            same(checks,table,observations,actual);
        }
        for(String table:List.of("spike_event","spike_outbox","notification_event")) {
            var actual=new TreeSet<>(jdbc.queryForList("SELECT spike_id::text FROM "+table+" WHERE run_id=?",String.class,c.runId()));
            check(actual.equals(spikes),"SPIKE_SET_"+table.toUpperCase(Locale.ROOT));
            checks.put(table,Map.of("expected",spikes.size(),"actual",actual.size(),"idsSha256",digest(actual)));
        }
        check(jdbc.queryForObject("SELECT count(*) FROM spike_outbox WHERE run_id=? AND published_at IS NULL",Long.class,c.runId())==0,"OUTBOX_NOT_DRAINED");
        return checks;
    }
    private static NavigableMap<String,String> eventRows(JdbcTemplate jdbc,String sql,Object... args) {
        var values=new TreeMap<String,String>();
        jdbc.query(sql,rs->{values.put(rs.getString(1),rs.getString(2)+"|"+rs.getTimestamp(3).toInstant()+"|"+rs.getString(4));},args);
        return values;
    }
    private static NavigableMap<String,Long> bucketRows(JdbcTemplate jdbc,String sql,Object... args) {
        var values=new TreeMap<String,Long>();
        jdbc.query(sql,rs->{values.put(rs.getString(1)+"|"+rs.getTimestamp(2).toInstant().getEpochSecond(),rs.getLong(3));},args);
        return values;
    }
    private static void same(Map<String,Object> report,String name,Map<?,?> expected,Map<?,?> actual) throws Exception {
        check(expected.equals(actual),"PROJECTION_MISMATCH_"+name.toUpperCase(Locale.ROOT));
        report.put(name,Map.of("expectedRows",expected.size(),"actualRows",actual.size(),"sha256",digest(expected.entrySet())));
    }
    static String digest(Collection<?> rows) throws Exception {
        var digest=MessageDigest.getInstance("SHA-256");
        for(var row:rows) { digest.update(row.toString().getBytes(StandardCharsets.UTF_8));digest.update((byte)0); }
        return HexFormat.of().formatHex(digest.digest());
    }
    static String hash(byte[] bytes) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    static Timestamp ts(Instant at) { return Timestamp.from(at); }
    static void check(boolean condition,String code) { if(!condition)throw new IllegalStateException(code); }
}
