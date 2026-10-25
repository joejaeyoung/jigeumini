package io.chatpulse.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.jdbc.core.JdbcTemplate;

/** 별도 assign 소비자는 시각만 관측하고 subscribe/commit/원본 저장을 하지 않는다. */
public final class RecoveryDiagnostics implements AutoCloseable {
    public record Target(String group, String projection, ProjectionConsumer consumer) { }
    public record Partition(int partition, Long committedOffset, long beginningOffset, long endOffset, Long lag,
            Long dbResolvedOffset, Instant lastProgressAt, Instant firstPendingAt, String state) { }
    public record ErrorCount(int partition, String errorCode, long count) { }
    public record Failure(String failureId, int partition, long offset, String errorCode, String channelId,
            Instant quarantinedAt, String sourceAvailability) { }
    public record Group(String group, String projection, Map<String,Object> consumer, List<Partition> partitions,
            List<ErrorCount> errors, List<Failure> recentFailures) { }
    public record Snapshot(Instant observedAt, String state, List<Group> groups) { }
    private record Progress(long offset, Instant at) { }
    private final Admin admin;
    private final KafkaConsumer<byte[],byte[]> probe;
    private final JdbcTemplate jdbc;
    private final String topic, run;
    private final List<Target> targets;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private volatile Snapshot snapshot = new Snapshot(null, "UNKNOWN", List.of());

    public RecoveryDiagnostics(String bootstrap, String topic, String run, JdbcTemplate jdbc, List<Target> targets) {
        this.topic=topic; this.run=run; this.jdbc=jdbc; this.targets=List.copyOf(targets);
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"default.api.timeout.ms",4000,"request.timeout.ms",3000));
        var settings=new HashMap<String,Object>();
        settings.put("bootstrap.servers",bootstrap); settings.put("enable.auto.commit",false);
        settings.put("allow.auto.create.topics",false); settings.put("auto.offset.reset","none");
        settings.put("max.poll.records",1); settings.put("default.api.timeout.ms",2000);
        settings.put("request.timeout.ms",2000); settings.put("fetch.max.wait.ms",100);
        probe=new KafkaConsumer<>(settings,new ByteArrayDeserializer(),new ByteArrayDeserializer());
    }
    public void start() { worker.scheduleWithFixedDelay(this::sample,0,5,TimeUnit.SECONDS); }
    public Snapshot snapshot() {
        Snapshot result=snapshot;
        if (result.state.equals("AVAILABLE") && Instant.now().isAfter(result.observedAt.plusSeconds(20)))
            return new Snapshot(result.observedAt,"STALE",result.groups);
        return result;
    }
    public synchronized void sample() {
        try {
            Instant at=Instant.now();
            var description=admin.describeTopics(List.of(topic)).allTopicNames().get(4,TimeUnit.SECONDS).get(topic);
            var earliest=new HashMap<TopicPartition,OffsetSpec>();
            var latest=new HashMap<TopicPartition,OffsetSpec>();
            for(var p:description.partitions()) {
                var tp=new TopicPartition(topic,p.partition()); earliest.put(tp,OffsetSpec.earliest()); latest.put(tp,OffsetSpec.latest());
            }
            var beginnings=admin.listOffsets(earliest).all().get(4,TimeUnit.SECONDS);
            var ends=admin.listOffsets(latest).all().get(4,TimeUnit.SECONDS);
            var groups=new ArrayList<Group>();
            for(var target:targets) {
                var offsets=admin.listConsumerGroupOffsets(target.group).partitionsToOffsetAndMetadata().get(4,TimeUnit.SECONDS);
                var progress=new HashMap<Integer,Progress>();
                jdbc.query("SELECT partition,next_resolved_offset,checked_at FROM projection_progress WHERE projection=? AND run_id=? AND topic=?",
                        rs->{ progress.put(rs.getInt(1),new Progress(rs.getLong(2),rs.getTimestamp(3).toInstant())); },target.projection,run,topic);
                var bound=jdbc.query("SELECT topic_id FROM projection_source WHERE projection=? AND run_id=? AND topic=?",
                        (rs,n)->rs.getString(1),target.projection,run,topic);
                boolean changed=!bound.isEmpty()&&!bound.getFirst().equals(description.topicId().toString());
                var partitions=new ArrayList<Partition>();
                for(var tp:earliest.keySet().stream().sorted(Comparator.comparingInt(TopicPartition::partition)).toList()) {
                    long begin=beginnings.get(tp).offset(),end=ends.get(tp).offset();
                    Long committed=offsets.containsKey(tp)?offsets.get(tp).offset():null;
                    var db=progress.get(tp.partition());
                    String state=changed?"SOURCE_IDENTITY_CHANGED":committed==null?"OFFSET_UNSET":committed<begin?"OFFSET_EXPIRED"
                            :committed>end?"OFFSET_AHEAD":committed==end?"CAUGHT_UP":"PENDING";
                    Long lag=Set.of("CAUGHT_UP","PENDING").contains(state)?end-committed:null;
                    Instant first=state.equals("PENDING")?headTimestamp(tp,committed):null;
                    partitions.add(new Partition(tp.partition(),committed,begin,end,lag,db==null?null:db.offset,
                            db==null?null:db.at,first,state));
                }
                var errors=jdbc.query("""
                        SELECT source_partition,error_code,count(*) FROM projection_failure
                        WHERE projection=? AND run_id=? AND source_topic=? GROUP BY source_partition,error_code ORDER BY source_partition,error_code
                        """,(rs,n)->new ErrorCount(rs.getInt(1),rs.getString(2),rs.getLong(3)),target.projection,run,topic);
                var failures=jdbc.query("""
                        SELECT failure_id,source_partition,source_offset,error_code,channel_id,quarantined_at,topic_id
                        FROM projection_failure WHERE projection=? AND run_id=? AND source_topic=? ORDER BY quarantined_at DESC,failure_id LIMIT 50
                        """,(rs,n)->{
                            var tp=new TopicPartition(topic,rs.getInt(2)); long offset=rs.getLong(3);
                            String availability=!rs.getString(7).equals(description.topicId().toString())?"TOPIC_CHANGED"
                                    :!beginnings.containsKey(tp)?"UNKNOWN":offset<beginnings.get(tp).offset()?"EXPIRED"
                                    :offset>=ends.get(tp).offset()?"UNKNOWN":"AVAILABLE";
                            return new Failure(rs.getString(1),tp.partition(),offset,rs.getString(4),rs.getString(5),rs.getTimestamp(6).toInstant(),availability);
                        },target.projection,run,topic);
                groups.add(new Group(target.group,target.projection,target.consumer.status(),partitions,errors,failures));
            }
            snapshot=new Snapshot(at,"AVAILABLE",List.copyOf(groups));
        } catch (Exception ignored) {
            // 관측 실패는 이전 결과의 시각을 유지하고 0 lag/복구 가능으로 덮어쓰지 않는다.
            Snapshot old=snapshot; snapshot=new Snapshot(old.observedAt,"UNAVAILABLE",old.groups);
        }
    }
    private Instant headTimestamp(TopicPartition tp,long offset) {
        try {
            probe.assign(List.of(tp)); probe.seek(tp,offset);
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(1);
            do {
                var records=probe.poll(Duration.ofMillis(200));
                for(var record:records) if(record.offset()==offset && record.timestamp()>=0
                        && record.timestamp()<=System.currentTimeMillis()+5000) return Instant.ofEpochMilli(record.timestamp());
                if(!records.isEmpty()) return null;
            } while(System.nanoTime()<until);
        } catch (RuntimeException ignored) { /* 보존 삭제·조회 지연이면 미관측 */ }
        return null;
    }
    @Override public void close() {
        worker.shutdownNow();
        synchronized(this) { probe.close(Duration.ofSeconds(1)); admin.close(Duration.ofSeconds(1)); }
    }
}
