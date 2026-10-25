package io.chatpulse.replay;

import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import static io.chatpulse.replay.ReplayFailure.Code.*;

/** 읽기 전용. topic 생성·consumer group offset commit·timestamp 기반 범위 축소를 하지 않는다. */
public final class ReplaySource implements AutoCloseable {
    private final Admin admin;
    private final KafkaConsumer<String,byte[]> consumer;
    private final String topic;
    public ReplaySource(String bootstrap, String topic, UUID run) {
        this.topic=topic;
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"request.timeout.ms",3000,"default.api.timeout.ms",5000));
        try { consumer=new KafkaConsumer<>(Map.of("bootstrap.servers",bootstrap,"group.id","replay-"+run,
                "enable.auto.commit",false,"auto.offset.reset","none","allow.auto.create.topics",false,
                "max.poll.records",200,"default.api.timeout.ms",5000,"request.timeout.ms",3000,
                "fetch.max.wait.ms",100),new StringDeserializer(),new ByteArrayDeserializer());
        } catch(RuntimeException e) {admin.close(Duration.ofSeconds(2));throw new ReplayFailure(SOURCE_UNAVAILABLE);}
    }
    private TopicDescription describe() throws Exception {
        try { return admin.describeTopics(List.of(topic)).allTopicNames().get(5,TimeUnit.SECONDS).get(topic); }
        catch(java.util.concurrent.ExecutionException error) {
            if(error.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)throw new ReplayFailure(TOPIC_NOT_FOUND);
            throw error;
        }
    }
    private Map<TopicPartition,ListOffsetsResult.ListOffsetsResultInfo> bounds(List<TopicPartition> partitions,OffsetSpec spec) throws Exception {
        var request=new HashMap<TopicPartition,OffsetSpec>(); partitions.forEach(p->request.put(p,spec));
        return admin.listOffsets(request).all().get(5,TimeUnit.SECONDS);
    }
    public ReplayManifest capture(Instant from,Instant to) {
        try {
            var description=describe();
            var partitions=description.partitions().stream().map(p->new TopicPartition(topic,p.partition())).sorted(Comparator.comparingInt(TopicPartition::partition)).toList();
            var starts=bounds(partitions,OffsetSpec.earliest());var ends=bounds(partitions,OffsetSpec.latest());
            var manifest=new ReplayManifest(1,topic,description.topicId().toString(),partitions.stream().map(p->new ReplayManifest.Range(p.partition(),starts.get(p).offset(),ends.get(p).offset())).toList(),from,to,1,ReplayManifest.ALGORITHM,Instant.now());
            validate(manifest);return manifest;
        } catch(ReplayFailure e) {throw e;} catch(Exception e) {throw new ReplayFailure(SOURCE_UNAVAILABLE);}
    }
    public void validate(ReplayManifest manifest) {
        if(!topic.equals(manifest.topic())) throw new ReplayFailure(INVALID_MANIFEST);
        try {
            var description=describe();
            if(!description.topicId().toString().equals(manifest.topicId())) throw new ReplayFailure(TOPIC_CHANGED);
            var partitions=description.partitions().stream().map(p->new TopicPartition(topic,p.partition())).toList();
            if(!new TreeSet<>(partitions.stream().map(TopicPartition::partition).toList()).equals(new TreeSet<>(manifest.partitions().stream().map(ReplayManifest.Range::partition).toList()))) throw new ReplayFailure(PARTITIONS_CHANGED);
            var starts=bounds(partitions,OffsetSpec.earliest());var ends=bounds(partitions,OffsetSpec.latest());
            for(var range:manifest.partitions()) {
                var tp=new TopicPartition(topic,range.partition());
                if(starts.get(tp).offset()>range.start()) throw new ReplayFailure(RETENTION_EXPIRED);
                if(ends.get(tp).offset()<range.end()) throw new ReplayFailure(OFFSET_TRUNCATED);
            }
        } catch(ReplayFailure e) {throw e;} catch(Exception e) {throw new ReplayFailure(SOURCE_UNAVAILABLE);}
    }
    public void assign(ReplayManifest manifest) {
        validate(manifest);
        var partitions=manifest.partitions().stream().map(r->new TopicPartition(topic,r.partition())).toList();
        consumer.assign(partitions);
        for(var range:manifest.partitions()) {
            var tp=new TopicPartition(topic,range.partition());consumer.seek(tp,range.start());
            if(range.start()==range.end()) consumer.pause(List.of(tp));
        }
    }
    /** offset 폭은 record 수가 아니다. 경계 표본의 timestamp도 eventTime 완전성을 뜻하지 않는다. */
    public Map<String,Object> retention(ReplayManifest manifest) {
        if(!topic.equals(manifest.topic()))throw new ReplayFailure(INVALID_MANIFEST);
        try {
            var description=describe();if(!description.topicId().toString().equals(manifest.topicId()))throw new ReplayFailure(TOPIC_CHANGED);
            var partitions=description.partitions().stream().map(p->new TopicPartition(topic,p.partition())).toList();
            if(!new TreeSet<>(partitions.stream().map(TopicPartition::partition).toList()).equals(new TreeSet<>(manifest.partitions().stream().map(ReplayManifest.Range::partition).toList())))throw new ReplayFailure(PARTITIONS_CHANGED);
            var starts=bounds(partitions,OffsetSpec.earliest());var ends=bounds(partitions,OffsetSpec.latest());var rows=new ArrayList<Map<String,Object>>();
            for(var range:manifest.partitions()) {
                var partition=new TopicPartition(topic,range.partition());long first=starts.get(partition).offset(),end=ends.get(partition).offset();
                long availableStart=Math.max(first,range.start()),availableEnd=Math.min(end,range.end());
                var row=new LinkedHashMap<String,Object>();row.put("partition",range.partition());row.put("capturedStart",range.start());row.put("capturedEnd",range.end());
                row.put("currentEarliest",first);row.put("currentEnd",end);row.put("capturedOffsetSpan",range.end()-range.start());row.put("availableCapturedOffsetSpan",Math.max(0,availableEnd-availableStart));
                row.put("missingPrefixOffsetSpan",Math.min(range.end()-range.start(),Math.max(0,first-range.start())));
                row.put("state",first>range.start()?"RETENTION_EXPIRED":end<range.end()?"OFFSET_TRUNCATED":"AVAILABLE");
                if(availableStart<availableEnd) {
                    row.put("firstProbe",probe(partition,availableStart,Math.min(availableEnd,availableStart+200),true));
                    row.put("lastProbe",probe(partition,Math.max(availableStart,availableEnd-200),availableEnd,false));
                } else { row.put("firstProbe",null);row.put("lastProbe",null); }
                rows.add(row);
            }
            return Map.of("topic",topic,"topicId",description.topicId().toString(),"observedAt",Instant.now().toString(),"partitions",rows,
                    "timestampMeaning","KAFKA_RECEIVED_AT_NOT_EVENT_TIME","limits",List.of("OFFSET_SPAN_IS_NOT_RECORD_COUNT","BOUNDARY_PROBE_MAX_200_OFFSETS_AND_1_SECOND","NO_EVENT_TIME_COMPLETENESS","NO_DELETED_OR_FUTURE_INPUT"));
        } catch(ReplayFailure failure) {throw failure;} catch(InterruptedException interrupted) {Thread.currentThread().interrupt();throw new ReplayFailure(SOURCE_UNAVAILABLE);}
        catch(Exception ignored) {throw new ReplayFailure(SOURCE_UNAVAILABLE);}
    }
    private Map<String,Object> probe(TopicPartition partition,long from,long to,boolean first) {
        consumer.assign(List.of(partition));consumer.resume(List.of(partition));consumer.seek(partition,from);
        long deadline=System.nanoTime()+Duration.ofSeconds(1).toNanos();ConsumerRecord<String,byte[]> found=null;
        while(System.nanoTime()<deadline) {
            var batch=consumer.poll(Duration.ofNanos(Math.max(1,Math.min(Duration.ofMillis(200).toNanos(),deadline-System.nanoTime()))));
            for(var record:batch.records(partition))if(record.offset()>=from&&record.offset()<to) {
                if(found==null || !first)found=record;
            }
            if(first&&found!=null || System.nanoTime()>=deadline || consumer.position(partition,Duration.ofNanos(Math.max(1,deadline-System.nanoTime())))>=to)break;
        }
        var result=new LinkedHashMap<String,Object>();result.put("fromOffset",from);result.put("toOffset",to);
        result.put("state",found==null?"NOT_OBSERVED":"SAMPLED");result.put("offset",found==null?null:found.offset());
        result.put("kafkaTimestamp",found==null||found.timestamp()<0?null:Instant.ofEpochMilli(found.timestamp()).toString());return result;
    }
    public ConsumerRecords<String,byte[]> poll() { return consumer.poll(Duration.ofMillis(500)); }
    public void pause(int partition) { consumer.pause(List.of(new TopicPartition(topic,partition))); }
    @Override public void close() { try {consumer.close(Duration.ofSeconds(2));} finally {admin.close(Duration.ofSeconds(2));} }
}
