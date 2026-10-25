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
        return admin.describeTopics(List.of(topic)).allTopicNames().get(5,TimeUnit.SECONDS).get(topic);
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
    public ConsumerRecords<String,byte[]> poll() { return consumer.poll(Duration.ofMillis(500)); }
    public void pause(int partition) { consumer.pause(List.of(new TopicPartition(topic,partition))); }
    @Override public void close() { try {consumer.close(Duration.ofSeconds(2));} finally {admin.close(Duration.ofSeconds(2));} }
}
