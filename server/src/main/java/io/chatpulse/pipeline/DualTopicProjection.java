package io.chatpulse.pipeline;

import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/** 두 입력 토픽의 원자 저장·진행/생존 관측 경계. 구현마다 별도 namespace를 사용한다. */
public interface DualTopicProjection {
    void bindSources(Map<String,String> identities);
    void initialize(TopicPartition partition,long offset);
    void apply(List<ConsumerRecord<String,byte[]>> records);
    void observe(TopicPartition partition,long end,Instant observedAt,Instant pollAt,boolean blocked);
    void unavailable(Collection<TopicPartition> partitions);
    int cleanup();
}
