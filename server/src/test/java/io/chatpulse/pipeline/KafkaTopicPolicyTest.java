package io.chatpulse.pipeline;

import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaTopicPolicyTest {
    private static Config config(Map<String,String> values) { return new Config(values.entrySet().stream().map(e -> new ConfigEntry(e.getKey(),e.getValue())).toList()); }
    private static TopicDescription topic(int replicas,int isr,boolean leader) {
        var nodes=java.util.stream.IntStream.rangeClosed(1,replicas).mapToObj(i->new Node(i,"synthetic-broker-"+i,9092)).toList();
        return new TopicDescription("synthetic",false,java.util.stream.IntStream.range(0,3)
                .mapToObj(i->new TopicPartitionInfo(i,leader?nodes.getFirst():Node.noNode(),nodes,nodes.subList(0,isr))).toList());
    }
    @Test void haAcceptsThreeReplicasWithTwoOrThreeInSyncAndRejectsRfOne() {
        var policy=KafkaTopicPolicy.ha();var config=config(policy.configurations());
        assertThatCode(()->policy.verify(topic(3,3,true),config)).doesNotThrowAnyException();
        assertThatCode(()->policy.verify(topic(3,2,true),config)).doesNotThrowAnyException();
        assertThatThrownBy(()->policy.verify(topic(1,1,true),config)).hasMessage("KAFKA_REPLICATION_FACTOR_MISMATCH");
        assertThatThrownBy(()->policy.verify(topic(3,1,true),config)).isInstanceOf(KafkaTopicPolicy.NotReady.class);
        assertThatThrownBy(()->policy.verify(topic(3,3,false),config)).isInstanceOf(KafkaTopicPolicy.NotReady.class);
    }
    @Test void retentionMinIsrSegmentAndUncleanElectionAreVerifiedWithoutRepairingExistingConfig() {
        var policy=KafkaTopicPolicy.ha();
        var wrong=Map.of(TopicConfig.RETENTION_MS_CONFIG,"3600000",TopicConfig.RETENTION_BYTES_CONFIG,"536870912",
                TopicConfig.SEGMENT_BYTES_CONFIG,"1048576",TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"1",TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,"true");
        wrong.forEach((key,value)->{
            var values=new HashMap<>(policy.configurations());values.put(key,value);var config=config(values);
            assertThatThrownBy(()->policy.verify(topic(3,3,true),config)).hasMessage("KAFKA_TOPIC_CONFIG_MISMATCH");
            assertThat(config.get(key).value()).isEqualTo(value);
        });
    }
    @Test void localAndHaAreExplicitAndUnboundedRetentionIsRejected() {
        var local=KafkaTopicPolicy.local();assertThat(local.replicationFactor()).isEqualTo((short)1);assertThat(local.minInSyncReplicas()).isEqualTo(1);
        assertThatCode(()->local.verify(topic(1,1,true),config(local.configurations()))).doesNotThrowAnyException();
        assertThatThrownBy(()->new KafkaTopicPolicy(KafkaTopicPolicy.Durability.HA,-1,268435456,33554432)).hasMessage("KAFKA_TOPIC_POLICY");
        assertThatThrownBy(()->new KafkaTopicPolicy(KafkaTopicPolicy.Durability.HA,86400000,-1,33554432)).hasMessage("KAFKA_TOPIC_POLICY");
        assertThatThrownBy(()->new KafkaTopicPolicy(KafkaTopicPolicy.Durability.HA,86400000,1048576,33554432)).hasMessage("KAFKA_TOPIC_POLICY");
    }
}
