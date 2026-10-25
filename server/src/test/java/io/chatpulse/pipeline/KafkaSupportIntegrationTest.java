package io.chatpulse.pipeline;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class KafkaSupportIntegrationTest {
    private static final String BOOTSTRAP = "localhost:19092";
    @Test void freshAndExistingTopicInitializationPreservesContractAcrossTwentyTopics() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) {
            for (int i=0;i<20;i++) {
                String topic="it.readiness."+UUID.randomUUID();
                try {
                    KafkaSupport.ensureTopic(BOOTSTRAP,topic);
                    KafkaSupport.ensureTopic(BOOTSTRAP,topic);
                    assertThat(admin.describeTopics(List.of(topic)).allTopicNames().get(10,TimeUnit.SECONDS).get(topic).partitions()).hasSize(3);
                    var resource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
                    assertThat(admin.describeConfigs(List.of(resource)).all().get(10,TimeUnit.SECONDS).get(resource).get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).value()).isEqualTo("CreateTime");
                } finally { admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS); }
            }
        }
    }
    @Test void existingInvalidTopicsAreRejectedWithoutChangingTheirSettings() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers",BOOTSTRAP))) {
            for (String wrong : List.of("partitions","timestamp","compaction","retention","min-isr","unclean")) {
                boolean wrongPartitions=wrong.equals("partitions");
                String cleanup=wrong.equals("compaction")?"compact":"delete";
                String topic="it.readiness."+UUID.randomUUID();
                String timestamp=wrong.equals("timestamp")?"LogAppendTime":"CreateTime";
                var settings=new HashMap<>(KafkaTopicPolicy.local().configurations());
                settings.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,timestamp);settings.put(TopicConfig.CLEANUP_POLICY_CONFIG,cleanup);
                if(wrong.equals("retention"))settings.put(TopicConfig.RETENTION_MS_CONFIG,"3600000");
                if(wrong.equals("min-isr"))settings.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"2");
                if(wrong.equals("unclean"))settings.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,"true");
                try {
                    admin.createTopics(List.of(new NewTopic(topic,wrongPartitions?2:3,(short)1).configs(settings)))
                            .all().get(10,TimeUnit.SECONDS);
                    assertThatThrownBy(() -> KafkaSupport.ensureTopic(BOOTSTRAP,topic)).hasMessage("KAFKA_TOPIC_INIT_FAILED")
                            .hasRootCauseMessage(wrongPartitions?"M1 requires 3 partitions":wrong.equals("timestamp")?"M1 requires CreateTime timestamps":wrong.equals("compaction")?"History requires delete cleanup policy":"KAFKA_TOPIC_CONFIG_MISMATCH");
                    assertThat(admin.describeTopics(List.of(topic)).allTopicNames().get(10,TimeUnit.SECONDS).get(topic).partitions()).hasSize(wrongPartitions?2:3);
                    var resource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
                    assertThat(admin.describeConfigs(List.of(resource)).all().get(10,TimeUnit.SECONDS).get(resource).get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).value()).isEqualTo(timestamp);
                    var actual=admin.describeConfigs(List.of(resource)).all().get(10,TimeUnit.SECONDS).get(resource);
                    for(var entry:settings.entrySet())assertThat(actual.get(entry.getKey()).value()).isEqualTo(entry.getValue());
                } finally { admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS); }
            }
        }
    }
}
