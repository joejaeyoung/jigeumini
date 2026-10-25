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
            for (boolean wrongPartitions : List.of(true,false)) {
                String topic="it.readiness."+UUID.randomUUID();
                String timestamp=wrongPartitions?"CreateTime":"LogAppendTime";
                try {
                    admin.createTopics(List.of(new NewTopic(topic,wrongPartitions?2:3,(short)1).configs(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,timestamp))))
                            .all().get(10,TimeUnit.SECONDS);
                    assertThatThrownBy(() -> KafkaSupport.ensureTopic(BOOTSTRAP,topic)).hasMessage("KAFKA_TOPIC_INIT_FAILED")
                            .hasRootCauseMessage(wrongPartitions?"M1 requires 3 partitions":"M1 requires CreateTime timestamps");
                    assertThat(admin.describeTopics(List.of(topic)).allTopicNames().get(10,TimeUnit.SECONDS).get(topic).partitions()).hasSize(wrongPartitions?2:3);
                    var resource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
                    assertThat(admin.describeConfigs(List.of(resource)).all().get(10,TimeUnit.SECONDS).get(resource).get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG).value()).isEqualTo(timestamp);
                } finally { admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS); }
            }
        }
    }
}
