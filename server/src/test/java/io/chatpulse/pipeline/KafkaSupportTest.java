package io.chatpulse.pipeline;

import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class KafkaSupportTest {
    private static final String TOPIC = "synthetic.readiness";
    private static final ConfigResource RESOURCE = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
    private static <T> KafkaFuture<T> failed(Throwable error) {
        var future = new KafkaFutureImpl<T>(); future.completeExceptionally(error); return future;
    }
    private Admin readyAdmin() {
        var admin = mock(Admin.class);
        var created = mock(CreateTopicsResult.class); when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(created);
        var topics = mock(DescribeTopicsResult.class);
        var node = new Node(1, "localhost", 19092);
        var partitions = List.of(new TopicPartitionInfo(0,node,List.of(node),List.of(node)),
                new TopicPartitionInfo(1,node,List.of(node),List.of(node)),new TopicPartitionInfo(2,node,List.of(node),List.of(node)));
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC,new TopicDescription(TOPIC,false,partitions))));
        when(admin.describeTopics(anyCollection())).thenReturn(topics);
        when(admin.describeTopics(anyCollection(),any(DescribeTopicsOptions.class))).thenReturn(topics);
        var configs = mock(DescribeConfigsResult.class);
        when(configs.all()).thenReturn(KafkaFuture.completedFuture(Map.of(RESOURCE,new Config(KafkaTopicPolicy.local().configurations().entrySet().stream().map(e -> new ConfigEntry(e.getKey(),e.getValue())).toList()))));
        when(admin.describeConfigs(anyCollection())).thenReturn(configs);
        when(admin.describeConfigs(anyCollection(),any(DescribeConfigsOptions.class))).thenReturn(configs);
        return admin;
    }
    @Test void creationAndTopicDescriptionCanSucceedBeforeConfigIsVisible() {
        var admin = readyAdmin(); var configs = mock(DescribeConfigsResult.class);
        when(configs.all()).thenReturn(failed(new UnknownTopicOrPartitionException("synthetic not visible")),
                KafkaFuture.completedFuture(Map.of(RESOURCE,new Config(KafkaTopicPolicy.local().configurations().entrySet().stream().map(e -> new ConfigEntry(e.getKey(),e.getValue())).toList()))));
        when(admin.describeConfigs(anyCollection())).thenReturn(configs);
        when(admin.describeConfigs(anyCollection(),any(DescribeConfigsOptions.class))).thenReturn(configs);
        try (var factory = mockStatic(Admin.class)) {
            factory.when(() -> Admin.create(anyMap())).thenReturn(admin);
            assertThatCode(() -> KafkaSupport.ensureTopic("localhost:19092",TOPIC)).doesNotThrowAnyException();
        }
        verify(admin,times(1)).createTopics(anyCollection());
        verify(configs,times(2)).all();
    }
    @Test void invisibleTopicStopsAtOneSharedDeadline() {
        var admin = readyAdmin(); var configs = mock(DescribeConfigsResult.class);
        when(configs.all()).thenReturn(failed(new UnknownTopicOrPartitionException("synthetic invisible")));
        when(admin.describeConfigs(anyCollection(),any(DescribeConfigsOptions.class))).thenReturn(configs);
        long[] nanos = {0};
        assertThatThrownBy(() -> KafkaSupport.awaitTopic(admin,TOPIC,java.time.Duration.ofMillis(100),() -> nanos[0],millis -> nanos[0] += millis * 1000000))
                .isInstanceOf(java.util.concurrent.TimeoutException.class).hasMessage("TOPIC_VISIBILITY_DEADLINE");
        assertThat(nanos[0]).isEqualTo(100000000);
        verify(configs,times(2)).all();
    }
    @Test void missingIsrIsBoundedByTheSameReadinessDeadline() {
        var admin=readyAdmin();var topics=mock(DescribeTopicsResult.class);var node=new Node(1,"localhost",19092);
        var parts=java.util.stream.IntStream.range(0,3).mapToObj(i->new TopicPartitionInfo(i,node,List.of(node),List.of())).toList();
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC,new TopicDescription(TOPIC,false,parts))));
        when(admin.describeTopics(anyCollection(),any(DescribeTopicsOptions.class))).thenReturn(topics);
        long[] nanos={0};
        assertThatThrownBy(()->KafkaSupport.awaitTopic(admin,TOPIC,java.time.Duration.ofMillis(100),()->nanos[0],millis->nanos[0]+=millis*1000000))
                .isInstanceOf(java.util.concurrent.TimeoutException.class).hasMessage("TOPIC_VISIBILITY_DEADLINE");
        verify(topics,times(2)).allTopicNames();assertThat(nanos[0]).isEqualTo(100000000);
    }
    @Test void authorityFailuresAndWrongConfigurationAreNeverRetried() {
        var admin = readyAdmin(); var configs = mock(DescribeConfigsResult.class);
        var denied = new org.apache.kafka.common.errors.TopicAuthorizationException(Set.of(TOPIC));
        when(configs.all()).thenReturn(failed(denied));
        when(admin.describeConfigs(anyCollection(),any(DescribeConfigsOptions.class))).thenReturn(configs);
        assertThatThrownBy(() -> KafkaSupport.awaitTopic(admin,TOPIC,java.time.Duration.ofSeconds(1),() -> 0L,millis -> fail("must not retry")))
                .hasCause(denied);
        when(configs.all()).thenReturn(KafkaFuture.completedFuture(Map.of(RESOURCE,new Config(List.of(new ConfigEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,"LogAppendTime"))))));
        assertThatThrownBy(() -> KafkaSupport.awaitTopic(admin,TOPIC,java.time.Duration.ofSeconds(1),() -> 0L,millis -> fail("must not retry")))
                .hasMessage("M1 requires CreateTime timestamps");
    }
    @Test void interruptedWaitRestoresInterruptAndClosesAdmin() throws Exception {
        var admin = readyAdmin(); var configs = mock(DescribeConfigsResult.class);
        @SuppressWarnings("unchecked") KafkaFuture<Map<ConfigResource,Config>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(),any(java.util.concurrent.TimeUnit.class))).thenThrow(new InterruptedException("synthetic interrupt"));
        when(configs.all()).thenReturn(future);
        when(admin.describeConfigs(anyCollection(),any(DescribeConfigsOptions.class))).thenReturn(configs);
        try (var factory = mockStatic(Admin.class)) {
            factory.when(() -> Admin.create(anyMap())).thenReturn(admin);
            assertThatThrownBy(() -> KafkaSupport.ensureTopic("localhost:19092",TOPIC)).hasMessage("KAFKA_TOPIC_INIT_FAILED");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
        verify(admin).close(java.time.Duration.ofSeconds(1));
    }
}
