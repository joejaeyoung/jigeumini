package io.chatpulse.pipeline;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BatchConsumersTest {
    private final TopicPartition retained = new TopicPartition("synthetic", 0);
    private final TopicPartition first = new TopicPartition("synthetic", 1);
    @SuppressWarnings("unchecked")
    private final Consumer<String, byte[]> consumer = mock(Consumer.class);

    @Test void earliestInitializesDatabaseBeforeCommittingOnlyNewPartitions() {
        when(consumer.committed(Set.of(retained, first))).thenReturn(Map.of(retained, new OffsetAndMetadata(7)));
        when(consumer.position(retained)).thenReturn(7L);
        when(consumer.position(first)).thenReturn(3L);
        var initialized = new ArrayList<Long>();
        doAnswer(invocation -> {
            assertThat(initialized).containsExactly(7L, 3L);
            return null;
        }).when(consumer).commitSync(Map.of(first, new OffsetAndMetadata(3)));

        BatchConsumers.initialOffsets("earliest", (partition, offset) -> initialized.add(offset), () -> { })
                .onPartitionsAssigned(consumer, List.of(retained, first));

        verify(consumer).commitSync(Map.of(first, new OffsetAndMetadata(3)));
    }

    @Test void databaseFailureDoesNotCommitAnyInitialPosition() {
        when(consumer.committed(Set.of(retained, first))).thenReturn(Map.of());
        when(consumer.position(retained)).thenReturn(3L);
        when(consumer.position(first)).thenReturn(9L);
        var listener = BatchConsumers.initialOffsets("earliest", (partition, offset) -> {
            if (partition.equals(first)) throw new IllegalStateException("SYNTHETIC_DB_FAILURE");
        }, () -> { });

        assertThatThrownBy(() -> listener.onPartitionsAssigned(consumer, List.of(retained, first)))
                .hasMessage("SYNTHETIC_DB_FAILURE");
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test void noneDoesNotResolveOrCommitAnUninitializedPosition() {
        when(consumer.committed(Set.of(first))).thenReturn(Map.of());
        BatchConsumers.initialOffsets("none").onPartitionsAssigned(consumer, List.of(first));
        verify(consumer, never()).position(any());
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test void noneStillInitializesProjectionProgressWithoutKafkaCommit() {
        when(consumer.committed(Set.of(retained))).thenReturn(Map.of(retained, new OffsetAndMetadata(7)));
        when(consumer.position(retained)).thenReturn(7L);
        var positions = new ArrayList<Long>();
        BatchConsumers.initialOffsets("none", (partition, offset) -> positions.add(offset), () -> { })
                .onPartitionsAssigned(consumer, List.of(retained));
        assertThat(positions).containsExactly(7L);
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test void emptyPollIsObservedButFailedPollDoesNotRefreshFreshness() {
        var tracker = new ConsumerPollTracker();
        var failure = new IllegalStateException("SYNTHETIC_POLL_FAILURE");
        when(consumer.poll(Duration.ZERO)).thenReturn(ConsumerRecords.empty()).thenThrow(failure);
        when(consumer.assignment()).thenReturn(Set.of(first));
        var tracked = tracker.track(consumer);
        assertThat(tracked.poll(Duration.ZERO).isEmpty()).isTrue();
        var snapshot = tracker.snapshot();
        assertThat(snapshot.at()).isNotNull();
        assertThat(snapshot.partitions()).containsExactly(first);

        assertThatThrownBy(() -> tracked.poll(Duration.ZERO)).isSameAs(failure);
        assertThat(tracker.snapshot()).isSameAs(snapshot);
        tracker.revoked();
        assertThat(tracker.snapshot().at()).isNull();
        assertThat(tracker.snapshot().partitions()).isEmpty();
    }
}
