package io.chatpulse.fleet;

import java.util.List;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CollectorStartupTest {
    @SuppressWarnings("unchecked") private final KafkaTemplate<String,byte[]> kafka=mock(KafkaTemplate.class);
    @Test void transientMetadataTimeoutCanRecoverWithinStartupAttempts() {
        when(kafka.partitionsFor("synthetic")).thenThrow(new TimeoutException("synthetic")).thenReturn(List.of(mock(PartitionInfo.class)));
        CollectorWorker.awaitKafkaMetadata(kafka,"synthetic");verify(kafka,times(2)).partitionsFor("synthetic");
    }
    @Test void persistentTimeoutStopsAfterFiveAttemptsAndPreservesCause() {
        var timeout=new TimeoutException("synthetic");when(kafka.partitionsFor("synthetic")).thenThrow(timeout);
        var failure=assertThrows(IllegalStateException.class,()->CollectorWorker.awaitKafkaMetadata(kafka,"synthetic"));
        assertEquals("FLEET_KAFKA_NOT_READY",failure.getMessage());assertSame(timeout,failure.getCause());verify(kafka,times(5)).partitionsFor("synthetic");
    }
    @Test void authenticationFailureIsNotRetried() {
        var denied=new AuthenticationException("synthetic");when(kafka.partitionsFor("synthetic")).thenThrow(denied);
        assertSame(denied,assertThrows(IllegalStateException.class,()->CollectorWorker.awaitKafkaMetadata(kafka,"synthetic")).getCause());verify(kafka).partitionsFor("synthetic");
    }
    @Test void emptyMetadataDoesNotAdmitAWorker() {
        when(kafka.partitionsFor("synthetic")).thenReturn(List.of());
        assertEquals("FLEET_KAFKA_NOT_READY",assertThrows(IllegalStateException.class,()->CollectorWorker.awaitKafkaMetadata(kafka,"synthetic")).getMessage());verify(kafka).partitionsFor("synthetic");
    }
}
