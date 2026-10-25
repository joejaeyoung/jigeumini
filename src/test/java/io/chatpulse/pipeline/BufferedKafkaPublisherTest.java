package io.chatpulse.pipeline;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BufferedKafkaPublisherTest {
    @Test void inFlightCountsTowardCapacityAndOverflowIsExplicit() throws Exception {
        @SuppressWarnings("unchecked") var kafka = (KafkaTemplate<String, byte[]>) mock(KafkaTemplate.class);
        var pending = new CompletableFuture<SendResult<String, byte[]>>();
        when(kafka.send(any(ProducerRecord.class))).thenReturn(pending);
        try (var publisher = new BufferedKafkaPublisher(kafka, "topic", new EventCodec(), 1)) {
            publisher.accept(List.of(EventCodecTest.event(Instant.now())));
            verify(kafka, timeout(1000)).send(any(ProducerRecord.class));
            assertThrows(io.chatpulse.poc.SourceFailure.class,
                    () -> publisher.accept(List.of(EventCodecTest.event(Instant.now()))));
            pending.complete(null);
            var result = publisher.finish();
            assertEquals(1L, result.get("acknowledged"));
            assertEquals(1L, result.get("rejected"));
            assertEquals(false, result.get("complete"));
        }
    }
    @Test void failedSendRemainsUnconfirmed() {
        @SuppressWarnings("unchecked") var kafka = (KafkaTemplate<String, byte[]>) mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("test")));
        try (var publisher = new BufferedKafkaPublisher(kafka, "topic", new EventCodec(), 2)) {
            publisher.accept(List.of(EventCodecTest.event(Instant.now())));
            var result = publisher.finish();
            assertEquals(1L, result.get("unconfirmed"));
            assertEquals(false, result.get("complete"));
        }
    }
}
