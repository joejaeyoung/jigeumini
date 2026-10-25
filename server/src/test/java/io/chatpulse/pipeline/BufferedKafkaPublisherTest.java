package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.poc.SourceFailure;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BufferedKafkaPublisherTest {
    private static final String HOT = "00000000000000000000000000000001", COLD = "00000000000000000000000000000002";
    private record Sent(ProducerRecord<String, byte[]> record, CompletableFuture<SendResult<String, byte[]>> future) { }
    private static final class Broker {
        @SuppressWarnings("unchecked") final KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        final LinkedBlockingQueue<Sent> calls = new LinkedBlockingQueue<>();
        Broker() {
            when(kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
                var sent = new Sent(invocation.getArgument(0), new CompletableFuture<>()); calls.add(sent); return sent.future;
            });
        }
        Sent next() throws Exception { var sent = calls.poll(3, TimeUnit.SECONDS); assertNotNull(sent); return sent; }
    }
    private static ChatEvent event(String channel) {
        var e = EventCodecTest.event(Instant.now());
        return new ChatEvent(e.schemaVersion(), e.eventId(), e.source(), channel, e.chatRoomId(), e.collectionSessionId(),
                e.sourceMessageId(), e.eventType(), e.eventTime(), e.receivedAt(), e.isHistorical(), e.collectorVersion());
    }
    private static BufferedKafkaPublisher publisher(Broker b, int capacity, int perChannel, int inFlight) {
        return new BufferedKafkaPublisher(b.kafka, "topic", new EventCodec(), new BufferedKafkaPublisher.Limits(capacity, perChannel, inFlight), Duration.ofSeconds(1));
    }
    private static void balances(java.util.Map<String, Object> result, long accepted, long ack, long rejected) {
        assertEquals(accepted, result.get("accepted")); assertEquals(ack, result.get("acknowledged"));
        assertEquals(rejected, result.get("rejected")); assertEquals(accepted - ack, result.get("unconfirmed"));
        assertEquals(accepted + rejected, result.get("attempted"));
    }
    @Test void inFlightCountsTowardCapacityAndOverflowIsExplicit() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 1, 1, 1)) {
            p.accept(List.of(event(HOT))); var sent = b.next();
            assertThrows(SourceFailure.class, () -> p.accept(List.of(event(COLD))));
            sent.future.complete(null);
            var result = p.finish(); balances(result, 1, 1, 1); assertEquals(false, result.get("complete"));
            assertEquals("PUBLISH_BUFFER_REJECTED", result.get("failureCode"));
        }
    }
    @Test void globalOverflowStillDrainsPreviouslyAcceptedQueueWhileBrokerIsHealthy() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 3, 3, 1)) {
            p.accept(List.of(event(HOT), event(HOT), event(COLD))); var sent = b.next();
            assertThrows(SourceFailure.class, () -> p.accept(List.of(event(COLD))));
            sent.future.complete(null); b.next().future.complete(null); b.next().future.complete(null);
            balances(p.finish(), 3, 3, 1); assertTrue(p.deliveryFailed());
        }
    }
    @Test void failedCallbackStopsNewDispatchBeforeCapacityIsReached() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 10, 10, 1)) {
            p.accept(List.of(event(HOT), event(COLD))); b.next().future.completeExceptionally(new RuntimeException("PRIVATE_CANARY"));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(p::deliveryFailed);
            assertThrows(SourceFailure.class, p::checkHealthy);
            assertThrows(SourceFailure.class, () -> p.accept(List.of(event(HOT))));
            var result = p.finish(); balances(result, 2, 0, 1); assertEquals(1L, result.get("failedCallbacks"));
            verify(b.kafka, times(1)).send(any(ProducerRecord.class));
            assertFalse(result.toString().contains("PRIVATE_CANARY"));
        }
    }
    @Test void callbackAlreadyFailedBeforeSendReturnsStillStopsTheNextDispatch() {
        var b = new Broker(); when(b.kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("synthetic")));
        try (var p = publisher(b, 4, 4, 1)) {
            p.accept(List.of(event(HOT), event(COLD))); balances(p.finish(), 2, 0, 0);
            assertTrue(p.deliveryFailed()); verify(b.kafka, times(1)).send(any(ProducerRecord.class));
        }
    }
    @Test void asynchronousWindowAndPhysicalDuplicatesRemainBounded() throws Exception {
        var b = new Broker(); var e = event(HOT);
        try (var p = publisher(b, 4, 4, 2)) {
            p.accept(List.of(e, e, e, e));
            var first = b.next(); var second = b.next();
            verify(b.kafka, times(2)).send(any(ProducerRecord.class));
            assertEquals(4, p.bufferSnapshot().get(HOT).count());
            first.future.complete(null); b.next().future.complete(null); b.next().future.complete(null); second.future.complete(null);
            var result = p.finish(); balances(result, 4, 4, 0);
            assertEquals(2, result.get("peakInFlight")); assertEquals(4, result.get("peakPending"));
            assertEquals(true, result.get("complete")); assertTrue(p.bufferSnapshot().isEmpty());
        }
    }
    @Test void channelOverflowStopsOnlyThatChannelAndOtherChannelsDrain() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 5, 2, 1)) {
            p.accept(List.of(event(HOT), event(HOT))); var first = b.next();
            assertEquals("PUBLISH_CHANNEL_LIMIT", assertThrows(SourceFailure.class, () -> p.accept(List.of(event(HOT)))).getMessage());
            assertDoesNotThrow(p::checkHealthy); assertFalse(p.deliveryFailed());
            p.accept(List.of(event(COLD)));
            first.future.complete(null); b.next().future.complete(null); b.next().future.complete(null);
            var result = p.finish(); balances(result, 3, 3, 1);
            var hot = p.channelSnapshot().get(HOT); assertEquals(1, hot.rejected()); assertTrue(hot.stopped());
            assertEquals(1, p.channelSnapshot().get(COLD).acknowledged()); assertEquals(false, result.get("complete"));
        }
    }
    @Test void dispatchRotatesQueuedChannelsAndKeepsEachChannelsAppendOrder() throws Exception {
        var b = new Broker(); var hot = List.of(event(HOT), event(HOT), event(HOT)); var cold = List.of(event(COLD), event(COLD));
        try (var p = publisher(b, 6, 4, 1)) {
            p.accept(hot); var first = b.next(); p.accept(cold);
            var records = new ArrayList<ProducerRecord<String, byte[]>>(); records.add(first.record); first.future.complete(null);
            for (int i = 0; i < 4; i++) { var next = b.next(); records.add(next.record); next.future.complete(null); }
            p.finish(); assertEquals(List.of(HOT, HOT, COLD, HOT, COLD), records.stream().map(ProducerRecord::key).toList());
            var codec = new EventCodec();
            assertEquals(hot.stream().map(ChatEvent::eventId).toList(), records.stream().filter(r -> r.key().equals(HOT)).map(r -> codec.decode(r.value(), r.key()).eventId()).toList());
            for (var record : records) assertEquals(codec.decode(record.value(), record.key()).receivedAt().toEpochMilli(), record.timestamp());
        }
    }
    @Test void invalidFrameRejectsEveryEventBeforeAnySend() {
        var b = new Broker(); var e = event(HOT);
        var invalid = new ChatEvent(1, UUID.randomUUID(), e.source(), HOT, e.chatRoomId(), e.collectionSessionId(),
                null, "CHAT", e.eventTime(), e.receivedAt(), true, "test");
        try (var p = publisher(b, 4, 4, 2)) {
            assertThrows(SourceFailure.class, () -> p.accept(List.of(e, invalid)));
            balances(p.finish(), 0, 0, 2); verifyNoInteractions(b.kafka);
        }
    }
    @Test void mixedChannelFrameIsAtomicWhenOneChannelCannotFit() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 6, 2, 1)) {
            p.accept(List.of(event(HOT), event(HOT))); var first = b.next();
            assertThrows(SourceFailure.class, () -> p.accept(List.of(event(HOT), event(COLD))));
            first.future.complete(null); b.next().future.complete(null);
            balances(p.finish(), 2, 2, 2); assertEquals(0, p.channelSnapshot().get(COLD).accepted());
        }
    }
    @Test void synchronousKafkaSendFailureRetainsTheWholeAcceptedFrame() {
        var b = new Broker(); when(b.kafka.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("PRIVATE_CANARY"));
        try (var p = publisher(b, 4, 4, 2)) {
            p.accept(List.of(event(HOT), event(COLD))); balances(p.finish(), 2, 0, 0);
            assertTrue(p.deliveryFailed()); verify(b.kafka, times(1)).send(any(ProducerRecord.class));
        }
    }
    @Test void blockedKafkaSendNeverHoldsTheAdmissionLock() throws Exception {
        var b = new Broker(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        when(b.kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return CompletableFuture.completedFuture(null);
        });
        try (var p = publisher(b, 4, 4, 2)) {
            try {
                p.accept(List.of(event(HOT))); assertTrue(entered.await(3, TimeUnit.SECONDS));
                var admitted = CompletableFuture.runAsync(() -> p.accept(List.of(event(COLD))));
                admitted.get(1, TimeUnit.SECONDS);
            } finally { release.countDown(); }
            balances(p.finish(), 2, 2, 0);
        }
    }
    @Test void timeoutFreezesTheReportAndLateAckCannotChangeIt() throws Exception {
        var b = new Broker();
        try (var p = new BufferedKafkaPublisher(b.kafka, "topic", new EventCodec(), new BufferedKafkaPublisher.Limits(4, 4, 2), Duration.ofMillis(50))) {
            p.accept(List.of(event(HOT), event(COLD))); var a = b.next(); var c = b.next();
            var result = p.finish(); balances(result, 2, 0, 0); assertEquals("PUBLISH_DRAIN_TIMEOUT", result.get("failureCode"));
            a.future.complete(null); c.future.complete(null);
            assertThrows(SourceFailure.class, () -> p.accept(List.of(event(HOT))));
            assertSame(result, p.finish()); balances(p.finish(), 2, 0, 0);
            assertEquals(1, p.channelSnapshot().get(HOT).pending());
        }
    }
    @Test void simultaneousFramesCannotOverrunTheSharedReservation() throws Exception {
        var b = new Broker(); var barrier = new CountDownLatch(1);
        try (var p = publisher(b, 8, 8, 2)) {
            var futures = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 4; i++) futures.add(CompletableFuture.runAsync(() -> {
                try { barrier.await(); p.accept(List.of(event(HOT), event(COLD))); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }));
            barrier.countDown(); CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(3, TimeUnit.SECONDS);
            assertEquals(8, p.bufferSnapshot().values().stream().mapToInt(BufferedKafkaPublisher.BufferState::count).sum());
            for (int i = 0; i < 8; i++) b.next().future.complete(null);
            var result = p.finish(); balances(result, 8, 8, 0); assertEquals(8, result.get("peakPending"));
        }
    }
    @Test void finishWaitsWithoutHoldingCallbackLockAndIsIdempotent() throws Exception {
        var b = new Broker();
        try (var p = publisher(b, 4, 4, 2)) {
            p.accept(List.of(event(HOT))); var sent = b.next();
            var closing = CompletableFuture.supplyAsync(p::finish); sent.future.complete(null);
            var result = closing.get(2, TimeUnit.SECONDS); balances(result, 1, 1, 0);
            assertSame(result, p.finish()); assertEquals(true, result.get("complete"));
        }
    }
}
