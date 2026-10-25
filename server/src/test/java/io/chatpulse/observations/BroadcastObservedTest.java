package io.chatpulse.observations;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.metadata.PublicBroadcastObservation;
import io.chatpulse.metadata.PublicBroadcastObservation.Quality;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BroadcastObservedTest {
    static final String CHANNEL = "a".repeat(32);
    static BroadcastObserved sample(Instant at, Long count) {
        return BroadcastObserved.observed("test-v1", CHANNEL, at, 120,
                new PublicBroadcastObservation(LiveStatus.LIVE, count, "123", "game", "합성 게임",
                        count == null ? Quality.MISSING_VIEWER_COUNT : Quality.OBSERVED));
    }
    @Test void roundTripPreservesObservedTimeZeroNullAndSourceIdentity() {
        for (Long count : new Long[]{0L, null, 12345L}) {
            var event = sample(Instant.parse("2026-10-25T12:00:00.123456Z"), count);
            assertEquals(event, BroadcastObserved.decode(CHANNEL, event.encode()));
            assertFalse(new String(event.encode(), StandardCharsets.UTF_8).contains("channelName"));
        }
        var unavailable = BroadcastObserved.observed("test-v1", CHANNEL, Instant.now(), 120,
                new PublicBroadcastObservation(LiveStatus.UNKNOWN, null, null, null, null, Quality.RATE_LIMITED));
        assertEquals(BroadcastObserved.BroadcastIdentity.UNAVAILABLE, unavailable.broadcastIdentity());
        assertEquals(unavailable, BroadcastObserved.decode(CHANNEL, unavailable.encode()));
    }
    @Test void rejectsCoercionIdentityMismatchUnsafeFieldsAndOversizeWithoutPayloadInError() {
        String payload = new String(sample(Instant.now(), 10L).encode(), StandardCharsets.UTF_8);
        for (String invalid : List.of(payload.replace("\"viewerCount\":10", "\"viewerCount\":\"10\""),
                payload.replace("\"viewerCount\":10", "\"viewerCount\":10.5"),
                payload.replace("\"viewerCount\":10", "\"viewerCount\":-1"),
                payload.replace("\"viewerCount\":10", "\"viewerCount\":9007199254740992"),
                payload.replace("\"ttlSeconds\":120", "\"ttlSeconds\":\"120\""),
                payload.replace("SOURCE_PROVIDED", "UNAVAILABLE"), payload.replace("OBSERVED", "OFFLINE"),
                payload.replace("LIVE", "OFFLINE"), payload.replace("합성 게임", "SYNTHETIC_PRIVATE\\nVALUE"),
                "SYNTHETIC_PRIVATE".repeat(300))) {
            assertEquals("OBSERVATION_CONTRACT_INVALID", assertThrows(IllegalArgumentException.class,
                    () -> BroadcastObserved.decode(CHANNEL, invalid.getBytes(StandardCharsets.UTF_8))).getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> BroadcastObserved.decode("b".repeat(32), payload.getBytes(StandardCharsets.UTF_8)));
    }
}
