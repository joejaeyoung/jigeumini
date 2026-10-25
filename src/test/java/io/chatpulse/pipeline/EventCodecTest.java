package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EventCodecTest {
    private final EventCodec codec = new EventCodec();
    static final String CHANNEL = "45e71a76e949e16a34764deb962f9d9f";
    static ChatEvent event(Instant at) {
        return new ChatEvent(1, UUID.randomUUID(), "CHZZK_UNOFFICIAL", CHANNEL, "room", UUID.randomUUID(),
                null, "CHAT", at, at, false, "test");
    }
    @Test void roundTripAndStableHash() {
        var e = event(Instant.now());
        assertEquals(e, codec.decode(codec.encode(e), CHANNEL));
        assertEquals(codec.hash(e), codec.hash(codec.decode(codec.encode(e), CHANNEL)));
    }
    @Test void rejectsWrongKeyNullOversizeAndMalformed() {
        assertThrows(EventContractException.class, () -> codec.decode(codec.encode(event(Instant.now())), "wrong"));
        assertThrows(EventContractException.class, () -> codec.decode(null, CHANNEL));
        assertThrows(EventContractException.class, () -> codec.decode(new byte[16385], CHANNEL));
        assertThrows(EventContractException.class, () -> codec.decode("{PRIVATE_CANARY".getBytes(), CHANNEL));
    }
    @Test void requiredFieldsAreNotSilentlyCoercedButOptionalFieldsAreIgnored() {
        String valid = new String(codec.encode(event(Instant.now())), java.nio.charset.StandardCharsets.UTF_8);
        assertThrows(EventContractException.class, () -> codec.decode(valid.replace("\"isHistorical\":false,", "").getBytes(), CHANNEL));
        assertThrows(EventContractException.class, () -> codec.decode(valid.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\"").getBytes(), CHANNEL));
        var decoded = codec.decode(valid.replaceFirst("\\{", "{\"futureField\":42,").getBytes(), CHANNEL);
        assertFalse(new String(codec.encode(decoded)).contains("futureField"));
    }
    @Test void rejectsUnsupportedHistoricalAndOutOfRetention() {
        var e = event(Instant.now().minusSeconds(49 * 3600));
        assertThrows(EventContractException.class, () -> codec.validateLiveAge(e, Instant.now()));
        var historical = new ChatEvent(1, e.eventId(), e.source(), e.channelId(), e.chatRoomId(),
                e.collectionSessionId(), null, "CHAT", e.eventTime(), e.receivedAt(), true, "test");
        assertThrows(EventContractException.class, () -> codec.encode(historical));
    }
}
