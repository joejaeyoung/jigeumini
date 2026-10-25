package io.chatpulse.poc;

import java.time.Instant;
import java.util.UUID;

/** 원문·작성자 식별정보를 담지 않는 다음 단계 Kafka 입력 후보. */
public record ChatEvent(int schemaVersion, UUID eventId, String source, String channelId,
        String chatRoomId, UUID collectionSessionId, String sourceMessageId, String eventType,
        Instant eventTime, Instant receivedAt, boolean isHistorical, String collectorVersion) {
    static ChatEvent live(String channel, String room, UUID session, Instant time, Instant received) {
        return new ChatEvent(1, UUID.randomUUID(), "CHZZK_UNOFFICIAL", channel, room, session,
                null, "CHAT", time, received, false, "0.1.0");
    }
}
