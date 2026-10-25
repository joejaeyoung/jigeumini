package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.time.Instant;
import java.security.MessageDigest;
import java.util.HexFormat;
import tools.jackson.databind.json.JsonMapper;

public final class EventCodec {
    private final JsonMapper json = JsonMapper.builder().disable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    public byte[] encode(ChatEvent event) {
        validate(event);
        byte[] bytes = json.writeValueAsBytes(event);
        if (bytes.length > 16_384) throw new EventContractException("EVENT_TOO_LARGE");
        return bytes;
    }
    public ChatEvent decode(byte[] bytes, String key) {
        if (bytes == null || bytes.length > 16_384) throw new EventContractException("EVENT_SIZE");
        try {
            var tree = json.readTree(bytes);
            if (tree == null || !tree.isObject() || !tree.path("schemaVersion").isIntegralNumber()
                    || !tree.path("schemaVersion").canConvertToInt() || !tree.path("isHistorical").isBoolean()
                    || !tree.has("sourceMessageId") || !tree.path("sourceMessageId").isNull()) {
                throw new EventContractException("INVALID_EVENT_FIELDS");
            }
            for (String field : java.util.List.of("eventId", "source", "channelId", "chatRoomId", "collectionSessionId",
                    "eventType", "eventTime", "receivedAt", "collectorVersion")) {
                if (!tree.path(field).isString()) throw new EventContractException("INVALID_EVENT_FIELDS");
            }
            ChatEvent event = json.treeToValue(tree, ChatEvent.class);
            validate(event);
            if (!event.channelId().equals(key)) throw new EventContractException("KEY_MISMATCH");
            return event;
        } catch (EventContractException e) { throw e; }
        catch (Exception e) { throw new EventContractException("INVALID_EVENT_JSON"); }
    }
    public void validate(ChatEvent e) {
        if (e == null || e.schemaVersion() != 1 || e.eventId() == null || e.collectionSessionId() == null
                || !"CHZZK_UNOFFICIAL".equals(e.source()) || !"CHAT".equals(e.eventType())
                || e.channelId() == null || !e.channelId().matches("[a-f0-9]{32}")
                || e.chatRoomId() == null || !e.chatRoomId().matches("[a-zA-Z0-9_-]{1,128}")
                || e.sourceMessageId() != null || e.isHistorical()
                || e.collectorVersion() == null || !e.collectorVersion().matches("[a-zA-Z0-9._-]{1,40}")
                || e.eventTime() == null || e.receivedAt() == null) {
            throw new EventContractException("INVALID_EVENT_CONTRACT");
        }
        if (e.eventTime().isBefore(e.receivedAt().minusSeconds(7 * 86400))
                || e.eventTime().isAfter(e.receivedAt().plusSeconds(5))) {
            throw new EventContractException("INVALID_EVENT_TIME");
        }
    }
    public void validateLiveAge(ChatEvent e, Instant now) {
        if (e.receivedAt().isBefore(now.minusSeconds(48 * 3600)) || e.receivedAt().isAfter(now.plusSeconds(5))
                || e.eventTime().isBefore(now.minusSeconds(7 * 86400))) {
            throw new EventContractException("OUTSIDE_LIVE_RETENTION");
        }
    }
    public String hash(ChatEvent event) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encode(event))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
