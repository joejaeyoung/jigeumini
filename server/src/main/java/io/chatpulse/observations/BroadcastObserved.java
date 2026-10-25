package io.chatpulse.observations;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.metadata.PublicBroadcastObservation;
import io.chatpulse.metadata.PublicBroadcastObservation.Quality;
import java.time.Instant;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

/** 시청자·방송 상태를 그 시각에 관측했다는 사실. 과거 시각으로 소급하지 않는다. */
public record BroadcastObserved(int schemaVersion, UUID eventId, String runId, String channelId,
        Instant observedAt, int ttlSeconds, Long viewerCount, LiveStatus liveStatus, Quality quality,
        String sourceBroadcastId, BroadcastIdentity broadcastIdentity, String categoryId, String categoryName) {
    public enum BroadcastIdentity { SOURCE_PROVIDED, UNAVAILABLE }
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public BroadcastObserved {
        if (schemaVersion != 1 || eventId == null || runId == null || !runId.matches("[a-zA-Z0-9_-]{1,80}")
                || channelId == null || !channelId.matches("[a-f0-9]{32}") || observedAt == null
                || observedAt.isBefore(Instant.parse("2000-01-01T00:00:00Z")) || observedAt.isAfter(Instant.parse("2100-01-01T00:00:00Z"))
                || ttlSeconds < 60 || ttlSeconds > 1800 || liveStatus == null || quality == null || broadcastIdentity == null
                || (viewerCount != null && (viewerCount < 0 || viewerCount > 9007199254740991L))
                || (sourceBroadcastId != null && !sourceBroadcastId.matches("[1-9][0-9]{0,18}"))
                || (sourceBroadcastId != null) != (broadcastIdentity == BroadcastIdentity.SOURCE_PROVIDED)
                || !safeCategory(categoryId) || !safeCategory(categoryName)) throw invalid();
        boolean consistent = switch (liveStatus) {
            case LIVE -> (quality == Quality.OBSERVED && viewerCount != null)
                    || (quality == Quality.MISSING_VIEWER_COUNT && viewerCount == null);
            case OFFLINE -> quality == Quality.OFFLINE;
            case RESTRICTED -> quality == Quality.RESTRICTED;
            case NOT_FOUND -> quality == Quality.NOT_FOUND;
            case UNKNOWN -> Set.of(Quality.RATE_LIMITED, Quality.UPSTREAM_ERROR, Quality.INVALID_RESPONSE).contains(quality);
        };
        if (!consistent || (liveStatus != LiveStatus.LIVE && (viewerCount != null || categoryId != null || categoryName != null))
                || (liveStatus != LiveStatus.LIVE && liveStatus != LiveStatus.OFFLINE && sourceBroadcastId != null)) throw invalid();
    }
    public static BroadcastObserved observed(String run, String channel, Instant at, int ttl, PublicBroadcastObservation data) {
        return new BroadcastObserved(1, UUID.randomUUID(), run, channel, at, ttl, data.viewerCount(), data.liveStatus(), data.quality(),
                data.sourceBroadcastId(), data.sourceBroadcastId() == null ? BroadcastIdentity.UNAVAILABLE : BroadcastIdentity.SOURCE_PROVIDED,
                data.categoryId(), data.categoryName());
    }
    public byte[] encode() { return JSON.writeValueAsBytes(this); }
    public static BroadcastObserved decode(String key, byte[] bytes) {
        try {
            if (bytes == null || bytes.length > 4096) throw invalid();
            var node = JSON.readTree(bytes);
            if (node == null || !node.isObject() || !node.path("schemaVersion").isIntegralNumber()
                    || !node.path("ttlSeconds").isIntegralNumber() || !node.path("ttlSeconds").canConvertToInt()) throw invalid();
            var viewers = node.path("viewerCount");
            if (!viewers.isNull() && (!viewers.isIntegralNumber() || !viewers.canConvertToLong())) throw invalid();
            for (String field : List.of("eventId", "runId", "channelId", "observedAt", "liveStatus", "quality", "broadcastIdentity"))
                if (!node.path(field).isString()) throw invalid();
            for (String field : List.of("sourceBroadcastId", "categoryId", "categoryName"))
                if (!node.path(field).isNull() && !node.path(field).isString()) throw invalid();
            var result = JSON.readValue(bytes, BroadcastObserved.class);
            if (!result.channelId().equals(key)) throw invalid();
            return result;
        } catch (RuntimeException ignored) { throw invalid(); }
    }
    private static boolean safeCategory(String value) {
        return value == null || (!value.isBlank() && value.length() <= 120 && value.codePoints().noneMatch(Character::isISOControl));
    }
    private static IllegalArgumentException invalid() { return new IllegalArgumentException("OBSERVATION_CONTRACT_INVALID"); }
}
