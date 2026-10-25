package io.chatpulse.metadata;

/** 이력으로 보존할 수 있는 공개 수치·방송 식별 정보. 표시명·제목·원천 시각은 포함하지 않는다. */
public record PublicBroadcastObservation(BroadcastMetadata.LiveStatus liveStatus, Long viewerCount,
        String sourceBroadcastId, String categoryId, String categoryName, Quality quality) {
    public enum Quality { OBSERVED, MISSING_VIEWER_COUNT, OFFLINE, RESTRICTED, NOT_FOUND, RATE_LIMITED, UPSTREAM_ERROR, INVALID_RESPONSE }
}
