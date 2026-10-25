package io.chatpulse.metadata;

import java.time.Instant;

/** 공개 방송 정보만 담는다. 원천 응답·채팅·인증 정보는 보관하지 않는다. */
public record BroadcastMetadata(String channelId, String channelName, String title, String categoryId,
        String categoryName, String thumbnailUrl, LiveStatus liveStatus, Freshness freshness,
        Reason reason, String checkedAt, String expiresAt) {
    public enum LiveStatus { LIVE, OFFLINE, UNKNOWN, RESTRICTED, NOT_FOUND }
    public enum Freshness { FRESH, STALE, UNAVAILABLE }
    public enum Reason { NONE, PENDING, RESTRICTED, NOT_FOUND, RATE_LIMITED, UPSTREAM_ERROR, INVALID_RESPONSE, DISABLED }

    static BroadcastMetadata empty(String id, LiveStatus status, Reason reason) {
        return new BroadcastMetadata(id, null, null, null, null, null, status,
                Freshness.UNAVAILABLE, reason, null, null);
    }
    BroadcastMetadata checked(Instant now, Instant expires) {
        return new BroadcastMetadata(channelId, channelName, title, categoryId, categoryName, thumbnailUrl,
                liveStatus, liveStatus == LiveStatus.UNKNOWN ? Freshness.UNAVAILABLE : Freshness.FRESH,
                reason, now.toString(), expires.toString());
    }
    BroadcastMetadata stale(Reason failure) {
        // 이전 방송 화면을 현재 방송으로 오인하지 않도록 이미지와 LIVE 표시를 내린다.
        return new BroadcastMetadata(channelId, channelName, title, categoryId, categoryName, null,
                LiveStatus.UNKNOWN, Freshness.STALE, failure, checkedAt, expiresAt);
    }
}
