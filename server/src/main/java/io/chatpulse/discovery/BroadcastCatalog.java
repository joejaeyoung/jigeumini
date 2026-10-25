package io.chatpulse.discovery;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** 원천 발견 결과. 채팅 접근권이나 실제 수집 상태를 의미하지 않는다. */
public interface BroadcastCatalog {
    Snapshot scan(Limits limits);

    record Limits(int maxPages, int requestBudget, Duration deadline, Duration requestInterval) {
        public Limits {
            if (maxPages < 1 || maxPages > 1000 || requestBudget < 1 || requestBudget > 2000
                    || deadline == null || deadline.compareTo(Duration.ofSeconds(1)) < 0
                    || deadline.compareTo(Duration.ofSeconds(120)) > 0 || requestInterval == null
                    || requestInterval.isNegative() || requestInterval.compareTo(Duration.ofSeconds(60)) > 0)
                throw new IllegalArgumentException("INVALID_SCAN_LIMITS");
        }
    }

    record Candidate(String channelId, long liveId, Long followerCount, Long viewerCount,
                     Boolean adult, boolean live, Instant observedAt, boolean ambiguousBroadcast) {
        public Candidate {
            requireChannel(channelId);
            if (liveId < 1 || observedAt == null || !count(followerCount) || !count(viewerCount))
                throw new IllegalArgumentException("INVALID_CANDIDATE");
        }
        private static boolean count(Long value) { return value == null || value >= 0 && value <= Integer.MAX_VALUE; }
        Candidate followers(Long value) {
            return new Candidate(channelId, liveId, value, viewerCount, adult, live, observedAt, ambiguousBroadcast);
        }
        Candidate ambiguous() {
            return new Candidate(channelId, liveId, followerCount, viewerCount, adult, live, observedAt, true);
        }
    }

    enum Result { COMPLETE, PAGE_LIMIT, REQUEST_LIMIT, DEADLINE, CURSOR_REPEAT, RATE_LIMITED,
        AUTHORIZATION_FAILED, SOURCE_UNAVAILABLE, INVALID_RESPONSE }

    record Snapshot(String source, Instant startedAt, Instant finishedAt, Result result,
                    int pages, int requests, int duplicateRows, List<Candidate> candidates) {
        public Snapshot {
            if (!List.of("OFFICIAL_CATALOG", "SYNTHETIC").contains(source) || startedAt == null
                    || finishedAt == null || finishedAt.isBefore(startedAt) || result == null
                    || pages < 0 || requests < 0 || duplicateRows < 0 || candidates == null || candidates.size() > 20000)
                throw new IllegalArgumentException("INVALID_SNAPSHOT");
            candidates = List.copyOf(candidates);
            if (candidates.stream().map(Candidate::channelId).distinct().count() != candidates.size())
                throw new IllegalArgumentException("DUPLICATE_CANDIDATE");
        }
        public boolean complete() { return result == Result.COMPLETE; }
    }

    static void requireChannel(String value) {
        if (value == null || !value.matches("[0-9a-f]{32}")) throw new IllegalArgumentException("INVALID_CHANNEL");
    }
}
