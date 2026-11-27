package io.chatpulse.quality.service;

import io.chatpulse.quality.RankingStore;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class RankingQueryService {
    private final RankingStore store;
    private final Clock clock;

    public RankingQueryService(RankingStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public RankingStore.RankingResponse rankings(int window, int limit) {
        if (window != 60 && window != 300) {
            throw invalid();
        }
        checkLimit(limit);
        return store.rankings(window, limit);
    }

    public RankingStore.SpikeResponse spikes(String cursor, int limit) {
        checkLimit(limit);
        if (cursor.isEmpty()) {
            return store.spikes(clock.instant().plusSeconds(1), new UUID(-1, -1), limit);
        }

        String[] parts = cursor.split("\\|", -1);
        if (parts.length != 2) {
            throw invalid();
        }
        UUID id;
        try {
            id = UUID.fromString(parts[1]);
        } catch (IllegalArgumentException e) {
            throw invalid();
        }
        return store.spikes(parseInstant(parts[0]), id, limit);
    }

    public RankingStore.ActivityResponse activity(String id, String from, String to, Integer seconds) {
        if (!id.matches("[a-f0-9]{32}")) {
            throw invalid();
        }
        if (seconds != null && ((seconds != 1800 && seconds != 3600) || from != null || to != null)) {
            throw invalid();
        }

        Instant now = clock.instant().truncatedTo(ChronoUnit.MINUTES);
        Instant end = to == null ? now : parseInstant(to);
        Instant start;
        try {
            start = from == null ? end.minusSeconds(seconds == null ? 1800 : seconds) : parseInstant(from);
        } catch (java.time.DateTimeException e) {
            throw invalid();
        }
        if (!minuteAligned(start) || !minuteAligned(end) || !end.isAfter(start)
                || end.isAfter(now) || start.isBefore(now.minusSeconds(3600))) {
            throw invalid();
        }
        return store.activity(id, start, end);
    }

    public Map<String, Object> collectionStatus() {
        var ranking = store.rankings(60, 100);
        var counts = new TreeMap<String, Long>();
        ranking.entries().forEach(entry -> counts.merge(entry.status(), 1L, Long::sum));
        ranking.excludedChannels().forEach(entry -> counts.merge(entry.status(), 1L, Long::sum));
        return Map.of("asOf", ranking.asOf(), "status", ranking.status(), "channelsByStatus", counts);
    }

    private static boolean minuteAligned(Instant value) {
        return value.getNano() == 0 && value.getEpochSecond() % 60 == 0;
    }

    private static Instant parseInstant(String text) {
        try {
            return Instant.parse(text);
        } catch (java.time.format.DateTimeParseException e) {
            throw invalid();
        }
    }

    private static void checkLimit(int limit) {
        if (limit < 1 || limit > 100) {
            throw invalid();
        }
    }

    private static ResponseStatusException invalid() {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, "조회 범위와 cursor를 확인해주세요");
    }
}
