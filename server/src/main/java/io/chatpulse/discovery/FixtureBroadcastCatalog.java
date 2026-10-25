package io.chatpulse.discovery;

import java.nio.file.*;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/** 합성 정규화 목록의 재현 어댑터. 공식 응답 원문을 fixture로 받지 않는다. */
public final class FixtureBroadcastCatalog implements BroadcastCatalog {
    static final JsonMapper JSON = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private final Snapshot snapshot;
    public FixtureBroadcastCatalog(Path input) throws java.io.IOException {
        snapshot = JSON.readValue(read(input, 4_194_304), Snapshot.class);
        if (!snapshot.source().equals("SYNTHETIC")) throw new IllegalArgumentException("SYNTHETIC_FIXTURE_REQUIRED");
    }
    @Override public Snapshot scan(Limits limits) {
        if (snapshot.pages() > limits.maxPages() || snapshot.requests() > limits.requestBudget()
                || java.time.Duration.between(snapshot.startedAt(), snapshot.finishedAt()).compareTo(limits.deadline()) > 0)
            throw new IllegalArgumentException("FIXTURE_EXCEEDS_LIMITS");
        return snapshot;
    }
    static byte[] read(Path path, int maximum) throws java.io.IOException {
        try (var input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(maximum + 1);
            if (bytes.length > maximum) throw new IllegalArgumentException("INPUT_TOO_LARGE");
            return bytes;
        }
    }
}
