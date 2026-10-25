package io.chatpulse.notifications;

import io.chatpulse.quality.TrendPolicy;
import java.math.*;
import java.time.Instant;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

/** 원문 없이 확정 판정의 불변 근거만 전달한다. 사후 품질은 조회 시 별도로 결합한다. */
public record SpikeDetected(int schemaVersion, UUID spikeId, String runId, String channelId, Instant evaluatedAt,
        String algorithmVersion, long count, BigDecimal baseline, BigDecimal ratio, String quality) {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public SpikeDetected {
        if (schemaVersion != 1 || spikeId == null || runId == null || !runId.matches("[a-zA-Z0-9_-]{1,80}")
                || channelId == null || !channelId.matches("[a-f0-9]{32}") || evaluatedAt == null
                || evaluatedAt.getNano() != 0 || evaluatedAt.getEpochSecond() % 10 != 0
                || !TrendPolicy.VERSION.equals(algorithmVersion) || !"HEALTHY_AT_DETECTION".equals(quality)
                || count < 100 || count > 9007199254740991L || baseline == null || ratio == null
                || baseline.signum() < 0 || baseline.precision() > 20 || Math.abs(baseline.scale()) > 8
                || ratio.precision() > 24 || Math.abs(ratio.scale()) > 8
                || BigDecimal.valueOf(count).subtract(baseline).compareTo(BigDecimal.valueOf(50)) < 0
                || ratio.compareTo(BigDecimal.valueOf(2)) < 0
                || BigDecimal.valueOf(count).divide(baseline.max(BigDecimal.TEN),8,RoundingMode.HALF_UP).compareTo(ratio) != 0)
            throw new IllegalArgumentException("SPIKE_CONTRACT_INVALID");
        baseline = baseline.stripTrailingZeros(); ratio = ratio.stripTrailingZeros();
    }
    public byte[] encode() { return JSON.writeValueAsBytes(this); }
    public static SpikeDetected decode(String key, byte[] bytes) {
        try {
            if (bytes == null || bytes.length > 4096) throw new IllegalArgumentException();
            var event = JSON.readValue(bytes, SpikeDetected.class);
            if (!event.channelId.equals(key)) throw new IllegalArgumentException();
            return event;
        } catch (RuntimeException ignored) { throw new IllegalArgumentException("SPIKE_CONTRACT_INVALID"); }
    }
}
