package io.chatpulse.quality;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

public final class TrendPolicy {
    public static final String VERSION = "chat-spike-v1";
    public record State(Instant evaluatedAt, int hits, Instant lastAlertAt) { }
    public record Result(State state, boolean alert, BigDecimal baseline, BigDecimal ratio) { }
    public static Result evaluate(State previous, Instant at, long current, long previousFiveMinutes, boolean healthy) {
        BigDecimal baseline = BigDecimal.valueOf(previousFiveMinutes).divide(BigDecimal.valueOf(5));
        BigDecimal ratio = BigDecimal.valueOf(current).divide(baseline.max(BigDecimal.TEN), 8, RoundingMode.HALF_UP);
        if (previous != null && !at.isAfter(previous.evaluatedAt)) return new Result(previous, false, baseline, ratio);
        boolean cooling = previous != null && previous.lastAlertAt != null && at.isBefore(previous.lastAlertAt.plusSeconds(300));
        boolean hit = healthy && !cooling && current >= 100 && BigDecimal.valueOf(current).subtract(baseline).compareTo(BigDecimal.valueOf(50)) >= 0
                && BigDecimal.valueOf(current).compareTo(baseline.max(BigDecimal.TEN).multiply(BigDecimal.valueOf(2))) >= 0;
        int hits = !hit ? 0 : previous != null && previous.evaluatedAt.equals(at.minusSeconds(10)) ? previous.hits + 1 : 1;
        boolean alert = hits >= 2;
        return new Result(new State(at, alert ? 0 : hits, alert ? at : previous == null ? null : previous.lastAlertAt), alert, baseline, ratio);
    }
}
