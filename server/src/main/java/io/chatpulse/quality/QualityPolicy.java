package io.chatpulse.quality;

import java.time.*;
import java.util.*;

/** 이벤트 건수와 독립적인 관측 품질. 기간은 모두 [from,to)다. */
public final class QualityPolicy {
    public record Interval(Instant from, Instant to) { }
    public record Result(String status, double coverage, List<String> reasons) { public boolean healthy() { return status.equals("HEALTHY"); } }
    public static double coverage(Instant from, Instant to, List<Interval> sessions) {
        List<Interval> clipped = sessions.stream().map(s -> new Interval(s.from.isAfter(from) ? s.from : from, s.to.isBefore(to) ? s.to : to))
                .filter(s -> s.to.isAfter(s.from)).sorted(Comparator.comparing(Interval::from)).toList();
        long covered = 0; Instant start = null, end = null;
        for (var interval : clipped) {
            if (start == null) { start = interval.from; end = interval.to; }
            else if (!interval.from.isAfter(end)) { if (interval.to.isAfter(end)) end = interval.to; }
            else { covered += Duration.between(start, end).toNanos(); start = interval.from; end = interval.to; }
        }
        if (start != null) covered += Duration.between(start, end).toNanos();
        return (double) covered / Duration.between(from, to).toNanos();
    }
    public static Result evaluate(boolean enabled, String state, Instant observedSince, Instant from, Instant to,
            List<Interval> sessions, boolean knownLoss, boolean collectorFresh, boolean sourceFresh,
            boolean bufferFresh, boolean bufferLag, boolean analysisKnown, boolean analysisFresh, boolean blocked) {
        double coverage = coverage(from, to, sessions);
        var reasons = new ArrayList<String>();
        if (!enabled) reasons.add("DISABLED");
        if (state.equals("OFFLINE")) reasons.add("OFFLINE");
        if (knownLoss) reasons.add("KNOWN_LOSS");
        boolean warming = observedSince == null || observedSince.isAfter(from);
        if (!warming && coverage < .95) reasons.add("LOW_COVERAGE");
        if (warming) reasons.add("WARMING_UP");
        if (!collectorFresh) reasons.add("COLLECTOR_UNKNOWN");
        if (!sourceFresh) reasons.add("SOURCE_UNKNOWN");
        if (!bufferFresh) reasons.add("BUFFER_UNKNOWN");
        if (bufferLag) reasons.add("BUFFER_LAG");
        if (!analysisKnown) reasons.add("ANALYSIS_UNKNOWN");
        else if (!analysisFresh) reasons.add("ANALYSIS_LAG");
        if (blocked) reasons.add("PROJECTION_BLOCKED");
        if (state.equals("UNAVAILABLE")) reasons.add("SOURCE_UNAVAILABLE");
        String status = !enabled ? "DISABLED" : state.equals("OFFLINE") ? "OFFLINE"
                : knownLoss || (!warming && coverage < .95) ? "INCOMPLETE" : warming ? "WARMING_UP"
                : bufferLag || (analysisKnown && !analysisFresh) ? "LAGGING" : reasons.isEmpty() ? "HEALTHY" : "UNKNOWN";
        return new Result(status, coverage, List.copyOf(reasons));
    }
}
