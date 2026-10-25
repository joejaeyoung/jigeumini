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
    /** 분별 조회를 위해 구간 합집합을 한 번만 만든다. 빈 분은 0이며 경계는 UTC 정각이다. */
    public static Map<Instant,Double> minuteCoverage(Instant from,Instant to,List<Interval> intervals) {
        var merged=new ArrayList<Interval>();
        for(var interval:intervals.stream().sorted(Comparator.comparing(Interval::from)).toList()) {
            Instant start=interval.from().isAfter(from)?interval.from():from,end=interval.to().isBefore(to)?interval.to():to;
            if(!end.isAfter(start))continue;
            if(merged.isEmpty()||start.isAfter(merged.getLast().to()))merged.add(new Interval(start,end));
            else {var last=merged.removeLast();merged.add(new Interval(last.from(),last.to().isAfter(end)?last.to():end));}
        }
        var nanos=new HashMap<Instant,Long>();
        for(var interval:merged)for(Instant start=interval.from();start.isBefore(interval.to());) {
            Instant minute=Instant.ofEpochSecond(Math.floorDiv(start.getEpochSecond(),60)*60),end=minute.plusSeconds(60);
            if(end.isAfter(interval.to()))end=interval.to();
            nanos.merge(minute,Duration.between(start,end).toNanos(),Long::sum);start=end;
        }
        var result=new HashMap<Instant,Double>();nanos.forEach((minute,value)->result.put(minute,value/60_000_000_000d));return Map.copyOf(result);
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
