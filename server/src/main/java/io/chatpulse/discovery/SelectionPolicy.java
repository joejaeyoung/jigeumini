package io.chatpulse.discovery;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import static io.chatpulse.discovery.BroadcastCatalog.*;

/** 결정적 선정. 원천 호출·수집기 시작·권한 검증은 수행하지 않는다. */
public record SelectionPolicy(Long minFollowers, int maxChannels, int metadataTtlSeconds,
                              SourceMode sourceMode, Set<String> allowedChannels, Set<String> excludedChannels) {
    public enum SourceMode { SYNTHETIC, UNOFFICIAL_POC, OFFICIAL_AUTHORIZED }
    public enum Reason { AMBIGUOUS_BROADCAST, NOT_LIVE, ADULT_OR_UNKNOWN, EXCLUDED,
        ACCESS_NOT_DECLARED, STALE_METADATA, FOLLOWERS_UNKNOWN, FOLLOWERS_BELOW_MINIMUM, CAPACITY, SELECTED }
    public SelectionPolicy {
        if (minFollowers != null && (minFollowers < 0 || minFollowers > Integer.MAX_VALUE)
                || maxChannels < 1 || maxChannels > 1000 || metadataTtlSeconds < 1 || metadataTtlSeconds > 3600
                || sourceMode == null || allowedChannels == null || excludedChannels == null
                || allowedChannels.size() > 20000 || excludedChannels.size() > 20000)
            throw new IllegalArgumentException("INVALID_SELECTION_POLICY");
        allowedChannels = Set.copyOf(allowedChannels); excludedChannels = Set.copyOf(excludedChannels);
        allowedChannels.forEach(BroadcastCatalog::requireChannel); excludedChannels.forEach(BroadcastCatalog::requireChannel);
    }

    public record Decision(String channelId, long liveId, Reason reason) { }
    public record Counts(int discovered, int eligible, int selected, Integer assigned, Integer connected, Integer healthyWindow) { }
    public record Report(String scope, String accessEvidence, boolean activationAllowed,
                         Instant asOf, boolean scanComplete, long scanAgeSeconds, Snapshot discovery,
                         Long minFollowers, int maxChannels, int metadataTtlSeconds, SourceMode sourceMode,
                         Counts counts, Map<Reason, Long> exclusionCounts, List<Candidate> selected,
                         List<Decision> decisions, Double selectedOfEligible, Double selectedOfDiscovered,
                         Double healthyOfSelected) { }

    public Report evaluate(Snapshot snapshot, Instant asOf) {
        if (asOf.isBefore(snapshot.finishedAt())) throw new IllegalArgumentException("INVALID_EVALUATION_TIME");
        var eligible = new ArrayList<Candidate>(); var decisions = new TreeMap<String, Decision>();
        for (var candidate : snapshot.candidates()) {
            Reason reason = reason(candidate, asOf);
            if (reason == null) eligible.add(candidate);
            else decisions.put(candidate.channelId(), new Decision(candidate.channelId(), candidate.liveId(), reason));
        }
        eligible.sort(Comparator.comparing(Candidate::viewerCount, Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(Candidate::followerCount, Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(Candidate::channelId));
        var selected = new ArrayList<Candidate>();
        for (var candidate : eligible) {
            boolean admitted = selected.size() < maxChannels;
            if (admitted) selected.add(candidate);
            decisions.put(candidate.channelId(), new Decision(candidate.channelId(), candidate.liveId(), admitted ? Reason.SELECTED : Reason.CAPACITY));
        }
        var excluded = new EnumMap<Reason, Long>(Reason.class);
        decisions.values().stream().filter(d -> d.reason() != Reason.SELECTED)
                .forEach(d -> excluded.merge(d.reason(), 1L, Long::sum));
        int discovered = snapshot.candidates().size();
        return new Report("DISCOVERED_SCAN_PREVIEW", "OPERATOR_DECLARED_NOT_RUNTIME_VERIFIED", false, asOf,
                snapshot.complete(), Duration.between(snapshot.finishedAt(), asOf).getSeconds(), snapshot,
                minFollowers, maxChannels, metadataTtlSeconds, sourceMode,
                new Counts(discovered, eligible.size(), selected.size(), null, null, null),
                Collections.unmodifiableMap(excluded), List.copyOf(selected), List.copyOf(decisions.values()),
                ratio(selected.size(), eligible.size()), ratio(selected.size(), discovered), null);
    }

    private Reason reason(Candidate candidate, Instant asOf) {
        if (candidate.ambiguousBroadcast()) return Reason.AMBIGUOUS_BROADCAST;
        if (!candidate.live()) return Reason.NOT_LIVE;
        if (!Boolean.FALSE.equals(candidate.adult())) return Reason.ADULT_OR_UNKNOWN;
        if (excludedChannels.contains(candidate.channelId())) return Reason.EXCLUDED;
        if (!allowedChannels.contains(candidate.channelId())) return Reason.ACCESS_NOT_DECLARED;
        if (candidate.observedAt().isAfter(asOf)
                || !candidate.observedAt().plusSeconds(metadataTtlSeconds).isAfter(asOf)) return Reason.STALE_METADATA;
        if (minFollowers != null && candidate.followerCount() == null) return Reason.FOLLOWERS_UNKNOWN;
        if (minFollowers != null && candidate.followerCount() < minFollowers) return Reason.FOLLOWERS_BELOW_MINIMUM;
        return null;
    }
    private static Double ratio(int numerator, int denominator) { return denominator == 0 ? null : (double) numerator / denominator; }
}
