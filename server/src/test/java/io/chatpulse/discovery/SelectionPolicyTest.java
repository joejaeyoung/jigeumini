package io.chatpulse.discovery;

import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.discovery.BroadcastCatalog.*;
import static io.chatpulse.discovery.SelectionPolicy.*;

class SelectionPolicyTest {
    static final Instant NOW = Instant.parse("2026-10-26T00:00:00Z");
    static String id(int n) { return String.format("%032x", n); }
    static Candidate candidate(int n, Long followers, Long viewers) {
        return new Candidate(id(n), n + 1, followers, viewers, false, true, NOW.minusSeconds(1), false);
    }
    static Snapshot snapshot(Candidate... candidates) {
        return new Snapshot("SYNTHETIC", NOW.minusSeconds(2), NOW, Result.COMPLETE, 1, 2, 0, List.of(candidates));
    }
    static SelectionPolicy policy(Long floor, int maximum, Integer... allowed) {
        return new SelectionPolicy(floor, maximum, 120, SourceMode.SYNTHETIC,
                new HashSet<>(Arrays.stream(allowed).map(SelectionPolicyTest::id).toList()), Set.of());
    }
    @Test void floorBoundaryCapacityStableOrderAndUnknownValues() {
        var input = snapshot(candidate(3,1000L,20L), candidate(2,1000L,20L), candidate(1,999L,90L), candidate(4,null,null));
        var result = policy(1000L,1,1,2,3,4).evaluate(input,NOW);
        assertThat(result.selected()).extracting(Candidate::channelId).containsExactly(id(2));
        assertThat(result.counts()).isEqualTo(new Counts(4,2,1,null,null,null));
        assertThat(result.exclusionCounts()).containsEntry(Reason.FOLLOWERS_BELOW_MINIMUM,1L)
                .containsEntry(Reason.FOLLOWERS_UNKNOWN,1L).containsEntry(Reason.CAPACITY,1L);
        assertThat(result.healthyOfSelected()).isNull(); assertThat(result.activationAllowed()).isFalse();
        assertThat(result.selectedOfEligible()).isEqualTo(0.5);
        assertThat(policy(null,4,1,2,3,4).evaluate(input,NOW).selected()).hasSize(4);
    }
    @Test void accessAdultUnknownExclusionAndStalenessHaveDistinctReasons() {
        var input = snapshot(candidate(1,5L,1L), new Candidate(id(2),2,5L,1L,null,true,NOW,false),
                new Candidate(id(3),3,5L,1L,false,true,NOW.minusSeconds(120),false),
                candidate(4,5L,1L), new Candidate(id(5),5,5L,1L,false,false,NOW,false),
                new Candidate(id(6),6,5L,1L,false,true,NOW,true));
        var policy = new SelectionPolicy(null,20,120,SourceMode.SYNTHETIC,Set.of(id(2),id(3),id(4),id(5),id(6)),Set.of(id(4)));
        assertThat(policy.evaluate(input,NOW).decisions()).extracting(Decision::reason).containsExactly(
                Reason.ACCESS_NOT_DECLARED,Reason.ADULT_OR_UNKNOWN,Reason.STALE_METADATA,Reason.EXCLUDED,Reason.NOT_LIVE,Reason.AMBIGUOUS_BROADCAST);
    }
    @Test void zeroFollowersAreKnownWhileMissingIsUnknownAndEmptyDenominatorIsNull() {
        var result = policy(0L,20,1,2).evaluate(snapshot(candidate(1,0L,0L),candidate(2,null,null)),NOW);
        assertThat(result.selected()).extracting(Candidate::followerCount).containsExactly(0L);
        var empty = policy(null,20).evaluate(snapshot(),NOW);
        assertThat(empty.selectedOfEligible()).isNull(); assertThat(empty.selectedOfDiscovered()).isNull();
    }
    @Test void partialAndStaleScanNeverBecomeCollectionSuccess() {
        var input = new Snapshot("SYNTHETIC",NOW.minusSeconds(2),NOW,Result.RATE_LIMITED,1,2,0,List.of(candidate(1,10L,0L)));
        var result = policy(null,20,1).evaluate(input,NOW.plusSeconds(120));
        assertThat(result.scanComplete()).isFalse(); assertThat(result.scanAgeSeconds()).isEqualTo(120);
        assertThat(result.selected()).isEmpty(); assertThat(result.counts().healthyWindow()).isNull();
    }
    @Test void invalidCountsDuplicatesAndUnboundedPoliciesAreRejected() {
        assertThatThrownBy(()->candidate(1,-1L,0L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->snapshot(candidate(1,0L,0L),candidate(1,0L,0L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->policy(null,1001,1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->policy(null,20,1).evaluate(snapshot(),NOW.minusSeconds(1))).isInstanceOf(IllegalArgumentException.class);
    }
}
