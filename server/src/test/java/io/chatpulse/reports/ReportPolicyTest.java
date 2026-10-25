package io.chatpulse.reports;

import io.chatpulse.observations.BroadcastObserved;
import io.chatpulse.metadata.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReportPolicyTest {
    private static final String CHANNEL="a".repeat(32);
    private static final Instant AT=Instant.parse("2026-10-25T23:58:00Z");
    private BroadcastObserved sample(long seconds,String source,BroadcastMetadata.LiveStatus state,Long viewers) {
        var quality=state==BroadcastMetadata.LiveStatus.LIVE?(viewers==null?PublicBroadcastObservation.Quality.MISSING_VIEWER_COUNT:PublicBroadcastObservation.Quality.OBSERVED):state==BroadcastMetadata.LiveStatus.OFFLINE?PublicBroadcastObservation.Quality.OFFLINE:PublicBroadcastObservation.Quality.UPSTREAM_ERROR;
        return BroadcastObserved.observed("test-v1",CHANNEL,AT.plusSeconds(seconds),120,new PublicBroadcastObservation(state,viewers,source,null,null,quality));
    }
    @Test void unknownSilenceAndReconnectNeverEndTheObservedBroadcast() {
        var live=sample(0,"123",BroadcastMetadata.LiveStatus.LIVE,0L);var unknown=sample(60,null,BroadcastMetadata.LiveStatus.UNKNOWN,null);var again=sample(120,"123",BroadcastMetadata.LiveStatus.LIVE,20L);
        var uncertain=ReportSessions.derive(List.of(live,unknown),Map.of(),AT.plusSeconds(180)).sessions().getFirst();
        assertEquals("UNKNOWN",uncertain.state());assertNull(uncertain.endedAt());assertEquals(0L,uncertain.viewerMaximum());
        var result=ReportSessions.derive(List.of(again,unknown,live),Map.of(),AT.plusSeconds(121));assertEquals(1,result.sessions().size());
        var session=result.sessions().getFirst();assertEquals(uncertain.id(),session.id());assertEquals("LIVE",session.state());assertEquals(20L,session.viewerMaximum());assertNull(session.endedAt());
        var expired=ReportSessions.derive(List.of(live,again),Map.of(),AT.plusSeconds(500)).sessions().getFirst();assertEquals("UNKNOWN",expired.state());assertNull(expired.endedAt());
    }
    @Test void offlineAndSourceChangeAreExplicitBoundariesAcrossMidnight() {
        var live=sample(0,"123",BroadcastMetadata.LiveStatus.LIVE,10L);var offline=sample(60,null,BroadcastMetadata.LiveStatus.OFFLINE,null);var reopen=sample(120,"123",BroadcastMetadata.LiveStatus.LIVE,20L);var other=sample(180,"456",BroadcastMetadata.LiveStatus.LIVE,30L);
        var closed=ReportSessions.derive(List.of(live,offline),Map.of(),AT.plusSeconds(61)).sessions().getFirst();assertEquals("OFFLINE",closed.state());assertEquals("OFFLINE_OBSERVED",closed.endReason());
        var sessions=ReportSessions.derive(List.of(other,reopen,offline,live),Map.of(),AT.plusSeconds(181)).sessions();assertEquals(2,sessions.size());
        var old=sessions.stream().filter(s->"123".equals(s.sourceBroadcastId())).findFirst().orElseThrow();assertEquals(closed.id(),old.id());assertEquals(AT,old.observedFrom());assertEquals("SOURCE_CHANGED",old.endReason());assertEquals(AT.plusSeconds(180),old.endedAt());
    }
    @Test void lateEarlierObservationKeepsInternalAnchorAndKnownIdentityCreatesAnAlias() {
        var first=sample(60,null,BroadcastMetadata.LiveStatus.LIVE,0L);var initial=ReportSessions.derive(List.of(first),Map.of(),AT.plusSeconds(61)).sessions().getFirst();
        var anchors=Map.of(first.eventId(),initial.id());var earlier=sample(0,null,BroadcastMetadata.LiveStatus.LIVE,10L);
        var extended=ReportSessions.derive(List.of(first,earlier),anchors,AT.plusSeconds(61)).sessions().getFirst();assertEquals(initial.id(),extended.id());assertEquals(AT,extended.observedFrom());assertTrue(extended.identityUnconfirmed());
        var known=sample(30,"123",BroadcastMetadata.LiveStatus.LIVE,20L);var revised=ReportSessions.derive(List.of(first,known,earlier),anchors,AT.plusSeconds(61));
        var source=revised.sessions().stream().filter(s->"123".equals(s.sourceBroadcastId())).findFirst().orElseThrow();assertEquals(source.id(),revised.aliases().get(initial.id()));
    }
    @Test void reportIdentityExpiresAtTwoMinutesEvenWhenSourceTtlIsLonger() {
        var source=sample(0,"123",BroadcastMetadata.LiveStatus.LIVE,0L);
        var longTtl=new BroadcastObserved(source.schemaVersion(),source.eventId(),source.runId(),source.channelId(),source.observedAt(),240,source.viewerCount(),source.liveStatus(),source.quality(),source.sourceBroadcastId(),source.broadcastIdentity(),source.categoryId(),source.categoryName());
        var session=ReportSessions.derive(List.of(longTtl),Map.of(),AT.plusSeconds(120)).sessions().getFirst();
        assertEquals("UNKNOWN",session.state());assertEquals(120,session.lastTtlSeconds());assertNull(session.endedAt());
    }
    @Test void peaksExcludeGapsMergeOverlapAndOrderTiesByTime() {
        long[] counts={1,10,2,9,1,0,100,1,10,1,0,1,8,1};var minutes=new ArrayList<ReportPeaks.Minute>();
        for(int i=0;i<counts.length;i++)minutes.add(new ReportPeaks.Minute(AT.plusSeconds(i*60L),AT.plusSeconds((i+1)*60L),counts[i],i!=6));
        var peaks=ReportPeaks.topThree(minutes);assertEquals(3,peaks.size());assertEquals(List.of(10L,10L,8L),peaks.stream().map(ReportPeaks.Peak::maximum).toList());
        assertEquals(AT.plusSeconds(60),peaks.getFirst().peakAt());assertEquals(AT,peaks.getFirst().from());assertEquals(AT.plusSeconds(300),peaks.getFirst().to());
        assertEquals(AT.plusSeconds(7*60),peaks.get(1).from());
        var plateau=List.of(new ReportPeaks.Minute(AT,AT.plusSeconds(60),5,true),new ReportPeaks.Minute(AT.plusSeconds(60),AT.plusSeconds(120),5,true));
        assertEquals(1,ReportPeaks.topThree(plateau).size());assertEquals(AT,ReportPeaks.topThree(plateau).getFirst().peakAt());
    }
}
