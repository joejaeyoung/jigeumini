package io.chatpulse.reports;

import io.chatpulse.quality.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

/** 관측된 숫자와 품질을 확인한 완료 분을 구분한다. 실제 방송 시작/종료 시각은 만들지 않는다. */
public final class ReportQueries {
    public record Header(String reportId,String channelId,String identity,String state,String endReason,boolean identityUnconfirmed,
            String observedFrom,String observedTo,String endedAt,Long viewerMaximum,String reportVersion,String updatedAt) { }
    public record Page(String channelId,String asOf,String status,int retentionDays,boolean truncated,long unassignedCount,List<Header> reports) { }
    public record Point(String from,String to,Long count,long referenceCount,double collectionCoverage,double identityCoverage,List<String> reasons) { }
    public record Peak(String from,String to,String peakAt,long maximum) { }
    public record Report(Header header,String asOf,String status,String scope,String metricsFrom,String metricsTo,boolean partialObservation,
            long referenceCount,Long qualifiedCount,Long maximumPerMinute,int completedMinutes,int healthyMinutes,long lateCount,long unassignedChannelCount,
            List<Peak> peaks,List<Point> points,boolean timelineTruncated) { }
    private final ReportStore store;
    private final QualityHistory quality;
    private final Clock clock;
    private static final JsonMapper JSON=JsonMapper.builder().build();
    public ReportQueries(ReportStore store,QualityHistory quality,Clock clock) { this.store=store;this.quality=quality;this.clock=clock; }
    private Header header(ReportStore.Stored stored,String version,Instant now) {
        var s=stored.session();String state=s.state();
        if(state.equals("LIVE")&&!now.isBefore(s.observedTo().plusSeconds(s.lastTtlSeconds())))state="UNKNOWN";
        return new Header(s.id().toString(),s.channelId(),s.sourceBroadcastId()==null?"INTERNAL_OBSERVATION_SESSION":"SOURCE_BROADCAST",state,s.endReason(),s.identityUnconfirmed(),
                s.observedFrom().toString(),s.observedTo().toString(),s.endedAt()==null?null:s.endedAt().toString(),s.viewerMaximum(),version,stored.updatedAt().toString());
    }
    public Page list(String channel) {
        var page=store.list(channel);Instant now=clock.instant();
        return new Page(channel,now.toString(),page.fresh()?"READY":"LAGGING",7,page.truncated(),page.unassigned(),page.reports().stream().map(s->header(s,"data-"+s.revision(),now)).toList());
    }
    public Report detail(UUID id) {
        return store.readSnapshot(()->detailInSnapshot(id));
    }
    private Report detailInSnapshot(UUID id) {
        var detail=store.detail(id);if(detail==null)return null;var stored=detail.stored();var session=stored.session();
        Instant now=clock.instant(),from=floor(session.observedFrom()),completed=floor(now.minusSeconds(8));
        Instant last=session.endedAt()==null?session.observedTo().plusSeconds(session.lastTtlSeconds()):session.endedAt();
        Instant to=floor(last.isBefore(completed)?last:completed);if(to.isBefore(from))to=from;
        var identity=QualityPolicy.minuteCoverage(from,to,detail.identityIntervals());
        var counts=new HashMap<Instant,Long>();detail.minutes().forEach(m->counts.put(m.at(),m.count()));
        var points=new ArrayList<Point>();var peakMinutes=new ArrayList<ReportPeaks.Minute>();long qualified=0;Long maximum=null;int healthy=0;
        for(var minute:quality.minutes(session.channelId(),from,to)) {
            double coverage=identity.getOrDefault(minute.from(),0d);long reference=counts.getOrDefault(minute.from(),0L);
            var reasons=new ArrayList<>(minute.reasons());if(coverage<1)reasons.add("BROADCAST_IDENTITY_GAP");if(!detail.fresh())reasons.add("REPORT_LAGGING");
            boolean complete=reasons.isEmpty();if(complete) { healthy++;qualified+=reference;maximum=maximum==null?reference:Math.max(maximum,reference); }
            points.add(new Point(minute.from().toString(),minute.to().toString(),complete?reference:null,reference,minute.coverage(),coverage,List.copyOf(reasons)));
            peakMinutes.add(new ReportPeaks.Minute(minute.from(),minute.to(),reference,complete));
        }
        var peaks=ReportPeaks.topThree(peakMinutes).stream().map(p->new Peak(p.from().toString(),p.to().toString(),p.peakAt().toString(),p.maximum())).toList();
        long reference=detail.minutes().stream().mapToLong(ReportStore.Minute::count).sum();
        var header=header(stored,"pending",now);
        // 조회 시각은 버전에 넣지 않는다. 입력/품질/만료로 실제 표시 내용이 달라질 때만 바뀐다.
        String version=digest(List.of(header,detail.fresh(),points,peaks,reference,qualified,detail.lateCount(),detail.unassigned()));
        return new Report(header(stored,version,now),now.toString(),detail.fresh()?"READY":"LAGGING","OBSERVED_ONLY",from.toString(),to.toString(),true,
                reference,healthy==0?null:qualified,maximum,points.size(),healthy,detail.lateCount(),detail.unassigned(),peaks,
                List.copyOf(points.subList(Math.max(0,points.size()-360),points.size())),points.size()>360);
    }
    private static Instant floor(Instant value) { return Instant.ofEpochSecond(Math.floorDiv(value.getEpochSecond(),60)*60); }
    private static String digest(Object value) {
        try { return "report-v1-"+HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(JSON.writeValueAsString(value).getBytes(StandardCharsets.UTF_8))); }
        catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("REPORT_HASH_UNAVAILABLE"); }
    }
}
