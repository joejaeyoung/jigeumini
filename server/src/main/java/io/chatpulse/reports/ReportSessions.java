package io.chatpulse.reports;

import io.chatpulse.metadata.BroadcastMetadata.LiveStatus;
import io.chatpulse.observations.BroadcastObserved;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/** 방송의 실제 시작/종료를 추정하지 않고 관측 근거로 세션을 구분한다. */
final class ReportSessions {
    record Session(UUID id,String channelId,String sourceBroadcastId,UUID anchorObservationId,
                   Instant observedFrom,Instant observedTo,Instant endedAt,String state,String endReason,
                   Long viewerMaximum,boolean identityUnconfirmed) { }
    record Result(List<Session> sessions,Map<UUID,UUID> observationReports,Map<UUID,UUID> aliases) { }
    private static final class Draft {
        final String channel,source;
        final BroadcastObserved first;
        BroadcastObserved last;
        final List<BroadcastObserved> observations=new ArrayList<>();
        Instant ended;
        String endReason="UNCONFIRMED";
        Long viewerMaximum;
        boolean identityUnconfirmed;
        Draft(BroadcastObserved first) { this.first=first;channel=first.channelId();source=first.sourceBroadcastId(); }
        void observe(BroadcastObserved sample) {
            last=sample;observations.add(sample);
            if(sample.liveStatus()==LiveStatus.LIVE) {
                ended=null;endReason="UNCONFIRMED";
                if(sample.sourceBroadcastId()==null)identityUnconfirmed=true;
                if(sample.viewerCount()!=null)viewerMaximum=viewerMaximum==null?sample.viewerCount():Math.max(viewerMaximum,sample.viewerCount());
            }
        }
        void end(Instant at,String reason) { ended=at;endReason=reason; }
    }
    static Result derive(List<BroadcastObserved> input,Map<UUID,UUID> existingAnchors,Instant now) {
        var sorted=input.stream().sorted(Comparator.comparing(BroadcastObserved::observedAt).thenComparing(s->s.eventId().toString())).toList();
        if(sorted.stream().map(BroadcastObserved::channelId).distinct().count()>1)throw new IllegalArgumentException("REPORT_SINGLE_CHANNEL");
        var drafts=new ArrayList<Draft>();var known=new HashMap<String,Draft>();Draft current=null;
        for(var sample:sorted) {
            if(sample.liveStatus()==LiveStatus.LIVE) {
                String source=sample.sourceBroadcastId();
                if(current==null || source!=null&&!source.equals(current.source)) {
                    if(current!=null)current.end(sample.observedAt(),"SOURCE_CHANGED");
                    if(source!=null && known.containsKey(source))current=known.get(source);
                    else { current=new Draft(sample);drafts.add(current);if(source!=null)known.put(source,current); }
                }
                current.observe(sample);
            } else if(current!=null) {
                current.observe(sample);
                if(sample.liveStatus()==LiveStatus.OFFLINE) { current.end(sample.observedAt(),"OFFLINE_OBSERVED");current=null; }
            }
        }
        var sessions=new ArrayList<Session>();var assignments=new HashMap<UUID,UUID>();var aliases=new HashMap<UUID,UUID>();
        for(var draft:drafts) {
            var prior=draft.observations.stream().filter(s->existingAnchors.containsKey(s.eventId())).toList();
            UUID anchor=draft.source==null?(prior.isEmpty()?draft.first.eventId():prior.getFirst().eventId()):null;
            UUID id=draft.source==null?(prior.isEmpty()?id("internal/"+draft.channel+"/"+anchor):existingAnchors.get(anchor)):id("source/"+draft.channel+"/"+draft.source);
            for(var observation:draft.observations) {
                assignments.put(observation.eventId(),id);var old=existingAnchors.get(observation.eventId());
                if(old!=null&&!old.equals(id))aliases.put(old,id);
            }
            String state=draft.ended!=null?"OFFLINE":draft.last.liveStatus()==LiveStatus.LIVE&&now.isBefore(draft.last.observedAt().plusSeconds(draft.last.ttlSeconds()))?"LIVE":"UNKNOWN";
            sessions.add(new Session(id,draft.channel,draft.source,anchor,draft.first.observedAt(),draft.last.observedAt(),draft.ended,state,draft.endReason,draft.viewerMaximum,draft.identityUnconfirmed));
        }
        sessions.sort(Comparator.comparing(Session::observedFrom).reversed().thenComparing(s->s.id().toString()));
        return new Result(List.copyOf(sessions),Map.copyOf(assignments),Map.copyOf(aliases));
    }
    private static UUID id(String key) { return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)); }
}
