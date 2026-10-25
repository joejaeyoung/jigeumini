package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy;
import java.time.Instant;
import java.util.*;

/** 선정 미리보기와 수집 시작 사이의 명시적 활성화 계약. 실제 접근 허가를 자동 추론하지 않는다. */
public record FleetPlan(String version, SelectionPolicy.SourceMode sourceMode, int discovered, int eligible,
        List<String> channels, int validSeconds) {
    public FleetPlan {
        channels=List.copyOf(channels);
        if(version==null||!version.matches("[a-f0-9]{64}")||sourceMode==null||sourceMode==SelectionPolicy.SourceMode.OFFICIAL_AUTHORIZED
                ||discovered<0||eligible<channels.size()||discovered<eligible||validSeconds<10||validSeconds>3600
                ||channels.size()>(sourceMode==SelectionPolicy.SourceMode.SYNTHETIC?20:3)||channels.stream().distinct().count()!=channels.size()
                ||channels.stream().anyMatch(c->!c.matches("[a-f0-9]{32}")))throw new IllegalArgumentException("FLEET_PLAN_INVALID");
    }
    public static FleetPlan from(SelectionPolicy.Report report,String hash,int validSeconds,Instant now) {
        if(report==null||!report.scanComplete())throw new IllegalArgumentException("FLEET_SCAN_INCOMPLETE");
        if(report.sourceMode()!=SelectionPolicy.SourceMode.SYNTHETIC && (report.asOf().isAfter(now)
                ||!report.asOf().plusSeconds(report.metadataTtlSeconds()).isAfter(now)))throw new IllegalArgumentException("FLEET_SELECTION_EXPIRED");
        return new FleetPlan(hash,report.sourceMode(),report.counts().discovered(),report.counts().eligible(),
                report.selected().stream().map(c->c.channelId()).toList(),validSeconds);
    }
}
