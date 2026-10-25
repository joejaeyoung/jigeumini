package io.chatpulse.reports;

import java.time.Instant;
import java.util.*;

final class ReportPeaks {
    record Minute(Instant from,Instant to,long count,boolean healthy) { }
    record Peak(Instant from,Instant to,Instant peakAt,long maximum) { }
    static List<Peak> topThree(List<Minute> input) {
        var minutes=input.stream().sorted(Comparator.comparing(Minute::from)).toList();var candidates=new ArrayList<Peak>();
        for(int i=0;i<minutes.size();i++) {
            var minute=minutes.get(i);if(!minute.healthy()||minute.count()==0)continue;
            boolean left=i>0&&minutes.get(i-1).healthy()&&minutes.get(i-1).to().equals(minute.from());
            boolean right=i+1<minutes.size()&&minutes.get(i+1).healthy()&&minute.to().equals(minutes.get(i+1).from());
            // 같은 값의 plateau는 첫 분을 선택한다. 품질 공백을 가로질러 피크를 확장하지 않는다.
            if(left&&minutes.get(i-1).count()>=minute.count() || right&&minutes.get(i+1).count()>minute.count())continue;
            candidates.add(new Peak(left?minutes.get(i-1).from():minute.from(),right?minutes.get(i+1).to():minute.to(),minute.from(),minute.count()));
        }
        var merged=new ArrayList<Peak>();
        for(var candidate:candidates) {
            if(merged.isEmpty() || !candidate.from().isBefore(merged.getLast().to()))merged.add(candidate);
            else {
                var last=merged.removeLast();var highest=last.maximum()>=candidate.maximum()?last:candidate;
                merged.add(new Peak(last.from(),last.to().isAfter(candidate.to())?last.to():candidate.to(),highest.peakAt(),highest.maximum()));
            }
        }
        return merged.stream().sorted(Comparator.comparingLong(Peak::maximum).reversed().thenComparing(Peak::peakAt)).limit(3).toList();
    }
}
