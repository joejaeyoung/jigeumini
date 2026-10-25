package io.chatpulse.quality;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class QualityPolicyTest {
    private final Instant t=Instant.parse("2026-10-25T12:01:00Z"),from=t.minusSeconds(60);
    @Test void minuteCoverageMergesClipsAndKeepsBoundaryGaps() {
        var spans=List.of(new QualityPolicy.Interval(from.minusSeconds(5),from.plusSeconds(40)),new QualityPolicy.Interval(from.plusSeconds(20),t.plusSeconds(57)),new QualityPolicy.Interval(t.plusSeconds(120),t.plusSeconds(200)));
        var actual=QualityPolicy.minuteCoverage(from,t.plusSeconds(180),spans);
        assertThat(actual.get(from)).isEqualTo(1d);assertThat(actual.get(t)).isEqualTo(.95);assertThat(actual.getOrDefault(t.plusSeconds(60),0d)).isZero();assertThat(actual.get(t.plusSeconds(120))).isEqualTo(1d);
    }
    private QualityPolicy.Result result(List<QualityPolicy.Interval> sessions,boolean loss,Instant since) {
        return QualityPolicy.evaluate(true,"CONNECTED",since,from,t,sessions,loss,true,true,true,false,true,true,false);
    }
    @Test void coverageBoundaryAndOverlappingSessionsAreExact() {
        var exact=List.of(new QualityPolicy.Interval(from,from.plusSeconds(57)));
        assertThat(result(exact,false,from).healthy()).isTrue();
        assertThat(result(List.of(new QualityPolicy.Interval(from,from.plusMillis(56999))),false,from).status()).isEqualTo("INCOMPLETE");
        assertThat(QualityPolicy.coverage(from,t,List.of(new QualityPolicy.Interval(from.minusSeconds(10),from.plusSeconds(40)),new QualityPolicy.Interval(from.plusSeconds(20),t.plusSeconds(10))))).isEqualTo(1);
        assertThat(result(exact,true,from).status()).isEqualTo("INCOMPLETE");
    }
    @Test void missingObservationAndWarmupAreNotHealthyZero() {
        assertThat(result(List.of(),false,from.plusSeconds(1)).status()).isEqualTo("WARMING_UP");
        var session=List.of(new QualityPolicy.Interval(from,t));
        var unknown=QualityPolicy.evaluate(true,"CONNECTED",from,from,t,session,false,false,true,true,false,false,false,false);
        assertThat(unknown.status()).isEqualTo("UNKNOWN");
        assertThat(unknown.reasons()).contains("COLLECTOR_UNKNOWN","ANALYSIS_UNKNOWN");
        assertThat(QualityPolicy.evaluate(true,"CONNECTED",from,from,t,session,false,true,true,true,true,true,true,false).status()).isEqualTo("LAGGING");
        assertThat(QualityPolicy.evaluate(false,"STOPPED",from,from,t,session,false,true,true,true,false,true,true,false).status()).isEqualTo("DISABLED");
    }
}
