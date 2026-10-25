package io.chatpulse.quality;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TrendPolicyTest {
    private final Instant t=Instant.parse("2026-10-25T12:00:00Z");
    @Test void decimalBaselineAndAllThreeThresholdsAreApplied() {
        var hit=TrendPolicy.evaluate(null,t,100,250,true);
        assertThat(hit.baseline()).isEqualByComparingTo("50");assertThat(hit.ratio()).isEqualByComparingTo("2");assertThat(hit.state().hits()).isEqualTo(1);
        var miss=TrendPolicy.evaluate(null,t,100,251,true);
        assertThat(miss.baseline()).isEqualByComparingTo("50.2");assertThat(miss.state().hits()).isZero();
        assertThat(TrendPolicy.evaluate(null,t,99,0,true).state().hits()).isZero();
        assertThat(TrendPolicy.evaluate(null,t,100,300,true).state().hits()).isZero();
        assertThat(TrendPolicy.evaluate(null,t,0,0,true).alert()).isFalse();
        assertThat(TrendPolicy.evaluate(null,t,100,0,false).state().hits()).isZero();
    }
    @Test void exactConsecutiveTicksAndCooldownAreDurableStateRules() {
        var first=TrendPolicy.evaluate(null,t,100,250,true);
        assertThat(TrendPolicy.evaluate(first.state(),t,100,250,true).alert()).isFalse();
        assertThat(TrendPolicy.evaluate(first.state(),t.plusSeconds(20),100,250,true).alert()).isFalse();
        var alert=TrendPolicy.evaluate(first.state(),t.plusSeconds(10),100,250,true);
        assertThat(alert.alert()).isTrue();
        var cooling=TrendPolicy.evaluate(alert.state(),t.plusSeconds(300),100,250,true);
        assertThat(cooling.state().hits()).isZero();
        var boundary=TrendPolicy.evaluate(cooling.state(),t.plusSeconds(310),100,250,true);
        assertThat(boundary.alert()).isFalse();assertThat(boundary.state().hits()).isEqualTo(1);
        assertThat(TrendPolicy.evaluate(boundary.state(),t.plusSeconds(320),100,250,true).alert()).isTrue();
    }
    @Test void qualityFailureResetsConsecutiveHits() {
        var first=TrendPolicy.evaluate(null,t,100,250,true);
        var gap=TrendPolicy.evaluate(first.state(),t.plusSeconds(10),100,250,false);
        assertThat(TrendPolicy.evaluate(gap.state(),t.plusSeconds(20),100,250,true).alert()).isFalse();
    }
}
