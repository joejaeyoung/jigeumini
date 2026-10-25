package io.chatpulse.notifications;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SpikeDetectedTest {
    @Test void onlyValidatedImmutableFieldsRoundTrip() {
        var event=new SpikeDetected(1,UUID.randomUUID(),"test-v1","a".repeat(32),Instant.parse("2026-10-25T12:00:00Z"),"chat-spike-v1",100,new BigDecimal("50.0"),new BigDecimal("2.00"),"HEALTHY_AT_DETECTION");
        assertThat(SpikeDetected.decode(event.channelId(),event.encode())).isEqualTo(event);
        assertThat(new String(event.encode(),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain("nickname","token","message");
        String encoded=new String(event.encode(),java.nio.charset.StandardCharsets.UTF_8);
        for(String malformed:java.util.List.of(encoded.replace("\"schemaVersion\":1","\"schemaVersion\":\"1\""),
                encoded.replace("\"count\":100","\"count\":100.5"),encoded.replace("\"count\":100","\"count\":\"100\"")))
            assertThatThrownBy(()->SpikeDetected.decode(event.channelId(),malformed.getBytes(java.nio.charset.StandardCharsets.UTF_8))).hasMessage("SPIKE_CONTRACT_INVALID");
        assertThatThrownBy(()->SpikeDetected.decode("b".repeat(32),event.encode())).hasMessage("SPIKE_CONTRACT_INVALID");
        assertThatThrownBy(()->SpikeDetected.decode(event.channelId(),"{synthetic secret}".getBytes())).hasMessage("SPIKE_CONTRACT_INVALID").hasNoCause();
    }
    @Test void invalidEvidenceCannotBecomeAnAlert() {
        assertThatThrownBy(()->new SpikeDetected(1,UUID.randomUUID(),"test-v1","a".repeat(32),Instant.now(),"chat-spike-v1",99,BigDecimal.TEN,BigDecimal.TEN,"HEALTHY_AT_DETECTION"))
                .hasMessage("SPIKE_CONTRACT_INVALID");
    }
    @Test void roundedTrendEvidenceRemainsDeliverable() {
        var at=Instant.parse("2026-10-25T12:00:00Z");
        var previous=new io.chatpulse.quality.TrendPolicy.State(at.minusSeconds(10),1,null);
        var result=io.chatpulse.quality.TrendPolicy.evaluate(previous,at,101,150,true);
        assertThat(result.alert()).isTrue();
        var event=new SpikeDetected(1,UUID.randomUUID(),"test-v1","a".repeat(32),at,
                io.chatpulse.quality.TrendPolicy.VERSION,101,result.baseline(),result.ratio(),"HEALTHY_AT_DETECTION");
        assertThat(event.ratio()).isEqualByComparingTo("3.36666667");
        assertThat(SpikeDetected.decode(event.channelId(),event.encode())).isEqualTo(event);
    }
}
