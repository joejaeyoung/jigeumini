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
        assertThatThrownBy(()->SpikeDetected.decode("b".repeat(32),event.encode())).hasMessage("SPIKE_CONTRACT_INVALID");
        assertThatThrownBy(()->SpikeDetected.decode(event.channelId(),"{synthetic secret}".getBytes())).hasMessage("SPIKE_CONTRACT_INVALID").hasNoCause();
    }
    @Test void invalidEvidenceCannotBecomeAnAlert() {
        assertThatThrownBy(()->new SpikeDetected(1,UUID.randomUUID(),"test-v1","a".repeat(32),Instant.now(),"chat-spike-v1",99,BigDecimal.TEN,BigDecimal.TEN,"HEALTHY_AT_DETECTION"))
                .hasMessage("SPIKE_CONTRACT_INVALID");
    }
}
