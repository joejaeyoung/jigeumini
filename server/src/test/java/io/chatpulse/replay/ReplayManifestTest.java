package io.chatpulse.replay;

import java.time.Instant;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ReplayManifestTest {
    private ReplayManifest example() {
        return new ReplayManifest(1,"test.replay",Uuid.randomUuid().toString(),List.of(new ReplayManifest.Range(0,10,12)),
                Instant.parse("2026-10-25T00:00:00Z"),Instant.parse("2026-10-25T00:01:00Z"),1,ReplayManifest.ALGORITHM,Instant.now());
    }
    @Test void roundTripAndHalfOpenEventTimeFilter() {
        var m=example();assertThat(ReplayManifest.parse(m.json())).isEqualTo(m);
        assertThat(m.includes(m.eventFrom())).isTrue();assertThat(m.includes(m.eventTo())).isFalse();
        assertThat(m.includes(m.eventFrom().minusNanos(1))).isFalse();
    }
    @Test void unsupportedVersionsMissingPartitionsAndNegativeOffsetsFailClosed() {
        var m=example();
        assertThatThrownBy(()->ReplayManifest.parse(m.json().replace("bucket-10s-v1","future"))).isInstanceOf(ReplayFailure.class);
        assertThatThrownBy(()->ReplayManifest.parse(m.json().replace("\"partition\":0","\"partition\":1"))).isInstanceOf(ReplayFailure.class);
        assertThatThrownBy(()->new ReplayManifest.Range(0,-1,2)).isInstanceOf(ReplayFailure.class);
        assertThatThrownBy(()->new ReplayManifest.Range(0,3,2)).isInstanceOf(ReplayFailure.class);
        assertThatThrownBy(()->ReplayManifest.parse("SYNTHETIC_PRIVATE_PAYLOAD")).hasMessage("INVALID_MANIFEST");
    }
    @Test void comparableInputIgnoresCaptureTimeButNotTopicIdentityOffsetsOrFilter() {
        var m=example();
        var same=new ReplayManifest(1,m.topic(),m.topicId(),m.partitions(),m.eventFrom(),m.eventTo(),1,m.algorithmVersion(),m.capturedAt().plusSeconds(1));
        assertThat(m.sameInput(same)).isTrue();
        var changed=new ReplayManifest(1,m.topic(),Uuid.randomUuid().toString(),m.partitions(),m.eventFrom(),m.eventTo(),1,m.algorithmVersion(),m.capturedAt());
        assertThat(m.sameInput(changed)).isFalse();
    }
}
