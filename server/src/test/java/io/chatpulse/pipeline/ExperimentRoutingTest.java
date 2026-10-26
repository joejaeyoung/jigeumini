package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ExperimentRoutingTest {
    private static String hotChannel() {
        return UUID.nameUUIDFromBytes("channel/20261025/0".getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
    }

    @Test void threeSuffixKeysCanAllCollideOnTheSamePartition() {
        assertThat(java.util.stream.IntStream.range(0, 3)
                .mapToObj(i -> ExperimentRouting.keyPartition(hotChannel() + "/" + i)).toList())
                .containsExactly(2, 2, 2);
        var audit = ExperimentRouting.SUFFIX_HASH.audit(Map.of(2, 24000L), 0);
        assertThat(audit).containsEntry("splitVerified", false).containsEntry("hotPartitionCount", 1L);
        assertThat(ExperimentRouting.EXPLICIT_EVENT_ID.audit(Map.of(2, 24000L), 0))
                .containsEntry("requiredDistributionPassed", false);
    }

    @Test void explicitRoutingPreservesThePayloadKeyAndDuplicateLocationAcrossPublisherSequences() {
        var counts = new HashMap<Integer, Long>();
        var codec = new EventCodec();
        for (int i = 0; i < 2000; i++) {
            var now = Instant.parse("2026-10-26T00:00:00Z");
            var event = new ChatEvent(1, UUID.nameUUIDFromBytes(("20261025/" + i).getBytes(StandardCharsets.UTF_8)),
                    "CHZZK_UNOFFICIAL", hotChannel(), "synthetic", new UUID(0, 1), null, "CHAT", now, now, false, "test");
            var first = ExperimentRouting.EXPLICIT_EVENT_ID.record("synthetic", event, codec.encode(event), i);
            var duplicate = ExperimentRouting.EXPLICIT_EVENT_ID.record("synthetic", event, codec.encode(event), i + 1);
            assertThat(first.partition()).isEqualTo(duplicate.partition());
            assertThat(codec.decode(first.value(), first.key())).isEqualTo(event);
            counts.merge(first.partition(), 1L, Long::sum);
        }
        assertThat(ExperimentRouting.EXPLICIT_EVENT_ID.audit(counts, 0))
                .containsEntry("splitVerified", true).containsEntry("requiredDistributionPassed", true);
        assertThat(ExperimentRouting.EXPLICIT_EVENT_ID.audit(counts, 1))
                .containsEntry("requiredDistributionPassed", false);
    }
}
