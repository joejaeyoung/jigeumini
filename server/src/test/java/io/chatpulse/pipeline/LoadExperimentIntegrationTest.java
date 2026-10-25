package io.chatpulse.pipeline;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class LoadExperimentIntegrationTest {
    @Test void isolatedSyntheticLoadReconcilesDuplicatesIndependentGroupsAndFixedRangeReplay() throws Exception {
        Path output=Path.of("../work/experiment-test-"+UUID.randomUUID()+".json");LoadExperiment.quiet();
        try {
            var result=LoadExperiment.run(ExperimentPlan.named("smoke"),output,null,false);
            assertThat(result.get("correct")).isEqualTo(true);assertThat(result.get("cleanupComplete")).isEqualTo(true);
            assertThat(result.get("generatedUnique")).isEqualTo(200L);assertThat(result.get("attemptedRecords")).isEqualTo(202L);assertThat(result.get("replayBucketDifferences")).isEqualTo(0L);
            assertThat(((Map<?,?>)result.get("databaseA")).get("processedEvents")).isEqualTo(200L);
            assertThat(((Map<?,?>)result.get("databaseA")).get("bucketTotal")).isEqualTo(200L);
            assertThat(((Map<?,?>)result.get("databaseB")).get("processedEvents")).isEqualTo(200L);
            assertThat(Files.readString(output.resolveSibling(output.getFileName()+".html"))).contains("TARGET_INPUT_COMPLETED");
        } finally {Files.deleteIfExists(output);Files.deleteIfExists(output.resolveSibling(output.getFileName()+".html"));Files.deleteIfExists(output.resolveSibling(output.getFileName()+".progress.json"));}
    }
}
