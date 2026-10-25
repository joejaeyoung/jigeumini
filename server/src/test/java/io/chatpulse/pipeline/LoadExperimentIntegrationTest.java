package io.chatpulse.pipeline;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class LoadExperimentIntegrationTest {
    @Test void liveBaselineReadsMissingAndPartialTablesWithoutMigratingThem() {
        String schema="exp_empty_"+UUID.randomUUID().toString().replace("-","");
        var root=new org.springframework.jdbc.core.JdbcTemplate(IntegrationSupport.ds("public"));root.execute("CREATE SCHEMA "+schema);
        try {
            var jdbc=new org.springframework.jdbc.core.JdbcTemplate(IntegrationSupport.ds(schema));
            assertThat(LoadExperiment.live(jdbc)).containsExactlyInAnyOrderEntriesOf(Map.of("ledgerPresent",false,"bucketPresent",false));
            assertThat(jdbc.queryForObject("SELECT count(*) FROM pg_tables WHERE schemaname=?",Long.class,schema)).isZero();
            jdbc.execute("CREATE TABLE processed_event(id int)");jdbc.update("INSERT INTO processed_event VALUES(1)");
            assertThat(LoadExperiment.live(jdbc)).containsEntry("ledgerPresent",true).containsEntry("ledger",1L).containsEntry("bucketPresent",false).doesNotContainKey("bucket");
            jdbc.execute("CREATE TABLE chat_bucket_10s(chat_count bigint)");jdbc.update("INSERT INTO chat_bucket_10s VALUES(2)");
            assertThat(LoadExperiment.live(jdbc)).containsEntry("bucketPresent",true).containsEntry("bucket",2L);
        } finally {root.execute("DROP SCHEMA "+schema+" CASCADE");}
    }

    @Test void productPublisherReconcilesPhysicalDuplicatesIndependentGroupsAndReplay() throws Exception {
        verifyLoad("product-smoke");
    }
    @Test void isolatedSyntheticLoadReconcilesDuplicatesIndependentGroupsAndFixedRangeReplay() throws Exception {
        verifyLoad("smoke");
    }
    private void verifyLoad(String profile) throws Exception {
        Path output=Path.of("../work/experiment-test-"+UUID.randomUUID()+".json");LoadExperiment.quiet();
        try {
            var result=LoadExperiment.run(ExperimentPlan.named(profile),output,null,false);
            assertThat(result.get("correct")).as("sanitized experiment result: %s",result).isEqualTo(true);assertThat(result.get("cleanupComplete")).isEqualTo(true);
            assertThat(result.get("generatedUnique")).isEqualTo(200L);assertThat(result.get("attemptedRecords")).isEqualTo(202L);assertThat(result.get("replayBucketDifferences")).isEqualTo(0L);
            assertThat(((Map<?,?>)result.get("databaseA")).get("processedEvents")).isEqualTo(200L);
            assertThat(((Map<?,?>)result.get("databaseA")).get("bucketTotal")).isEqualTo(200L);
            assertThat(((Map<?,?>)result.get("databaseB")).get("processedEvents")).isEqualTo(200L);
            assertThat(Files.readString(output.resolveSibling(output.getFileName()+".html"))).contains("TARGET_INPUT_COMPLETED");
        } finally {Files.deleteIfExists(output);Files.deleteIfExists(output.resolveSibling(output.getFileName()+".html"));Files.deleteIfExists(output.resolveSibling(output.getFileName()+".progress.json"));}
    }
}
