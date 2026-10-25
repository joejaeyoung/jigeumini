package io.chatpulse.fleet;

import io.chatpulse.discovery.BroadcastCatalog;
import io.chatpulse.discovery.SelectionPolicy;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

class FleetCliTest {
    @TempDir Path directory;
    @Test void disabledWorkerIsRejectedBeforeOpeningDatabaseOrOutput() {
        Path output=directory.resolve("result.json");
        var failure=assertThrows(IllegalArgumentException.class,()->FleetCli.execute(new String[]{"worker","1","10",output.toString()},Map.of("PIPELINE_JDBC_URL","invalid")));
        assertEquals("FLEET_LIVE_DISABLED",failure.getMessage());assertFalse(Files.exists(output));
    }
    @Test void previewDoesNotOpenDatabaseOrRequireLiveActivation() throws Exception {
        var now=Instant.now();
        var snapshot=new BroadcastCatalog.Snapshot("SYNTHETIC",now,now,BroadcastCatalog.Result.COMPLETE,1,1,0,List.of());
        var report=new SelectionPolicy(null,3,120,SelectionPolicy.SourceMode.UNOFFICIAL_POC,Set.of(),Set.of()).evaluate(snapshot,now);
        var input=directory.resolve("selection.json");Files.writeString(input,JsonMapper.builder().build().writeValueAsString(report));
        assertEquals(0,FleetCli.execute(new String[]{"preview",input.toString(),"120"},Map.of("PIPELINE_JDBC_URL","invalid")));
    }
}
