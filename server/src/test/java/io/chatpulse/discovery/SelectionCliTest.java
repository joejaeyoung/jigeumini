package io.chatpulse.discovery;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.discovery.SelectionPolicyTest.*;

class SelectionCliTest {
    @TempDir Path directory;
    @Test void fixtureIsDeterministicDoesNotOverwriteAndNeverStartsCollection() throws Exception {
        var json=FixtureBroadcastCatalog.JSON;Path config=directory.resolve("config.json"),input=directory.resolve("input.json"),out=directory.resolve("result.json"),again=directory.resolve("again.json");
        Files.writeString(config,json.writeValueAsString(new SelectionCli.Configuration(policy(1000L,20,1),OfficialBroadcastCatalogTest.limits(5,6))));
        Files.writeString(input,json.writeValueAsString(snapshot(candidate(1,1000L,0L))));
        assertThat(SelectionCli.run(new String[]{"fixture",config.toString(),input.toString(),out.toString()},Map.of())).isZero();
        assertThat(SelectionCli.run(new String[]{"fixture",config.toString(),input.toString(),again.toString()},Map.of())).isZero();
        assertThat(Files.readString(out)).isEqualTo(Files.readString(again)).contains("\"activationAllowed\" : false");
        String before=Files.readString(out);
        assertThat(SelectionCli.run(new String[]{"fixture",config.toString(),input.toString(),out.toString()},Map.of())).isEqualTo(2);
        assertThat(Files.readString(out)).isEqualTo(before);
    }
    @Test void officialModeIsDisabledAndUnexpectedInputFieldsAreRejected() throws Exception {
        var json=FixtureBroadcastCatalog.JSON;Path config=directory.resolve("config.json"),input=directory.resolve("input.json"),out=directory.resolve("result.json");
        Files.writeString(config,json.writeValueAsString(new SelectionCli.Configuration(policy(null,20,1),OfficialBroadcastCatalogTest.limits(5,6))));
        assertThat(SelectionCli.run(new String[]{"official",config.toString(),out.toString()},Map.of())).isEqualTo(2);
        var object=json.valueToTree(snapshot(candidate(1,0L,0L)));((tools.jackson.databind.node.ObjectNode)object).put("unexpected","synthetic-private");
        Files.writeString(input,json.writeValueAsString(object));
        assertThat(SelectionCli.run(new String[]{"fixture",config.toString(),input.toString(),out.toString()},Map.of())).isEqualTo(2);
        assertThat(Files.exists(out)).isFalse();
    }
}
