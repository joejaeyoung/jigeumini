package io.chatpulse.operations;

import java.nio.file.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RuntimeEnvironmentTest {
    @TempDir Path directory;
    @Test void deploymentNeverFallsBackToDevelopmentPassword() {
        var failure=assertThrows(IllegalArgumentException.class,()->RuntimeEnvironment.from(Map.of("DEPLOYMENT_ENABLED","true")));
        assertEquals("SECRET_FILE_REQUIRED",failure.getMessage());
        assertThrows(IllegalArgumentException.class,()->RuntimeEnvironment.password(Map.of("PIPELINE_SCHEMA_MODE","VALIDATE")));
    }
    @Test void diagnosticsDiscardUnknownMessagesAndKeepOnlyStableCodes() {
        var failure=new IllegalStateException("SYNTHETIC_SECRET",new IllegalStateException("SCHEMA_NOT_READY"));
        assertEquals(java.util.List.of("SCHEMA_NOT_READY"),FailureDiagnostics.codes(failure));
        assertEquals(java.util.List.of("IllegalStateException"),FailureDiagnostics.types(failure));
        assertEquals(java.util.List.of("KAFKA_TOPIC_INIT_FAILED"),FailureDiagnostics.codes(new IllegalStateException("KAFKA_TOPIC_INIT_FAILED")));
    }
    @Test void secretFileWinsWithoutAppearingInDiagnosticsAndInvalidFileFailsClosed() throws Exception {
        Path file=directory.resolve("password");Files.writeString(file,"SYNTHETIC_SECRET_VALUE\n");
        var settings=RuntimeEnvironment.from(Map.of("PIPELINE_DB_PASSWORD_FILE",file.toString(),"PIPELINE_KAFKA_DURABILITY","HA"));
        assertEquals("SYNTHETIC_SECRET_VALUE",settings.password());assertFalse(settings.toString().contains(settings.password()));assertEquals(3,settings.policy().replicationFactor());
        Files.writeString(file,"a\nb");var failure=assertThrows(IllegalArgumentException.class,()->RuntimeEnvironment.secret("fallback",file.toString()));
        assertEquals("SECRET_FILE_UNAVAILABLE",failure.getMessage());assertNull(failure.getCause());
        Files.delete(file);assertThrows(IllegalArgumentException.class,()->RuntimeEnvironment.secret("fallback",file.toString()));
    }
}
