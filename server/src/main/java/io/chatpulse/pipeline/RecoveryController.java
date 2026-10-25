package io.chatpulse.pipeline;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
public class RecoveryController {
    private final RecoveryDiagnostics diagnostics;
    public RecoveryController(RecoveryDiagnostics diagnostics) { this.diagnostics=diagnostics; }
    @GetMapping("/api/operations/recovery")
    public RecoveryDiagnostics.Snapshot recovery() { return diagnostics.snapshot(); }
}
