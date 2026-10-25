package io.chatpulse.operations;

import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@ConditionalOnProperty(name="deployment.enabled",havingValue="true")
public final class DeploymentController {
    private final DeploymentHealth health;private final Map<String,String> identity;
    public DeploymentController(DeploymentHealth health) {
        this.health=health;String revision=System.getenv().getOrDefault("APP_REVISION","UNKNOWN");
        if(!revision.matches("[a-f0-9]{40}"))revision="UNKNOWN";
        String build="UNKNOWN";try(var input=getClass().getResourceAsStream("/replay-build-id.txt")){
            if(input!=null){String value=new String(input.readNBytes(128),java.nio.charset.StandardCharsets.UTF_8).strip();if(value.matches("[a-f0-9]{64}"))build=value;}
        }catch(Exception ignored){}identity=Map.of("revision",revision,"buildId",build,"role","analyzer-api","schemaContract","V12");
    }
    @GetMapping("/livez") public Map<String,Object> live(){return Map.of("live",true,"identity",identity);}
    @GetMapping("/readyz") public ResponseEntity<Map<String,Object>> ready(){var status=health.status();return ResponseEntity.status(Boolean.TRUE.equals(status.get("ready"))?200:503).body(Map.of("identity",identity,"health",status));}
    @EventListener(ContextClosedEvent.class) public void closing(){health.draining();}
}
