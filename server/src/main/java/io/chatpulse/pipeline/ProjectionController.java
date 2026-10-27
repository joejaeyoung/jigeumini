package io.chatpulse.pipeline;

import java.time.Instant;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
public class ProjectionController {
    private final ProjectionStore store;
    private final ProjectionConsumer consumer;
    public ProjectionController(ProjectionStore store, ProjectionConsumer consumer) {
        this.store = store;
        this.consumer = consumer;
    }
    @GetMapping("/api/status")
    public Map<String, Object> status() {
        return Map.of("asOf", Instant.now().toString(), "consumer", consumer.status(), "totals", store.totals(),
                "progress", store.progress(), "retention",store.retention(), "scope", "COLLECTED_CHANNELS_ONLY", "coverageMeasured", false);
    }
    @GetMapping("/api/buckets")
    public Map<String, Object> buckets(@RequestParam(defaultValue="") String channelId,
            @RequestParam(defaultValue="300") int seconds) {
        if ((!channelId.isEmpty() && !channelId.matches("[a-f0-9]{32}")) || seconds < 10 || seconds > 3600 || seconds % 10 != 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "channelId 또는 seconds(10~3600, 10의 배수) 확인");
        }
        return Map.of("asOf", Instant.now().toString(), "windowSeconds", seconds, "bucketSeconds", 10,
                "includesOpenBucket", true, "coverageMeasured", false, "buckets", store.buckets(channelId, seconds));
    }
}
