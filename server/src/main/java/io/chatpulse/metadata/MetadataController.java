package io.chatpulse.metadata;

import io.chatpulse.pipeline.ProjectionStore;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
public class MetadataController {
    public record ChannelsResponse(String asOf, String scope, List<BroadcastMetadata> channels) { }
    private final MetadataService metadata;
    private final ProjectionStore store;
    public MetadataController(MetadataService metadata, ProjectionStore store) { this.metadata = metadata; this.store = store; }
    @GetMapping("/api/channels")
    public ResponseEntity<ChannelsResponse> channels(@RequestParam(required=false) String ids) {
        List<String> channels;
        if (ids == null) channels = store.recentChannelIds(20);
        else {
            var values = ids.split(",", -1);
            if (values.length > 20 || Arrays.stream(values).anyMatch(id -> !id.matches("[a-f0-9]{32}"))) throw invalid();
            channels = Arrays.stream(values).distinct().toList();
        }
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(new ChannelsResponse(Instant.now().toString(),
                ids == null ? "COLLECTED_CHANNELS_ONLY" : "REQUESTED_CHANNELS", channels.stream().map(metadata::get).toList()));
    }
    @GetMapping("/api/channels/{id}")
    public ResponseEntity<BroadcastMetadata> channel(@PathVariable String id) {
        if (!id.matches("[a-f0-9]{32}")) throw invalid();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(metadata.get(id));
    }
    private ResponseStatusException invalid() { return new ResponseStatusException(HttpStatus.BAD_REQUEST, "채널 ID는 32자리 소문자 hex, 목록은 최대 20개입니다"); }
}
