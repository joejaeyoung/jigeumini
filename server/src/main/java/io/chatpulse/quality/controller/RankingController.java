package io.chatpulse.quality.controller;

import io.chatpulse.quality.RankingStore;
import io.chatpulse.quality.service.RankingQueryService;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name = "pipeline.role", havingValue = "analyzer")
public class RankingController {
    private final RankingQueryService service;

    public RankingController(RankingQueryService service) {
        this.service = service;
    }

    @GetMapping("/api/v1/rankings")
    public ResponseEntity<RankingStore.RankingResponse> rankings(
            @RequestParam(defaultValue = "60") int window, @RequestParam(defaultValue = "20") int limit) {
        return response(service.rankings(window, limit));
    }

    @GetMapping("/api/v1/spikes")
    public ResponseEntity<RankingStore.SpikeResponse> spikes(
            @RequestParam(defaultValue = "") String cursor, @RequestParam(defaultValue = "20") int limit) {
        return response(service.spikes(cursor, limit));
    }

    @GetMapping("/api/v1/channels/{id}/activity")
    public ResponseEntity<RankingStore.ActivityResponse> activity(
            @PathVariable String id, @RequestParam(required = false) String from,
            @RequestParam(required = false) String to, @RequestParam(required = false) Integer seconds) {
        return response(service.activity(id, from, to, seconds));
    }

    @GetMapping("/api/v1/collection-status")
    public ResponseEntity<Map<String, Object>> collectionStatus() {
        return response(service.collectionStatus());
    }

    @ExceptionHandler({DataAccessException.class, TransactionException.class})
    public ResponseEntity<Map<String, String>> unavailable() {
        return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason", "QUERY_UNAVAILABLE"));
    }

    private <T> ResponseEntity<T> response(T value) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value);
    }
}
