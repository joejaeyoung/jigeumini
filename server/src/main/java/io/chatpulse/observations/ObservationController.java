package io.chatpulse.observations;

import io.chatpulse.quality.RankingStore;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role", havingValue="analyzer")
public final class ObservationController {
    public record Timeline(String channelId, String asOf, String from, String to,
            List<RankingStore.ActivityPoint> chat, List<BroadcastObserved> observations, boolean truncated) { }
    private final ObservationStore store;
    private final RankingStore ranking;
    private final ObservationConsumer consumer;
    private final Clock clock;
    @Autowired public ObservationController(ObservationStore store, RankingStore ranking, ObservationConsumer consumer) {
        this(store, ranking, consumer, Clock.systemUTC());
    }
    ObservationController(ObservationStore store, RankingStore ranking, ObservationConsumer consumer, Clock clock) {
        this.store = store; this.ranking = ranking; this.consumer = consumer; this.clock = clock;
    }
    @GetMapping("/api/v1/channels/{id}/timeline")
    public ResponseEntity<Timeline> timeline(@PathVariable String id, @RequestParam(defaultValue="3600") int seconds) {
        if (!id.matches("[a-f0-9]{32}") || (seconds != 1800 && seconds != 3600))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "관측 채널과 조회 범위를 확인해주세요");
        Instant now = clock.instant(), to = Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond(), 60) * 60), from = to.minusSeconds(seconds);
        var chat = ranking.activity(id, from, to);
        var history = store.history(id, from, to);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(
                new Timeline(id, now.toString(), from.toString(), to.toString(), chat.points(), history.samples(), history.truncated()));
    }
    @GetMapping("/api/operations/observations")
    public ResponseEntity<Map<String,Object>> status() { return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(consumer.status()); }
    @ExceptionHandler({DataAccessException.class, org.springframework.transaction.TransactionException.class, IllegalArgumentException.class})
    public ResponseEntity<Map<String,String>> unavailable() {
        return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason", "QUERY_UNAVAILABLE"));
    }
}
