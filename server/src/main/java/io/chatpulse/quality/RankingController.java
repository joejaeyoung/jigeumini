package io.chatpulse.quality;

import java.time.Instant;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
public class RankingController {
    private final RankingStore store;
    private final java.time.Clock clock;
    @org.springframework.beans.factory.annotation.Autowired
    public RankingController(RankingStore store) { this(store, java.time.Clock.systemUTC()); }
    RankingController(RankingStore store, java.time.Clock clock) { this.store=store; this.clock=clock; }
    @GetMapping("/api/v1/rankings")
    public ResponseEntity<RankingStore.RankingResponse> rankings(@RequestParam(defaultValue="60") int window,@RequestParam(defaultValue="20") int limit) {
        if(window!=60&&window!=300)throw invalid();checkLimit(limit);
        return response(store.rankings(window,limit));
    }
    @GetMapping("/api/v1/spikes")
    public ResponseEntity<RankingStore.SpikeResponse> spikes(@RequestParam(defaultValue="") String cursor,@RequestParam(defaultValue="20") int limit) {
        checkLimit(limit); Instant at=Instant.now().plusSeconds(1);UUID id=new UUID(-1,-1);
        if(!cursor.isEmpty()) {
            try {var parts=cursor.split("\\|",-1);if(parts.length!=2)throw new IllegalArgumentException();at=Instant.parse(parts[0]);id=UUID.fromString(parts[1]);}
            catch(RuntimeException ignored) {throw invalid();}
        }
        return response(store.spikes(at,id,limit));
    }
    @GetMapping("/api/v1/channels/{id}/activity")
    public ResponseEntity<RankingStore.ActivityResponse> activity(@PathVariable String id,@RequestParam(required=false) String from,@RequestParam(required=false) String to, @RequestParam(required=false) Integer seconds) {
        if(!id.matches("[a-f0-9]{32}"))throw invalid();
        Instant now=Instant.ofEpochSecond(Math.floorDiv(clock.instant().getEpochSecond(),60)*60),start,end;
        if(seconds != null && ((seconds != 1800 && seconds != 3600) || from != null || to != null)) throw invalid();
        try {end=to==null?now:Instant.parse(to);start=from==null?end.minusSeconds(seconds == null ? 1800 : seconds):Instant.parse(from);}
        catch(RuntimeException ignored) {throw invalid();}
        if(start.getNano()!=0||end.getNano()!=0||start.getEpochSecond()%60!=0||end.getEpochSecond()%60!=0||!end.isAfter(start)||end.isAfter(now)||start.isBefore(now.minusSeconds(3600)))throw invalid();
        return response(store.activity(id,start,end));
    }
    @GetMapping("/api/v1/collection-status")
    public ResponseEntity<Map<String,Object>> collectionStatus() {
        var ranking=store.rankings(60,100);var counts=new TreeMap<String,Long>();
        ranking.entries().forEach(e->counts.merge(e.status(),1L,Long::sum));ranking.excludedChannels().forEach(e->counts.merge(e.status(),1L,Long::sum));
        return response(Map.of("asOf",ranking.asOf(),"status",ranking.status(),"channelsByStatus",counts));
    }
    @ExceptionHandler({DataAccessException.class, org.springframework.transaction.TransactionException.class})
    public ResponseEntity<Map<String,String>> unavailable() { return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason","QUERY_UNAVAILABLE")); }
    private void checkLimit(int limit) {if(limit<1||limit>100)throw invalid();}
    private ResponseStatusException invalid() {return new ResponseStatusException(HttpStatus.BAD_REQUEST,"조회 범위와 cursor를 확인해주세요");}
    private <T> ResponseEntity<T> response(T value) {return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value);}
}
