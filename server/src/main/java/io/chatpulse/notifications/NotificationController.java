package io.chatpulse.notifications;

import java.time.*;
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
public class NotificationController {
    private final NotificationStore store;
    private final SpikeOutbox outbox;
    private final NotificationConsumer consumer;
    public NotificationController(NotificationStore store,SpikeOutbox outbox,NotificationConsumer consumer){this.store=store;this.outbox=outbox;this.consumer=consumer;}
    @GetMapping("/api/v1/notifications")
    public ResponseEntity<NotificationStore.Page> page(@RequestParam(defaultValue="") String channels,@RequestParam(defaultValue="") String cursor,@RequestParam(defaultValue="20") int limit){
        if(limit<1||limit>100||channels.length()>660||cursor.length()>100)throw invalid();
        List<String> ids=channels.isEmpty()?List.of():Arrays.stream(channels.split(",",-1)).distinct().toList();
        if(ids.size()>20||ids.stream().anyMatch(id->!id.matches("[a-f0-9]{32}")))throw invalid();
        Instant before=Instant.now().plusSeconds(1);UUID beforeId=new UUID(-1,-1);
        try {if(!cursor.isEmpty()){var parts=cursor.split("\\|",-1);if(parts.length!=2)throw invalid();before=Instant.parse(parts[0]);beforeId=UUID.fromString(parts[1]);}}
        catch(RuntimeException ignored){throw invalid();}
        if(before.isAfter(Instant.now().plusSeconds(2))||before.isBefore(Instant.parse("2000-01-01T00:00:00Z")))throw invalid();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(store.page(ids,before,beforeId,limit));
    }
    @GetMapping("/api/operations/notifications")
    public ResponseEntity<Map<String,Object>> status(){return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("outbox",outbox.status(),"consumer",consumer.status()));}
    @ExceptionHandler({DataAccessException.class,org.springframework.transaction.TransactionException.class})
    public ResponseEntity<Map<String,String>> unavailable(){return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason","QUERY_UNAVAILABLE"));}
    private ResponseStatusException invalid(){return new ResponseStatusException(HttpStatus.BAD_REQUEST,"관심 채널과 조회 범위를 확인해주세요");}
}
