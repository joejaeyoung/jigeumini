package io.chatpulse.reports;

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
public final class ReportController {
    private final ReportQueries queries;
    private final ReportConsumer consumer;
    public ReportController(ReportQueries queries,ReportConsumer consumer) { this.queries=queries;this.consumer=consumer; }
    @GetMapping("/api/v1/channels/{channelId}/reports") public ResponseEntity<ReportQueries.Page> reports(@PathVariable String channelId) {
        if(!channelId.matches("[a-f0-9]{32}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"채널을 확인해주세요");
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(queries.list(channelId));
    }
    @GetMapping("/api/v1/reports/{id}") public ResponseEntity<ReportQueries.Report> report(@PathVariable String id) {
        if(!id.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"리포트를 확인해주세요");
        var report=queries.detail(UUID.fromString(id));if(report==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"관측 기록이 없어요");
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(report);
    }
    @GetMapping("/api/operations/reports") public ResponseEntity<Map<String,Object>> status() { return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(consumer.status()); }
    @ExceptionHandler({DataAccessException.class,org.springframework.transaction.TransactionException.class,IllegalArgumentException.class})
    public ResponseEntity<Map<String,String>> unavailable() { return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason","QUERY_UNAVAILABLE")); }
}
