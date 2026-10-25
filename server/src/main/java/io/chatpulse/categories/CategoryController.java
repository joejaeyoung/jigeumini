package io.chatpulse.categories;

import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("pipeline")
@ConditionalOnProperty(name="pipeline.role",havingValue="analyzer")
public final class CategoryController {
    private final CategoryQueries queries;
    private final CategoryConsumer consumer;
    public CategoryController(CategoryQueries queries,CategoryConsumer consumer) { this.queries=queries;this.consumer=consumer; }
    @GetMapping("/api/v1/categories") public ResponseEntity<CategoryQueries.Response> categories() { return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(queries.current()); }
    @GetMapping("/api/v1/categories/{key}/timeline") public ResponseEntity<CategoryQueries.Timeline> timeline(@PathVariable String key) {
        if(!key.matches("[a-f0-9]{64}|unknown"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"카테고리를 확인해주세요");
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(queries.timeline(key));
    }
    @GetMapping("/api/operations/categories") public ResponseEntity<Map<String,Object>> status() { return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(consumer.status()); }
    @ExceptionHandler({DataAccessException.class,org.springframework.transaction.TransactionException.class,IllegalArgumentException.class})
    public ResponseEntity<Map<String,String>> unavailable() { return ResponseEntity.status(503).cacheControl(CacheControl.noStore()).body(Map.of("reason","QUERY_UNAVAILABLE")); }
}
