package io.chatpulse.retention;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;

/** 정리 실패는 소비 offset을 바꾸지 않는다. 고정 코드/숫자로 진행을 남기고 다음 주기에 재개한다. */
public final class RetentionMaintenance implements AutoCloseable {
    public record Task(String name,Supplier<Map<String,Object>> action) { }
    private final JdbcTemplate jdbc;
    private final String run;
    private final List<Task> tasks;
    private final ScheduledExecutorService worker=Executors.newSingleThreadScheduledExecutor();
    private int cursor;
    public RetentionMaintenance(JdbcTemplate jdbc,String run,List<Task> tasks) {
        this.jdbc=jdbc;this.run=run;this.tasks=List.copyOf(tasks);
    }
    public void start() { worker.scheduleWithFixedDelay(this::tick,10,10,TimeUnit.SECONDS); }
    public void tick() {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
        for(int attempted=0;attempted<tasks.size() && !Thread.currentThread().isInterrupted() && System.nanoTime()<deadline;attempted++) {
            Task task=tasks.get(cursor);cursor=(cursor+1)%tasks.size();long started=System.nanoTime();
            try {
                jdbc.update("""
                        INSERT INTO retention_task(task,run_id,attempted_at,state) VALUES (?,?,clock_timestamp(),'RUNNING')
                        ON CONFLICT(task,run_id) DO UPDATE SET attempted_at=excluded.attempted_at,state='RUNNING'
                        """,task.name(),run);
                var result=task.action().get();
                jdbc.update("""
                        UPDATE retention_task SET state='SUCCEEDED',succeeded_at=clock_timestamp(),deleted_rows=?,duration_millis=?,limited=?
                        WHERE task=? AND run_id=?
                        """,result.get("deletedRows"),elapsed(started),result.get("limited"),task.name(),run);
            } catch(RuntimeException failure) {
                try { jdbc.update("UPDATE retention_task SET state='RETRYING',failures=failures+1,duration_millis=? WHERE task=? AND run_id=?",elapsed(started),task.name(),run); }
                catch(RuntimeException unavailable) { /* DB가 복구되면 다음 시도에서 갱신한다. stale attempted_at도 경보다. */ }
                org.slf4j.LoggerFactory.getLogger(RetentionMaintenance.class).warn("RETENTION_RETRYING");
            }
        }
    }
    public static Map<String,Object> counted(int count) { return Map.of("deletedRows",count,"limited",count>=10000); }
    private static long elapsed(long started) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started); }
    @Override public void close() { worker.shutdownNow(); }
}
