package io.chatpulse.quality;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QualityScheduleTest {
    @Test void evaluationCostAndClockStepsDoNotAccumulateIntoMissingTicks() {
        for (long cost : List.of(300L,3000L,5000L)) for (long step : List.of(-2000L,2000L)) {
            var millis = new AtomicLong(5400);
            Clock clock = new Clock() {
                public ZoneId getZone() { return ZoneOffset.UTC; }
                public Clock withZone(ZoneId zone) { return this; }
                public Instant instant() { return Instant.ofEpochMilli(millis.get()); }
            };
            record Pending(Runnable task,long delay) { }
            var queue = new ArrayDeque<Pending>();
            var scheduler = new ScheduledThreadPoolExecutor(1) {
                @Override public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
                    queue.add(new Pending(task,unit.toMillis(delay))); return null;
                }
            };
            var ticks = new ArrayList<Long>();
            try {
                QualityRuntime.scheduleEvaluation(scheduler, clock, () -> {
                    ticks.add(Math.floorDiv(clock.millis() - 7000,10000)*10);
                    millis.addAndGet(cost);
                });
                int executions=0;
                while (ticks.size()<120 && executions<125) {
                    var pending=queue.removeFirst();
                    millis.addAndGet(pending.delay() + (executions++ == 20 ? step : 0));
                    pending.task().run();
                }
                assertEquals(120,ticks.size());
                for(int i=0;i<ticks.size();i++) assertEquals(i*10L,ticks.get(i));
            } finally { scheduler.shutdownNow(); }
        }
    }
}
