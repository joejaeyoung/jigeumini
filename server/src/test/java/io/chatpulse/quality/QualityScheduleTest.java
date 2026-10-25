package io.chatpulse.quality;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QualityScheduleTest {
    @Test void evaluationCostDoesNotAccumulateIntoMissingTenSecondTicks() {
        var millis = new AtomicLong(5400);
        Clock clock = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return Instant.ofEpochMilli(millis.get()); }
        };
        var ticks = new ArrayList<Long>();
        var scheduler = new ScheduledThreadPoolExecutor(1) {
            @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initial, long period, TimeUnit unit) {
                long next = millis.get() + unit.toMillis(initial);
                for (int i = 0; i < 120; i++) {
                    millis.set(next); task.run(); next += unit.toMillis(period);
                }
                return null;
            }
            @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long initial, long delay, TimeUnit unit) {
                millis.addAndGet(unit.toMillis(initial));
                for (int i = 0; i < 120; i++) { task.run(); millis.addAndGet(unit.toMillis(delay)); }
                return null;
            }
        };
        try {
            QualityRuntime.scheduleEvaluation(scheduler, clock, () -> {
                ticks.add(Math.floorDiv(clock.millis(), 10000) * 10);
                millis.addAndGet(300); // 합성 DB 평가 비용. fixed delay라면 약 34회마다 tick을 건너뛴다.
            });
            assertEquals(120, ticks.size());
            for (int i = 0; i < ticks.size(); i++) assertEquals((i + 1) * 10L, ticks.get(i));
        } finally { scheduler.shutdownNow(); }
    }
}
