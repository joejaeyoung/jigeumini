package io.chatpulse.poc;

import java.util.List;
import java.util.Map;

/** WebSocket callback은 네트워크 발행을 기다리지 않고 bounded sink에 넘긴다. */
public interface EventSink {
    EventSink DISCARD = events -> { };
    void accept(List<ChatEvent> events);
    default void checkHealthy() { }
    default Map<String, Object> finish() { return Map.of("mode", "POC_COUNTER_ONLY", "complete", true); }
}
