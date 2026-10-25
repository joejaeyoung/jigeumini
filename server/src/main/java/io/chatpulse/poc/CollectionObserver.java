package io.chatpulse.poc;

import java.time.Instant;
import java.util.UUID;

/** 콜백에서는 메모리 관측만 갱신한다. DB flush는 별도 worker가 수행한다. */
public interface CollectionObserver {
    CollectionObserver NONE = new CollectionObserver() { };
    static CollectionObserver combine(java.util.List<CollectionObserver> observers) {
        var targets = java.util.List.copyOf(observers);
        return new CollectionObserver() {
            public void start(String channel) { targets.forEach(o -> o.start(channel)); }
            public void state(String channel, String state) { targets.forEach(o -> o.state(channel, state)); }
            public void connected(String channel, UUID session, Instant at) { targets.forEach(o -> o.connected(channel, session, at)); }
            public void alive(UUID session, Instant at) { targets.forEach(o -> o.alive(session, at)); }
            public void ended(UUID session, Instant at, String reason) { targets.forEach(o -> o.ended(session, at, reason)); }
            public void loss(String channel, Instant from, Instant to, String reason) { targets.forEach(o -> o.loss(channel, from, to, reason)); }
            public void stopped(String channel) { targets.forEach(o -> o.stopped(channel)); }
        };
    }
    default void start(String channel) { }
    default void state(String channel, String state) { }
    default void connected(String channel, UUID session, Instant at) { }
    default void alive(UUID session, Instant at) { }
    default void ended(UUID session, Instant at, String reason) { }
    default void loss(String channel, Instant from, Instant to, String reason) { }
    default void stopped(String channel) { }
}
