package io.chatpulse.poc;

import java.time.Instant;
import java.util.UUID;

/** 콜백에서는 메모리 관측만 갱신한다. DB flush는 별도 worker가 수행한다. */
public interface CollectionObserver {
    CollectionObserver NONE = new CollectionObserver() { };
    default void start(String channel) { }
    default void state(String channel, String state) { }
    default void connected(String channel, UUID session, Instant at) { }
    default void alive(UUID session, Instant at) { }
    default void ended(UUID session, Instant at, String reason) { }
    default void loss(String channel, Instant from, Instant to, String reason) { }
    default void stopped(String channel) { }
}
