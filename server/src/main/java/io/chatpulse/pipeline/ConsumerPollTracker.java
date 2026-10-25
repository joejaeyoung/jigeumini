package io.chatpulse.pipeline;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** 빈 poll도 포함한 마지막 성공 poll과 할당을 하나의 snapshot으로 관측한다. */
final class ConsumerPollTracker {
    record State(Instant at, Set<TopicPartition> partitions) { }
    private volatile State state = new State(null, Set.of());

    State snapshot() { return state; }
    void revoked() { state = new State(null, Set.of()); }

    @SuppressWarnings("unchecked")
    Consumer<String, byte[]> track(Consumer<String, byte[]> consumer) {
        return (Consumer<String, byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[] { Consumer.class }, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(consumer, args);
                        if (method.getName().equals("poll"))
                            state = new State(Instant.now(), Set.copyOf(consumer.assignment()));
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }
}
