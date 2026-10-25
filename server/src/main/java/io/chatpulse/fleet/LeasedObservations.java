package io.chatpulse.fleet;

import io.chatpulse.metadata.PublicBroadcastObservation;
import io.chatpulse.observations.*;
import io.chatpulse.poc.CollectionObserver;
import java.util.*;
import java.util.function.*;

/** 실제 임대한 채널만 관측한다. 채팅 HTTP와 예산을 공유하고 응답 뒤 권한을 다시 검사한다. */
public final class LeasedObservations implements CollectionObserver,AutoCloseable {
    private final BudgetedChatSource budget;private final String run;private final int interval,ttl;
    private final Function<String,PublicBroadcastObservation> source;private final Consumer<BroadcastObserved> publisher;
    private final Map<String,ObservationCollector> active=new HashMap<>();private boolean closed;
    public LeasedObservations(BudgetedChatSource budget,String run,int interval,int ttl,
            Function<String,PublicBroadcastObservation> source,Consumer<BroadcastObserved> publisher) {
        this.budget=budget;this.run=run;this.interval=interval;this.ttl=ttl;this.source=source;this.publisher=publisher;
        if(interval<60||interval>900||ttl<interval||ttl>1800)throw new IllegalArgumentException("OBSERVATION_CONFIGURATION");
    }
    @Override public synchronized void start(String channel) {
        if(closed||active.containsKey(channel))return;if(active.size()>=20)throw new IllegalStateException("OBSERVATION_CAPACITY");
        budget.authorize(channel).check();
        var collector=new ObservationCollector(List.of(channel),run,interval,ttl,this::observe,this::publish);
        active.put(channel,collector);collector.start(channel);collector.start();
    }
    PublicBroadcastObservation observe(String channel) {
        var permit=budget.authorize(channel);var data=source.apply(channel);permit.check();return data;
    }
    void publish(BroadcastObserved event){budget.authorize(event.channelId()).check();publisher.accept(event);}
    @Override public void stopped(String channel) {ObservationCollector collector;synchronized(this){collector=active.remove(channel);}if(collector!=null){collector.stopped(channel);collector.close();}}
    @Override public void close(){List<String> channels;synchronized(this){closed=true;channels=List.copyOf(active.keySet());}channels.forEach(this::stopped);}
}
