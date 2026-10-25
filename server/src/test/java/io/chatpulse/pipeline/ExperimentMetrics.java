package io.chatpulse.pipeline;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

final class ExperimentMetrics {
    private final EventCodec codec=new EventCodec();
    private final Set<UUID> seen=ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Double> latency=new ConcurrentLinkedQueue<>();
    private final LongAdder delivered=new LongAdder(),duplicates=new LongAdder();
    private final ConcurrentHashMap<Integer,LongAdder> partitionCount=new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,LongAdder> channelCount=new ConcurrentHashMap<>();
    void committed(List<ConsumerRecord<String,byte[]>> records) {
        Instant committed=Instant.now();
        for(var record:records) {
            var event=codec.decode(record.value(),record.key());delivered.increment();partitionCount.computeIfAbsent(record.partition(),p->new LongAdder()).increment();
            if(seen.add(event.eventId())) {latency.add(Math.max(0,Duration.between(event.receivedAt(),committed).toNanos()/1_000_000d));channelCount.computeIfAbsent(event.channelId(),id->new LongAdder()).increment();}
            else duplicates.increment();
        }
    }
    Map<String,Object> snapshot() {
        var partitions=new TreeMap<Integer,Long>();partitionCount.forEach((p,n)->partitions.put(p,n.sum()));
        var channels=new TreeMap<String,Long>();channelCount.forEach((p,n)->channels.put(p,n.sum()));
        return Map.of("deliveredRecords",delivered.sum(),"duplicateDeliveries",duplicates.sum(),"uniqueObserved",seen.size(),"partitionRecords",partitions,"channelRecords",channels,"receivedToDbMillis",percentiles(latency));
    }
    static Map<String,Object> percentiles(Collection<Double> values) {
        var sorted=values.stream().sorted().toList();var result=new LinkedHashMap<String,Object>();result.put("samples",sorted.size());
        for(var quantile:Map.of("p50",.5,"p95",.95,"p99",.99).entrySet())result.put(quantile.getKey(),sorted.isEmpty()?null:sorted.get(Math.min(sorted.size()-1,(int)Math.ceil(quantile.getValue()*sorted.size())-1)));
        result.put("maximum",sorted.isEmpty()?null:sorted.getLast());return result;
    }
}
