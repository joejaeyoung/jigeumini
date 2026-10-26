package io.chatpulse.fleet;

import io.chatpulse.pipeline.EventCodec;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.*;

/** 실제 producer를 감싸 합성 event ID와 호출 경계만 기록한다. Kafka I/O thread는 기다리지 않는다. */
final class FleetProducerProbe {
    enum Boundary { NONE, BEFORE_SEND, AFTER_BROKER_ACK }
    private final Boundary boundary;
    private final Set<UUID> sent=ConcurrentHashMap.newKeySet(),acknowledged=ConcurrentHashMap.newKeySet();
    private final CountDownLatch reached=new CountDownLatch(1),released=new CountDownLatch(1);
    private final AtomicReference<Runnable> callback=new AtomicReference<>();
    FleetProducerProbe(){this(Boundary.NONE);}
    FleetProducerProbe(Boundary boundary){this.boundary=boundary;}
    @SuppressWarnings("unchecked")
    Producer<String,byte[]> wrap(Producer<String,byte[]> delegate) {
        return (Producer<String,byte[]>)Proxy.newProxyInstance(Producer.class.getClassLoader(),new Class<?>[]{Producer.class},(proxy,method,args)->{
            if(method.getName().equals("send")&&args.length==2) {
                var record=(ProducerRecord<String,byte[]>)args[0];
                UUID id=new EventCodec().decode(record.value(),record.key()).eventId();
                if(boundary==Boundary.BEFORE_SEND){reached.countDown();if(!released.await(15,TimeUnit.SECONDS))throw new IllegalStateException("SEND_BARRIER_TIMEOUT");}
                sent.add(id);var original=(Callback)args[1];
                return delegate.send(record,(metadata,error)->{
                    if(error==null)acknowledged.add(id);
                    Runnable deliver=()->original.onCompletion(metadata,error);
                    if(boundary==Boundary.AFTER_BROKER_ACK&&error==null) {
                        if(!callback.compareAndSet(null,deliver))throw new IllegalStateException("CALLBACK_BARRIER_REUSED");
                        reached.countDown();
                    } else deliver.run();
                });
            }
            try{return method.invoke(delegate,args);}
            catch(InvocationTargetException failure){throw failure.getCause();}
        });
    }
    boolean awaitBoundary(Duration timeout) throws InterruptedException{return reached.await(timeout.toMillis(),TimeUnit.MILLISECONDS);}
    void release(){released.countDown();var pending=callback.getAndSet(null);if(pending!=null)pending.run();}
    Set<UUID> sent(){return Set.copyOf(sent);}
    Set<UUID> acknowledged(){return Set.copyOf(acknowledged);}
    Map<String,Object> snapshot(){return Map.of("sentEventIds",sent.stream().map(UUID::toString).sorted().toList(),"brokerAcknowledgedEventIds",acknowledged.stream().map(UUID::toString).sorted().toList());}
}
