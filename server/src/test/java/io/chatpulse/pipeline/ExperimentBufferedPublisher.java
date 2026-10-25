package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.poc.EventSink;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.springframework.kafka.core.KafkaTemplate;

/** 실제 두 발행기의 수락/계수/버퍼 관측을 같은 실험에서 대조한다. */
record ExperimentBufferedPublisher(EventSink sink, Supplier<Map<String, BufferedKafkaPublisher.BufferState>> buffers,
        BooleanSupplier failure) implements AutoCloseable {
    static ExperimentBufferedPublisher legacy(KafkaTemplate<String,byte[]> kafka,String topic,int capacity) {
        var publisher=new LegacySerialPublisher(kafka,topic,new EventCodec(),capacity);
        return new ExperimentBufferedPublisher(publisher,publisher::bufferSnapshot,publisher::deliveryFailed);
    }
    static ExperimentBufferedPublisher product(KafkaTemplate<String,byte[]> kafka,String topic,int capacity) {
        var publisher=new BufferedKafkaPublisher(kafka,topic,new EventCodec(),capacity);
        return new ExperimentBufferedPublisher(publisher,publisher::bufferSnapshot,publisher::deliveryFailed);
    }
    void accept(List<ChatEvent> events){sink.accept(events);}
    Map<String,Object> finish(){return sink.finish();}
    Map<String,BufferedKafkaPublisher.BufferState> bufferSnapshot(){return buffers.get();}
    boolean deliveryFailed(){return failure.getAsBoolean();}
    @Override public void close(){sink.finish();}
}
