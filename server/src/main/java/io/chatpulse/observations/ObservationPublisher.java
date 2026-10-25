package io.chatpulse.observations;

import io.chatpulse.pipeline.KafkaSupport;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.*;

public final class ObservationPublisher implements Consumer<BroadcastObserved>, AutoCloseable {
    private final String topic;
    private final DefaultKafkaProducerFactory<String,byte[]> factory;
    private final KafkaTemplate<String,byte[]> kafka;
    public ObservationPublisher(String bootstrap, String topic) {
        this.topic = topic; factory = KafkaSupport.producerFactory(bootstrap); kafka = new KafkaTemplate<>(factory);
    }
    @Override public void accept(BroadcastObserved event) {
        try {
            byte[] bytes = event.encode();
            if (bytes.length > 4096) throw new IllegalArgumentException("OBSERVATION_CONTRACT_INVALID");
            kafka.send(new ProducerRecord<>(topic, null, event.observedAt().toEpochMilli(), event.channelId(), bytes)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new IllegalStateException("OBSERVATION_ACK_UNCONFIRMED");
        } catch (Exception ignored) { throw new IllegalStateException("OBSERVATION_ACK_UNCONFIRMED"); }
    }
    @Override public void close() { factory.destroy(); }
}
