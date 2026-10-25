package io.chatpulse.observations;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;

public final class ObservationCrashProcess {
    public static void main(String[] args) throws Exception {
        var store = ObservationIntegrationTest.store(args[0], Clock.systemUTC());
        try (var consumer = new ObservationConsumer(ObservationIntegrationTest.BOOTSTRAP,args[1],args[2],"earliest",store,
                () -> Runtime.getRuntime().halt(97))) {
            consumer.start(); new CountDownLatch(1).await();
        }
    }
}
