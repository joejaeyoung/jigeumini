package io.chatpulse.metadata;

@FunctionalInterface
public interface PublicObservationSource {
    PublicBroadcastObservation observe(String channelId);
}
