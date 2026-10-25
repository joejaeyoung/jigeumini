package io.chatpulse.metadata;

@FunctionalInterface
interface MetadataSource {
    BroadcastMetadata fetch(String channelId);
}
