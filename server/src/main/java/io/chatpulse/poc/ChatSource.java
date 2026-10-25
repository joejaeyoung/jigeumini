package io.chatpulse.poc;

import java.net.URI;

public interface ChatSource {
    String resolveRoom(String channelId);
    String readToken(String roomId);
    URI socketUri(String roomId);
}
