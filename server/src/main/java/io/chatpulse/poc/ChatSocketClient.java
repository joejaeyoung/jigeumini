package io.chatpulse.poc;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.tomcat.websocket.WsWebSocketContainer;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

/** 한 ChannelProbe 실행이 소유하는 transport. Tomcat의 공유 IO 등록 해제까지 책임진다. */
final class ChatSocketClient implements AutoCloseable {
    static final int TEXT_CHUNK_CHARACTERS = 8192;
    private final WsWebSocketContainer container = new WsWebSocketContainer();
    private final StandardWebSocketClient client = new StandardWebSocketClient(container);
    private final AtomicBoolean closed = new AtomicBoolean();

    ChatSocketClient() {
        container.setDefaultMaxTextMessageBufferSize(TEXT_CHUNK_CHARACTERS);
        container.setDefaultMaxSessionIdleTimeout(60_000);
        container.setAsyncSendTimeout(5_000);
        client.setUserProperties(Map.of("org.apache.tomcat.websocket.BLOCKING_SEND_TIMEOUT", 5_000L));
    }

    CompletableFuture<WebSocketSession> connect(WebSocketHandler handler, URI uri) {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("WS_CLIENT_CLOSED"));
        var connecting = client.execute(handler, new WebSocketHttpHeaders(), uri);
        connecting.whenComplete((session, failure) -> {
            if (closed.get()) {
                try { if (session != null && session.isOpen()) session.close(); }
                catch (Exception ignored) { }
                // close와 handshake가 겹쳐 늦게 등록된 session/group도 이 owner가 정리한다.
                container.destroy();
            }
        });
        return connecting;
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) container.destroy();
    }
}
