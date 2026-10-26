package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.tomcat.websocket.AsyncChannelGroupUtil;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

class ChatSocketClientTest {
    @Test void lateConnectionIsClosedWithoutStoppingAnotherOwner() throws Exception {
        int originalUsers = groupUsers();
        var started = new CountDownLatch(1);
        var connected = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var source = new WebSocketServer(new InetSocketAddress("127.0.0.1", 0), 2) {
            public void onStart() { started.countDown(); }
            public void onOpen(WebSocket socket, ClientHandshake request) { }
            public void onMessage(WebSocket socket, String text) { }
            public void onClose(WebSocket socket, int code, String reason, boolean remote) { }
            public void onError(WebSocket socket, Exception error) { }
        };
        source.start();
        try (var anchor = new ChatSocketClient(); var late = new ChatSocketClient()) {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            var uri = URI.create("ws://127.0.0.1:" + source.getPort());
            var anchorSession = anchor.connect(new TextWebSocketHandler(), uri).get(5, TimeUnit.SECONDS);
            var connecting = late.connect(new TextWebSocketHandler() {
                @Override public void afterConnectionEstablished(WebSocketSession session) throws Exception {
                    connected.countDown();
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("TEST_RELEASE_TIMEOUT");
                }
            }, uri);
            try {
                assertThat(connected.await(5, TimeUnit.SECONDS)).isTrue();
                late.close(); // transport 등록 직전에 owner 수명이 끝난다.
                release.countDown();
                var lateSession = connecting.get(5, TimeUnit.SECONDS);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                        .until(() -> !lateSession.isOpen() && source.getConnections().size() == 1);
                assertThat(anchorSession.isOpen()).isTrue();
                assertThat(groupUsers()).isEqualTo(originalUsers + 1);
                assertThatThrownBy(() -> late.connect(new TextWebSocketHandler(), uri).join())
                        .hasRootCauseMessage("WS_CLIENT_CLOSED");
            } finally { release.countDown(); }
        } finally { release.countDown(); source.stop(1_000); }
        assertThat(groupUsers()).isEqualTo(originalUsers);
    }

    private static int groupUsers() throws Exception {
        // 고정 Tomcat 버전의 실제 IO 등록 수명 회귀. 제품 코드에는 내부 계수 접근을 넣지 않는다.
        var field = AsyncChannelGroupUtil.class.getDeclaredField("usageCount"); field.setAccessible(true);
        return field.getInt(null);
    }
}
