package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChzzkHttpSourceTest {
    private HttpServer server;
    private ChzzkHttpSource source;
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicReference<String> body = new AtomicReference<>();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> cookie = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            path.set(exchange.getRequestURI().toString());
            cookie.set(exchange.getRequestHeaders().getFirst("Cookie"));
            byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status.get(), bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        source = new ChzzkHttpSource(base, base);
    }

    @AfterEach
    void stop() { server.stop(0); }

    @Test
    void resolvesRoomAndReadTokenWithoutLoginCookie() {
        body.set("{\"code\":200,\"content\":{\"status\":\"OPEN\",\"adult\":false,\"chatChannelId\":\"N2pG7U\"}}");
        assertThat(source.resolveRoom("abc")).isEqualTo("N2pG7U");
        assertThat(path.get()).isEqualTo("/polling/v2/channels/abc/live-status");
        assertThat(cookie.get()).isNull();
        body.set("{\"code\":200,\"content\":{\"accessToken\":\"TEST_ONLY\"}}");
        assertThat(source.readToken("N2pG7U")).isEqualTo("TEST_ONLY");
        assertThat(path.get()).isEqualTo("/nng_main/v1/chats/access-token?channelId=N2pG7U&chatType=STREAMING");
        assertThat(source.socketUri("N2pG7U").toString()).isEqualTo("wss://kr-ss2.chat.naver.com/chat");
    }

    @Test
    void offlineAndRestrictedChannelsStopWithoutCredentialsFallback() {
        body.set("{\"code\":200,\"content\":{\"status\":\"CLOSE\"}}");
        assertThatThrownBy(() -> source.resolveRoom("abc")).hasMessage("OFFLINE");
        body.set("{\"code\":200,\"content\":{\"status\":\"OPEN\",\"adult\":true}}");
        assertThatThrownBy(() -> source.resolveRoom("abc")).hasMessage("RESTRICTED");
    }

    @Test
    void httpAndApiRejectionsDoNotExposeBody() {
        status.set(403);
        body.set("PRIVATE_RESPONSE_TOKEN");
        assertThatThrownBy(() -> source.readToken("room")).hasMessage("HTTP_403");
        status.set(200);
        body.set("{\"code\":42601,\"message\":\"PRIVATE_RESPONSE_TOKEN\"}");
        assertThatThrownBy(() -> source.readToken("room")).hasMessage("API_42601");
    }
}
