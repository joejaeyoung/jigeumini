package io.chatpulse.poc;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Component
public final class ChzzkHttpSource implements ChatSource {
    private final RestClient client;
    private final String apiBase;
    private final String gameBase;
    private final JsonMapper json = JsonMapper.builder().build();

    public ChzzkHttpSource() {
        this("https://api.chzzk.naver.com", "https://comm-api.game.naver.com");
    }

    ChzzkHttpSource(String apiBase, String gameBase) {
        this.apiBase = apiBase;
        this.gameBase = gameBase;
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(10));
        client = RestClient.builder().requestFactory(factory)
                .defaultHeader("User-Agent", "ChatPulse-PoC/0.1")
                .defaultHeader("Accept", "application/json").build();
    }

    @Override
    public String resolveRoom(String channelId) {
        var content = get(apiBase + "/polling/v2/channels/" + channelId + "/live-status");
        if (content.path("adult").asBoolean(false)) throw new SourceFailure("RESTRICTED", true);
        if (!"OPEN".equals(content.path("status").asText())) throw new SourceFailure("OFFLINE", true);
        String room = content.path("chatChannelId").asText("");
        if (!room.matches("[a-zA-Z0-9_-]{1,128}")) throw new SourceFailure("ROOM_UNAVAILABLE", true);
        return room;
    }

    @Override
    public String readToken(String roomId) {
        String token = get(gameBase + "/nng_main/v1/chats/access-token?channelId=" + roomId
                + "&chatType=STREAMING").path("accessToken").asText("");
        if (token.isBlank()) throw new SourceFailure("TOKEN_UNAVAILABLE", true);
        return token;
    }

    @Override
    public URI socketUri(String roomId) {
        int server = roomId.chars().sum() % 9 + 1;
        return URI.create("wss://kr-ss" + server + ".chat.naver.com/chat");
    }

    private JsonNode get(String url) {
        try {
            return client.get().uri(url).exchange((request, response) -> {
                int status = response.getStatusCode().value();
                if (status != 200) throw new SourceFailure("HTTP_" + status,
                        status >= 400 && status < 500);
                byte[] bytes = response.getBody().readNBytes(1_048_577);
                if (bytes.length > 1_048_576) throw new SourceFailure("HTTP_BODY_TOO_LARGE", true);
                var root = json.readTree(bytes);
                int code = root.path("code").asInt(-1);
                if (code != 200) throw new SourceFailure("API_" + code, true);
                if (!root.path("content").isObject()) throw new SourceFailure("CONTENT_UNAVAILABLE", true);
                return root.path("content");
            });
        } catch (SourceFailure e) {
            throw e;
        } catch (Exception e) {
            throw new SourceFailure("HTTP_TRANSPORT_OR_JSON", false);
        }
    }
}
