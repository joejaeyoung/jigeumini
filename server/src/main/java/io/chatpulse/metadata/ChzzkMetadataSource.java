package io.chatpulse.metadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.metadata.BroadcastMetadata.*;

/** 익명 공개 조회 어댑터. 인증·쿠키·리디렉션이나 접근 제한 우회는 사용하지 않는다. */
final class ChzzkMetadataSource implements MetadataSource {
    private final RestClient client;
    private final String base;
    private final JsonMapper json = JsonMapper.builder().build();

    ChzzkMetadataSource() { this("https://api.chzzk.naver.com"); }
    ChzzkMetadataSource(String base) {
        this.base = base;
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(5));
        client = RestClient.builder().requestFactory(factory).defaultHeader("User-Agent", "Jigeumini/0.1")
                .defaultHeader("Accept", "application/json").build();
    }
    @Override public BroadcastMetadata fetch(String id) {
        try {
            var live = get("/service/v2/channels/" + id + "/live-detail");
            if (live.isNull()) {
                var channel = get("/service/v1/channels/" + id);
                if (!id.equals(channel.path("channelId").asText()) || !channel.path("openLive").isBoolean()) {
                    throw new Failure(Reason.INVALID_RESPONSE);
                }
                if (channel.path("openLive").asBoolean()) throw new Failure(Reason.INVALID_RESPONSE);
                return publicData(id, channel, null, LiveStatus.OFFLINE);
            }
            if (!live.isObject()) throw new Failure(Reason.INVALID_RESPONSE);
            if (live.path("adult").asBoolean(false) || restricted(live)) throw new Failure(Reason.RESTRICTED);
            if (!live.path("adult").isBoolean()) throw new Failure(Reason.INVALID_RESPONSE);
            var channel = live.path("channel");
            if (!id.equals(channel.path("channelId").asText())) throw new Failure(Reason.INVALID_RESPONSE);
            return switch (live.path("status").asText()) {
                case "OPEN" -> publicData(id, channel, live, LiveStatus.LIVE);
                case "CLOSE", "CLOSED" -> publicData(id, channel, null, LiveStatus.OFFLINE);
                default -> throw new Failure(Reason.INVALID_RESPONSE);
            };
        } catch (Failure failure) {
            return empty(id, failure.reason == Reason.RESTRICTED ? LiveStatus.RESTRICTED
                    : failure.reason == Reason.NOT_FOUND ? LiveStatus.NOT_FOUND : LiveStatus.UNKNOWN, failure.reason);
        } catch (Exception ignored) {
            // 예외 메시지/원천 body를 로그나 HTTP 응답에 포함하지 않는다.
            return empty(id, LiveStatus.UNKNOWN, Reason.UPSTREAM_ERROR);
        }
    }
    private boolean restricted(JsonNode live) {
        String blind = live.path("blindType").asText("");
        return !blind.isBlank() && !"NONE".equals(blind);
    }
    private BroadcastMetadata publicData(String id, JsonNode channel, JsonNode live, LiveStatus status) {
        String name = text(channel, "channelName", 120);
        if (name == null) throw new Failure(Reason.INVALID_RESPONSE);
        return new BroadcastMetadata(id, name, live == null ? null : text(live, "liveTitle", 300),
                live == null ? null : text(live, "liveCategory", 120),
                live == null ? null : text(live, "liveCategoryValue", 120),
                live == null ? null : thumbnail(live), status, Freshness.FRESH, Reason.NONE, null, null);
    }
    private String thumbnail(JsonNode live) {
        String image = safeImage(text(live, "liveImageUrl", 2048));
        return image != null ? image : safeImage(text(live, "defaultThumbnailImageUrl", 2048));
    }
    static String safeImage(String value) {
        if (value == null) return null;
        try {
            String expanded = value.replace("{type}", "480");
            URI uri = URI.create(expanded);
            String host = uri.getHost();
            if (!"https".equals(uri.getScheme()) || uri.getUserInfo() != null || uri.getPort() != -1 || host == null
                    || !(host.equals("livecloud-thumb.akamaized.net") || host.endsWith(".pstatic.net") || host.endsWith(".naver.net"))) return null;
            return expanded;
        } catch (IllegalArgumentException ignored) { return null; }
    }
    private static String text(JsonNode node, String field, int max) {
        var value = node.path(field);
        if (!value.isString()) return null;
        String text = value.asText().strip();
        return text.isEmpty() || text.length() > max ? null : text;
    }
    private JsonNode get(String path) {
        return client.get().uri(base + path).exchange((request, response) -> {
            int status = response.getStatusCode().value();
            if (status != 200) throw new Failure(reason(status));
            byte[] body = response.getBody().readNBytes(1_048_577);
            if (body.length > 1_048_576) throw new Failure(Reason.INVALID_RESPONSE);
            JsonNode root;
            try { root = json.readTree(body); }
            catch (Exception ignored) { throw new Failure(Reason.INVALID_RESPONSE); }
            if (root == null || root.path("code").asInt(-1) != 200) throw new Failure(reason(root == null ? -1 : root.path("code").asInt(-1)));
            if (!root.has("content")) throw new Failure(Reason.INVALID_RESPONSE);
            return root.path("content");
        });
    }
    private static Reason reason(int code) {
        return switch (code) {
            case 401, 403 -> Reason.RESTRICTED;
            case 404 -> Reason.NOT_FOUND;
            case 429 -> Reason.RATE_LIMITED;
            default -> code >= 500 ? Reason.UPSTREAM_ERROR : Reason.INVALID_RESPONSE;
        };
    }
    private static final class Failure extends RuntimeException {
        final Reason reason;
        Failure(Reason reason) { super(reason.name(), null, false, false); this.reason = reason; }
    }
}
