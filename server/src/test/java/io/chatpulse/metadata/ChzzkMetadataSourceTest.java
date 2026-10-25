package io.chatpulse.metadata;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.metadata.BroadcastMetadata.*;

class ChzzkMetadataSourceTest {
    static final String ID = "a".repeat(32);
    private HttpServer server;
    private ChzzkMetadataSource source;
    private volatile String body;
    private volatile String channelBody;
    private volatile int status;
    private final List<String> paths = new ArrayList<>();
    private final List<String> credentials = new ArrayList<>();
    @BeforeEach void start() throws Exception {
        status = 200;
        body = live("false", "OPEN", "https://evil.test/not-allowed.jpg");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            for (String header : List.of("Cookie", "Authorization")) {
                if (exchange.getRequestHeaders().containsKey(header)) credentials.add(header);
            }
            byte[] bytes = (exchange.getRequestURI().getPath().endsWith("live-detail") ? body : channelBody).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        server.start();
        source = new ChzzkMetadataSource("http://127.0.0.1:" + server.getAddress().getPort());
    }
    @AfterEach void stop() { server.stop(0); }
    static String live(String adult, String status, String image) {
        return """
                {"code":200,"content":{"adult":%s,"status":"%s","liveTitle":"합성 모험 방송",
                "liveCategory":"test-game","liveCategoryValue":"테스트 게임","liveImageUrl":"%s",
                "channel":{"channelId":"%s","channelName":"합성 채널"},"ignoredPrivateField":"SYNTHETIC_SECRET"}}
                """.formatted(adult, status, image, ID);
    }
    @Test void returnsOnlyPublicFieldsAndExpandsThumbnailWithoutCredentials() {
        body = live("false", "OPEN", "https://livecloud.pstatic.net/thumb/{type}.jpg");
        var result = source.fetch(ID);
        assertThat(result.liveStatus()).isEqualTo(LiveStatus.LIVE);
        assertThat(result.channelName()).isEqualTo("합성 채널");
        assertThat(result.title()).isEqualTo("합성 모험 방송");
        assertThat(result.categoryName()).isEqualTo("테스트 게임");
        assertThat(result.thumbnailUrl()).isEqualTo("https://livecloud.pstatic.net/thumb/480.jpg");
        assertThat(result.toString()).doesNotContain("SYNTHETIC_SECRET");
        assertThat(paths).containsExactly("/service/v2/channels/" + ID + "/live-detail");
        assertThat(credentials).isEmpty();
    }
    @Test void rejectsAdultBlindAndMissingSafetyFields() {
        body = live("true", "OPEN", "https://livecloud.pstatic.net/a.jpg");
        assertHidden(source.fetch(ID), LiveStatus.RESTRICTED);
        body = live("false", "OPEN", "").replace("\"status\"", "\"blindType\":\"BLIND\",\"status\"");
        assertHidden(source.fetch(ID), LiveStatus.RESTRICTED);
        body = live("null", "OPEN", "");
        assertHidden(source.fetch(ID), LiveStatus.UNKNOWN);
    }
    @Test void closedBroadcastDoesNotReturnLastTitleOrImage() {
        body = live("false", "CLOSE", "https://livecloud.pstatic.net/a.jpg");
        var result = source.fetch(ID);
        assertThat(result.liveStatus()).isEqualTo(LiveStatus.OFFLINE);
        assertThat(result.channelName()).isEqualTo("합성 채널");
        assertThat(result.title()).isNull();
        assertThat(result.thumbnailUrl()).isNull();
    }
    @Test void observationFallbackReservesEveryActualHttpRequestAndRejectsExhaustedBudget() {
        body="{\"code\":200,\"content\":null}";
        channelBody="{\"code\":200,\"content\":{\"channelId\":\""+ID+"\",\"openLive\":false}}";
        var reservations=new java.util.concurrent.atomic.AtomicInteger();
        var counted=new ChzzkMetadataSource("http://127.0.0.1:"+server.getAddress().getPort(),id->{assertThat(id).isEqualTo(ID);reservations.incrementAndGet();});
        assertThat(counted.observe(ID).quality()).isEqualTo(PublicBroadcastObservation.Quality.OFFLINE);
        assertThat(reservations.get()).isEqualTo(2);assertThat(paths).hasSize(2);
        paths.clear();reservations.set(0);
        var limited=new ChzzkMetadataSource("http://127.0.0.1:"+server.getAddress().getPort(),id->{if(reservations.incrementAndGet()>1)throw new io.chatpulse.poc.SourceFailure("SOURCE_BUDGET_EXHAUSTED",true);});
        assertThatThrownBy(()->limited.observe(ID)).isInstanceOf(PublicObservationSource.Skipped.class);
        assertThat(paths).hasSize(1);
    }
    @Test void unexpectedBudgetHookFailureIsNotSilentlySkipped() {
        var broken=new ChzzkMetadataSource("http://127.0.0.1:"+server.getAddress().getPort(),id->{throw new NullPointerException("SYNTHETIC_PRIVATE_FAILURE");});
        assertThat(broken.observe(ID).quality()).isEqualTo(PublicBroadcastObservation.Quality.UPSTREAM_ERROR);
        assertThat(paths).isEmpty();
    }
    @Test void nullLiveRequiresExplicitOfflineFromChannel() {
        body = "{\"code\":200,\"content\":null}";
        channelBody = "{\"code\":200,\"content\":{\"channelId\":\"" + ID + "\",\"channelName\":\"합성 채널\",\"openLive\":false}}";
        assertThat(source.fetch(ID).liveStatus()).isEqualTo(LiveStatus.OFFLINE);
        channelBody = channelBody.replace("false", "true");
        assertHidden(source.fetch(ID), LiveStatus.UNKNOWN);
    }
    @Test void httpFailuresMapToSafeStatesWithoutBodyOrRedirects() {
        body = "SYNTHETIC_SECRET";
        for (int code : List.of(401, 403, 404, 429, 500, 302)) {
            status = code;
            var result = source.fetch(ID);
            assertHidden(result, code == 401 || code == 403 ? LiveStatus.RESTRICTED : code == 404 ? LiveStatus.NOT_FOUND : LiveStatus.UNKNOWN);
            assertThat(result.toString()).doesNotContain(body);
        }
        assertThat(paths).hasSize(6);
    }
    @Test void rejectsMismatchedChannelMalformedAndOversizedPayloads() {
        body = live("false", "OPEN", "").replace(ID, "b".repeat(32));
        assertHidden(source.fetch(ID), LiveStatus.UNKNOWN);
        body = "{";
        assertThat(source.fetch(ID).reason()).isEqualTo(Reason.INVALID_RESPONSE);
        body = "x".repeat(1_048_577);
        assertThat(source.fetch(ID).reason()).isEqualTo(Reason.INVALID_RESPONSE);
    }
    @Test void imageUrlMustUseKnownHttpsCdnWithoutUserinfoOrPort() {
        for (String value : List.of("http://livecloud.pstatic.net/a.jpg", "https://pstatic.net.evil.test/a.jpg", "https://evil.test/a.jpg", "https://user@livecloud.pstatic.net/a.jpg", "https://livecloud.pstatic.net:8080/a.jpg", "data:image/png;base64,AA", "not a url")) {
            assertThat(ChzzkMetadataSource.safeImage(value)).isNull();
        }
        assertThat(ChzzkMetadataSource.safeImage("https://livecloud.naver.net/a.jpg")).isNotNull();
        assertThat(ChzzkMetadataSource.safeImage("https://livecloud-thumb.akamaized.net/thumb/{type}.jpg")).isEqualTo("https://livecloud-thumb.akamaized.net/thumb/480.jpg");
        assertThat(ChzzkMetadataSource.safeImage("https://other.akamaized.net/a.jpg")).isNull();
        assertThat(source.fetch(ID).thumbnailUrl()).isNull();
    }
    @Test void observationUsesOnlyVerifiedNumericFieldsAndNeverGuessesMissingValues() {
        body = live("false", "OPEN", "").replace("\"adult\"", "\"liveId\":123,\"concurrentUserCount\":456,\"openDate\":\"2026-10-25 12:00:00\",\"adult\"");
        var value = source.observe(ID);
        assertThat(value.viewerCount()).isEqualTo(456L);
        assertThat(value.sourceBroadcastId()).isEqualTo("123");
        assertThat(value.quality()).isEqualTo(PublicBroadcastObservation.Quality.OBSERVED);
        assertThat(value.toString()).doesNotContain("합성 채널", "합성 모험 방송", "SYNTHETIC_SECRET", "2026-10-25");
        for (String invalid : List.of("null", "\"456\"", "-1", "456.5", "9007199254740992")) {
            body = live("false", "OPEN", "").replace("\"adult\"", "\"concurrentUserCount\":" + invalid + ",\"adult\"");
            var missing = source.observe(ID);
            assertThat(missing.viewerCount()).isNull();
            assertThat(missing.sourceBroadcastId()).isNull();
            assertThat(missing.quality()).isEqualTo(PublicBroadcastObservation.Quality.MISSING_VIEWER_COUNT);
        }
        body = live("false", "OPEN", "").replace("\"adult\"", "\"concurrentUserCount\":0,\"adult\"");
        assertThat(source.observe(ID).viewerCount()).isZero();
        body = body.replace("test-game", "test\\ncategory").replace("테스트 게임", "test\\nname");
        var malformedCategory = source.observe(ID);
        assertThat(malformedCategory.viewerCount()).isZero();
        assertThat(malformedCategory.categoryId()).isNull();
        assertThat(malformedCategory.categoryName()).isNull();
        assertThat(credentials).isEmpty();
    }
    @Test void failedOrClosedObservationContainsNoPreviousMeasurements() {
        body = live("false", "CLOSE", "");
        assertThat(source.observe(ID).quality()).isEqualTo(PublicBroadcastObservation.Quality.OFFLINE);
        body = "SYNTHETIC_SECRET";
        for (int code : List.of(429, 500, 403)) {
            status = code;
            var value = source.observe(ID);
            assertThat(value.viewerCount()).isNull();
            assertThat(value.sourceBroadcastId()).isNull();
            assertThat(value.categoryName()).isNull();
            assertThat(value.toString()).doesNotContain(body);
            assertThat(value.quality().name()).isEqualTo(code == 429 ? "RATE_LIMITED" : code == 403 ? "RESTRICTED" : "UPSTREAM_ERROR");
        }
    }
    private void assertHidden(BroadcastMetadata result, LiveStatus status) {
        assertThat(result.liveStatus()).isEqualTo(status);
        assertThat(result.channelName()).isNull();
        assertThat(result.title()).isNull();
        assertThat(result.categoryName()).isNull();
        assertThat(result.thumbnailUrl()).isNull();
    }
}
