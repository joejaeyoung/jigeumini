package io.chatpulse.discovery;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** 공식 Client 인증 읽기 API. 응답 원문·비밀 헤더·cursor는 결과/예외에 남기지 않는다. */
public final class OfficialBroadcastCatalog implements BroadcastCatalog {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final int MAX_BODY = 1_048_576;
    private final URI base;
    private final String clientId, clientSecret;
    private final Clock clock;
    private final HttpClient client;

    public OfficialBroadcastCatalog(String clientId, String clientSecret) {
        this(URI.create("https://openapi.chzzk.naver.com"), clientId, clientSecret, Clock.systemUTC());
    }
    OfficialBroadcastCatalog(URI base, String clientId, String clientSecret, Clock clock) {
        if (clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank())
            throw new IllegalArgumentException("CLIENT_CREDENTIALS_REQUIRED");
        this.base = base; this.clientId = clientId; this.clientSecret = clientSecret; this.clock = clock;
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
    }

    @Override public Snapshot scan(Limits limits) {
        var state = new Scan(limits);
        try {
            String next = null; var cursors = new HashSet<String>();
            do {
                if (state.pages == limits.maxPages()) throw new Failure(Result.PAGE_LIMIT);
                var content = state.get("/open/v1/lives?size=20" + (next == null ? "" : "&next=" + encode(next)));
                var data = data(content); state.pages++;
                Instant observed = clock.instant();
                for (var row : data) {
                    String channel = channel(row); long liveId = positiveLong(row.path("liveId"));
                    var item = new Candidate(channel, liveId, null, optionalCount(row.path("concurrentUserCount")),
                            row.path("adult").isBoolean() ? row.path("adult").booleanValue() : null, true, observed, false);
                    var prior = state.candidates.putIfAbsent(channel, item);
                    if (prior != null) {
                        state.duplicates++;
                        if (prior.liveId() != liveId) state.candidates.put(channel, prior.ambiguous());
                    }
                }
                if (!content.path("page").isObject()) throw new Failure(Result.INVALID_RESPONSE);
                var cursor = content.path("page").path("next");
                if (!cursor.isMissingNode() && !cursor.isNull() && !cursor.isString()) throw new Failure(Result.INVALID_RESPONSE);
                next = cursor.asText("");
                if (next.length() > 4096) throw new Failure(Result.INVALID_RESPONSE);
                if (next.isEmpty()) next = null;
                else if (!cursors.add(next)) throw new Failure(Result.CURSOR_REPEAT);
            } while (next != null);
            var channels = new ArrayList<>(state.candidates.keySet());
            for (int start = 0; start < channels.size(); start += 20) {
                var batch = channels.subList(start, Math.min(channels.size(), start + 20));
                String query = String.join("&", batch.stream().map(id -> "channelIds=" + id).toList());
                var found = new HashSet<String>();
                for (var row : data(state.get("/open/v1/channels?" + query))) {
                    String id = channel(row);
                    if (!batch.contains(id) || !found.add(id)) throw new Failure(Result.INVALID_RESPONSE);
                    state.candidates.put(id, state.candidates.get(id).followers(optionalCount(row.path("followerCount"))));
                }
            }
        } catch (Failure failure) { state.result = failure.result; }
          catch (Exception failure) { state.result = Result.INVALID_RESPONSE; }
        return new Snapshot("OFFICIAL_CATALOG", state.started, clock.instant(), state.result,
                state.pages, state.requests, state.duplicates, List.copyOf(state.candidates.values()));
    }

    private final class Scan {
        final Limits limits;
        final Instant started = clock.instant();
        final long end, interval;
        final Map<String, Candidate> candidates = new LinkedHashMap<>();
        int pages, requests, duplicates;
        long nextRequest;
        Result result = Result.COMPLETE;
        Scan(Limits limits) {
            this.limits = limits; nextRequest = System.nanoTime(); end = nextRequest + limits.deadline().toNanos();
            // 실원천은 최소 1초 간격. 이는 자체 보수적 예산이며 공식 허용량이라는 뜻이 아니다.
            interval = Math.max(base.getHost().equals("openapi.chzzk.naver.com") ? 1_000_000_000L : 0,
                    limits.requestInterval().toNanos());
        }
        JsonNode get(String path) {
            if (requests >= limits.requestBudget()) throw new Failure(Result.REQUEST_LIMIT);
            try {
                long remaining = end - System.nanoTime(), delay = Math.max(0, nextRequest - System.nanoTime());
                if (remaining <= delay) throw new Failure(Result.DEADLINE);
                if (delay > 0) TimeUnit.NANOSECONDS.sleep(delay);
                remaining = end - System.nanoTime();
                if (remaining <= 0) throw new Failure(Result.DEADLINE);
                Duration timeout = Duration.ofNanos(Math.min(remaining, TimeUnit.SECONDS.toNanos(5)));
                var request = HttpRequest.newBuilder(base.resolve(path)).timeout(timeout)
                        .header("Client-Id", clientId).header("Client-Secret", clientSecret)
                        .header("Accept", "application/json").header("User-Agent", "Jigeumini/0.1").GET().build();
                requests++; nextRequest = System.nanoTime() + interval;
                var future = client.sendAsync(request, ignored -> new BoundedBody());
                HttpResponse<byte[]> response;
                try { response = future.get(timeout.toNanos(), TimeUnit.NANOSECONDS); }
                catch (TimeoutException failure) { future.cancel(true); throw new Failure(end - System.nanoTime() <= 0 ? Result.DEADLINE : Result.SOURCE_UNAVAILABLE); }
                int status = response.statusCode();
                if (status == 429) throw new Failure(Result.RATE_LIMITED);
                if (status == 401 || status == 403) throw new Failure(Result.AUTHORIZATION_FAILED);
                if (status != 200) throw new Failure(Result.SOURCE_UNAVAILABLE);
                JsonNode root;
                try { root = JSON.readTree(response.body()); }
                catch (Exception invalid) { throw new Failure(Result.INVALID_RESPONSE); }
                if (!root.path("code").isIntegralNumber() || root.path("code").longValue() != 200 || !root.path("content").isObject())
                    throw new Failure(Result.INVALID_RESPONSE);
                return root.path("content");
            } catch (Failure failure) { throw failure; }
              catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new Failure(Result.SOURCE_UNAVAILABLE); }
              catch (ExecutionException failure) {
                  if (failure.getCause() instanceof Failure bounded) throw bounded;
                  throw new Failure(Result.SOURCE_UNAVAILABLE);
              }
              catch (Exception failure) { throw new Failure(Result.SOURCE_UNAVAILABLE); }
        }
    }

    private static JsonNode data(JsonNode content) {
        var data = content.path("data");
        if (!data.isArray() || data.size() > 20) throw new Failure(Result.INVALID_RESPONSE);
        return data;
    }
    private static String channel(JsonNode row) {
        String value = row.path("channelId").asText(""); BroadcastCatalog.requireChannel(value); return value;
    }
    private static long positiveLong(JsonNode node) {
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 1) throw new Failure(Result.INVALID_RESPONSE);
        return node.longValue();
    }
    private static Long optionalCount(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) return null;
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 0 || node.longValue() > Integer.MAX_VALUE)
            throw new Failure(Result.INVALID_RESPONSE);
        return node.longValue();
    }
    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    private static final class Failure extends RuntimeException {
        final Result result;
        Failure(Result result) { super(result.name(), null, false, false); this.result = result; }
    }
    private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
        Flow.Subscription subscription; int bytes; boolean failed;
        public CompletionStage<byte[]> getBody() { return delegate.getBody(); }
        public void onSubscribe(Flow.Subscription value) { subscription = value; delegate.onSubscribe(value); }
        public void onNext(List<ByteBuffer> buffers) {
            if (failed) return;
            for (var buffer : buffers) {
                bytes += buffer.remaining();
                if (bytes > MAX_BODY) { failed = true; subscription.cancel(); delegate.onError(new Failure(Result.INVALID_RESPONSE)); return; }
            }
            delegate.onNext(buffers);
        }
        public void onError(Throwable failure) { if (!failed) { failed = true; delegate.onError(new Failure(Result.SOURCE_UNAVAILABLE)); } }
        public void onComplete() { if (!failed) delegate.onComplete(); }
    }
}
