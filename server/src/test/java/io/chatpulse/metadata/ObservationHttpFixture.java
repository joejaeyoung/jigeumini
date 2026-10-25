package io.chatpulse.metadata;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/** 외부 플랫폼을 호출하지 않는 실제 HTTP 관측 fixture. */
public final class ObservationHttpFixture implements AutoCloseable {
    private final HttpServer server;
    public final AtomicInteger requests = new AtomicInteger();
    public ObservationHttpFixture(String channel) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/service/v2/channels/"+channel+"/live-detail", exchange -> {
            requests.incrementAndGet();
            byte[] body=("{\"code\":200,\"content\":{\"adult\":false,\"status\":\"OPEN\",\"concurrentUserCount\":73,\"liveId\":123,\"channel\":{\"channelId\":\""+channel+"\"}}}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type","application/json"); exchange.sendResponseHeaders(200,body.length);
            try(var out=exchange.getResponseBody()) { out.write(body); }
        });
        server.start();
    }
    public PublicObservationSource source() { return new ChzzkMetadataSource("http://127.0.0.1:"+server.getAddress().getPort()); }
    @Override public void close() { server.stop(0); }
}
