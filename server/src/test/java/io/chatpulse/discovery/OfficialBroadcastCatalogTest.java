package io.chatpulse.discovery;

import com.sun.net.httpserver.*;
import java.net.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static io.chatpulse.discovery.BroadcastCatalog.*;
import static io.chatpulse.discovery.SelectionPolicyTest.*;

class OfficialBroadcastCatalogTest {
    static final JsonMapper JSON = JsonMapper.builder().build();
    HttpServer server; ExecutorService executor;
    @AfterEach void close() { if(server!=null)server.stop(0);if(executor!=null)executor.shutdownNow(); }
    OfficialBroadcastCatalog source(HttpHandler handler) throws Exception {
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        executor=Executors.newVirtualThreadPerTaskExecutor();server.setExecutor(executor);server.createContext("/",handler);server.start();
        return new OfficialBroadcastCatalog(URI.create("http://127.0.0.1:"+server.getAddress().getPort()),"synthetic-client","synthetic-secret",Clock.fixed(NOW,ZoneOffset.UTC));
    }
    static Limits limits(int pages,int requests) { return new Limits(pages,requests,Duration.ofSeconds(5),Duration.ZERO); }
    static Map<String,Object> live(int n) {
        return Map.of("channelId",id(n),"liveId",n+1,"adult",false,"concurrentUserCount",0,
                "liveTitle","synthetic-private-title","channelName","synthetic-private-name");
    }
    static void respond(HttpExchange exchange,int status,Object content) throws java.io.IOException {
        byte[] bytes=JSON.writeValueAsBytes(Map.of("code",status,"content",content));exchange.sendResponseHeaders(status,bytes.length);
        try(var body=exchange.getResponseBody()){body.write(bytes);}
    }
    @Test void pagesBatchFollowersDuplicatesMissingValuesAndPrivacy() throws Exception {
        var count=new AtomicInteger();
        var source=source(exchange->{
            count.incrementAndGet();
            assertThat(exchange.getRequestHeaders().getFirst("Client-Secret")).isEqualTo("synthetic-secret");
            String query=exchange.getRequestURI().getRawQuery();
            if(exchange.getRequestURI().getPath().endsWith("lives")) {
                boolean next=query.contains("next=");
                respond(exchange,200,Map.of("data",next?List.of(live(1),live(2)):List.of(live(1)),"page",next?Map.of():Map.of("next","opaque+cursor")));
                if(next)assertThat(query).contains("next=opaque%2Bcursor");
            }else {
                assertThat(query).isEqualTo("channelIds="+id(1)+"&channelIds="+id(2));
                respond(exchange,200,Map.of("data",List.of(Map.of("channelId",id(1),"followerCount",0,"channelName","synthetic-private-name"))));
            }
        });
        var result=source.scan(limits(5,6));
        assertThat(result.result()).isEqualTo(Result.COMPLETE);assertThat(result.duplicateRows()).isEqualTo(1);
        assertThat(count.get()).isEqualTo(3);assertThat(result.candidates()).extracting(Candidate::followerCount).containsExactly(0L,null);
        String output=JSON.writeValueAsString(result);
        assertThat(output).doesNotContain("synthetic-private","synthetic-secret","opaque+cursor","channelName","liveTitle");
    }
    @Test void cursorLoopAndChangedBroadcastCannotProduceConfidentSelection() throws Exception {
        var count=new AtomicInteger();var source=source(exchange->{
            var row=new HashMap<>(live(1));row.put("liveId",count.incrementAndGet());
            respond(exchange,200,Map.of("data",List.of(row),"page",Map.of("next","repeat")));
        });
        var result=source.scan(limits(5,6));
        assertThat(result.result()).isEqualTo(Result.CURSOR_REPEAT);assertThat(count.get()).isEqualTo(2);
        assertThat(result.candidates().getFirst().ambiguousBroadcast()).isTrue();
        assertThat(policy(null,20,1).evaluate(result,NOW).selected()).isEmpty();
    }
    @Test void pageAndRequestBudgetsStopWithoutMoreSourceCalls() throws Exception {
        var source=source(exchange->respond(exchange,200,Map.of("data",List.of(live(1)),"page",Map.of("next","more"))));
        var first=source.scan(limits(1,8));assertThat(first.result()).isEqualTo(Result.PAGE_LIMIT);assertThat(first.requests()).isEqualTo(1);
        var second=source.scan(limits(8,1));assertThat(second.result()).isEqualTo(Result.REQUEST_LIMIT);assertThat(second.requests()).isEqualTo(1);
    }
    @Test void quotaKeepsPartialDiscoveryAndDoesNotRetryOrExposeBody() throws Exception {
        var count=new AtomicInteger();var source=source(exchange->{
            if(count.incrementAndGet()==1)respond(exchange,200,Map.of("data",List.of(live(1)),"page",Map.of()));
            else respond(exchange,429,Map.of("message","synthetic-private-error"));
        });
        var result=source.scan(limits(5,6));assertThat(result.result()).isEqualTo(Result.RATE_LIMITED);
        assertThat(result.candidates()).hasSize(1);assertThat(count.get()).isEqualTo(2);
        assertThat(JSON.writeValueAsString(result)).doesNotContain("synthetic-private");
    }
    @Test void missingPageIsAnInvalidResponse() throws Exception {
        var source=source(exchange->respond(exchange,200,Map.of("data",List.of(live(1)))));
        assertThat(source.scan(limits(5,6)).result()).isEqualTo(Result.INVALID_RESPONSE);
    }
    @Test void authorizationServerRedirectAndMalformedJsonRemainSafeFailures() throws Exception {
        var status=new AtomicInteger(401);var calls=new AtomicInteger();
        var source=source(exchange->{
            calls.incrementAndGet();exchange.getResponseHeaders().add("Location","http://127.0.0.1:1/synthetic-secret");
            if(status.get()!=200)respond(exchange,status.get(),Map.of("message","synthetic-private-error"));
            else {byte[] bytes="not-json-synthetic-private".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200,bytes.length);try(var body=exchange.getResponseBody()){body.write(bytes);}}
        });
        assertThat(source.scan(limits(5,6)).result()).isEqualTo(Result.AUTHORIZATION_FAILED);
        status.set(500);assertThat(source.scan(limits(5,6)).result()).isEqualTo(Result.SOURCE_UNAVAILABLE);
        status.set(302);assertThat(source.scan(limits(5,6)).result()).isEqualTo(Result.SOURCE_UNAVAILABLE);
        status.set(200);var result=source.scan(limits(5,6));assertThat(result.result()).isEqualTo(Result.INVALID_RESPONSE);
        assertThat(calls).hasValue(4);assertThat(JSON.writeValueAsString(result)).doesNotContain("synthetic-private","synthetic-secret");
    }
    @Test void slowBodyIsBoundedByScanDeadline() throws Exception {
        var source=source(exchange->{
            exchange.sendResponseHeaders(200,100);
            try{Thread.sleep(2000);}catch(InterruptedException e){Thread.currentThread().interrupt();}
            exchange.close();
        });
        long start=System.nanoTime();
        var result=source.scan(new Limits(5,6,Duration.ofSeconds(1),Duration.ZERO));
        assertThat(result.result()).isIn(Result.DEADLINE,Result.SOURCE_UNAVAILABLE);
        assertThat((System.nanoTime()-start)/1e9).isLessThan(3);
    }
    @Test void oversizedResponseIsStoppedBeforeJsonParsing() throws Exception {
        var source=source(exchange->{
            byte[] bytes=new byte[1_048_577];Arrays.fill(bytes,(byte)' ');exchange.sendResponseHeaders(200,bytes.length);
            try(var body=exchange.getResponseBody()){body.write(bytes);}catch(java.io.IOException ignored){}
        });
        assertThat(source.scan(limits(5,6)).complete()).isFalse();
    }
    @Test void missingCredentialsAreRejectedBeforeHttp() {
        assertThatThrownBy(()->new OfficialBroadcastCatalog(null,null)).isInstanceOf(IllegalArgumentException.class).hasMessage("CLIENT_CREDENTIALS_REQUIRED");
    }
}
