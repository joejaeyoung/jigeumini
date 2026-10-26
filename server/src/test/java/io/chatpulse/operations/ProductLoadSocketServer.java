package io.chatpulse.operations;

import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import tools.jackson.databind.json.JsonMapper;

/** collector와 다른 컨테이너에서 실행하는 합성 원천. HTTP 상태/토큰과 READ/heartbeat만 제공한다. */
public final class ProductLoadSocketServer {
    private final JsonMapper json=JsonMapper.builder().build();
    private final ProductLoadPlan plan;
    private final Path output;
    private final ProductLoadFixture fixture;
    private final ConcurrentMap<String,WebSocket> connections=new ConcurrentHashMap<>();
    private final ConcurrentMap<String,LongAdder> sent=new ConcurrentHashMap<>();
    private final AtomicLong startAt=new AtomicLong();
    private final AtomicInteger peak=new AtomicInteger(),protocolErrors=new AtomicInteger(),socketErrors=new AtomicInteger(),duplicates=new AtomicInteger();
    private final LongAdder statusRequests=new LongAdder(),tokenRequests=new LongAdder(),pings=new LongAdder();
    private volatile boolean finished;
    private volatile String failure="NONE";
    private volatile double maximumPacingDelay;
    private volatile double actualSeconds;
    private volatile long wireBytes;
    private final ExecutorService generator=Executors.newSingleThreadExecutor();
    private ProductLoadSocketServer(ProductLoadPlan plan,Path output){this.plan=plan;this.output=output;fixture=new ProductLoadFixture(plan.channels());}
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        new ProductLoadSocketServer(ProductLoadPlan.read(args[0]),Path.of(args[1])).serve();
    }
    private void serve() throws Exception {
        var socket=new WebSocketServer(new InetSocketAddress("0.0.0.0",18085),1) {
            @Override public void onOpen(WebSocket connection,ClientHandshake request) { }
            @Override public void onStart() { }
            @Override public void onError(WebSocket connection,Exception error){socketErrors.incrementAndGet();}
            @Override public void onClose(WebSocket connection,int code,String reason,boolean remote){String channel=connection.getAttachment();if(channel!=null)connections.remove(channel,connection);}
            @Override public void onMessage(WebSocket connection,String text) {
                try {
                    var message=json.readTree(text);int command=message.path("cmd").asInt();
                    if(command==100) {
                        String channel=message.path("cid").asText();
                        if(!fixture.categories.containsKey(channel)||!"READ".equals(message.path("bdy").path("auth").asText())||!message.path("bdy").path("uid").isNull())throw new IllegalArgumentException();
                        if(connections.putIfAbsent(channel,connection)!=null){duplicates.incrementAndGet();connection.close();return;}
                        connection.setAttachment(channel);peak.accumulateAndGet(connections.size(),Math::max);
                        connection.send("{\"cmd\":10100,\"retCode\":0,\"bdy\":{\"sid\":\"synthetic-session\"}}");
                        connection.send("{\"cmd\":0,\"ver\":\"2\"}");
                    }else if(command==0){pings.increment();connection.send("{\"cmd\":10000,\"ver\":\"2\"}");}
                    else if(command!=10000)throw new IllegalArgumentException();
                }catch(Exception error){protocolErrors.incrementAndGet();connection.close();}
            }
        };
        socket.setConnectionLostTimeout(0);socket.start();
        var http=HttpServer.create(new InetSocketAddress("0.0.0.0",18086),128);
        http.setExecutor(Executors.newFixedThreadPool(8));
        http.createContext("/stats",exchange->respond(exchange,200,snapshot()));
        http.createContext("/start",exchange->{
            try {
                if(!exchange.getRequestMethod().equals("POST"))throw new IllegalArgumentException();
                long at=Long.parseLong(new String(exchange.getRequestBody().readNBytes(32),StandardCharsets.US_ASCII));
                long wait=at-System.currentTimeMillis();
                if(wait<500||wait>10000||connections.size()!=plan.channels()||!startAt.compareAndSet(0,at))throw new IllegalArgumentException();
                generator.submit(()->generate(at));respond(exchange,200,Map.of("startAtEpochMillis",at,"serverNowEpochMillis",System.currentTimeMillis()));
            }catch(Exception failure){respond(exchange,409,Map.of("error","SOURCE_START_REJECTED"));}
        });
        http.createContext("/polling/v2/channels/",exchange->{
            String path=exchange.getRequestURI().getPath();String channel=path.replace("/polling/v2/channels/","").replace("/live-status","");
            if(!fixture.categories.containsKey(channel)){respond(exchange,404,Map.of("code",404));return;}
            statusRequests.increment();respond(exchange,200,Map.of("code",200,"content",Map.of("adult",false,"status","OPEN","chatChannelId",channel)));
        });
        http.createContext("/nng_main/v1/chats/access-token",exchange->{tokenRequests.increment();respond(exchange,200,Map.of("code",200,"content",Map.of("accessToken","TEST_ONLY_TOKEN")));});
        http.start();
        var snapshots=Executors.newSingleThreadScheduledExecutor();
        snapshots.scheduleWithFixedDelay(()->{try{Path temp=output.resolveSibling(output.getFileName()+".tmp");Files.writeString(temp,json.writeValueAsString(snapshot()));Files.move(temp,output,StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);}catch(Exception error){failure="SOURCE_RESULT_WRITE_FAILED";}},0,1,TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{http.stop(0);generator.shutdownNow();snapshots.shutdownNow();try{socket.stop(1000);}catch(Exception ignored){}}));
        new CountDownLatch(1).await();
    }
    private void generate(long epochMillis) {
        String chatText=plan.syntheticChatText();
        long start=System.nanoTime()+(epochMillis-System.currentTimeMillis())*1_000_000L,ordinal=0;int elapsedSeconds=0;
        try {
            for(var stage:plan.stages())for(int second=0;second<stage.seconds();second++,elapsedSeconds++)for(int tick=0;tick<10;tick++) {
                long due=start+elapsedSeconds*1_000_000_000L+tick*100_000_000L;pace(due);
                maximumPacingDelay=Math.max(maximumPacingDelay,(System.nanoTime()-due)/1e6);
                if(maximumPacingDelay>10000)throw new IllegalStateException("SOURCE_TOO_SLOW");
                var frames=new LinkedHashMap<String,List<Map<String,Object>>>();
                for(int i=0;i<stage.eventsPerSecond()/10;i++) {
                    String channel=fixture.channel(ordinal++,stage.hot());
                    frames.computeIfAbsent(channel,ignored->new ArrayList<>()).add(Map.of("msgTypeCode",1,"msgTime",epochMillis-2000+elapsedSeconds*1000L+tick*100L,"msg",chatText));
                }
                for(var entry:frames.entrySet()) {
                    var connection=connections.get(entry.getKey());
                    if(connection==null||!connection.isOpen())throw new IllegalStateException("SOURCE_CONNECTION_MISSING");
                    String frame=json.writeValueAsString(Map.of("cmd",93101,"bdy",entry.getValue()));
                    connection.send(frame);sent.computeIfAbsent(entry.getKey(),ignored->new LongAdder()).add(entry.getValue().size());wireBytes+=frame.getBytes(StandardCharsets.UTF_8).length;
                }
            }
            pace(start+plan.seconds()*1_000_000_000L);actualSeconds=(System.nanoTime()-start)/1e9;
        }catch(Exception error){failure=error.getMessage()!=null&&error.getMessage().matches("[A-Z_]+")?error.getMessage():"SOURCE_FAILURE";}
        finally{finished=true;}
    }
    private static void pace(long due){long left;while((left=due-System.nanoTime())>0){LockSupport.parkNanos(Math.min(left,100_000_000));if(Thread.currentThread().isInterrupted())throw new IllegalStateException("SOURCE_INTERRUPTED");}}
    private Map<String,Object> snapshot() {
        var result=new LinkedHashMap<String,Object>();result.put("at",Instant.now().toString());result.put("connections",connections.size());result.put("peakConnections",peak.get());
        result.put("sentByChannel",sent.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().sum())));
        result.put("sent",sent.values().stream().mapToLong(LongAdder::sum).sum());result.put("wireBytes",wireBytes);
        result.put("finished",finished);result.put("failure",failure);result.put("maximumPacingDelayMillis",maximumPacingDelay);result.put("actualSeconds",actualSeconds);
        result.put("protocolErrors",protocolErrors.get());result.put("socketErrors",socketErrors.get());result.put("duplicateConnections",duplicates.get());
        result.put("statusRequests",statusRequests.sum());result.put("tokenRequests",tokenRequests.sum());result.put("clientPings",pings.sum());
        result.put("bufferedConnections",connections.values().stream().filter(WebSocket::hasBufferedData).count());return result;
    }
    private void respond(HttpExchange exchange,int status,Object value) throws java.io.IOException {
        try(exchange){byte[] bytes=json.writeValueAsBytes(value);exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(status,bytes.length);exchange.getResponseBody().write(bytes);}
    }
}
