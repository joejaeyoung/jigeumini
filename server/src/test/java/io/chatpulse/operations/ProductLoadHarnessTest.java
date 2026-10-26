package io.chatpulse.operations;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProductLoadHarnessTest {
    @Test void channelCountChangesUniformAndHotInputWithoutChangingHotShare() {
        for(int size:List.of(2,20,137,1000)) {
            var fixture=new ProductLoadFixture(size);var uniform=new HashMap<String,Integer>();var hot=new HashMap<String,Integer>();
            for(int i=0;i<size*100;i++) {uniform.merge(fixture.channel(i,false),1,Integer::sum);hot.merge(fixture.channel(i,true),1,Integer::sum);}
            assertEquals(size,uniform.size());assertTrue(uniform.values().stream().allMatch(n->n==100));
            assertEquals(size*80,hot.get(fixture.channels.get(0)));assertEquals(size,hot.size());
        }
    }
    @Test void histogramUsesUpperBoundsAndNeverClampsOverflowToSuccess() {
        var latency=new ProductLoadLatency();assertNull(latency.snapshot().get("p95UpperMillis"));
        for(int i=0;i<100;i++)latency.add(10.1);assertEquals(11,latency.snapshot().get("p95UpperMillis"));
        for(int i=0;i<10;i++)latency.add(12000);assertNull(latency.snapshot().get("p95UpperMillis"));
        assertEquals(10L,latency.snapshot().get("above10000Millis"));assertThrows(IllegalArgumentException.class,()->latency.add(Double.NaN));
    }
    @Test @SuppressWarnings("unchecked") void slowHttpKeepsOfferingRequestsAndCountsConcurrencyDrops() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var workers=Executors.newCachedThreadPool();
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);server.setExecutor(workers);
        server.createContext("/",exchange->{try(exchange){entered.countDown();try{release.await(3,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}exchange.sendResponseHeaders(503,-1);}});server.start();
        var plan=new ProductLoadPlan("test",2,List.of(new ProductLoadPlan.Stage("steady",30,10,false)),-1,0,"DIRECT",50,1,"BASELINE");
        try(var api=new ProductLoadApiTraffic(plan,List.of("synthetic"),()->"steady","http://127.0.0.1:"+server.getAddress().getPort())) {
            api.start();assertTrue(entered.await(2,TimeUnit.SECONDS));
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);Map<String,Object> phase;
            do {phase=(Map<String,Object>)((Map<?,?>)api.snapshot().get("phases")).get("steady");if(((Number)phase.get("notIssuedConcurrentLimit")).longValue()>=5)break;Thread.sleep(10);}while(System.nanoTime()<deadline);
            assertTrue(((Number)phase.get("notIssuedConcurrentLimit")).longValue()>=5);assertEquals(1L,phase.get("issued"));
            release.countDown();api.close();phase=(Map<String,Object>)((Map<?,?>)api.snapshot().get("phases")).get("steady");
            assertEquals(0L,phase.get("successful"));assertTrue(((Number)phase.get("failed")).longValue()>=1);
        }finally{release.countDown();server.stop(0);workers.shutdownNow();}
    }
    @Test void syntheticSourceCannotPointToPublicService() {
        assertThrows(IllegalArgumentException.class,()->io.chatpulse.poc.ProductLoadHttpSource.local(URI.create("https://api.chzzk.naver.com"),URI.create("wss://chat.naver.com")));
        assertThrows(IllegalArgumentException.class,()->io.chatpulse.poc.ProductLoadHttpSource.local(URI.create("http://127.0.0.1"),URI.create("ws://remote.example")));
    }
    @Test void eventAndConnectionBudgetsRemainFinite() {
        assertThrows(IllegalArgumentException.class,()->new ProductLoadPlan("test",1001,List.of(new ProductLoadPlan.Stage("steady",30,10,false)),-1,0,"DIRECT",10,32,"BASELINE"));
        assertThrows(IllegalArgumentException.class,()->new ProductLoadPlan("test",20,List.of(new ProductLoadPlan.Stage("steady",1800,10000,false)),-1,0,"DIRECT",10,32,"BASELINE"));
        assertEquals(90000,ProductLoadPlan.named("soak").uniqueEvents());
    }
}
