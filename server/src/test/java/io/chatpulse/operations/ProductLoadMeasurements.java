package io.chatpulse.operations;

import io.chatpulse.pipeline.BufferedKafkaPublisher;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;

/** 부하 생성과 독립된 표본. 관측 실패를 0으로 바꾸지 않고 유형과 소요 시간을 남긴다. */
final class ProductLoadMeasurements implements AutoCloseable {
    private final RuntimeEnvironment config;
    private final JdbcTemplate jdbc;
    private final BufferedKafkaPublisher publisher;
    private final Admin admin;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final LinkedHashMap<String,List<String>> groups=new LinkedHashMap<>();
    private final List<String> paths;
    private final Map<String,Long> previous=new HashMap<>();
    private long previousAt=System.nanoTime();
    private JMXConnector jmx;
    ProductLoadMeasurements(RuntimeEnvironment config,JdbcTemplate jdbc,BufferedKafkaPublisher publisher,String channel) {
        this.config=config;this.jdbc=jdbc;this.publisher=publisher;
        admin=Admin.create(Map.of("bootstrap.servers",config.bootstrap(),"default.api.timeout.ms",3000,"request.timeout.ms",2000));
        groups.put(config.group(),List.of(config.topic()));
        groups.put(config.group()+"-trend",List.of(config.topic()));
        groups.put(config.group()+"-observations",List.of(config.observationsTopic()));
        groups.put(config.group()+"-categories",List.of(config.topic(),config.observationsTopic()));
        groups.put(config.group()+"-reports",List.of(config.topic(),config.observationsTopic()));
        groups.put(config.group()+"-notifications",List.of(config.notificationsTopic()));
        paths=List.of("/api/status","/api/v1/rankings","/api/v1/spikes","/api/v1/categories","/api/v1/channels/"+channel+"/timeline",
                "/api/v1/channels/"+channel+"/reports","/api/v1/notifications?channels="+channel,"/readyz");
    }
    Map<String,Object> sample(String phase,double elapsed) {
        long started=System.nanoTime();var out=new LinkedHashMap<String,Object>();
        out.put("at",Instant.now());out.put("phase",phase);out.put("elapsedSeconds",elapsed);
        out.put("publisherChannels",publisher.channelSnapshot());
        var channels=publisher.channelSnapshot().values();long accepted=0,ack=0,rejected=0;int pending=0;long oldest=0;
        for(var c:channels) {accepted+=c.accepted();ack+=c.acknowledged();rejected+=c.rejected();pending+=c.pending();if(c.oldest()!=null)oldest=Math.max(oldest,Duration.between(c.oldest(),Instant.now()).toMillis());}
        out.put("publisher",Map.of("attempted",accepted+rejected,"accepted",accepted,"acknowledged",ack,"rejected",rejected,"unconfirmed",accepted-ack,"pending",pending,"oldestAgeMillis",oldest));
        try {out.put("partitions",partitions());}catch(Exception error){out.put("kafkaError",error.getClass().getSimpleName());}
        long dbStart=System.nanoTime();
        try {out.put("database",counts());}catch(RuntimeException error){out.put("databaseError",error.getClass().getSimpleName());}
        out.put("databaseQueryMillis",(System.nanoTime()-dbStart)/1e6);
        var responses=new ArrayList<Map<String,Object>>();
        for(String path:paths) {
            long begin=System.nanoTime();var value=new LinkedHashMap<String,Object>();value.put("path",path);
            try {value.put("status",http.send(HttpRequest.newBuilder(URI.create("http://api:8080"+path)).timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.discarding()).statusCode());}
            catch(Exception error){value.put("error",error.getClass().getSimpleName());}
            value.put("millis",(System.nanoTime()-begin)/1e6);responses.add(value);
        }
        out.put("api",responses);
        out.put("generatorHeapUsedBytes",ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        try {out.put("generatorRssBytes",rss());}catch(Exception e){out.put("generatorRssError",e.getClass().getSimpleName());}
        try {
            if(jmx==null)jmx=JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://api:9010/jmxrmi"));
            var memory=(CompositeData)jmx.getMBeanServerConnection().getAttribute(new ObjectName("java.lang:type=Memory"),"HeapMemoryUsage");
            out.put("analyzerHeapUsedBytes",memory.get("used"));out.put("analyzerHeapMaxBytes",memory.get("max"));
        }catch(Exception error){out.put("analyzerHeapError",error.getClass().getSimpleName());try{if(jmx!=null)jmx.close();}catch(Exception ignored){}jmx=null;}
        out.put("sampleSeconds",(System.nanoTime()-started)/1e9);return out;
    }
    synchronized List<Map<String,Object>> partitions() throws Exception {
        var specifications=new HashMap<TopicPartition,OffsetSpec>();
        for(String topic:List.of(config.topic(),config.observationsTopic(),config.notificationsTopic()))for(int p=0;p<3;p++)specifications.put(new TopicPartition(topic,p),OffsetSpec.latest());
        var ends=admin.listOffsets(specifications).all().get(4,TimeUnit.SECONDS);
        specifications.replaceAll((p,v)->OffsetSpec.earliest());
        var begins=admin.listOffsets(specifications).all().get(4,TimeUnit.SECONDS);
        var values=new ArrayList<Map<String,Object>>();long now=System.nanoTime();double seconds=(now-previousAt)/1e9;
        for(var group:groups.entrySet()) {
            var committed=admin.listConsumerGroupOffsets(group.getKey()).partitionsToOffsetAndMetadata().get(4,TimeUnit.SECONDS);
            for(String topic:group.getValue())for(int p=0;p<3;p++) {
                var tp=new TopicPartition(topic,p);var value=new LinkedHashMap<String,Object>();
                value.put("group",group.getKey());value.put("topic",topic);value.put("partition",p);value.put("begin",begins.get(tp).offset());value.put("end",ends.get(tp).offset());
                var offset=committed.get(tp);String key=group.getKey()+"/"+topic+"/"+p;
                value.put("committed",offset==null?null:offset.offset());value.put("lag",offset==null?null:ends.get(tp).offset()-offset.offset());
                if(offset!=null) {Long prior=previous.put(key,offset.offset());value.put("committedRecordsPerSecond",prior==null?null:(offset.offset()-prior)/seconds);}
                values.add(value);
            }
        }
        previousAt=now;return values;
    }
    Map<String,Long> counts() {
        var counts=new LinkedHashMap<String,Long>();
        for(String projection:List.of(config.projection(),config.projection()+"-trend"))counts.put(projection,jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE run_id=? AND projection=?",Long.class,config.runId(),projection));
        for(String table:List.of("category_event","report_event","broadcast_observation","notification_event"))counts.put(table,jdbc.queryForObject("SELECT count(*) FROM "+table+" WHERE run_id=?",Long.class,config.runId()));
        counts.put("pendingOutbox",jdbc.queryForObject("SELECT count(*) FROM spike_outbox WHERE run_id=? AND published_at IS NULL",Long.class,config.runId()));
        return counts;
    }
    Map<String,Object> transactionTimestampLatencies() {
        var result=new LinkedHashMap<String,Object>();
        for(String projection:List.of(config.projection(),config.projection()+"-trend"))result.put(projection,jdbc.queryForMap("SELECT count(*) AS records, percentile_cont(0.5) WITHIN GROUP(ORDER BY extract(epoch FROM processed_at-received_at)*1000) AS p50, percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM processed_at-received_at)*1000) AS p95, percentile_cont(0.99) WITHIN GROUP(ORDER BY extract(epoch FROM processed_at-received_at)*1000) AS p99, max(extract(epoch FROM processed_at-received_at)*1000) AS maximum FROM processed_event WHERE run_id=? AND projection=?",config.runId(),projection));
        return result;
    }
    private static long rss() throws Exception {
        for(String line:Files.readAllLines(Path.of("/proc/self/status")))if(line.startsWith("VmRSS:"))return Long.parseLong(line.trim().split("\\s+")[1])*1024;
        throw new IllegalStateException("RSS_UNAVAILABLE");
    }
    @Override public void close() {admin.close(Duration.ofSeconds(3));http.close();try{if(jmx!=null)jmx.close();}catch(Exception ignored){}}
}
