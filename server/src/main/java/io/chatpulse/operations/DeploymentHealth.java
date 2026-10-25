package io.chatpulse.operations;

import io.chatpulse.pipeline.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.Admin;

/** 유한 단일 계측 작업의 snapshot만 HTTP에 반환한다. 외부 장애는 liveness를 죽이지 않는다. */
public final class DeploymentHealth implements AutoCloseable {
    private final DataSource ds;private final Admin admin;private final KafkaTopicPolicy policy;private final List<String> topics;
    private final Map<String,Supplier<Map<String,Object>>> consumers;private final Supplier<Boolean> pollFresh;
    private final ScheduledExecutorService worker=Executors.newSingleThreadScheduledExecutor();
    private volatile Map<String,Object> snapshot=Map.of("ready",false,"code","STARTING");private volatile long sampled;private volatile boolean draining;
    public DeploymentHealth(DataSource ds,String bootstrap,KafkaTopicPolicy policy,List<String> topics,
            Map<String,Supplier<Map<String,Object>>> consumers,Supplier<Boolean> pollFresh) {
        this.ds=ds;this.policy=policy;this.topics=List.copyOf(topics);this.consumers=Map.copyOf(consumers);this.pollFresh=pollFresh;
        admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"request.timeout.ms",2000,"default.api.timeout.ms",3000));
    }
    public void start(){worker.scheduleWithFixedDelay(this::sample,0,2,TimeUnit.SECONDS);}
    private void sample() {
        boolean database=false,kafka=false,processing=true;
        try(var connection=ds.getConnection();var query=connection.createStatement()) {query.setQueryTimeout(2);try(var rows=query.executeQuery("SELECT 1")){database=rows.next();}}
        catch(Exception ignored) { }
        try {
            var descriptions=admin.describeTopics(topics).allTopicNames().get(3,TimeUnit.SECONDS);
            kafka=descriptions.values().stream().allMatch(d->d.partitions().size()==policy.partitions()&&d.partitions().stream().allMatch(p->p.leader()!=null&&p.leader().id()>=0&&p.replicas().size()==policy.replicationFactor()&&p.isr().size()>=policy.minInSyncReplicas()));
        } catch(Exception ignored) { }
        var states=new TreeMap<String,String>();
        try {
            for(var item:consumers.entrySet()) {
                var s=item.getValue().get();String state=String.valueOf(s.get("state"));states.put(item.getKey(),state);
                processing&=Boolean.TRUE.equals(s.get("running"))&&((Number)s.get("assignedPartitions")).intValue()>0&&Set.of("RUNNING","WAITING_FOR_EVENTS").contains(state);
            }
            processing&=pollFresh.get();
        }catch(RuntimeException ignored){processing=false;}
        boolean ready=database&&kafka&&processing&&!draining;
        snapshot=Map.of("ready",ready,"database",database,"kafka",kafka,"consumers",states,"processing",processing,
                "observedAt",Instant.now().toString(),"code",ready?"READY":"DEPENDENCY_OR_CONSUMER_UNAVAILABLE");sampled=System.nanoTime();
    }
    public Map<String,Object> status() {
        if(draining)return Map.of("ready",false,"code","DRAINING");
        if(sampled==0||System.nanoTime()-sampled>Duration.ofSeconds(12).toNanos())return Map.of("ready",false,"code","HEALTH_SAMPLE_STALE");
        return snapshot;
    }
    public void draining(){draining=true;}
    @Override public void close(){draining=true;worker.shutdownNow();admin.close(Duration.ofSeconds(1));}
}
