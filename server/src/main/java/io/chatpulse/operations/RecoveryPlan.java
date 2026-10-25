package io.chatpulse.operations;

import io.chatpulse.pipeline.KafkaSupport;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;

/** DB 복원 위치·원천 identity·retention·Kafka commit을 consumer 시작 전에 대조한다. */
public final class RecoveryPlan implements AutoCloseable {
    private record Target(String group,String table,String projection,List<String> topics) { }
    private final JdbcTemplate jdbc;private final RuntimeEnvironment config;private final Admin admin;
    public RecoveryPlan(JdbcTemplate jdbc,RuntimeEnvironment config) {
        if(!config.group().matches("[a-zA-Z0-9._-]{1,80}")||!config.projection().matches("[a-zA-Z0-9_-]{1,30}")
                ||!config.runId().matches("[a-zA-Z0-9_-]{1,80}"))throw new IllegalArgumentException("RECOVERY_CONFIGURATION");
        this.jdbc=jdbc;this.config=config;
        admin=Admin.create(Map.of("bootstrap.servers",config.bootstrap(),"request.timeout.ms",3000,"default.api.timeout.ms",5000));
    }
    private List<Target> targets() {
        var chat=List.of(config.topic());var observation=List.of(config.observationsTopic());var both=List.of(config.topic(),config.observationsTopic());
        return List.of(new Target(config.group(),"projection_progress",config.projection(),chat),
                new Target(config.group()+"-trend","projection_progress",config.projection()+"-trend",chat),
                new Target(config.group()+"-observations","observation_progress",null,observation),
                new Target(config.group()+"-notifications","notification_progress",null,List.of(config.notificationsTopic())),
                new Target(config.group()+"-categories","category_progress",null,both),
                new Target(config.group()+"-reports","report_progress",null,both));
    }
    public List<String> topics(){return List.of(config.topic(),config.topic()+".dlq",config.observationsTopic(),config.notificationsTopic());}
    /** 명시적인 관리 작업. 기존 identity는 덮어쓰지 않는다. */
    public void bindSources() {
        try {
            for(String topic:topics())KafkaSupport.ensureTopic(config.bootstrap(),topic,config.policy());
            String cluster=admin.describeCluster().clusterId().get(5,TimeUnit.SECONDS);
            var descriptions=admin.describeTopics(topics()).allTopicNames().get(5,TimeUnit.SECONDS);
            for(String topic:topics()) {
                var d=descriptions.get(topic);
                jdbc.update("INSERT INTO deployment_source_identity(run_id,topic,cluster_id,topic_id,partitions) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                        config.runId(),topic,cluster,d.topicId().toString(),d.partitions().size());
            }
            verifyIdentities(cluster,descriptions);
        } catch(Exception failure){throw new IllegalStateException("SOURCE_BIND_FAILED",failure);}
    }
    private void verifyIdentities(String cluster,Map<String,TopicDescription> descriptions) {
        for(var e:descriptions.entrySet()) {
            Integer matches=jdbc.queryForObject("SELECT count(*) FROM deployment_source_identity WHERE run_id=? AND topic=? AND cluster_id=? AND topic_id=? AND partitions=?",
                    Integer.class,config.runId(),e.getKey(),cluster,e.getValue().topicId().toString(),e.getValue().partitions().size());
            if(matches!=1)throw new IllegalStateException("SOURCE_IDENTITY_MISMATCH");
        }
    }
    public Map<String,Object> inspect() {
        try {
            String cluster=admin.describeCluster().clusterId().get(5,TimeUnit.SECONDS);
            var descriptions=admin.describeTopics(topics()).allTopicNames().get(5,TimeUnit.SECONDS);verifyIdentities(cluster,descriptions);
            var first=new HashMap<TopicPartition,OffsetSpec>();var last=new HashMap<TopicPartition,OffsetSpec>();
            descriptions.forEach((topic,d)->d.partitions().forEach(p->{var tp=new TopicPartition(topic,p.partition());first.put(tp,OffsetSpec.earliest());last.put(tp,OffsetSpec.latest());}));
            var starts=admin.listOffsets(first).all().get(5,TimeUnit.SECONDS);var ends=admin.listOffsets(last).all().get(5,TimeUnit.SECONDS);
            var rows=new ArrayList<Map<String,Object>>();boolean safe=true,recoverable=true;
            for(Target target:targets()) {
                var offsets=admin.listConsumerGroupOffsets(target.group()).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
                for(String topic:target.topics())for(var partition:descriptions.get(topic).partitions()) {
                    var tp=new TopicPartition(topic,partition.partition());long db=databaseNext(target,tp),begin=starts.get(tp).offset(),end=ends.get(tp).offset();
                    Long committed=offsets.containsKey(tp)?offsets.get(tp).offset():null;String code;
                    if(db<begin)code="RETENTION_GAP";else if(db>end)code="DATABASE_AHEAD_SOURCE";
                    else if(committed==null)code="MISSING_GROUP_OFFSET";else if(committed>db)code="KAFKA_AHEAD_DATABASE";
                    else if(committed<begin)code="COMMITTED_OFFSET_EXPIRED";else code=committed==db?"ALIGNED":"SAFE_OVERLAP";
                    safe&=Set.of("ALIGNED","SAFE_OVERLAP").contains(code);recoverable&=db>=begin&&db<=end;
                    var row=new LinkedHashMap<String,Object>();row.put("group",target.group());row.put("topic",topic);row.put("partition",partition.partition());
                    row.put("begin",begin);row.put("end",end);row.put("databaseNext",db);row.put("committed",committed);row.put("code",code);rows.add(row);
                }
            }
            return Map.of("clusterId",cluster,"safeForStartup",safe,"recoverableFromRetainedLog",recoverable,"partitions",rows);
        } catch(Exception failure){throw new IllegalStateException("RECOVERY_INSPECTION_FAILED",failure);}
    }
    private long databaseNext(Target target,TopicPartition tp) {
        String column=target.projection()==null?"next_offset":"next_resolved_offset";
        var args=new ArrayList<Object>(List.of(config.runId(),tp.topic(),tp.partition()));if(target.projection()!=null)args.add(target.projection());
        return jdbc.queryForObject("SELECT coalesce(max("+column+"),0) FROM "+target.table()+" WHERE run_id=? AND topic=? AND partition=?"+(target.projection()==null?"":" AND projection=?"),Long.class,args.toArray());
    }
    public void requireSafeStartup(){if(!Boolean.TRUE.equals(inspect().get("safeForStartup")))throw new IllegalStateException("RECOVERY_ALIGNMENT_REQUIRED");}
    /** 정지한 환경의 새 group에만 DB 위치를 설정한다. 기존 group offset을 고치지 않는다. */
    @SuppressWarnings("unchecked")
    public Map<String,Object> prepareNewGroups(String newPrefix,boolean initialize) {
        if(!newPrefix.matches("[a-zA-Z0-9._-]{1,80}")||(!initialize&&newPrefix.equals(config.group())))throw new IllegalArgumentException("RECOVERY_NEW_GROUP_REQUIRED");
        var plan=inspect();if(!Boolean.TRUE.equals(plan.get("recoverableFromRetainedLog")))throw new IllegalStateException("RECOVERY_RETENTION_GAP");
        try {
            var requests=new LinkedHashMap<String,Map<TopicPartition,OffsetAndMetadata>>();
            for(Target target:targets()) {
                // 원래 group도 정지해야 복원 환경과 병행하여 같은 projection을 변경하지 않는다.
                try{var old=admin.describeConsumerGroups(List.of(target.group())).all().get(5,TimeUnit.SECONDS);if(!old.get(target.group()).members().isEmpty())throw new IllegalStateException("RECOVERY_GROUP_ACTIVE");}
                catch(java.util.concurrent.ExecutionException e){if(!(e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException))throw e;}
                String group=newPrefix+target.group().substring(config.group().length());
                if(!admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS).isEmpty())throw new IllegalStateException("RECOVERY_GROUP_EXISTS");
                try{var existing=admin.describeConsumerGroups(List.of(group)).all().get(5,TimeUnit.SECONDS);if(!existing.get(group).members().isEmpty())throw new IllegalStateException("RECOVERY_GROUP_ACTIVE");}
                catch(java.util.concurrent.ExecutionException e){if(!(e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException))throw e;}
                var offsets=new HashMap<TopicPartition,OffsetAndMetadata>();
                for(var row:(List<Map<String,Object>>)plan.get("partitions"))if(row.get("group").equals(target.group())) {
                    long db=((Number)row.get("databaseNext")).longValue();if(initialize&&db!=0)throw new IllegalStateException("INITIALIZE_REQUIRES_EMPTY_DATABASE");
                    offsets.put(new TopicPartition((String)row.get("topic"),((Number)row.get("partition")).intValue()),new OffsetAndMetadata(db));
                }
                requests.put(group,offsets);
            }
            for(var request:requests.entrySet())admin.alterConsumerGroupOffsets(request.getKey(),request.getValue()).all().get(5,TimeUnit.SECONDS);
            return Map.of("newGroupPrefix",newPrefix,"groups",requests.size(),"offsetSource","RESTORED_DATABASE_PROGRESS","existingGroupsModified",false);
        } catch(Exception failure){throw new IllegalStateException("RECOVERY_GROUP_PREPARATION_FAILED",failure);}
    }
    @Override public void close(){admin.close(Duration.ofSeconds(1));}
}
