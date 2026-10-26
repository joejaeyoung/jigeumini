package io.chatpulse.operations;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.poc.ChatEvent;
import io.chatpulse.pipeline.*;
import io.chatpulse.observations.BroadcastObserved;
import io.chatpulse.metadata.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** 로컬 이미지 리허설 전용. 프로덕션 이미지에 포함하지 않고 합성 클래스만 read-only mount한다. */
public final class DeploymentProbe {
    private static final String CHANNEL="a".repeat(32);
    public static void main(String[] args) throws Exception {
        if(org.slf4j.LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext c)c.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.OFF);
        var config=RuntimeEnvironment.from(System.getenv());
        try(var ds=PostgresSupport.open(config.jdbcUrl(),config.user(),config.password(),false)) {
            var jdbc=new JdbcTemplate(ds);Object result;
            if(args[0].equals("seed")) {
                int batch=Integer.parseInt(args[1]);if(batch<0||batch>1)throw new IllegalArgumentException("SYNTHETIC_BATCH");
                Instant base=Instant.parse(args[2]);var factory=KafkaSupport.producerFactory(config.bootstrap());var kafka=new KafkaTemplate<>(factory);kafka.setProducerListener(null);
                try {
                    Instant at=base.plusSeconds(batch*30L);
                    var observation=new BroadcastObserved(1,id("observation/"+batch),config.runId(),CHANNEL,at,120,100L+batch,
                            BroadcastMetadata.LiveStatus.LIVE,PublicBroadcastObservation.Quality.OBSERVED,"12345",BroadcastObserved.BroadcastIdentity.SOURCE_PROVIDED,"synthetic","합성 분류");
                    kafka.send(new ProducerRecord<>(config.observationsTopic(),0,at.toEpochMilli(),CHANNEL,observation.encode())).get(10,TimeUnit.SECONDS);
                    for(int i=batch*10;i<(batch+1)*10;i++) {
                        Instant time=at.plusMillis(i%10+100);
                        var event=new ChatEvent(1,id("chat/"+i),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic",id("session"),null,"CHAT",time,time,false,"deployment-test");
                        kafka.send(new ProducerRecord<>(config.topic(),0,time.toEpochMilli(),CHANNEL,new EventCodec().encode(event))).get(10,TimeUnit.SECONDS);
                    }
                    if(batch==0)outbox(jdbc,config.runId(),0,at);
                    result=Map.of("synthetic",true,"chatAcknowledged",10,"observationsAcknowledged",1,"outboxCreated",batch==0?1:0,"batch",batch);
                }finally{factory.destroy();}
            } else if(args[0].equals("pending-outbox")) {
                outbox(jdbc,config.runId(),1,Instant.parse(args[1]).plusSeconds(30));result=Map.of("pendingCreated",1);
            } else if(args[0].equals("snapshot")) {
                var data=new LinkedHashMap<String,Object>();
                for(String table:List.of("processed_event","chat_bucket_10s","broadcast_observation","category_event","category_bucket","report_event","report_bucket","report_observation","notification_event","spike_event","spike_outbox")) {
                    String sql=switch(table) {
                        case "processed_event"->"SELECT projection,event_id::text,source_topic,source_partition,source_offset FROM processed_event WHERE run_id=? ORDER BY projection,event_id";
                        case "chat_bucket_10s"->"SELECT projection,channel_id,bucket_start,chat_count FROM chat_bucket_10s WHERE run_id=? ORDER BY projection,channel_id,bucket_start";
                        case "category_bucket"->"SELECT category_key,channel_id,bucket_start,message_count FROM category_bucket WHERE run_id=? ORDER BY category_key,channel_id,bucket_start";
                        case "report_bucket"->"SELECT report_id::text,bucket_start,message_count FROM report_bucket WHERE run_id=? ORDER BY report_id,bucket_start";
                        case "spike_outbox"->"SELECT spike_id::text,(published_at IS NOT NULL) AS published FROM spike_outbox WHERE run_id=? ORDER BY spike_id";
                        case "spike_event","notification_event"->"SELECT spike_id::text FROM "+table+" WHERE run_id=? ORDER BY spike_id";
                        default->"SELECT event_id::text FROM "+table+" WHERE run_id=? ORDER BY event_id";
                    };
                    var rows=jdbc.queryForList(sql,config.runId());
                    // DB 반환 타입의 차이를 제거하고 비교할 값과 순서를 고정한다.
                    var normalized=rows.stream().map(row->{var values=new TreeMap<String,String>();row.forEach((k,v)->values.put(k,String.valueOf(v)));return values;}).toList();
                    data.put(table,Map.of("rows",rows.size(),"sha256",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(JsonMapper.builder().build().writeValueAsBytes(normalized)))));
                }
                data.put("rankingCount",jdbc.queryForObject("SELECT coalesce(sum(chat_count),0) FROM chat_bucket_10s WHERE run_id=? AND projection=?",Long.class,config.runId(),config.projection()));
                data.put("trendCount",jdbc.queryForObject("SELECT coalesce(sum(chat_count),0) FROM chat_bucket_10s WHERE run_id=? AND projection=?",Long.class,config.runId(),config.projection()+"-trend"));
                data.put("notificationCount",jdbc.queryForObject("SELECT count(*) FROM notification_event WHERE run_id=?",Long.class,config.runId()));
                data.put("pendingOutbox",jdbc.queryForObject("SELECT count(*) FROM spike_outbox WHERE run_id=? AND published_at IS NULL",Long.class,config.runId()));
                result=data;
            } else if(args[0].equals("trim-retained-chat")) {
                // 이번 리허설의 합성 topic/partition0 범위만 대상으로 실제 retention gap을 만든다.
                if(args.length!=2||!config.runId().equals("deploy-v1")||!config.topic().equals("chat.received.v1"))throw new IllegalArgumentException("SYNTHETIC_TRIM_SCOPE");
                try(var admin=org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers",config.bootstrap()))) {
                    if(!args[1].equals(admin.describeCluster().clusterId().get(10,TimeUnit.SECONDS)))throw new IllegalStateException("SYNTHETIC_TRIM_SCOPE");
                    var partition=new org.apache.kafka.common.TopicPartition(config.topic(),0);
                    long begin=admin.listOffsets(Map.of(partition,org.apache.kafka.clients.admin.OffsetSpec.earliest())).all().get(10,TimeUnit.SECONDS).get(partition).offset();
                    long end=admin.listOffsets(Map.of(partition,org.apache.kafka.clients.admin.OffsetSpec.latest())).all().get(10,TimeUnit.SECONDS).get(partition).offset();
                    if(begin!=0||end!=20)throw new IllegalStateException("SYNTHETIC_TRIM_BOUNDS");
                    long low=admin.deleteRecords(Map.of(partition,org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(11))).lowWatermarks().get(partition).get(10,TimeUnit.SECONDS).lowWatermark();
                    if(low!=11)throw new IllegalStateException("SYNTHETIC_TRIM_WATERMARK");
                    result=Map.of("partition",0,"previousBegin",begin,"end",end,"lowWatermark",low);
                }
            }else throw new IllegalArgumentException("SYNTHETIC_COMMAND");
            System.out.println(JsonMapper.builder().build().writeValueAsString(result));
        }
    }
    private static void outbox(JdbcTemplate jdbc,String run,int batch,Instant at) {
        Instant evaluated=Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(),10)*10);UUID spike=id("spike/"+batch);
        jdbc.update("INSERT INTO spike_event VALUES (?,?,?,?,?,?,?,?,NULL) ON CONFLICT DO NOTHING",spike,run,CHANNEL,java.sql.Timestamp.from(evaluated),"chat-spike-v1",100,50,2);
        jdbc.update("INSERT INTO spike_outbox(spike_id,run_id) VALUES (?,?) ON CONFLICT DO NOTHING",spike,run);
    }
    private static UUID id(String value){return UUID.nameUUIDFromBytes(("deployment/20261026/"+value).getBytes(StandardCharsets.UTF_8));}
}
