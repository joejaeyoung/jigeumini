package io.chatpulse.operations;

import io.chatpulse.db.PostgresSupport;
import io.chatpulse.pipeline.KafkaTopicPolicy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Tag("integration") @Timeout(90)
class RecoveryPlanIntegrationTest {
    private com.zaxxer.hikari.HikariDataSource root,ds;private JdbcTemplate jdbc;private RuntimeEnvironment config;
    private RecoveryPlan plan;private Admin admin;private String schema,prefix;private final List<String> extraGroups=new ArrayList<>();
    @BeforeEach void setup() {
        String id=UUID.randomUUID().toString().replace("-","");schema="deployment_"+id;prefix="deploy-"+id;
        System.out.println("DEPLOYMENT_TEST_OWNED "+schema+" "+prefix);
        root=PostgresSupport.open("jdbc:postgresql://localhost:15432/chatpulse","chatpulse","local-chatpulse",false);new JdbcTemplate(root).execute("CREATE SCHEMA "+schema);
        config=new RuntimeEnvironment("jdbc:postgresql://localhost:15432/chatpulse?currentSchema="+schema,"chatpulse","local-chatpulse","localhost:19092",
                "deploy.chat."+id,"deploy.observed."+id,"deploy.spikes."+id,prefix,"ranking","test",KafkaTopicPolicy.local());
        ds=PostgresSupport.openMigrated(config.jdbcUrl(),config.user(),config.password());jdbc=new JdbcTemplate(ds);plan=new RecoveryPlan(jdbc,config);plan.bindSources();
        admin=Admin.create(Map.of("bootstrap.servers","localhost:19092","default.api.timeout.ms",5000,"request.timeout.ms",3000));
    }
    @AfterEach void cleanup() throws Exception {
        if(plan!=null){try(var cleanupAdmin=Admin.create(Map.of("bootstrap.servers","localhost:19092"))){cleanupAdmin.deleteTopics(plan.topics()).all().get(10,TimeUnit.SECONDS);}finally{plan.close();}}
        if(admin!=null){var allGroups=new ArrayList<>(groups(prefix));allGroups.addAll(extraGroups);for(String group:allGroups)try{admin.deleteConsumerGroups(List.of(group)).all().get(5,TimeUnit.SECONDS);}catch(Exception ignored){}admin.close(Duration.ofSeconds(1));}
        if(ds!=null)ds.close();if(root!=null){new JdbcTemplate(root).execute("DROP SCHEMA "+schema+" CASCADE");root.close();}
    }
    private List<String> groups(String p){return List.of(p,p+"-trend",p+"-observations",p+"-notifications",p+"-categories",p+"-reports");}
    private RuntimeEnvironment group(String value){return new RuntimeEnvironment(config.jdbcUrl(),config.user(),config.password(),config.bootstrap(),config.topic(),config.observationsTopic(),config.notificationsTopic(),value,config.projection(),config.runId(),config.policy());}
    private void send(int count) throws Exception {
        var properties=Map.<String,Object>of("bootstrap.servers",config.bootstrap(),"acks","all","key.serializer","org.apache.kafka.common.serialization.StringSerializer","value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        try(var producer=new KafkaProducer<String,byte[]>(properties)){for(int i=0;i<count;i++)producer.send(new ProducerRecord<>(config.topic(),0,"synthetic",new byte[]{1})).get(5,TimeUnit.SECONDS);}
    }
    @Test void freshDatabaseRequiresExplicitGroupsAndRejectsKafkaAheadThenPreparesNewGroups() throws Exception {
        assertFalse((Boolean)plan.inspect().get("safeForStartup"));assertThrows(IllegalStateException.class,plan::requireSafeStartup);
        plan.prepareNewGroups(prefix,true);assertDoesNotThrow(plan::requireSafeStartup);send(1);
        admin.alterConsumerGroupOffsets(prefix,Map.of(new TopicPartition(config.topic(),0),new OffsetAndMetadata(1))).all().get(5,TimeUnit.SECONDS);
        assertThrows(IllegalStateException.class,plan::requireSafeStartup);assertTrue((Boolean)plan.inspect().get("recoverableFromRetainedLog"));
        String next=prefix+"-recovered";extraGroups.addAll(groups(next));plan.prepareNewGroups(next,false);
        try(var recovered=new RecoveryPlan(jdbc,group(next))){assertDoesNotThrow(recovered::requireSafeStartup);}
        assertEquals(1,admin.listConsumerGroupOffsets(prefix).partitionsToOffsetAndMetadata().get().get(new TopicPartition(config.topic(),0)).offset());
        assertThrows(IllegalStateException.class,()->plan.prepareNewGroups(next,false));
    }
    @Test void expiredSourceCannotBeMarkedAsRecoverable() throws Exception {
        plan.prepareNewGroups(prefix,true);send(2);var tp=new TopicPartition(config.topic(),0);
        admin.deleteRecords(Map.of(tp,RecordsToDelete.beforeOffset(1))).all().get(5,TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).until(()->!Boolean.TRUE.equals(plan.inspect().get("recoverableFromRetainedLog")));
        assertThrows(IllegalStateException.class,plan::requireSafeStartup);assertThrows(IllegalStateException.class,()->plan.prepareNewGroups(prefix+"-recovery",false));
    }
    @Test void sameNameTopicRecreationFailsIdentityCheck() throws Exception {
        admin.deleteTopics(List.of(config.topic())).all().get(5,TimeUnit.SECONDS);
        io.chatpulse.pipeline.KafkaSupport.ensureTopic(config.bootstrap(),config.topic(),config.policy());
        assertThrows(IllegalStateException.class,plan::inspect);assertThrows(IllegalStateException.class,plan::bindSources);
    }
    @Test void runtimeValidationRejectsChangedAndFutureSchemaWithoutMigrating() {
        try(var validated=PostgresSupport.openValidated(config.jdbcUrl(),config.user(),config.password())){assertFalse(validated.isClosed());}
        Integer original=jdbc.queryForObject("SELECT checksum FROM flyway_schema_history WHERE version='1'",Integer.class);
        jdbc.update("UPDATE flyway_schema_history SET checksum=0 WHERE version='1'");
        assertThrows(IllegalStateException.class,()->PostgresSupport.openValidated(config.jdbcUrl(),config.user(),config.password()));
        jdbc.update("UPDATE flyway_schema_history SET checksum=? WHERE version='1'",original);
        jdbc.update("INSERT INTO flyway_schema_history(installed_rank,version,description,type,script,checksum,installed_by,execution_time,success) VALUES (999,'999','synthetic future','SQL','V999__synthetic.sql',1,current_user,1,true)");
        assertThrows(IllegalStateException.class,()->PostgresSupport.openValidated(config.jdbcUrl(),config.user(),config.password()));
        assertEquals(1,jdbc.queryForObject("SELECT count(*) FROM flyway_schema_history WHERE version='999'",Integer.class));
    }
}
