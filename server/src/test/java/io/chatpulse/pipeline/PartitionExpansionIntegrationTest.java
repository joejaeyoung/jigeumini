package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import io.chatpulse.replay.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import static org.assertj.core.api.Assertions.*;

@Tag("integration")
class PartitionExpansionIntegrationTest {
    @Test void partitionIncreaseMovesAnExistingKeyAndRequiresNewReplayManifest() throws Exception {
        LoadExperiment.quiet();String suffix=UUID.randomUUID().toString().replace("-","");
        String topic="it.expansion."+suffix,schema="it_expansion_"+suffix;
        var root=new JdbcTemplate(IntegrationSupport.ds("public"));root.execute("CREATE SCHEMA "+schema);
        var ds=IntegrationSupport.ds(schema);org.flywaydb.core.Flyway.configure().dataSource(ds).defaultSchema(schema).load().migrate();
        var codec=new EventCodec();var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(ds));tx.setTimeout(20);
        String channel=null;for(int i=0;channel==null;i++){String candidate=String.format("%032x",i);int hash=Utils.toPositive(Utils.murmur2(candidate.getBytes(StandardCharsets.UTF_8)));if(hash%3!=hash%6)channel=candidate;}
        final String key=channel;Instant at=Instant.now().minusSeconds(5);
        try(var admin=Admin.create(Map.of("bootstrap.servers",IntegrationSupport.BOOTSTRAP));var source=new ReplaySource(IntegrationSupport.BOOTSTRAP,topic,UUID.randomUUID())) {
            KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic);
            int before=send(topic,key,event(key,at),codec);var original=source.capture(null,null);
            admin.createPartitions(Map.of(topic,NewPartitions.increaseTo(6))).all().get(10,TimeUnit.SECONDS);
            int after=send(topic,key,event(key,at.minusSeconds(120)),codec);
            assertThat(after).isNotEqualTo(before);
            assertThatThrownBy(()->source.validate(original)).isInstanceOf(ReplayFailure.class).hasMessage("PARTITIONS_CHANGED");
            assertThatThrownBy(()->KafkaSupport.ensureTopic(IntegrationSupport.BOOTSTRAP,topic)).isInstanceOf(IllegalStateException.class);
            var expanded=source.capture(null,null);assertThat(expanded.partitions()).hasSize(6);source.assign(expanded);
            var jobs=new io.chatpulse.replay.ReplayStore(ds);var id=jobs.create(expanded);jobs.claim(UUID.randomUUID());
            var store=new ProjectionStore(new JdbcTemplate(ds),tx,codec,"replay",id.toString());
            var records=new ArrayList<ConsumerRecord<String,byte[]>>();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(records.size()<2&&System.nanoTime()<deadline)source.poll().forEach(records::add);
            assertThat(records).hasSize(2);store.applyReplay(records);store.applyReplay(records);
            assertThat(store.totals().get("processedEvents")).isEqualTo(2L);assertThat(store.totals().get("bucketTotal")).isEqualTo(2L);
            assertThat(new JdbcTemplate(ds).queryForObject("SELECT count(*) FROM chat_bucket_10s",Long.class)).isEqualTo(2L);
        } finally {
            try(var admin=Admin.create(Map.of("bootstrap.servers",IntegrationSupport.BOOTSTRAP))){admin.deleteTopics(List.of(topic)).all().get(10,TimeUnit.SECONDS);}
            root.execute("DROP SCHEMA "+schema+" CASCADE");
        }
    }
    private ChatEvent event(String channel,Instant at){return new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL",channel,"synthetic-room",UUID.randomUUID(),null,"CHAT",at,Instant.now(),false,"experiment-v1");}
    private int send(String topic,String key,ChatEvent event,EventCodec codec) throws Exception {
        var factory=KafkaSupport.producerFactory(IntegrationSupport.BOOTSTRAP);
        try{return new KafkaTemplate<>(factory).send(topic,key,codec.encode(event)).get(10,TimeUnit.SECONDS).getRecordMetadata().partition();}
        finally{factory.destroy();}
    }
}
