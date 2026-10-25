package io.chatpulse.notifications;

import io.chatpulse.pipeline.KafkaSupport;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.*;
import org.springframework.transaction.support.*;

public final class SpikeOutbox implements AutoCloseable {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String run, topic;
    private final DefaultKafkaProducerFactory<String,byte[]> factory;
    private final KafkaTemplate<String,byte[]> kafka;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private volatile String state = "STARTING";
    private volatile Instant lastAck;
    public SpikeOutbox(JdbcTemplate jdbc,TransactionTemplate tx,String bootstrap,String topic,String run) {
        this.jdbc=jdbc;this.tx=tx;this.topic=topic;this.run=run;
        this.factory=KafkaSupport.producerFactory(bootstrap);this.kafka=new KafkaTemplate<>(factory);
    }
    public static void enqueue(JdbcTemplate jdbc,UUID spikeId,String run) {
        if (!TransactionSynchronizationManager.hasResource(Objects.requireNonNull(jdbc.getDataSource())))
            throw new IllegalStateException("OUTBOX_REQUIRES_TRANSACTION");
        jdbc.update("INSERT INTO spike_outbox(spike_id,run_id) VALUES (?,?)",spikeId,run);
    }
    public void start() {
        worker.scheduleWithFixedDelay(() -> {
            try { publishOne();state="READY"; }
            catch (RuntimeException ignored) { state="RETRYING"; }
        },0,1000,TimeUnit.MILLISECONDS);
        worker.scheduleWithFixedDelay(() -> {try {cleanup();}catch(RuntimeException ignored){state="RETRYING";}},60,3600,TimeUnit.SECONDS);
    }
    public boolean publishOne() { return publishOne(() -> { }); }
    // ACK와 전달 완료 기록 사이의 실제 JVM 종료 시험 경계.
    boolean publishOne(Runnable afterAck) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            jdbc.execute("SET LOCAL statement_timeout='5s'");
            var rows=jdbc.query("""
                    SELECT s.* FROM spike_outbox o JOIN spike_event s USING(spike_id)
                    WHERE o.run_id=? AND o.published_at IS NULL
                    ORDER BY o.created_at,o.spike_id LIMIT 1 FOR UPDATE OF o SKIP LOCKED
                    """,(rs,n)->new SpikeDetected(1,rs.getObject("spike_id",UUID.class),rs.getString("run_id"),rs.getString("channel_id"),
                    rs.getTimestamp("evaluated_at").toInstant(),rs.getString("algorithm_version"),rs.getLong("count"),
                    rs.getBigDecimal("baseline"),rs.getBigDecimal("ratio"),"HEALTHY_AT_DETECTION"),run);
            if(rows.isEmpty())return false;
            var event=rows.getFirst();
            try { kafka.send(new ProducerRecord<>(topic,null,event.evaluatedAt().toEpochMilli(),event.channelId(),event.encode())).get(10,TimeUnit.SECONDS); }
            catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException("OUTBOX_DELIVERY_UNAVAILABLE");}
            catch(Exception ignored){throw new IllegalStateException("OUTBOX_DELIVERY_UNAVAILABLE");}
            afterAck.run();
            jdbc.update("UPDATE spike_outbox SET published_at=clock_timestamp() WHERE spike_id=?",event.spikeId());
            lastAck=Instant.now();return true;
        }));
    }
    public int cleanup() {
        // feed 반영이 확인된 7일 경과 항목만 지운다. 미전달/미소비 및 feed ledger는 유지한다.
        return jdbc.update("""
                DELETE FROM spike_outbox o WHERE o.run_id=? AND o.published_at<now()-interval '7 days'
                AND EXISTS(SELECT 1 FROM notification_event n WHERE n.run_id=o.run_id AND n.spike_id=o.spike_id)
                """,run);
    }
    public Map<String,Object> status() {
        var result=new LinkedHashMap<String,Object>();result.put("state",state);result.put("lastAckAt",lastAck);
        result.put("pending",jdbc.queryForObject("SELECT count(*) FROM spike_outbox WHERE run_id=? AND published_at IS NULL",Long.class,run));
        return result;
    }
    @Override public void close(){worker.shutdownNow();factory.destroy();}
}
