package io.chatpulse.replay;

import io.chatpulse.pipeline.EventContractException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import static io.chatpulse.replay.ReplayFailure.Code.*;

public final class ReplayWorker {
    private final ReplayStore store;
    private final String bootstrap,topic;
    private final Consumer<ReplayStore.Job> afterBatch;
    private final Duration maxDuration,noProgress;
    public ReplayWorker(ReplayStore store,String bootstrap,String topic) {
        this(store,bootstrap,topic,job->{},Duration.ofMinutes(30),Duration.ofSeconds(30));
    }
    ReplayWorker(ReplayStore store,String bootstrap,String topic,Consumer<ReplayStore.Job> afterBatch,Duration maxDuration,Duration noProgress) {
        this.store=store;this.bootstrap=bootstrap;this.topic=topic;this.afterBatch=afterBatch;this.maxDuration=maxDuration;this.noProgress=noProgress;
    }
    /** 한 작업만 처리하고 종료한다. 실패한 run을 재개하거나 group offset을 커밋하지 않는다. */
    public UUID runOne() throws java.sql.SQLException {
        try(var lock=store.tryWorkerLock()) {
            if(lock==null) throw new ReplayFailure(WORKER_BUSY);
            store.reapWhileLocked();
            var job=store.claim(UUID.randomUUID());if(job==null) return null;
            try(var source=new ReplaySource(bootstrap,topic,job.id())) {
                source.assign(job.manifest());
                var next=new TreeMap<Integer,Long>();var ends=new TreeMap<Integer,Long>();
                for(var range:job.manifest().partitions()) {next.put(range.partition(),range.start());ends.put(range.partition(),range.end());}
                long started=System.nanoTime(),lastProgress=started,lastHeartbeat=0;
                while(!next.equals(ends)) {
                    long now=System.nanoTime();
                    if(Thread.currentThread().isInterrupted()) throw new ReplayFailure(CANCELLED);
                    if(now-started>maxDuration.toNanos()) throw new ReplayFailure(TIME_LIMIT);
                    if(now-lastProgress>noProgress.toNanos()) throw new ReplayFailure(NO_PROGRESS);
                    if(now-lastHeartbeat>Duration.ofSeconds(5).toNanos()) {store.heartbeat(job);lastHeartbeat=now;}
                    lock.check();source.validate(job.manifest());
                    org.apache.kafka.clients.consumer.ConsumerRecords<String,byte[]> records;
                    try {records=source.poll();}
                    catch(org.apache.kafka.clients.consumer.OffsetOutOfRangeException e) {
                        source.validate(job.manifest());throw new ReplayFailure(OFFSET_GAP);
                    }var batch=new ArrayList<ConsumerRecord<String,byte[]>>();
                    var candidate=new TreeMap<>(next);
                    for(var partition:records.partitions()) for(var record:records.records(partition)) {
                        long end=ends.get(record.partition());
                        if(record.offset()>=end) continue;
                        if(record.offset()!=candidate.get(record.partition())) throw new ReplayFailure(OFFSET_GAP);
                        batch.add(record);candidate.put(record.partition(),record.offset()+1);
                    }
                    if(!batch.isEmpty()) {
                        source.validate(job.manifest()); // 읽는 도중 삭제·재생성됐으면 부분 성공으로 확정하지 않는다.
                        lock.check();store.apply(job,batch,candidate);next=candidate;lastProgress=System.nanoTime();
                        for(var partition:next.keySet()) if(next.get(partition).equals(ends.get(partition))) source.pause(partition);
                        afterBatch.accept(job);
                    }
                }
                lock.check();source.validate(job.manifest());store.complete(job);
            } catch(ReplayFailure e) {store.fail(job,e.code());}
              catch(EventContractException e) {store.fail(job,INVALID_EVENT);}
              catch(org.springframework.dao.DataAccessException | org.springframework.transaction.TransactionException e) {store.fail(job,DB_UNAVAILABLE);}
              catch(org.apache.kafka.common.KafkaException e) {store.fail(job,SOURCE_UNAVAILABLE);}
              catch(RuntimeException e) {store.fail(job,INTERNAL_ERROR);}
            return job.id();
        }
    }
}
