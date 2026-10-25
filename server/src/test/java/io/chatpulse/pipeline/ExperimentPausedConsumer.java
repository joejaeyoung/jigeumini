package io.chatpulse.pipeline;

import io.chatpulse.db.PostgresSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** 자신이 만든 별도 JVM 전체를 SIGSTOP하여 poll 지연과 heartbeat 단절을 구분한다. */
public final class ExperimentPausedConsumer {
    public static void main(String[] args) throws Exception {
        LoadExperiment.quiet();
        if(args.length!=3||!args[0].matches("exp_[a-f0-9]{32}")||!args[1].matches("exp\\.chat\\.[a-f0-9]{32}")||!args[2].matches("exp\\.a\\.[a-f0-9]{32}"))throw new IllegalArgumentException("EXPERIMENT_CHILD_ARGUMENTS");
        try(var ds=PostgresSupport.open(LoadExperiment.JDBC+"?currentSchema="+args[0],LoadExperiment.USER,LoadExperiment.PASSWORD,false)) {
            var tx=new TransactionTemplate(new DataSourceTransactionManager(ds));tx.setTimeout(20);
            var store=new ProjectionStore(new JdbcTemplate(ds),tx,new EventCodec(),"load-a","experiment");
            try(var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(LoadExperiment.BOOTSTRAP,args[2],"earliest"),args[1],store)) {
                consumer.start();long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);
                while(System.nanoTime()<until)Thread.sleep(100);
            }
        }
    }
}
