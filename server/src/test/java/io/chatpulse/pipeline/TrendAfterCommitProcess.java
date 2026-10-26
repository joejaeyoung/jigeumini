package io.chatpulse.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** trend의 복구 소비 경로를 DB commit 직후·Kafka commit 전에 종료한다. */
public final class TrendAfterCommitProcess {
    public static void main(String[] args) throws Exception {
        var ds = IntegrationSupport.ds(args[0]);
        var jdbc = new JdbcTemplate(ds);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds)); tx.setTimeout(20);
        var store = new ProjectionStore(jdbc, tx, new EventCodec(), "ranking-trend", "test-v1");
        try (var recovery = new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP, args[1], jdbc, "ranking-trend", "test-v1");
             var consumer = new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, args[2], "earliest"),
                     args[1], store, recovery, records -> Runtime.getRuntime().halt(91))) {
            consumer.start(); Thread.sleep(60000);
        }
    }
}
