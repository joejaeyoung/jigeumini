package io.chatpulse.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;

/** 별도 JVM에서 DLQ ACK 직후 또는 DB commit 직후 종료한다. */
public final class DlqCrashProcess {
    public static void main(String[] args) throws Exception {
        var jdbc=new JdbcTemplate(IntegrationSupport.ds(args[0]));
        var recovery=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,args[1],jdbc,"ranking","test-v1",
                failure->{ if(args[3].equals("ack")) Runtime.getRuntime().halt(93); });
        var tx = new org.springframework.transaction.support.TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(jdbc.getDataSource()));
        tx.setTimeout(20);
        var clock = args.length > 4 ? java.time.Clock.fixed(java.time.Instant.parse(args[4]),java.time.ZoneOffset.UTC) : java.time.Clock.systemUTC();
        var store = new ProjectionStore(jdbc,tx,new EventCodec(),"ranking","test-v1",clock);
        var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,args[2],"earliest"),
                args[1],store,recovery,records->Runtime.getRuntime().halt(94));
        consumer.start(); Thread.sleep(60000); System.exit(95);
    }
}
