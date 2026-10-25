package io.chatpulse.pipeline;

import org.springframework.jdbc.core.JdbcTemplate;

/** 별도 JVM에서 DLQ ACK 직후 또는 DB commit 직후 종료한다. */
public final class DlqCrashProcess {
    public static void main(String[] args) throws Exception {
        var jdbc=new JdbcTemplate(IntegrationSupport.ds(args[0]));
        var recovery=new DeadLetterRecovery(IntegrationSupport.BOOTSTRAP,args[1],jdbc,"ranking","test-v1",
                failure->{ if(args[3].equals("ack")) Runtime.getRuntime().halt(93); });
        var consumer=new ProjectionConsumer(KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP,args[2],"earliest"),
                args[1],IntegrationSupport.store(args[0]),recovery,records->Runtime.getRuntime().halt(94));
        consumer.start(); Thread.sleep(60000); System.exit(95);
    }
}
