package io.chatpulse.pipeline;

/** 별도 JVM을 DB commit 직후 강제 종료해 offset 미확정 상태를 만든다. */
public class CrashAfterCommitProcess {
    public static void main(String[] args) throws Exception {
        var factory = KafkaSupport.consumerFactory(IntegrationSupport.BOOTSTRAP, args[2], "earliest");
        var consumer = new ProjectionConsumer(factory, args[1], IntegrationSupport.store(args[0]), records -> Runtime.getRuntime().halt(91));
        consumer.start();
        Thread.sleep(60000);
        System.exit(92);
    }
}
