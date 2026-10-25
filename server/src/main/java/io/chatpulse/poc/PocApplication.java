package io.chatpulse.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(scanBasePackages = "io.chatpulse")
@EnableConfigurationProperties(PocProperties.class)
public class PocApplication {
    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0 && "ops".equals(args[0])) {
            System.exit(io.chatpulse.operations.OperationsCli.run(java.util.Arrays.copyOfRange(args,1,args.length)));return;
        }
        if (args.length > 0 && "fleet".equals(args[0])) {
            System.exit(io.chatpulse.fleet.FleetCli.run(java.util.Arrays.copyOfRange(args,1,args.length)));return;
        }
        if (args.length > 0 && "selection".equals(args[0])) {
            System.exit(io.chatpulse.discovery.SelectionCli.run(java.util.Arrays.copyOfRange(args,1,args.length)));return;
        }
        if (args.length > 0 && "replay".equals(args[0])) {
            System.exit(io.chatpulse.replay.ReplayCli.run(java.util.Arrays.copyOfRange(args,1,args.length)));return;
        }
        int code;
        try (var context = SpringApplication.run(PocApplication.class, args)) {
            if ("analyzer".equals(context.getEnvironment().getProperty("pipeline.role"))) {
                // 분석기와 조회 서버는 Spring lifecycle에 따라 계속 실행된다.
                new java.util.concurrent.CountDownLatch(1).await();
            }
            code = context.getBean(PocRunner.class).run();
        }
        System.exit(code);
    }
}
