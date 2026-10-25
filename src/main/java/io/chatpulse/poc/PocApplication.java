package io.chatpulse.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(scanBasePackages = "io.chatpulse")
@EnableConfigurationProperties(PocProperties.class)
public class PocApplication {
    public static void main(String[] args) throws InterruptedException {
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
