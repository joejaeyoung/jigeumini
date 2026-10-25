package io.chatpulse.replay;

import java.time.Duration;

/** 첫 DB 배치 commit 후, 작업 완료 전에 JVM을 실제 종료한다. */
public final class ReplayCrashProcess {
    public static void main(String[] args) throws Exception {
        var store=new ReplayStore(ReplayTestSupport.ds(args[0]));
        new ReplayWorker(store,ReplayTestSupport.BOOTSTRAP,args[1],job->Runtime.getRuntime().halt(91),Duration.ofMinutes(1),Duration.ofSeconds(10)).runOne();
        System.exit(1);
    }
}
