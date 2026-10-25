package io.chatpulse.pipeline;

import java.util.concurrent.*;

/** 표본 수집 종료는 실행 중인 계측을 기다린다. 정상 종료를 측정 실패로 만들지 않는다. */
final class ExperimentSampler implements AutoCloseable {
    @FunctionalInterface interface Sample { void run() throws Exception; }
    private final ScheduledExecutorService executor=Executors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> task;
    ExperimentSampler(Sample sample,Runnable failed) {
        task=executor.scheduleWithFixedDelay(()->{
            try{sample.run();}
            catch(Exception failure){failed.run();throw new IllegalStateException("EXPERIMENT_SAMPLE_FAILED");}
        },0,5,TimeUnit.SECONDS);
    }
    @Override public synchronized void close() throws Exception {
        task.cancel(false);executor.shutdown();
        if(!executor.awaitTermination(15,TimeUnit.SECONDS)) {
            executor.shutdownNow();executor.awaitTermination(5,TimeUnit.SECONDS);
            throw new IllegalStateException("EXPERIMENT_SAMPLER_STOP_TIMEOUT");
        }
    }
}
