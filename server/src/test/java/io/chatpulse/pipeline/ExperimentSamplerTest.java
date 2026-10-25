package io.chatpulse.pipeline;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ExperimentSamplerTest {
    @Test void closingWaitsForInFlightSampleWithoutInterruptingIt() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var closed=new CountDownLatch(1);
        var failures=new AtomicInteger();
        try(var sampler=new ExperimentSampler(()->{entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new IllegalStateException();},failures::incrementAndGet)) {
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            var closer=Thread.ofPlatform().start(()->{try{sampler.close();closed.countDown();}catch(Exception failure){failures.incrementAndGet();}});
            try{assertThat(closed.await(50,TimeUnit.MILLISECONDS)).isFalse();}finally{release.countDown();}
            closer.join(3000);assertThat(closed.getCount()).isZero();assertThat(failures).hasValue(0);
        }finally{release.countDown();}
    }
    @Test void realSampleFailureIsReportedAndDoesNotBecomeSuccessfulShutdown() throws Exception {
        var failed=new CountDownLatch(1);var attempts=new AtomicInteger();
        try(var sampler=new ExperimentSampler(()->{attempts.incrementAndGet();throw new IllegalStateException("synthetic");},failed::countDown)) {
            assertThat(failed.await(2,TimeUnit.SECONDS)).isTrue();
        }
        assertThat(attempts).hasValue(1);
    }
}
