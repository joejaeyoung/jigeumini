package io.chatpulse.fleet;

import io.chatpulse.poc.ChatSource;
import io.chatpulse.metadata.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class LeasedObservationsTest {
    @Test void responseAfterRevocationCannotPublishAndUsesSharedHttpBudget() throws Exception {
        String channel="a".repeat(32);var authority=new LeaseAuthority();
        authority.update(new CollectorLeases.Grant(Instant.EPOCH,List.of(new CollectorLeases.Lease(channel,1,Instant.EPOCH.plusSeconds(10),"version"))),System.nanoTime());
        var budget=new BudgetedChatSource(mock(ChatSource.class),authority,20,Duration.ofMillis(100));
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var finished=new CountDownLatch(1);var published=new AtomicInteger();
        try(var observations=new LeasedObservations(budget,"test",60,120,id->{
            entered.countDown();try{assertTrue(release.await(3,TimeUnit.SECONDS));}catch(InterruptedException e){throw new IllegalStateException();}finally{finished.countDown();}
            return new PublicBroadcastObservation(BroadcastMetadata.LiveStatus.LIVE,10L,"123",null,null,PublicBroadcastObservation.Quality.OBSERVED);
        },event->published.incrementAndGet())) {
            observations.start(channel);assertTrue(entered.await(3,TimeUnit.SECONDS));assertEquals(1,budget.requests());
            authority.revoke();release.countDown();assertTrue(finished.await(3,TimeUnit.SECONDS));observations.stopped(channel);assertEquals(0,published.get());
        }
    }
}
