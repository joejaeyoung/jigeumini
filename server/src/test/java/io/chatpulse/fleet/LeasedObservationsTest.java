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
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var published=new AtomicInteger();
        try(var observations=new LeasedObservations(budget,"test",60,120,id->{
            budget.reserveFor(id);entered.countDown();try{assertTrue(release.await(3,TimeUnit.SECONDS));}catch(InterruptedException e){throw new IllegalStateException();}
            return new PublicBroadcastObservation(BroadcastMetadata.LiveStatus.LIVE,10L,"123",null,null,PublicBroadcastObservation.Quality.OBSERVED);
        },event->published.incrementAndGet())) {
            try(var executor=Executors.newSingleThreadExecutor()) {
                var request=executor.submit(()->observations.observe(channel));
                assertTrue(entered.await(3,TimeUnit.SECONDS));assertEquals(1,budget.requests());
                authority.revoke();release.countDown();
                var failure=assertThrows(ExecutionException.class,()->request.get(3,TimeUnit.SECONDS));
                assertInstanceOf(io.chatpulse.poc.SourceFailure.class,failure.getCause());
                var sample=new PublicBroadcastObservation(BroadcastMetadata.LiveStatus.UNKNOWN,null,null,null,null,PublicBroadcastObservation.Quality.UPSTREAM_ERROR);
                var event=io.chatpulse.observations.BroadcastObserved.observed("test",channel,Instant.now(),120,sample);
                assertThrows(io.chatpulse.poc.SourceFailure.class,()->observations.publish(event));assertEquals(0,published.get());
            }
        }
    }
}
