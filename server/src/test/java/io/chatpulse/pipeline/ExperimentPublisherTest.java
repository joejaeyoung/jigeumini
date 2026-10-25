package io.chatpulse.pipeline;

import io.chatpulse.poc.ChatEvent;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ExperimentPublisherTest {
    @SuppressWarnings("unchecked") @Test void slowCallbacksKeepTheGlobalBoundAndWholeFrameRejectionAndFailedCallbacksRemainUnconfirmed() {
        var kafka=(KafkaTemplate<String,byte[]>)mock(KafkaTemplate.class);var pending=new ArrayList<CompletableFuture<SendResult<String,byte[]>>>();
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(call->{var future=new CompletableFuture<SendResult<String,byte[]>>();pending.add(future);return future;});
        var event=new ChatEvent(1,UUID.randomUUID(),"CHZZK_UNOFFICIAL","a".repeat(32),"synthetic",UUID.randomUUID(),null,"CHAT",Instant.now(),Instant.now(),false,"test");
        try(var publisher=new ExperimentPublisher(kafka,"synthetic",3,false)) {
            publisher.accept(List.of(event,event),0);assertThat(publisher.pending()).isEqualTo(2);
            assertThatThrownBy(()->publisher.accept(List.of(event,event),2)).hasMessage("EXPERIMENT_REJECTED");assertThat(pending).hasSize(2);
            pending.getFirst().complete(null);pending.getLast().completeExceptionally(new IllegalStateException("SYNTHETIC_BROKER_FAILURE"));
            assertThat(publisher.stats()).containsEntry("accepted",2L).containsEntry("acknowledged",1L).containsEntry("rejected",2L).containsEntry("unconfirmed",1L).containsEntry("highWater",2);
            assertThat(publisher.oldestAgeMillis()).isGreaterThanOrEqualTo(0);assertThat(publisher.pending()).isEqualTo(1);
        }
    }
    @Test void profilesAndFiniteLimitsAreExplicit() {
        assertThat(LoadExperiment.plan("steady").rate()).isEqualTo(500);assertThat(LoadExperiment.plan("steady").seconds()).isEqualTo(600);assertThat(LoadExperiment.plan("burst").rate()).isEqualTo(2000);
        assertThatThrownBy(()->LoadExperiment.plan("unbounded")).hasMessage("EXPERIMENT_PROFILE");
        assertThatThrownBy(()->new LoadExperiment.Plan("invalid",2001,60,1,false,false,false,false,0,"none",false,false,false,false)).hasMessage("EXPERIMENT_PLAN");
    }
}
