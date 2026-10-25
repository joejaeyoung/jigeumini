package io.chatpulse.pipeline;

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

class DeadLetterTest {
    @Test void identitySurvivesRedeliveryAndSeparatesRecreatedTopicsAndOffsets() {
        Uuid topic=Uuid.randomUuid();
        assertEquals(DeadLetter.id(topic,0,1),DeadLetter.id(topic,0,1));
        assertNotEquals(DeadLetter.id(topic,0,1),DeadLetter.id(topic,1,1));
        assertNotEquals(DeadLetter.id(topic,0,1),DeadLetter.id(topic,0,2));
        assertNotEquals(DeadLetter.id(topic,0,1),DeadLetter.id(Uuid.randomUuid(),0,1));
    }
    @Test void malformedInputCopiesNeitherKeyPayloadHeadersNorExceptionBody() {
        var record=new ConsumerRecord<String,byte[]>("synthetic",0,1,"PRIVATE_KEY_CANARY","PRIVATE_BODY_CANARY".getBytes());
        record.headers().add("private","PRIVATE_HEADER_CANARY".getBytes());
        Instant now=Instant.parse("2026-10-25T12:00:07.123Z");
        var failure=DeadLetter.create(Uuid.randomUuid(),record,"INVALID_EVENT_JSON",null,now);
        String json=JsonMapper.builder().build().writeValueAsString(failure);
        assertFalse(json.contains("PRIVATE")); assertNull(failure.channelId());
        assertEquals(Instant.parse("2026-10-18T12:00:00Z"),failure.affectedFrom());
        assertEquals(Instant.parse("2026-10-25T12:00:20Z"),failure.affectedTo());
        var source=EventCodecTest.event(now);
        var future=new io.chatpulse.poc.ChatEvent(1,source.eventId(),source.source(),source.channelId(),source.chatRoomId(),
                source.collectionSessionId(),null,"CHAT",now.plusSeconds(10),now.plusSeconds(5),false,"test");
        new EventCodec().validate(future); new EventCodec().validateLiveAge(future,now);
        assertTrue(failure.affectedTo().isAfter(future.eventTime()));
        assertThrows(IllegalArgumentException.class,()->DeadLetter.create(Uuid.randomUuid(),record,"PRIVATE_EXCEPTION_CANARY",null,now));
    }
    @Test void onlyValidatedConflictNarrowsQualityToChannelAndBucket() {
        var event=EventCodecTest.event(Instant.now());
        var record=new ConsumerRecord<String,byte[]>("synthetic",0,1,event.channelId(),new EventCodec().encode(event));
        var conflict=DeadLetter.create(Uuid.randomUuid(),record,"EVENT_ID_CONFLICT",event,Instant.now());
        assertEquals(event.channelId(),conflict.channelId());
        assertEquals(10,java.time.Duration.between(conflict.affectedFrom(),conflict.affectedTo()).toSeconds());
        var stale=DeadLetter.create(Uuid.randomUuid(),record,"OUTSIDE_LIVE_RETENTION",event,Instant.now());
        assertNull(stale.channelId());
    }
}
