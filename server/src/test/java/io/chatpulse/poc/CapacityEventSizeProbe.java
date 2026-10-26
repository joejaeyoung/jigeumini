package io.chatpulse.poc;
import io.chatpulse.pipeline.EventCodec;
import java.time.Instant;
import java.util.UUID;
/** 현재 codec의 합성 value 크기만 출력한다. 원문/식별자 값은 출력하지 않는다. */
public final class CapacityEventSizeProbe {
 public static void main(String[] args) {
  var codec=new EventCodec();int minimum=Integer.MAX_VALUE,maximum=0;long total=0;String channel="00000000000000000000000000000001";
  for(int i=0;i<1000;i++) {
   var at=Instant.parse("2026-10-26T00:00:00Z").plusMillis(i*100L);
   var event=ChatEvent.live(channel,channel,new UUID(0,i),at,at.plusMillis(2).plusNanos(i*997L));
   int size=codec.encode(event).length;minimum=Math.min(minimum,size);maximum=Math.max(maximum,size);total+=size;
  }
  System.out.println("{\"syntheticEvents\":1000,\"meanValueBytes\":"+(total/1000.0)+",\"minimumValueBytes\":"+minimum+",\"maximumValueBytes\":"+maximum+",\"keyBytes\":32,\"scope\":\"EVENT_CODEC_VALUE_ONLY; EXCLUDES_KAFKA_RECORD_BATCH_INDEX_REPLICATION\"}");
 }
}
