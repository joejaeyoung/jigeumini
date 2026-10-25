package io.chatpulse.replay;

import java.time.Instant;
import java.util.UUID;
import io.chatpulse.poc.ChatEvent;
import org.postgresql.ds.PGSimpleDataSource;

final class ReplayTestSupport {
    static final String BOOTSTRAP="localhost:19092",CHANNEL="a".repeat(32);
    static PGSimpleDataSource ds(String schema) {
        var ds=new PGSimpleDataSource();ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");ds.setUser("chatpulse");ds.setPassword("local-chatpulse");
        ds.setCurrentSchema(schema);ds.setConnectTimeout(3);ds.setSocketTimeout(10);return ds;
    }
    static ChatEvent event(int id,Instant time,Instant received) {
        return new ChatEvent(1,UUID.nameUUIDFromBytes(("replay-fixture-"+id).getBytes(java.nio.charset.StandardCharsets.UTF_8)),"CHZZK_UNOFFICIAL",CHANNEL,"synthetic-room",
                UUID.nameUUIDFromBytes("replay-session".getBytes(java.nio.charset.StandardCharsets.UTF_8)),null,"CHAT",time,received,false,"replay-test");
    }
}
