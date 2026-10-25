package io.chatpulse.notifications;

import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class NotificationCrashProcess {
    public static void main(String[] args){
        var jdbc=new JdbcTemplate(NotificationFixture.ds(args[0]));var tx=NotificationFixture.tx(jdbc);
        if(args[2].equals("commit")){
            tx.executeWithoutResult(status->NotificationFixture.insert(jdbc,UUID.fromString(args[3]),Instant.parse(args[4]),NotificationFixture.CHANNEL));
            Runtime.getRuntime().halt(95);
        }
        try(var outbox=new SpikeOutbox(jdbc,tx,NotificationFixture.BOOTSTRAP,args[1],NotificationFixture.RUN)){
            outbox.publishOne(()->Runtime.getRuntime().halt(96));
        }
        throw new IllegalStateException("CRASH_POINT_NOT_REACHED");
    }
}
