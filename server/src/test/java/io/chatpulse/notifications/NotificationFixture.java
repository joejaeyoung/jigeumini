package io.chatpulse.notifications;

import java.time.*;
import java.util.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

final class NotificationFixture {
    static final String BOOTSTRAP="localhost:19092",RUN="test-v1",CHANNEL="a".repeat(32);
    static PGSimpleDataSource ds(String schema){var ds=new PGSimpleDataSource();ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");ds.setUser("chatpulse");ds.setPassword("local-chatpulse");ds.setCurrentSchema(schema);ds.setConnectTimeout(3);ds.setSocketTimeout(10);return ds;}
    static TransactionTemplate tx(JdbcTemplate jdbc){var tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));tx.setTimeout(20);return tx;}
    static void insert(JdbcTemplate jdbc,UUID id,Instant at,String channel){
        jdbc.update("INSERT INTO spike_event VALUES (?,?,?,?,?,?,?,?,NULL)",id,RUN,channel,java.sql.Timestamp.from(at),"chat-spike-v1",100,50,2);
        SpikeOutbox.enqueue(jdbc,id,RUN);
    }
    static NotificationStore store(JdbcTemplate jdbc,Clock clock){return new NotificationStore(jdbc,tx(jdbc),RUN,clock);}
}
