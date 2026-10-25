package io.chatpulse.pipeline;

import org.postgresql.ds.PGSimpleDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

final class IntegrationSupport {
    static final String BOOTSTRAP = "localhost:19092";
    static PGSimpleDataSource ds(String schema) {
        var ds = new PGSimpleDataSource();
        ds.setURL("jdbc:postgresql://localhost:15432/chatpulse");
        ds.setUser("chatpulse");
        ds.setPassword("local-chatpulse");
        ds.setCurrentSchema(schema);
        ds.setConnectTimeout(3);
        ds.setSocketTimeout(10);
        return ds;
    }
    static ProjectionStore store(String schema) {
        var ds = ds(schema);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        tx.setTimeout(20);
        return new ProjectionStore(new JdbcTemplate(ds), tx, new EventCodec(), "ranking", "test-v1");
    }
}
