package io.chatpulse.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

/** 서버와 로컬 관리 CLI가 공유하는 연결 한도·migration 초기화 계약. */
public final class PostgresSupport {
    private PostgresSupport() { }

    /** 호출자는 반환된 pool을 닫는다. 초기화 실패 시에는 이 메서드가 정리한다. */
    public static HikariDataSource openMigrated(String jdbcUrl, String user, String password) {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(3000);
        config.setValidationTimeout(2000);
        config.addDataSourceProperty("connectTimeout", "3");
        config.addDataSourceProperty("socketTimeout", "10");
        var dataSource = new HikariDataSource(config);
        try {
            Flyway.configure().dataSource(dataSource).load().migrate();
            return dataSource;
        } catch (RuntimeException | Error exception) {
            try {
                dataSource.close();
            } catch (RuntimeException | Error closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }
}
