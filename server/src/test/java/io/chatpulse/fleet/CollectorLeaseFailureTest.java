package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import io.chatpulse.poc.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import javax.sql.DataSource;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.TransactionSystemException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 실제 transaction 경계에 합성 JDBC 실패를 넣어 재시도 분류의 안전 조건을 검사한다. */
class CollectorLeaseFailureTest {
    private final DataSource ds=mock(DataSource.class);
    private final Connection connection=mock(Connection.class);
    private final Statement statement=mock(Statement.class);
    @BeforeEach void setup() throws Exception {
        when(ds.getConnection()).thenReturn(connection);when(connection.getAutoCommit()).thenReturn(true);
        when(connection.createStatement()).thenReturn(statement);
    }
    private CollectorLeases.Grant tick() {
        return new CollectorLeases(ds).tick(UUID.randomUUID(),1,Duration.ofSeconds(6),SourceMode.SYNTHETIC,Map.of(),Set.of(),true);
    }
    @Test void onlyConfirmedLockFailureAfterRollbackBecomesContention() throws Exception {
        when(statement.executeQuery(anyString())).thenThrow(new SQLException("synthetic","55P03"));
        assertThrows(CollectorLeases.PlanContended.class,this::tick);verify(connection).rollback();verify(connection,never()).commit();
    }
    @ParameterizedTest @ValueSource(strings={"08006","08007","57014","40001"})
    void otherSqlFailuresDoNotBecomeContention(String state) throws Exception {
        when(statement.executeQuery(anyString())).thenThrow(new SQLException("synthetic",state));
        assertThrows(DataAccessException.class,this::tick);verify(connection).rollback();verify(connection,never()).commit();
    }
    @Test void failedRollbackCannotBecomeRetryableContention() throws Exception {
        when(statement.executeQuery(anyString())).thenThrow(new SQLException("synthetic","55P03"));
        doThrow(new SQLException("synthetic","08006")).when(connection).rollback();
        assertThrows(TransactionSystemException.class,this::tick);verify(connection,never()).commit();
    }
    @Test void uncertainTransactionOutcomeStopsUnassignedWorkerWithoutRetry() {
        var leases=mock(CollectorLeases.class);var source=mock(ChatSource.class);
        when(leases.tick(any(),anyInt(),any(),any(),anyMap(),anySet(),anyBoolean()))
                .thenThrow(new TransactionSystemException("synthetic",new SQLException("synthetic","08007")));
        @SuppressWarnings("unchecked") KafkaTemplate<String,byte[]> kafka=mock(KafkaTemplate.class);
        when(kafka.partitionsFor("synthetic")).thenReturn(List.of(mock(PartitionInfo.class)));
        try(var worker=new CollectorWorker(leases,SourceMode.SYNTHETIC,CollectorWorker.Limits.defaults(1,Duration.ofSeconds(5)),source,kafka,"synthetic",CollectionObserver.NONE)) {
            var report=worker.run();assertEquals(false,report.get("complete"));assertEquals(0,report.get("leaseContentionRetries"));
            verify(leases).tick(any(),anyInt(),any(),any(),anyMap(),anySet(),anyBoolean());verifyNoInteractions(source);
        }
    }
}
