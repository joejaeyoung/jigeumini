package io.chatpulse.fleet;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/** 실제 plan 행 잠금 응답 직후에만 클라이언트 진행을 멈추는 시험 장벽. SQL/응답 본문은 저장하지 않는다. */
final class PlanLockGate extends DelegatingDataSource {
    private final Path report;
    private final AtomicBoolean paused=new AtomicBoolean();
    PlanLockGate(DataSource target,Path report){super(target);this.report=report;}
    @Override public Connection getConnection() throws SQLException{return wrap(super.getConnection());}
    @Override public Connection getConnection(String user,String password) throws SQLException{return wrap(super.getConnection(user,password));}
    private Connection wrap(Connection connection) {
        return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
            Object result=invoke(connection,method,args);
            if(result instanceof Statement statement) {
                String prepared=args!=null&&args.length>0&&args[0] instanceof String sql?sql:null;
                Class<?> type=statement instanceof PreparedStatement?PreparedStatement.class:Statement.class;
                return Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{type},(ignored,operation,values)->{
                    Object value=invoke(statement,operation,values);
                    String sql=prepared!=null?prepared:values!=null&&values.length>0&&values[0] instanceof String text?text:null;
                    if(operation.getName().equals("executeQuery")&&"SELECT singleton FROM collector_plan WHERE singleton=1 FOR UPDATE".equals(sql)
                            &&Files.exists(Path.of(report+".plan-lock-arm"))&&paused.compareAndSet(false,true))pause(connection);
                    return value;
                });
            }
            return result;
        });
    }
    private static Object invoke(Object target,Method method,Object[] args) throws Throwable {
        try{return method.invoke(target,args);}catch(InvocationTargetException failure){throw failure.getCause();}
    }
    private void pause(Connection connection) throws SQLException {
        try {
            int pid=connection.unwrap(org.postgresql.PGConnection.class).getBackendPID();
            Files.writeString(Path.of(report+".plan-locked"),Integer.toString(pid),StandardOpenOption.CREATE_NEW);
            long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
            while(!Files.exists(Path.of(report+".plan-lock-release"))) {
                if(System.nanoTime()>deadline)throw new SQLException("SYNTHETIC_PLAN_LOCK_GATE_TIMEOUT","57014");
                Thread.sleep(20);
            }
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new SQLException("SYNTHETIC_PLAN_LOCK_GATE_INTERRUPTED","57014");}
        catch(java.io.IOException failure){throw new SQLException("SYNTHETIC_PLAN_LOCK_GATE_IO","58000");}
    }
}
