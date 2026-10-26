package io.chatpulse.operations;

import java.lang.management.*;
import java.util.*;
import javax.management.MBeanServerConnection;

/** 내부 시험 JMX에서 허용한 설정과 스레드 개수만 남긴다. 이름·인수·원본 stack은 저장하지 않는다. */
final class ProductLoadJvm {
    private ProductLoadJvm() { }
    static Map<String,Object> runtime(MBeanServerConnection connection) {
        var result=new LinkedHashMap<String,Object>();
        try {
            var collectors=ManagementFactory.getPlatformMXBeans(connection,GarbageCollectorMXBean.class);
            result.put("collectors",collectors.stream().map(GarbageCollectorMXBean::getName).sorted().toList());
            var diagnostic=ManagementFactory.getPlatformMXBean(connection,com.sun.management.HotSpotDiagnosticMXBean.class);
            var options=new TreeMap<String,String>();
            for(String name:List.of("MaxHeapSize","InitialHeapSize","UseG1GC","UseSerialGC","UseParallelGC","UseZGC","ActiveProcessorCount"))
                options.put(name,diagnostic.getVMOption(name).getValue());
            result.put("options",options);
        } catch(Exception failure) {result.put("error",failure.getClass().getSimpleName());}
        return result;
    }
    static Map<String,Object> threads(ThreadMXBean bean) {
        var states=new TreeMap<String,Integer>();var work=new TreeMap<String,Integer>();var waits=new TreeMap<String,Integer>();
        int sampled=0;
        for(var info:bean.getThreadInfo(bean.getAllThreadIds(),40)) {
            if(info==null)continue;sampled++;states.merge(info.getThreadState().name(),1,Integer::sum);
            var stack=Arrays.stream(info.getStackTrace()).map(StackTraceElement::getClassName).toList();
            String role=stack.contains("io.chatpulse.categories.CategoryStore")?"CATEGORY":stack.contains("io.chatpulse.reports.ReportStore")?"REPORT":"OTHER";
            work.merge(role,1,Integer::sum);
            boolean pool=stack.stream().anyMatch(name->name.equals("com.zaxxer.hikari.pool.HikariPool")||name.equals("com.zaxxer.hikari.util.ConcurrentBag"));
            boolean jdbc=stack.stream().anyMatch(name->name.startsWith("org.postgresql.core."));
            boolean socket=stack.stream().anyMatch(name->name.startsWith("org.apache.tomcat.websocket."));
            boolean http=stack.stream().anyMatch(name->name.startsWith("jdk.internal.net.http."));
            String location=pool?"POOL_ACQUIRE":jdbc?"JDBC":socket?"WEBSOCKET":http?"HTTP_CLIENT":"OTHER";
            waits.merge(role+"/"+location,1,Integer::sum);
        }
        return Map.of("sampled",sampled,"states",states,"work",work,"locations",waits,"maximumStackDepth",40,
                "scope","PLATFORM_THREAD_INSTANT_SAMPLES; NOT_WAIT_DURATION; NOT_VIRTUAL_THREADS");
    }
}
