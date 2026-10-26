package io.chatpulse.operations;

import java.util.*;

/** 원시 응답을 보존하지 않는 1ms 단위 상한 histogram. 10초 초과 quantile은 unknown이다. */
final class ProductLoadLatency {
    private final long[] bins=new long[10002];
    private long count;
    private double maximum;
    synchronized void add(double millis) {
        if(!Double.isFinite(millis)||millis<0)throw new IllegalArgumentException("LATENCY_VALUE");
        bins[(int)Math.min(10001,Math.ceil(millis))]++;count++;maximum=Math.max(maximum,millis);
    }
    synchronized Map<String,Object> snapshot() {
        var out=new LinkedHashMap<String,Object>();out.put("samples",count);out.put("resolutionMillis",1);
        out.put("p50UpperMillis",quantile(.50));out.put("p95UpperMillis",quantile(.95));out.put("p99UpperMillis",quantile(.99));
        out.put("maximumMillis",maximum);out.put("above10000Millis",bins[10001]);return out;
    }
    private Integer quantile(double fraction) {
        if(count==0)return null;
        long target=(long)Math.ceil(count*fraction),seen=0;
        for(int i=0;i<bins.length;i++)if((seen+=bins[i])>=target)return i==10001?null:i;
        throw new IllegalStateException("LATENCY_COUNT");
    }
}
