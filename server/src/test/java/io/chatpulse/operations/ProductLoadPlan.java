package io.chatpulse.operations;

import java.nio.file.*;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

/** 기존 회귀 profile과 용량 실험이 공유하는 유한 입력 계약. */
record ProductLoadPlan(String name, int channels, List<Stage> stages, int faultAtSeconds, int faultSeconds,
        String sourceMode, int apiRequestsPerSecond, int apiMaxInFlight, String resourceProfile) {
    record Stage(String name, int seconds, int eventsPerSecond, boolean hot) {
        Stage {
            if (name==null || !name.matches("[a-z][a-z0-9_-]{0,39}") || seconds<10 || seconds>1800
                    || eventsPerSecond<10 || eventsPerSecond>10000 || eventsPerSecond%10!=0)
                throw new IllegalArgumentException("LOAD_STAGE");
        }
    }
    ProductLoadPlan {
        if(name==null || !name.matches("[a-z][a-z0-9_-]{0,39}") || channels<2 || channels>1000
                || stages==null || stages.isEmpty() || stages.size()>10 || sourceMode==null
                || !Set.of("DIRECT","WEBSOCKET").contains(sourceMode)
                || apiRequestsPerSecond<0 || apiRequestsPerSecond>200 || apiMaxInFlight<1 || apiMaxInFlight>100
                || resourceProfile==null || !Set.of("BASELINE","API_2_CPU","DB_2_CPU","KAFKA_2_CPU","COLLECTOR_2_CPU").contains(resourceProfile))
            throw new IllegalArgumentException("LOAD_PLAN");
        stages=List.copyOf(stages);
        int seconds=stages.stream().mapToInt(Stage::seconds).sum();
        long events=stages.stream().mapToLong(s->(long)s.seconds()*s.eventsPerSecond()).sum();
        if(seconds>1800 || events>300000 || stages.stream().map(Stage::name).distinct().count()!=stages.size()
                || faultAtSeconds < -1 || faultSeconds<0 || faultSeconds>30
                || (faultAtSeconds==-1)!=(faultSeconds==0)
                || (faultAtSeconds>=0 && faultAtSeconds+faultSeconds>=seconds))
            throw new IllegalArgumentException("LOAD_BUDGET");
    }
    int seconds() {return stages.stream().mapToInt(Stage::seconds).sum();}
    long uniqueEvents() {return stages.stream().mapToLong(s->(long)s.seconds()*s.eventsPerSecond()).sum();}
    boolean websocket() {return sourceMode.equals("WEBSOCKET");}
    String phaseAtMillis(long millis) {
        if(millis<0)return "INITIALIZE";
        for(var stage:stages){if(millis<stage.seconds()*1000L)return stage.name();millis-=stage.seconds()*1000L;}
        return "DRAIN";
    }
    static ProductLoadPlan read(String input) throws Exception {
        if(Files.isRegularFile(Path.of(input))) {
            if(Files.size(Path.of(input))>65536)throw new IllegalArgumentException("LOAD_PLAN_SIZE");
            return JsonMapper.builder().build().readValue(Files.readString(Path.of(input)),ProductLoadPlan.class);
        }
        throw new IllegalArgumentException("LOAD_PLAN_FILE_REQUIRED");
    }
}
