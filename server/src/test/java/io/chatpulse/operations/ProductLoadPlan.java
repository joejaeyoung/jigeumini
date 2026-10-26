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
    static ProductLoadPlan read(String input) throws Exception {
        if(Files.isRegularFile(Path.of(input))) {
            if(Files.size(Path.of(input))>65536)throw new IllegalArgumentException("LOAD_PLAN_SIZE");
            return JsonMapper.builder().build().readValue(Files.readString(Path.of(input)),ProductLoadPlan.class);
        }
        return named(input);
    }
    static ProductLoadPlan named(String name) {
        var stages=switch(name) {
            case "smoke" -> List.of(new Stage(name,60,20,false));
            case "uniform","hot" -> List.of(new Stage(name,120,100,name.equals("hot")));
            case "steps" -> List.of(new Stage("rate50",60,50,false),new Stage("rate100",60,100,false),new Stage("rate200",60,200,false));
            case "soak" -> List.of(new Stage(name,1800,50,false));
            default -> throw new IllegalArgumentException("LOAD_PROFILE");
        };
        return new ProductLoadPlan(name,20,stages,name.equals("smoke")?15:name.equals("soak")?300:-1,
                Set.of("smoke","soak").contains(name)?20:0,"DIRECT",0,32,"BASELINE");
    }
}
