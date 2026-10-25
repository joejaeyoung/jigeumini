package io.chatpulse.pipeline;

import java.util.Set;

/** 실험 프로필과 시간·입력·consumer 자원 상한의 정본. */
record ExperimentPlan(String name,int rate,int seconds,int consumers,boolean hot,boolean sharded,boolean serial,boolean independent,int pauseSeconds,String fault,boolean membership,boolean pollStall,boolean duplicates,boolean twoProducers) {
    ExperimentPlan { if(rate<1||rate>2000||seconds<1||seconds>600||rate*(long)seconds>400000||consumers<1||consumers>4||pauseSeconds<0||pauseSeconds>60||!Set.of("none","delay","failure").contains(fault))throw new IllegalArgumentException("EXPERIMENT_PLAN"); }
    static ExperimentPlan named(String name) {
        if(name.matches("(uniform|hot)-[1-4]"))return new ExperimentPlan(name,2000,15,Integer.parseInt(name.substring(name.length()-1)),name.startsWith("hot"),false,false,false,0,"none",false,false,false,false);
        return switch(name) {
            case "smoke" -> new ExperimentPlan(name,100,2,1,false,false,false,true,0,"none",false,false,true,false);
            case "serial-control","async-control" -> new ExperimentPlan(name,100,15,1,false,false,name.equals("serial-control"),false,0,"none",false,false,false,false);
            case "baseline" -> new ExperimentPlan(name,500,30,1,false,false,true,false,0,"none",false,false,false,false);
            case "async-baseline" -> new ExperimentPlan(name,500,30,1,false,false,false,false,0,"none",false,false,false,false);
            case "steady" -> new ExperimentPlan(name,500,600,3,false,false,false,true,0,"none",false,false,false,false);
            case "burst" -> new ExperimentPlan(name,2000,60,3,false,false,false,false,0,"none",false,false,false,false);
            case "recovery" -> new ExperimentPlan(name,500,100,1,false,false,false,true,60,"none",false,false,false,false);
            case "db-delay","db-failure" -> new ExperimentPlan(name,500,30,1,false,false,false,true,0,name.equals("db-delay")?"delay":"failure",false,false,false,false);
            case "membership" -> new ExperimentPlan(name,500,40,1,false,false,false,true,0,"none",true,false,true,true);
            case "heartbeat" -> new ExperimentPlan(name,500,35,1,false,false,false,true,0,"none",false,false,true,false);
            case "poll-stall" -> new ExperimentPlan(name,500,30,2,false,false,false,true,0,"none",false,true,true,false);
            case "sharded-3" -> new ExperimentPlan(name,2000,15,3,true,true,false,false,0,"none",false,false,true,true);
            case "hot-duplicates" -> new ExperimentPlan(name,2000,15,3,true,false,false,false,0,"none",false,false,true,true);
            default -> throw new IllegalArgumentException("EXPERIMENT_PROFILE");
        };
    }
}
