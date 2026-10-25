package io.chatpulse.reports;

import io.chatpulse.pipeline.DualTopicConsumer;
import java.util.List;

public final class ReportConsumer extends DualTopicConsumer {
    public ReportConsumer(String bootstrap,List<String> topics,String group,String reset,ReportStore store) {
        super(bootstrap,topics,group,reset,store,"REPORT");
    }
}
