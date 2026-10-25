package io.chatpulse.categories;

import io.chatpulse.pipeline.DualTopicConsumer;
import java.util.List;

public final class CategoryConsumer extends DualTopicConsumer {
    public CategoryConsumer(String bootstrap,List<String> topics,String group,String reset,CategoryStore store) {
        super(bootstrap,topics,group,reset,store,"CATEGORY");
    }
}
