package io.chatpulse.pipeline;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("pipeline")
public record PipelineProperties(
        @DefaultValue("collector") String role,
        @DefaultValue("localhost:19092") String bootstrapServers,
        @DefaultValue("chat.received.v1") String topic,
        @DefaultValue("chat-ranking-live-v1") String group,
        @DefaultValue("ranking") String projection,
        @DefaultValue("live-v1") String runId,
        @DefaultValue("jdbc:postgresql://localhost:15432/chatpulse") String jdbcUrl,
        @DefaultValue("chatpulse") String dbUser,
        @DefaultValue("local-chatpulse") String dbPassword,
        @DefaultValue("none") String offsetReset,
        @DefaultValue("1000") int bufferCapacity) {
    public void validate() {
        if (!role.equals("collector") && !role.equals("analyzer")) throw new IllegalArgumentException("pipeline.role");
        if (!topic.matches("[a-zA-Z0-9._-]{1,200}") || !group.matches("[a-zA-Z0-9._-]{1,100}")
                || !projection.matches("[a-zA-Z0-9_-]{1,40}") || !runId.matches("[a-zA-Z0-9_-]{1,80}")
                || bufferCapacity < 1 || bufferCapacity > 1000
                || (!offsetReset.equals("none") && !offsetReset.equals("earliest"))) {
            throw new IllegalArgumentException("pipeline configuration");
        }
    }
}
