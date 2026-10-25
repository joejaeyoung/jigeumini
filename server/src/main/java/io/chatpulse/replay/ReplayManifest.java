package io.chatpulse.replay;

import java.time.Instant;
import java.util.*;
import org.apache.kafka.common.Uuid;
import tools.jackson.databind.json.JsonMapper;
import static io.chatpulse.replay.ReplayFailure.Code.INVALID_MANIFEST;

public record ReplayManifest(int formatVersion, String topic, String topicId, List<Range> partitions,
        Instant eventFrom, Instant eventTo, int schemaVersion, String algorithmVersion, Instant capturedAt) {
    public static final String ALGORITHM = "bucket-10s-v1";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public record Range(int partition, long start, long end) {
        public Range { if(partition<0 || start<0 || end<start) throw new ReplayFailure(INVALID_MANIFEST); }
    }
    public ReplayManifest {
        if(formatVersion!=1 || schemaVersion!=1 || !ALGORITHM.equals(algorithmVersion)
                || topic==null || !topic.matches("[a-zA-Z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                || topicId==null || capturedAt==null || partitions==null || partitions.isEmpty() || partitions.size()>256
                || (eventFrom==null)!=(eventTo==null) || (eventFrom!=null && !eventFrom.isBefore(eventTo))) throw new ReplayFailure(INVALID_MANIFEST);
        try { if(Uuid.ZERO_UUID.equals(Uuid.fromString(topicId))) throw new IllegalArgumentException(); }
        catch(RuntimeException e) { throw new ReplayFailure(INVALID_MANIFEST); }
        partitions=partitions.stream().sorted(Comparator.comparingInt(Range::partition)).toList();
        for(int i=0;i<partitions.size();i++) if(partitions.get(i).partition()!=i) throw new ReplayFailure(INVALID_MANIFEST);
    }
    public boolean includes(Instant time) { return eventFrom==null || (!time.isBefore(eventFrom) && time.isBefore(eventTo)); }
    public boolean sameInput(ReplayManifest other) {
        return topic.equals(other.topic) && topicId.equals(other.topicId) && partitions.equals(other.partitions)
                && Objects.equals(eventFrom,other.eventFrom) && Objects.equals(eventTo,other.eventTo)
                && schemaVersion==other.schemaVersion && algorithmVersion.equals(other.algorithmVersion);
    }
    public String json() { return JSON.writeValueAsString(this); }
    public static ReplayManifest parse(String value) {
        if(value==null || value.length()>65536) throw new ReplayFailure(INVALID_MANIFEST);
        try { return JSON.readValue(value,ReplayManifest.class); }
        catch(RuntimeException e) { throw new ReplayFailure(INVALID_MANIFEST); }
    }
}
