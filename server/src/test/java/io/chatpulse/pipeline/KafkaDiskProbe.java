package io.chatpulse.pipeline;

import java.nio.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.apache.kafka.common.record.FileRecords;

/** 정지한 소유 replica의 readonly mount에서 CRC·offset·업무 bytes의 digest만 출력한다. */
public final class KafkaDiskProbe {
    public static void main(String[] args) throws Exception {
        Path root=Path.of(args[0]);String topic=args[1];
        if(!topic.matches("disk\\.[a-f0-9]{32}"))throw new IllegalArgumentException("DISK_TOPIC_SCOPE");
        var properties=new Properties();Path meta=root.resolve("meta.properties");
        if(Files.exists(meta))try(var input=Files.newInputStream(meta)){properties.load(input);}
        long entries;try(var files=Files.list(root)){entries=files.count();}
        var result=new StringBuilder("{\"directoryEntries\":").append(entries).append(",\"metadata\":{");
        boolean first=true;
        for(String key:List.of("cluster.id","node.id","directory.id")) {
            String value=properties.getProperty(key);if(value==null)continue;
            if(!value.matches("[A-Za-z0-9_-]{1,100}"))throw new IllegalStateException("DISK_METADATA_INVALID");
            if(!first)result.append(',');first=false;result.append('"').append(key).append("\":\"").append(value).append('"');
        }
        result.append("},\"records\":[");first=true;long total=0;
        for(int partition=0;partition<3;partition++) {
            Path directory=root.resolve(topic+"-"+partition);if(!Files.exists(directory))continue;
            List<Path> segments;try(var files=Files.list(directory)){segments=files.filter(p->p.getFileName().toString().matches("[0-9]{20}\\.log")).sorted().toList();}
            for(Path segment:segments) {
                var file=FileRecords.open(segment.toFile(),false);try {
                for(var batch:file.batches()) {
                    batch.ensureValid();
                    for(var record:batch) {
                        record.ensureValid();if(++total>1000)throw new IllegalStateException("DISK_RECORD_LIMIT");
                        if(!first)result.append(',');first=false;
                        result.append("{\"partition\":").append(partition).append(",\"offset\":").append(record.offset())
                                .append(",\"leaderEpoch\":").append(batch.partitionLeaderEpoch()).append(",\"control\":").append(batch.isControlBatch())
                                .append(",\"sha256\":\"").append(digest(record.key(),record.value())).append("\"}");
                    }
                }
                } finally {file.closeHandlers();}
            }
        }
        result.append("],\"crcValid\":true}");System.out.println(result);
    }
    static String digest(ByteBuffer key,ByteBuffer value) throws Exception {
        var digest=MessageDigest.getInstance("SHA-256");
        for(ByteBuffer part:new ByteBuffer[]{key,value}) {
            digest.update(ByteBuffer.allocate(4).putInt(part==null?-1:part.remaining()).array());
            if(part!=null)digest.update(part.duplicate());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
