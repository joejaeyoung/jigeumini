package io.chatpulse.pipeline;

import java.nio.file.*;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

final class ExperimentResources {
    static final long MIN_FREE=2L*1024*1024*1024,MAX_DATA=2L*1024*1024*1024;
    private static final JsonMapper JSON=JsonMapper.builder().build();
    static void preflight(Path directory) throws Exception {
        if(Files.getFileStore(directory).getUsableSpace()<MIN_FREE||available("kafka","/var/lib/kafka/data")<MIN_FREE||available("postgres","/var/lib/postgresql/data")<MIN_FREE)
            throw new IllegalStateException("EXPERIMENT_LOW_DISK");
    }
    private static long available(String service,String directory) throws Exception {
        String[] lines=command(List.of("docker","compose","exec","-T",service,"df","-Pk",directory)).strip().split("\n");
        return Long.parseLong(lines[lines.length-1].trim().split("\\s+")[3])*1024;
    }
    static Map<String,Object> sample(Admin admin,String topic,JdbcTemplate jdbc,String schema,Path directory) throws Exception {
        var result=new LinkedHashMap<String,Object>();
        long kafka=admin.describeLogDirs(List.of(1)).allDescriptions().get(8,TimeUnit.SECONDS).values().stream().flatMap(m->m.values().stream()).flatMap(d->d.replicaInfos().entrySet().stream()).filter(e->e.getKey().topic().equals(topic)).mapToLong(e->e.getValue().size()).sum();
        long db=jdbc.queryForObject("SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname=? AND c.relkind IN ('r','m')",Long.class,schema);
        if(kafka+db>MAX_DATA)throw new IllegalStateException("EXPERIMENT_DATA_LIMIT");preflight(directory);
        result.put("kafkaTopicBytes",kafka);result.put("databaseSchemaBytes",db);result.put("hostFreeBytes",Files.getFileStore(directory).getUsableSpace());
        var os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        result.put("clientCpuLoad",os.getProcessCpuLoad());result.put("clientCpuNanos",os.getProcessCpuTime());result.put("clientHeapUsedBytes",ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        result.put("clientRssBytes",Long.parseLong(command(List.of("ps","-o","rss=","-p",Long.toString(ProcessHandle.current().pid()))).trim())*1024);
        var services=new ArrayList<Map<String,String>>();
        for(String line:command(List.of("docker","stats","--no-stream","--format","{{json .}}","chat-pulse-kafka-1","chat-pulse-postgres-1")).strip().split("\n")) {
            var value=JSON.readTree(line);services.add(Map.of("name",value.path("Name").asText(),"cpuPercent",value.path("CPUPerc").asText(),"memory",value.path("MemUsage").asText(),"blockIo",value.path("BlockIO").asText()));
        }
        result.put("services",services);return result;
    }
    static String command(List<String> args) throws Exception {
        var process=new ProcessBuilder(args).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        if(!process.waitFor(10,TimeUnit.SECONDS)) {process.destroyForcibly().waitFor();throw new IllegalStateException("EXPERIMENT_RESOURCE_TIMEOUT");}
        byte[] bytes=process.getInputStream().readNBytes(65536);if(process.exitValue()!=0)throw new IllegalStateException("EXPERIMENT_RESOURCE_UNAVAILABLE");return new String(bytes,java.nio.charset.StandardCharsets.UTF_8);
    }
}
