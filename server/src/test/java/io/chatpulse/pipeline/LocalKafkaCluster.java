package io.chatpulse.pipeline;

import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Uuid;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** #29/#31만 쓰는 소유권이 있는6노드 리허설. 공유 Compose 프로젝트와 이름·네트워크가 다르다. */
final class LocalKafkaCluster implements AutoCloseable {
    static final long MAX_CONTAINER_MEMORY = 3L * (384 + 512) * 1024 * 1024;
    private final String owner = UUID.randomUUID().toString().replace("-", "");
    private final String project = "jigeumini-ha-" + owner;
    private final Map<String,String> environment = new HashMap<>();
    private final Path compose;
    private final List<Integer> ports = new ArrayList<>();
    private boolean started, cleaned;
    private Path diskProbeClasses;private String diskProbeHash;
    private final Thread cleanupHook = new Thread(this::close, "ha-owned-cleanup");
    LocalKafkaCluster(Path repository) throws Exception {
        compose = repository.resolve("infra/kafka-ha.compose.yaml").toAbsolutePath();
        if (!Files.isRegularFile(compose)) throw new IllegalArgumentException("HA_COMPOSE_MISSING");
        environment.put("JIGEUMINI_HA_OWNER", owner);
        environment.put("JIGEUMINI_HA_CLUSTER_ID", Uuid.randomUuid().toString());
        var sockets = new ArrayList<ServerSocket>();
        try {
            for (int i=1;i<=3;i++) {
                var socket = new ServerSocket(0); sockets.add(socket); ports.add(socket.getLocalPort());
                environment.put("JIGEUMINI_HA_PORT_"+i,Integer.toString(socket.getLocalPort()));
            }
        } finally { for (var socket : sockets) socket.close(); }
    }
    String bootstrap() { return String.join(",",ports.stream().map(port->"localhost:"+port).toList()); }
    Map<String,Object> identity() {
        return Map.of("project",project,"owner",owner,"clusterId",environment.get("JIGEUMINI_HA_CLUSTER_ID"),
                "bootstrap",bootstrap(),"controllers",3,"brokers",3,"image","apache/kafka:4.1.2",
                "maximumContainerMemoryBytes",MAX_CONTAINER_MEMORY,"scope","ONE_HOST_SYNTHETIC_NO_AZ_GUARANTEE");
    }
    void start() throws Exception {
        if (started) throw new IllegalStateException("HA_ALREADY_STARTED");
        long memory=Long.parseLong(command(List.of("docker","info","--format","{{.MemTotal}}"),Map.of(),Duration.ofSeconds(10)).strip());
        if(memory<6L*1024*1024*1024||Files.getFileStore(compose).getUsableSpace()<2L*1024*1024*1024)
            throw new IllegalStateException("HA_RESOURCE_PREFLIGHT");
        double used=0;
        for(String line:command(List.of("docker","stats","--no-stream","--format","{{.MemUsage}}"),Map.of(),Duration.ofSeconds(10)).lines().toList()) {
            String amount=line.split(" / ")[0].strip();var match=java.util.regex.Pattern.compile("([0-9.]+)([KMGT]?i?B)").matcher(amount);
            if(!match.matches())throw new IllegalStateException("HA_MEMORY_OBSERVATION_UNAVAILABLE");
            int power=switch(match.group(2)){case "B"->0;case "kB","KB","KiB"->1;case "MB","MiB"->2;case "GB","GiB"->3;case "TB","TiB"->4;default->throw new IllegalStateException("HA_MEMORY_UNIT");};
            used+=Double.parseDouble(match.group(1))*Math.pow(match.group(2).contains("i")?1024:1000,power);
        }
        if(used+MAX_CONTAINER_MEMORY+(diskProbeClasses==null?0:128L*1024*1024)>memory*.9)throw new IllegalStateException("HA_MEMORY_HEADROOM");
        // UUID project 충돌을 확인한 뒤 자기 자원만 생성한다.
        if(!compose("ps","--all","--quiet").isBlank()) throw new IllegalStateException("HA_PROJECT_EXISTS");
        started=true;Runtime.getRuntime().addShutdownHook(cleanupHook);
        compose("up","-d","--no-build","--pull","never");
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);
        var admin=admin();try {
            boolean ready=false;
            while(System.nanoTime()<end) {
                try {
                    if(admin.describeCluster().nodes().get(3,TimeUnit.SECONDS).size()==3
                            &&admin.describeMetadataQuorum().quorumInfo().get(3,TimeUnit.SECONDS).voters().size()==3) {ready=true;break;}
                } catch(Exception pending) { if(pending instanceof InterruptedException)throw pending; }
                Thread.sleep(200);
            }
            if(!ready)throw new IllegalStateException("HA_CLUSTER_READY_TIMEOUT");
        } finally {admin.close(Duration.ofSeconds(1));}
        assertOwned();
    }
    Admin admin() { return Admin.create(Map.of("bootstrap.servers",bootstrap(),"default.api.timeout.ms",8000,"request.timeout.ms",3000)); }
    void stopBroker(int id) throws Exception { service("stop", "broker",id); }
    void startBroker(int id) throws Exception { service("start", "broker",id); }
    void stopController(int id) throws Exception { service("stop", "controller",id); }
    void startController(int id) throws Exception { service("start", "controller",id); }
    Map<String,Object> freezeDiskProbe(Path directory) throws Exception {
        if(started||diskProbeClasses!=null)throw new IllegalStateException("HA_DISK_PROBE_ALREADY_PREPARED");
        diskProbeClasses=Files.createDirectory(directory.toAbsolutePath());
        Path target=diskProbeClasses.resolve("io/chatpulse/pipeline/KafkaDiskProbe.class");Files.createDirectories(target.getParent());
        try(var input=KafkaDiskProbe.class.getResourceAsStream("KafkaDiskProbe.class")) {
            if(input==null)throw new IllegalStateException("HA_DISK_PROBE_MISSING");Files.copy(input,target);
        }
        diskProbeHash=HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(target)));
        return Map.of("class","io.chatpulse.pipeline.KafkaDiskProbe","sha256",diskProbeHash,"bytes",Files.size(target));
    }
    Map<String,Object> diskSnapshot(int id,String topic) throws Exception {
        var info=stoppedBroker(id);return probeVolume(volume(info,id),topic,id);
    }
    Map<String,Object> eraseBrokerDisk(int id,String topic) throws Exception {
        var info=stoppedBroker(id);String name=volume(info,id),container=info.path("Id").asText();
        String references=command(List.of("docker","ps","-a","--no-trunc","--filter","volume="+name,"--format","{{.ID}}"),Map.of(),Duration.ofSeconds(10)).strip();
        if(!references.equals(container))throw new IllegalStateException("HA_DISK_REFERENCED_ELSEWHERE");
        compose("rm","-f","broker"+id);
        command(List.of("docker","volume","rm",name),Map.of(),Duration.ofSeconds(10));
        if(!command(List.of("docker","volume","ls","--filter","name=^"+name+"$","--format","{{.Name}}"),Map.of(),Duration.ofSeconds(10)).isBlank())throw new IllegalStateException("HA_DISK_DELETE_UNCONFIRMED");
        command(List.of("docker","volume","create","--label","io.chatpulse.ha.owner="+owner,"--label","com.docker.compose.project="+project,"--label","com.docker.compose.volume=broker"+id+"-data",name),Map.of(),Duration.ofSeconds(10));
        var empty=probeVolume(name,topic,id);
        if(((Number)empty.get("directoryEntries")).longValue()!=0||!((Map<?,?>)empty.get("metadata")).isEmpty())throw new IllegalStateException("HA_NEW_DISK_NOT_EMPTY");
        return Map.of("broker",id,"volume",name,"deletionConfirmed",true,"emptyBeforeFormat",empty);
    }
    void recreateBroker(int id) throws Exception {
        if(!started||id<1||id>3)throw new IllegalArgumentException("HA_NODE");
        assertOwned();checkVolume(project+"_broker"+id+"-data",id);
        compose("up","-d","--no-deps","--no-build","--pull","never","broker"+id);
    }
    List<Map<String,Object>> brokerStates() throws Exception {
        var states=new ArrayList<Map<String,Object>>();
        for(int id=1;id<=3;id++){var info=brokerInfo(id);var state=info.path("State");states.add(Map.of("broker",id,"running",state.path("Running").asBoolean(),"exitCode",state.path("ExitCode").asInt(),"oomKilled",state.path("OOMKilled").asBoolean()));}
        return List.copyOf(states);
    }
    private JsonNode stoppedBroker(int id) throws Exception {
        var info=brokerInfo(id);if(info.path("State").path("Running").asBoolean())throw new IllegalStateException("HA_DISK_BROKER_RUNNING");return info;
    }
    private JsonNode brokerInfo(int id) throws Exception {
        if(!started||id<1||id>3)throw new IllegalArgumentException("HA_NODE");assertOwned();
        String cid=compose("ps","--all","--quiet","broker"+id).strip();if(!cid.matches("[a-f0-9]{64}"))throw new IllegalStateException("HA_DISK_CONTAINER_MISSING");
        var info=JsonMapper.builder().build().readTree(command(List.of("docker","inspect",cid),Map.of(),Duration.ofSeconds(10))).get(0);
        var labels=info.path("Config").path("Labels");
        if(!owner.equals(labels.path("io.chatpulse.ha.owner").asText())||!project.equals(labels.path("com.docker.compose.project").asText())
            ||!("broker"+id).equals(labels.path("com.docker.compose.service").asText()))throw new IllegalStateException("HA_DISK_CONTAINER_SCOPE");
        return info;
    }
    private String volume(JsonNode info,int id) throws Exception {
        String name=null;
        for(var mount:info.path("Mounts"))if(mount.path("Destination").asText().equals("/var/lib/kafka/data")) {
            if(name!=null||!mount.path("Type").asText().equals("volume"))throw new IllegalStateException("HA_DISK_MOUNT_SCOPE");name=mount.path("Name").asText();
        }
        checkVolume(name,id);return name;
    }
    private void checkVolume(String name,int id) throws Exception {
        if(!(project+"_broker"+id+"-data").equals(name))throw new IllegalStateException("HA_DISK_VOLUME_SCOPE");
        var labels=JsonMapper.builder().build().readTree(command(List.of("docker","volume","inspect",name),Map.of(),Duration.ofSeconds(10))).get(0).path("Labels");
        if(!owner.equals(labels.path("io.chatpulse.ha.owner").asText())||!project.equals(labels.path("com.docker.compose.project").asText())
            ||!("broker"+id+"-data").equals(labels.path("com.docker.compose.volume").asText()))throw new IllegalStateException("HA_DISK_VOLUME_OWNER");
    }
    @SuppressWarnings("unchecked")
    private Map<String,Object> probeVolume(String name,String topic,int id) throws Exception {
        checkVolume(name,id);String helper=project+"-disk-probe-"+id;
        Path classes=diskProbeClasses;
        if(classes==null||!diskProbeHash.equals(HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(classes.resolve("io/chatpulse/pipeline/KafkaDiskProbe.class"))))))throw new IllegalStateException("HA_DISK_PROBE_CHANGED");
        try {
            String output=command(List.of("docker","run","--pull","never","--name",helper,"--label","io.chatpulse.ha.owner="+owner,"--network","none","--read-only","--memory","128m","--cpus","0.5",
                "--mount","type=volume,src="+name+",dst=/data,readonly","--mount","type=bind,src="+classes+",dst=/probe,readonly","--entrypoint","java","apache/kafka:4.1.2","-Xmx64m","-XX:-UsePerfData","-cp","/opt/kafka/libs/*:/probe","io.chatpulse.pipeline.KafkaDiskProbe","/data",topic),Map.of(),Duration.ofSeconds(30));
            return JsonMapper.builder().build().readValue(output,Map.class);
        } finally {
            String ids=command(List.of("docker","ps","-a","--filter","name=^/"+helper+"$","--filter","label=io.chatpulse.ha.owner="+owner,"--format","{{.ID}}"),Map.of(),Duration.ofSeconds(10)).strip();
            if(!ids.isBlank())command(List.of("docker","rm","-f",helper),Map.of(),Duration.ofSeconds(10));
        }
    }
    private void service(String operation,String kind,int id) throws Exception {
        if(!started||id<1||id>3)throw new IllegalArgumentException("HA_NODE");
        assertOwned();
        if(operation.equals("stop"))compose("stop","--timeout","0",kind+id);else compose("start",kind+id);
    }
    private void assertOwned() throws Exception { assertOwned(false); }
    private void assertOwned(boolean allowEmpty) throws Exception {
        String ids=compose("ps","--all","--quiet").strip();
        if(ids.isEmpty()) {if(allowEmpty)return;throw new IllegalStateException("HA_RESOURCES_MISSING");}
        for(String id:ids.split("\\s+")) {
            String actual=command(List.of("docker","inspect","--format","{{index .Config.Labels \"io.chatpulse.ha.owner\"}}",id),Map.of(),Duration.ofSeconds(10)).strip();
            if(!owner.equals(actual))throw new IllegalStateException("HA_OWNER_MISMATCH");
        }
    }
    String compose(String... args) throws Exception {
        var command=new ArrayList<>(List.of("docker","compose","--project-name",project,"--file",compose.toString()));
        command.addAll(List.of(args));return command(command,environment,Duration.ofSeconds(30));
    }
    synchronized boolean cleaned() { return cleaned; }
    @Override public synchronized void close() {
        if(!started){cleaned=true;return;}
        if(cleaned)return;
        try {
            String helpers=command(List.of("docker","ps","-a","--filter","label=io.chatpulse.ha.owner="+owner,"--filter","name=^/"+project+"-disk-probe-[1-3]$","--format","{{.Names}}"),Map.of(),Duration.ofSeconds(10)).strip();
            if(!helpers.isBlank())for(String helper:helpers.split("\\s+")) {
                if(!helper.matches(java.util.regex.Pattern.quote(project)+"-disk-probe-[1-3]"))throw new IllegalStateException("HA_DISK_HELPER_SCOPE");
                command(List.of("docker","rm","-f",helper),Map.of(),Duration.ofSeconds(10));
            }
            assertOwned(true);compose("down","--timeout","2");
            String names=command(List.of("docker","volume","ls","--filter","label=io.chatpulse.ha.owner="+owner,"--format","{{.Name}}"),Map.of(),Duration.ofSeconds(10)).strip();
            if(!names.isEmpty())for(String name:names.split("\\s+")) {
                if(!name.matches(java.util.regex.Pattern.quote(project)+"_(controller|broker)[1-3]-data"))throw new IllegalStateException("HA_VOLUME_OWNER_MISMATCH");
                command(List.of("docker","volume","rm",name),Map.of(),Duration.ofSeconds(10));
            }
            cleaned=compose("ps","--all","--quiet").isBlank();
        } catch(Exception ignored) { cleaned=false; }
        try {Runtime.getRuntime().removeShutdownHook(cleanupHook);} catch(IllegalStateException ignored) { }
    }
    static String command(List<String> command,Map<String,String> environment,Duration timeout) throws Exception {
        var builder=new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().putAll(environment);var process=builder.start();
        var output=CompletableFuture.supplyAsync(()->{
            try(var input=process.getInputStream();var bytes=new ByteArrayOutputStream()) {
                byte[] buffer=new byte[4096];int count;
                while((count=input.read(buffer))!=-1) if(bytes.size()<65536)bytes.write(buffer,0,Math.min(count,65536-bytes.size()));
                return bytes.toString(StandardCharsets.UTF_8);
            }catch(Exception failure){throw new CompletionException(failure);}
        });
        try {
            if(!process.waitFor(timeout.toMillis(),TimeUnit.MILLISECONDS))throw new IllegalStateException("HA_COMMAND_TIMEOUT");
            if(process.exitValue()!=0)throw new IllegalStateException("HA_COMMAND_FAILED");
            return output.get(2,TimeUnit.SECONDS);
        } finally { if(process.isAlive())process.destroyForcibly();output.cancel(true); }
    }
}
