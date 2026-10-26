package io.chatpulse.poc;

import java.time.Instant;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

/** 수신량에 비례해 늘어나지 않는 집계만 보관한다. */
public final class ProbeStats {
    private final String channel;
    private final Instant startedAt;
    private final Map<String, Long> commands = new LinkedHashMap<>();
    private final Map<String, Long> skipped = new LinkedHashMap<>();
    private final Map<String, Long> idFields = new LinkedHashMap<>();
    private final Map<String, Long> reasons = new LinkedHashMap<>();
    private final Map<String, Long> closeCodes = new LinkedHashMap<>();
    private final Map<String, Long> handshakeFailures = new LinkedHashMap<>();
    private long messages, afterReconnect, frames, bytes, maxFrameBytes, attempts, connections, pings, pongs;
    private long lateMessages, totalDelayMillis, maxDelayMillis;
    private Instant firstEventTime, lastEventTime;
    private String state = "STARTING";
    private final long startNano = System.nanoTime();
    private long connectedSinceNano, connectedNanos;

    public ProbeStats(String channel) {this(channel,Clock.systemUTC());}
    public ProbeStats(String channel,Clock clock) {this.channel=channel;this.startedAt=clock.instant();}
    public synchronized void attempt() { attempts++; state = "CONNECTING"; }
    public synchronized void connected() { connections++; state = "CONNECTED"; connectedSinceNano = System.nanoTime(); }
    public synchronized void disconnected() {
        if (connectedSinceNano != 0) {
            connectedNanos += System.nanoTime() - connectedSinceNano;
            connectedSinceNano = 0;
        }
    }
    public synchronized void ping() { pings++; }
    public synchronized void pong() { pongs++; }
    synchronized void socketClosed(int code) {
        String key = code >= 1000 && code <= 4999 ? Integer.toString(code) : "OTHER";
        if (!closeCodes.containsKey(key) && closeCodes.size() >= 32) key = "OTHER";
        closeCodes.merge(key, 1L, Long::sum);
    }
    synchronized void handshakeFailed(boolean waitingForApproval, boolean timeout) {
        String key = (waitingForApproval ? "APPROVAL_" : "CONNECT_") + (timeout ? "TIMEOUT" : "FAILURE");
        handshakeFailures.merge(key, 1L, Long::sum);
    }
    public synchronized void reason(String reason) {
        // 진단 코드는 코드가 소유하며 외부 메시지를 key로 사용하지 않는다.
        reasons.merge(reason, 1L, Long::sum);
        state = reason;
    }
    public synchronized void accept(ChatProtocol.Frame frame, int byteCount) {
        frames++;
        bytes += byteCount;
        maxFrameBytes = Math.max(maxFrameBytes, byteCount);
        String command = Integer.toString(frame.command());
        if (!commands.containsKey(command) && commands.size() >= 32) command = "OTHER";
        commands.merge(command, 1L, Long::sum);
        frame.skipped().forEach((key, value) -> skipped.merge(key, value, Long::sum));
        frame.candidateIdFieldPresence().forEach((key, value) -> idFields.merge(key, value, Long::sum));
        for (var event : frame.events()) {
            messages++;
            if (connections >= 2) afterReconnect++;
            if (firstEventTime == null || event.eventTime().isBefore(firstEventTime)) firstEventTime = event.eventTime();
            if (lastEventTime == null || event.eventTime().isAfter(lastEventTime)) lastEventTime = event.eventTime();
            long delay = event.receivedAt().toEpochMilli() - event.eventTime().toEpochMilli();
            totalDelayMillis += delay;
            maxDelayMillis = Math.max(maxDelayMillis, delay);
            if (delay > 120_000) lateMessages++;
        }
    }

    public synchronized Map<String, Object> snapshot(boolean reconnectRequired) {
        var result = new LinkedHashMap<String, Object>();
        boolean connected = connections > 0;
        boolean chat = messages > 0;
        boolean heartbeat = pongs > 0;
        boolean reconnected = connections > 1 && afterReconnect > 0;
        String verdict = "DURATION_COMPLETE".equals(state) && connected && chat && heartbeat && (!reconnectRequired || reconnected)
                ? "PASS" : connected && !chat ? "INCONCLUSIVE_NO_CHAT" : "FAIL";
        result.put("channelId", channel);
        result.put("startedAt", startedAt.toString());
        result.put("state", state);
        result.put("verdict", verdict);
        result.put("checks", Map.of("handshake", connected, "liveChat", chat, "heartbeatPong", heartbeat,
                "reconnectRequired", reconnectRequired, "chatAfterReconnect", reconnected));
        long elapsed = System.nanoTime() - startNano;
        long connectedTime = connectedNanos + (connectedSinceNano == 0 ? 0 : System.nanoTime() - connectedSinceNano);
        result.put("elapsedSeconds", elapsed / 1_000_000_000.0);
        result.put("connectedSeconds", connectedTime / 1_000_000_000.0);
        result.put("connectionTimeRatio", elapsed == 0 ? 0 : (double) connectedTime / elapsed);
        result.put("attempts", attempts);
        result.put("connections", connections);
        result.put("chatCount", messages);
        result.put("chatAfterReconnect", afterReconnect);
        result.put("frames", frames);
        result.put("receivedFrameBytes", bytes);
        result.put("maxFrameBytes", maxFrameBytes);
        result.put("clientPings", pings);
        result.put("serverPongs", pongs);
        result.put("commands", Map.copyOf(commands));
        result.put("skipped", Map.copyOf(skipped));
        result.put("reasons", Map.copyOf(reasons));
        result.put("closeCodes", Map.copyOf(closeCodes));
        result.put("handshakeFailures", Map.copyOf(handshakeFailures));
        result.put("candidateIdFieldPresence", Map.copyOf(idFields));
        result.put("firstEventTime", firstEventTime == null ? null : firstEventTime.toString());
        result.put("lastEventTime", lastEventTime == null ? null : lastEventTime.toString());
        result.put("lateMessagesOver120s", lateMessages);
        result.put("meanSourceToReceiveMillis", messages == 0 ? null : (double) totalDelayMillis / messages);
        result.put("maxSourceToReceiveMillis", maxDelayMillis);
        return result;
    }
}
