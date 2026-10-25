package io.chatpulse.poc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ChatProtocol {
    public static final int MAX_FRAME_BYTES = 1_048_576;
    private final JsonMapper json = JsonMapper.builder().build();

    public String connect(String room, String token) {
        var body = new LinkedHashMap<String, Object>();
        body.put("uid", null);
        body.put("devType", 2001);
        body.put("accTkn", token);
        body.put("auth", "READ");
        return json.writeValueAsString(Map.of("cmd", 100, "tid", 1, "ver", "2", "svcid", "game", "cid", room, "bdy", body));
    }

    public static String ping() { return "{\"cmd\":0,\"ver\":\"2\"}"; }
    public static String pong() { return "{\"cmd\":10000,\"ver\":\"2\"}"; }

    public Frame decode(String text, String channel, String room, UUID session, Instant received) {
        if (text.length() > MAX_FRAME_BYTES) throw new SourceFailure("FRAME_TOO_LARGE", false);
        JsonNode root;
        try { root = json.readTree(text); }
        catch (Exception e) { throw new SourceFailure("INVALID_JSON", false); }
        if (root == null || !root.isObject() || (!root.path("cmd").isIntegralNumber() || !root.path("cmd").canConvertToInt())) {
            throw new SourceFailure("INVALID_ENVELOPE", false);
        }
        int command = root.path("cmd").asInt();
        if (root.has("retCode") && root.path("retCode").asInt(-1) != 0) {
            throw new SourceFailure("PROTOCOL_REJECTED", true);
        }
        var body = root.path("bdy");
        if (command == 10100 && body.path("sid").asText("").isBlank()) {
            throw new SourceFailure("HANDSHAKE_MISSING_SID", true);
        }
        if (command == 94005 || command == 94006 || command == 94015) {
            throw new SourceFailure("PROTOCOL_ACCESS_ENDED", true);
        }
        if (command != 93101 && command != 93102 && command != 15101) {
            return new Frame(command, List.of(), Map.of(), Map.of());
        }
        JsonNode messages = body.isArray() ? body : body.path("messageList");
        if (!messages.isArray()) throw new SourceFailure("INVALID_MESSAGE_LIST", false);
        var events = new ArrayList<ChatEvent>();
        var skipped = new LinkedHashMap<String, Long>();
        var fields = new LinkedHashMap<String, Long>();
        for (var message : messages) {
            if (!message.isObject()) { increment(skipped, "INVALID_MESSAGE"); continue; }
            if (command == 15101) { increment(skipped, "HISTORICAL"); continue; }
            if (command == 93102) { increment(skipped, "DONATION"); continue; }
            var type = message.has("msgTypeCode") ? message.path("msgTypeCode") : message.path("messageTypeCode");
            if (!type.isIntegralNumber() || !type.canConvertToInt()) { increment(skipped, "INVALID_TYPE"); continue; }
            if (type.asInt() != 1) { increment(skipped, "NON_TEXT"); continue; }
            var timestamp = message.has("msgTime") ? message.path("msgTime") : message.path("messageTime");
            if (!timestamp.isIntegralNumber() || !timestamp.canConvertToLong()) {
                increment(skipped, "INVALID_TIME"); continue;
            }
            long millis = timestamp.asLong();
            // 단위를 추측해 초→밀리초로 바꾸지 않는다. 날짜가 범위 밖이면 격리 계수한다.
            if (millis < received.minusSeconds(7 * 86400).toEpochMilli()
                    || millis > received.plusSeconds(5).toEpochMilli()) {
                increment(skipped, "INVALID_TIME"); continue;
            }
            for (String name : List.of("msgId", "messageId", "msgTid", "serialNumber")) {
                if (message.hasNonNull(name)) increment(fields, name);
            }
            events.add(ChatEvent.live(channel, room, session, Instant.ofEpochMilli(millis), received));
        }
        return new Frame(command, List.copyOf(events), Map.copyOf(skipped), Map.copyOf(fields));
    }

    private static void increment(Map<String, Long> map, String key) { map.merge(key, 1L, Long::sum); }
    public record Frame(int command, List<ChatEvent> events, Map<String, Long> skipped,
            Map<String, Long> candidateIdFieldPresence) { }
}
