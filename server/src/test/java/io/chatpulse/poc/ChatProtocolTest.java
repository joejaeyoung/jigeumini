package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class ChatProtocolTest {
    private final ChatProtocol protocol = new ChatProtocol();
    private final Instant received = Instant.ofEpochMilli(1792897201000L);
    private final UUID session = UUID.randomUUID();

    private ChatProtocol.Frame decode(String text) {
        return protocol.decode(text, "test-channel", "test-room", session, received);
    }

    @Test
    void anonymousHandshakeCarriesOnlyReadAuthority() {
        var json = JsonMapper.builder().build().readTree(protocol.connect("room", "test-token"));
        assertThat(json.path("cmd").asInt()).isEqualTo(100);
        assertThat(json.path("bdy").path("auth").asText()).isEqualTo("READ");
        assertThat(json.path("bdy").path("uid").isNull()).isTrue();
    }

    @Test
    void batchSeparatesChatDonationSystemAndInvalidTimeWithoutRetainingPrivateFields() throws Exception {
        String fixture;
        try (var stream = getClass().getResourceAsStream("/fixtures/live-batch.json")) {
            fixture = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
        var frame = decode(fixture);
        assertThat(frame.events()).hasSize(2);
        assertThat(frame.skipped()).containsEntry("NON_TEXT", 2L).containsEntry("INVALID_TIME", 1L);
        assertThat(frame.events().getFirst().eventTime()).isEqualTo(Instant.ofEpochMilli(1792897200120L));
        assertThat(frame.events().getFirst().collectionSessionId()).isEqualTo(session);
        String serialized = JsonMapper.builder().build().writeValueAsString(frame);
        assertThat(serialized).doesNotContain("PRIVATE_", "profile", "nickname", "accTkn", "extras");
    }

    @Test
    void historyNeverBecomesLiveChat() {
        var frame = decode("{\"cmd\":15101,\"bdy\":{\"messageList\":[{\"msgTypeCode\":1,\"msgTime\":1792897200120}]}}");
        assertThat(frame.events()).isEmpty();
        assertThat(frame.skipped()).containsEntry("HISTORICAL", 1L);
    }

    @Test
    void donationCommandCannotBeCountedEvenWhenBodyLooksLikeText() {
        var frame = decode("{\"cmd\":93102,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":1792897200120}]}");
        assertThat(frame.events()).isEmpty();
        assertThat(frame.skipped()).containsEntry("DONATION", 1L);
    }

    @Test
    void rejectsSecondsFutureAndMissingTimesWithoutGuessing() {
        var frame = decode("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":1792897200},"
                + "{\"msgTypeCode\":1,\"msgTime\":1792897206001},{\"msgTypeCode\":1}]}");
        assertThat(frame.events()).isEmpty();
        assertThat(frame.skipped()).containsEntry("INVALID_TIME", 3L);
    }

    @Test
    void acceptsFutureToleranceBoundaryAndTracksIdFieldPresenceWithoutCopyingValues() {
        var frame = decode("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":1792897206000,\"msgId\":\"PRIVATE_ID\"}]}");
        assertThat(frame.events()).hasSize(1);
        assertThat(frame.events().getFirst().sourceMessageId()).isNull();
        assertThat(frame.candidateIdFieldPresence()).containsEntry("msgId", 1L);
    }

    @Test
    void invalidEnvelopeDoesNotBecomePing() {
        for (String raw : new String[]{"{}", "null", "{\"cmd\":4295060397}", "{\"cmd\":\"0\"}", "bad PRIVATE_MESSAGE_CANARY"}) {
            assertThatThrownBy(() -> decode(raw)).isInstanceOf(SourceFailure.class)
                    .hasMessageNotContaining("PRIVATE_MESSAGE_CANARY");
        }
    }

    @Test
    void connectedResponseMustIncludeSessionAndNoRejection() {
        assertThatThrownBy(() -> decode("{\"cmd\":10100,\"bdy\":{}}"))
                .hasMessage("HANDSHAKE_MISSING_SID");
        assertThatThrownBy(() -> decode("{\"cmd\":10100,\"retCode\":403,\"retMsg\":\"PRIVATE_TOKEN\"}"))
                .hasMessage("PROTOCOL_REJECTED");
    }

    @Test
    void frameSizeIsBounded() {
        assertThatThrownBy(() -> decode("x".repeat(ChatProtocol.MAX_FRAME_BYTES + 1)))
                .hasMessage("FRAME_TOO_LARGE");
        assertThatThrownBy(() -> decode("한".repeat(ChatProtocol.MAX_FRAME_BYTES / 3 + 1)))
                .hasMessage("FRAME_TOO_LARGE");
        assertThat(decode("{\"cmd\":0}" + " ".repeat(ChatProtocol.MAX_FRAME_BYTES - 9)).command()).isZero();
    }

    @Test
    void overflowingMessageTypeCannotWrapIntoTextType() {
        var frame = decode("{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":4294967297,\"msgTime\":1792897200120}]}");
        assertThat(frame.events()).isEmpty();
        assertThat(frame.skipped()).containsEntry("INVALID_TYPE", 1L);
    }

    @Test
    void noUpstreamDedupClaimForRepeatedSourcePayload() {
        String text = "{\"cmd\":93101,\"bdy\":[{\"msgTypeCode\":1,\"msgTime\":1792897200120}]}";
        assertThat(decode(text).events().getFirst().eventId())
                .isNotEqualTo(decode(text).events().getFirst().eventId());
    }
}
