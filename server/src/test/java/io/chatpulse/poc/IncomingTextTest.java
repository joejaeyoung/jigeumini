package io.chatpulse.poc;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class IncomingTextTest {
    @Test void unicodeAndSurrogatePairsAcrossChunksHaveTheSameBytesAsTheWholeMessage() {
        String text = "ASCII 한글 😀 끝";
        for (int split = 0; split <= text.length(); split++) {
            var incoming = new IncomingText();
            assertThat(incoming.accept(text.substring(0, split), false)).isNull();
            var result = incoming.accept(text.substring(split), true);
            assertThat(result.text()).isEqualTo(text);
            assertThat(result.bytes()).isEqualTo(text.getBytes(StandardCharsets.UTF_8).length);
            assertThat(result.toString()).doesNotContain(text);
        }
    }

    @Test void totalUtf8LimitIncludesAllChunksAndResetsAfterCompletionAndFailure() {
        var incoming = new IncomingText();
        String chunk = "한".repeat(2730);
        int bytes = chunk.getBytes(StandardCharsets.UTF_8).length;
        int count = ChatProtocol.MAX_FRAME_BYTES / bytes;
        for (int n = 0; n < count; n++) assertThat(incoming.accept(chunk, false)).isNull();
        var complete = incoming.accept("a".repeat(ChatProtocol.MAX_FRAME_BYTES - count * bytes), true);
        assertThat(complete.bytes()).isEqualTo(ChatProtocol.MAX_FRAME_BYTES);
        assertThat(incoming.accept("ok", true).bytes()).isEqualTo(2);
        assertThat(incoming.accept("a".repeat(ChatProtocol.MAX_FRAME_BYTES), false)).isNull();
        assertThatThrownBy(() -> incoming.accept("한", true)).hasMessage("FRAME_TOO_LARGE");
        assertThat(incoming.accept("새", true).text()).isEqualTo("새");
    }

    @Test void incompleteOrInvalidSurrogatesNeverCorruptByteAccounting() {
        var incoming = new IncomingText();
        assertThat(incoming.accept("\uD83D", false)).isNull();
        assertThatThrownBy(() -> incoming.accept("x", true)).hasMessage("INVALID_TEXT_ENCODING");
        assertThatThrownBy(() -> incoming.accept("\uDE00", true)).hasMessage("INVALID_TEXT_ENCODING");
        assertThatThrownBy(() -> incoming.accept("\uD83D", true)).hasMessage("INVALID_TEXT_ENCODING");
        assertThat(incoming.accept("😀", true).bytes()).isEqualTo(4);
    }

    @Test void closingAPartialMessageDiscardsItsState() {
        var incoming = new IncomingText();
        incoming.accept("discard \uD83D", false); incoming.clear();
        assertThat(incoming.accept("next", true).text()).isEqualTo("next");
    }
}
