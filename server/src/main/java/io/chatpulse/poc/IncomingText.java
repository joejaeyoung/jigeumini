package io.chatpulse.poc;

/** 부분 수신을 UTF-8 전체 한도 안에서 조립한다. 완료·실패·종료 시 큰 버퍼를 보관하지 않는다. */
final class IncomingText {
    private StringBuilder parts;
    private int bytes;
    private boolean highSurrogate;

    synchronized Complete accept(String text, boolean last) {
        try {
            for (int n = 0; n < text.length(); n++) {
                char value = text.charAt(n);
                if (highSurrogate) {
                    if (!Character.isLowSurrogate(value)) throw invalid();
                    highSurrogate = false;
                } else if (Character.isHighSurrogate(value)) {
                    highSurrogate = true;
                    bytes += 4;
                } else if (Character.isLowSurrogate(value)) {
                    throw invalid();
                } else {
                    bytes += value <= 0x7f ? 1 : value <= 0x7ff ? 2 : 3;
                }
                if (bytes > ChatProtocol.MAX_FRAME_BYTES) throw new SourceFailure("FRAME_TOO_LARGE", false);
            }
            if (last && highSurrogate) throw invalid();
            if (parts == null && last) {
                var complete = new Complete(text, bytes); clear(); return complete;
            }
            if (parts == null) parts = new StringBuilder(Math.min(8192, ChatProtocol.MAX_FRAME_BYTES));
            parts.append(text);
            if (!last) return null;
            var complete = new Complete(parts.toString(), bytes); clear(); return complete;
        } catch (SourceFailure failure) {
            clear(); throw failure;
        }
    }

    synchronized void clear() { parts = null; bytes = 0; highSurrogate = false; }
    static Complete complete(String text) { return new IncomingText().accept(text, true); }
    private static SourceFailure invalid() { return new SourceFailure("INVALID_TEXT_ENCODING", false); }

    static final class Complete {
        private final String text;
        private final int bytes;
        private Complete(String text, int bytes) { this.text = text; this.bytes = bytes; }
        String text() { return text; }
        int bytes() { return bytes; }
        @Override public String toString() { return "Complete[bytes=" + bytes + "]"; }
    }
}
