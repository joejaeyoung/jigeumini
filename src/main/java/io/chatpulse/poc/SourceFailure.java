package io.chatpulse.poc;

/** 외부 응답·토큰이 예외 메시지로 새지 않는 진단 코드. */
public final class SourceFailure extends RuntimeException {
    private final boolean terminal;

    public SourceFailure(String code, boolean terminal) {
        super(code);
        this.terminal = terminal;
    }

    public boolean terminal() { return terminal; }
}
