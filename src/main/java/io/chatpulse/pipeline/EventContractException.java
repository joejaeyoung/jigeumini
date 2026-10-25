package io.chatpulse.pipeline;

/** 원본 payload나 사용자 입력을 예외 문자열에 포함하지 않는다. */
public final class EventContractException extends RuntimeException {
    public EventContractException(String code) { super(code); }
}
