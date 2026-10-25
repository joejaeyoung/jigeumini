package io.chatpulse.pipeline;

/** 하위 DB/Kafka 예외의 본문·원본 입력은 listener 로그로 전달하지 않는다. */
final class ProjectionRetryException extends RuntimeException {
    ProjectionRetryException(String code) { super(code); }
}
