package io.chatpulse.replay;

/** 출력 가능한 고정 코드만 사용한다. 원본 예외·payload는 연결하지 않는다. */
public final class ReplayFailure extends RuntimeException {
    public enum Code { INVALID_MANIFEST, TOPIC_CHANGED, PARTITIONS_CHANGED, RETENTION_EXPIRED, OFFSET_TRUNCATED,
        OFFSET_GAP, SOURCE_UNAVAILABLE, INVALID_EVENT, DB_UNAVAILABLE, CANCELLED, OWNER_LOST, WORKER_LOST,
        INTERNAL_ERROR, NO_PROGRESS, TIME_LIMIT, INCOMPATIBLE_RUNS, RUN_NOT_FOUND, WORKER_BUSY, INVALID_COMMAND, INVALID_DATE, TOPIC_NOT_FOUND, BUILD_UNAVAILABLE,
        RESULT_EXPIRED, QUEUE_FULL }
    private final Code code;
    public ReplayFailure(Code code) { super(code.name()); this.code = code; }
    public Code code() { return code; }
}
