package io.chatpulse.observations;

/** 같은 검증된 이력 토픽을 독립 projection들이 구독한다. */
public record ObservationTopic(String name) { }
