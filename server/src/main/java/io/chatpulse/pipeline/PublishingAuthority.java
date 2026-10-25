package io.chatpulse.pipeline;

/** admission에서 받은 동일 permit을 dispatch 직전 다시 확인한다. broker 자체의 fencing이 아니다. */
@FunctionalInterface
public interface PublishingAuthority {
    @FunctionalInterface interface Permit { void check(); }
    Permit acquire(String channelId);
    PublishingAuthority UNRESTRICTED = channel -> () -> { };
}
