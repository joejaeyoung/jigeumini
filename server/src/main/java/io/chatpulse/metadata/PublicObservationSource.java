package io.chatpulse.metadata;

@FunctionalInterface
public interface PublicObservationSource {
    PublicBroadcastObservation observe(String channelId);
    /** 로컬 권한/예산으로 HTTP를 요청하지 않았으므로 원천 오류 관측을 만들지 않는다. */
    final class Skipped extends RuntimeException {
        public Skipped(){super("OBSERVATION_REQUEST_SKIPPED",null,false,false);}
    }
}
