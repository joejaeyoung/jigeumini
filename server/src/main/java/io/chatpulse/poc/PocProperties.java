package io.chatpulse.poc;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("poc")
public record PocProperties(
        List<String> channels,
        @DefaultValue("60s") Duration duration,
        @DefaultValue("0s") Duration reconnectAfter,
        @DefaultValue("work/poc-result.json") Path output) {
    public PocProperties {
        channels = channels == null ? List.of() : List.copyOf(channels);
    }

    public void validate() {
        if (channels.isEmpty() || channels.size() > 3 || channels.stream().distinct().count() != channels.size()
                || channels.stream().anyMatch(c -> !c.matches("[a-f0-9]{32}"))) {
            throw new IllegalArgumentException("poc.channels: 서로 다른 공개 채널 ID 1~3개가 필요합니다 (32자리 소문자 hex)");
        }
        if (duration.compareTo(Duration.ofSeconds(1)) < 0 || duration.compareTo(Duration.ofMinutes(30)) > 0) {
            throw new IllegalArgumentException("poc.duration: 1s~30m 범위여야 합니다");
        }
        if (reconnectAfter.isNegative() || (!reconnectAfter.isZero()
                && reconnectAfter.plusSeconds(15).compareTo(duration) >= 0)) {
            throw new IllegalArgumentException("poc.reconnect-after: 0s 또는 종료보다 최소 15초 앞선 시각이어야 합니다");
        }
    }
}
