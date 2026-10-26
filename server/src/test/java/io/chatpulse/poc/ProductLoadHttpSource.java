package io.chatpulse.poc;

import java.net.URI;
import java.util.Set;

/** 실제 HTTP 파싱을 재사용하되 용량 실험의 목적지는 내부 fixture로 고정한다. */
public final class ProductLoadHttpSource {
    private ProductLoadHttpSource() { }
    public static ChatSource local(URI http,URI socket) {
        if(!"http".equals(http.getScheme()) || !"ws".equals(socket.getScheme())
                || !Set.of("synthetic-source","127.0.0.1").contains(http.getHost()) || !http.getHost().equals(socket.getHost()))
            throw new IllegalArgumentException("SYNTHETIC_SOURCE_ONLY");
        var delegate=new ChzzkHttpSource(http.toString(),http.toString());
        return new ChatSource() {
            public String resolveRoom(String channel){return delegate.resolveRoom(channel);}
            public String readToken(String room){return delegate.readToken(room);}
            public URI socketUri(String room){return socket;}
        };
    }
}
