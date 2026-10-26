# Spring 수집 PoC — 프로토콜과 구현 결정

2026-10-27. 이 문서는 현재 구현 기준이며 [제품 HLD](../design/HLD.md)는 다음 단계까지의 목표 설계다.

## 수신 경로

1. `GET https://api.chzzk.naver.com/polling/v2/channels/{channelId}/live-status`에서 OPEN과 공개 방송 여부, chatChannelId 확인.
2. `GET https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={room}&chatType=STREAMING`에서 익명 채팅 토큰 발급.
3. room 문자 코드 합 `% 9 + 1`로 `wss://kr-ss{n}.chat.naver.com/chat` 연결.
4. cmd=100, uid=null, auth=READ 전송. cmd=10100의 sid 확인 후 수신 인정.
5. cmd=93101의 배열 또는 bdy.messageList를 메시지별로 분류한다. msgTypeCode/messageTypeCode=1이 일반 채팅이다.

Spring RestClient와 StandardWebSocketClient를 사용한다. STOMP나 공식 API의 OAuth·Socket.IO를 사용하지 않는다.
토큰은 수신 연결을 위한 프로토콜 필드이며 파일·로그에 남기지 않는다. HTTP 리다이렉트는 따라가지 않고 로그인 쿠키도 보내지 않는다.

## 분류·정규화

| 입력 | 동작 |
|---|---|
| 93101 + 일반 채팅 type=1 | 발생 시각 검사 후 내부 UUID와 ChatEvent 생성, 통계에 반영 |
| 15101 최근 이력 | HISTORICAL로 제외; 최근 이력 요청 자체는 보내지 않음 |
| 93102 후원 | DONATION으로 제외 |
| 일반 채팅 배열 안의 다른 type | NON_TEXT로 제외 |
| 잘못된 시각·타입 | 건수를 계수하고 해당 메시지 제외 |
| malformed frame·잘못된 envelope | 연결 실패로 기록, 제한된 재접속 |
| retCode 오류·접근 종료 명령 | 해당 채널 검증 중단 |
| 그 밖의 command | 종류·건수만 관측 |

msgTime/messageTime의 정수 epoch milliseconds를 사용한다. 현재보다 5초 초과 미래 또는 7일 초과 과거면 잘못된 시각으로 제외한다.
초 단위를 밀리초로 추측 변환하지 않는다. UUID는 내부 식별자이며 원천 메시지의 재전송 중복을 제거하지 않는다.
원문·profile·extras는 정규화 이벤트에 복사하지 않는다. msgId/messageId/msgTid/serialNumber는 존재 건수만 기록하고 값은 보존하지 않는다.

## 연결·자원 경계

- HTTP connect timeout 5초, read timeout 10초, HTTP 응답 최대 1MiB.
- WS 연결 대기 10초, 프로토콜 승인 대기 10초, 완성 text 메시지 최대 UTF-8 1MiB, 송신 timeout 5초.
  transport는8,192문자 청크로 수신하고 `IncomingText`가 조각 전체의 byte 수를 제한한다.
  UTF-8 byte/UTF-16 surrogate 경계와 마지막 조각을 구분하며 완성된 JSON만 한 번 처리·계수한다.
  누적 초과는 FRAME_TOO_LARGE, 잘못된 surrogate는 INVALID_TEXT_ENCODING으로 실패·손실 기록한다.
  완성·실패·연결 종료 때 조립 버퍼를 버리며 이전 연결의 부분 JSON을 재사용하지 않는다.
  실패가 확정되면 즉시 수신 처리를 비활성화한다. 실제 socket close 전 도착한 후속 조각·완성 메시지도 처리·발행하지 않는다.
- `ChatSocketClient`는 ChannelProbe 실행당 소유 Tomcat container를 닫고 공유 IO group 등록을 해제한다.
  재접속은 같은 owner 안에서 진행하며 종료와 겹쳐 늦게 완료된 연결도 닫는다.
  다른 실행의 container·연결은 유지한다. 메시지별 조립 한도만 있으며 연결 전체의 조립 메모리 예산은 없다.
  여러 연결에 큰 메시지가 동시에 오면 실행 중 heap 부족이 가능하며 T11 후속 검증 범위다.
- 독립 heartbeat scheduler에서 20초마다 JSON ping을 전송하고 10초 안에 pong을 기다린다.
  채팅이 계속 들어와도 pong이 없으면 timeout이다. 서버 ping에는 즉시 pong을 보낸다.
- 채팅방 상태를 30초마다 다시 확인하고 room 변경 시 토큰부터 재발급한다.
  HTTP 조회가 heartbeat scheduler를 막지 않는다.
- 연결 종료·일시 오류에는 1/2/4/8/16/30초 상한 backoff, 채널당 최대 6회 연결 시도.
  PoC는 소수 채널·유한 실행이므로 jitter·공통 rate limiter는 후속 collector에서 추가한다.
- OFFLINE, 연령 제한, HTTP 4xx/API 접근 거절은 중단한다. 토큰·로그인 경로를 바꿔 재시도하지 않는다.
- 통계는 합계·최댓값·제한된 종류별 counter만 보관한다. 이벤트 목록을 장기 보관하지 않는다.
  연결/승인 대기 실패는 원래 실패를 확정한 뒤 소켓을 정리한다. 정리 callback의 WS_CLOSED로 원인을 덮지 않는다.
  close code는 숫자 최대32종과 OTHER, handshake 실패는 CONNECT/APPROVAL과 TIMEOUT/FAILURE의4종으로 집계한다.
  외부 close reason·예외 본문·토큰은 보관하지 않는다. 종료 code 자체만으로 종료 원인을 확정하지 않는다.
- `scripts/poc`는 저장소 루트의 `work/poc.lock`으로 중복 실행을 막는다. 직접 JAR를 실행할 때는 작업 디렉터리 기준이다. 다른 디렉터리·호스트의 lease/fencing은 범위 밖이다.

성공은 실제 관측한 채팅·heartbeat·재접속의 기능 검증이다. 알려지지 않은 원천 누락과 재전송, 강제 재접속 공백을 없애는 보장은 없다.

## 제품 품질 관측 연결

수신 프로토콜과 READ/heartbeat 송신은 유지한다. pipeline collector에서만 `CollectionObserver`가 승인·원천 생존·종료·메시지 손실을 관측하고 별도 worker가 DB에 기록한다. 일반 PoC는 기본 무동작 observer를 사용해 DB 없이 실행된다. WebSocket callback에서 DB I/O를 수행하지 않으며 메타데이터나 채팅 원문을 관측 객체에 전달하지 않는다. [품질 계약](../api/QUALITY_RANKINGS.md)을 참고한다.

관측 수신자는 여러 개를 결합한다. 시청자 표본 수집기는 채널 start/stopped에서 활성 집합만 바꾸고 별도 worker가 공개 HTTP를 조회한다. 따라서 파일 잠금 획득 전이나 중단된 채널에서는 새 요청을 예약하지 않는다. READ/heartbeat 송신·채팅 분류·재접속 계약은 그대로다.

## 검증 계층

- `ChatProtocolTest`: 합성 frame의 배치·역사·후원·타입·시각·크기·식별정보 제거.
- `ChzzkHttpSourceTest`: 실제 loopback HTTP로 URL·쿠키 미사용·오프라인·제한·오류 정보 비노출.
- `ChannelProbeTest`: 실제 loopback WebSocket으로 READ 승인·서버/클라이언트 heartbeat·강제/서버 종료 재접속·room 변경·승인 거절·취소 시 종료.
- `IncomingTextTest`·`ChannelProbeTextTest`: UTF-8/surrogate 중간 분할·정확히1MiB 수신·누적 초과·부분 JSON 중단 후 재접속. `ChatSocketClientTest`는 owner 종료 뒤 늦은 연결 정리와 다른 owner 보존을 확인한다.
- `scripts/collector-memory`: 별도 Xmx64MiB JVM에서 다중 실제 loopback 연결·heartbeat·종료 후 heap/FD/IO 등록을 측정한다. 전체 제품·공개 원천 용량과 구분한다.
- `scripts/poc`: 실제 공개 방송으로 소스 가용성과 현재 프로토콜 확인. 위 로컬 테스트로 live 성공을 대체하지 않는다.

## 근거

- [Node 구현 검토](../chzzk-unofficial-source-review.md), [Go 구현 검토](../chzzk-go-source-review.md).
- [Node 채팅 구현](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts).
- [Go 채팅 codec](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat_codec.go).
- [Spring Boot 4.0 요구사항](https://docs.spring.io/spring-boot/4.0/system-requirements.html), [Spring WebSocket API](https://docs.spring.io/spring-framework/reference/web/websocket/server.html).

기존 라이브러리 코드를 의존성으로 포함하거나 번역 복제하지 않고, 관찰한 wire contract에 맞춰 최소 READ 클라이언트를 구현했다.

### heartbeat 회귀 시험의 관측 대기

PR #23 CI40204570648의 pong 미응답 시험은10.046초 뒤 WS_CLOSED로 끝나 HEARTBEAT_TIMEOUT을 관측하지 못했다. 해당 연결 종료의 내부 원인은 재현하지 못했다. 별도로 fixture의 승인 응답을1.2초 지연하면 기존1초 전체실행 예산이 heartbeat 검사 전에 끝나는 결함을 재현했다. 시험은 최대30초 실행 안에서 최대20초 동안 실제 채팅·client ping·pong0·HEARTBEAT_TIMEOUT을 기다린 뒤 종료한다. 승인 지연을 회귀 입력으로 유지하고 실패를 단순 재실행으로 처리하지 않는다. 수신 프로토콜·운영 heartbeat 간격/제한은 바꾸지 않았다.

### 수집기 임대 확인 #28

fleet 경로는 HTTP resolve/token/WS 연결 전, 연결감독·frame수락·Kafka dispatch 직전에 로컬 임대permit을 확인한다. ChannelProbe는 원천 호출 전에 EventSink 건강검사를 추가하며 wire contract·READ/heartbeat·수신 분류는 유지한다. [임대 계약](../fleet/README.md)의 캐시 기한·인계/중복 한계를 따른다. ChannelProbeTest는 이미 상실한 임대에서 HTTP/WS 호출0회를 검증한다. 기존 ChatProtocolTest/ChzzkHttpSourceTest 입력 계약은 그대로 실행한다.
