# chzzk-go 비공식 채팅 수집 코드 검토

검토일: 2026-10-25. 대상은 `sdkim96/chzzk-go`의 고정 커밋 `3ac96faac23536fe99f5f519b0a68b280f8667e8`이다. 체크아웃 커밋의 기록 시각은 **2026-08-30 17:05:52 KST**이며 비공식 채팅 코드 변경을 포함한다. 얕은 복제이므로 이 날짜로 전체 유지보수 이력을 단정하지 않는다. [커밋](https://github.com/sdkim96/chzzk-go/commit/3ac96faac23536fe99f5f519b0a68b280f8667e8)

상위 경로와 저장소에 적용할 `AGENTS.md`가 없음을 확인했다. 소스만 읽었으며 라이브 연결, 코드 실행, 테스트 실행은 하지 않았다.

## 결론

**로그인이나 스트리머 OAuth 동의 없이 공개 채팅을 읽으려는 구현이 실제로 존재한다.** 공식 API와 다른 내부 HTTP·WebSocket 경로를 사용한다. 따라서 공식 API의 채널별 동의 제한만을 근거로 기술적으로 불가능하다고 결론 내리면 안 된다. 다만 이 소스의 존재는 현재 서비스에서 동작한다는 실측이나 운영 허가를 의미하지 않는다.

## 소스에서 확인한 연결 흐름

1. `https://api.chzzk.naver.com/polling/v2/channels/{channelID}/live-status`에서 `chatChannelId`를 얻는다. 라이브 API의 숫자 `liveId`와 구분해야 한다. 이 라이브러리의 변수 이름 `liveID`는 실제로 채팅 채널 ID다. [live.go 16–46](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/live.go#L16-L46)
2. `https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={chatChannelId}&chatType=STREAMING`에서 채팅용 토큰을 얻는다. 공식 개발자 OAuth 토큰과 별개다. [chat.go 147–178](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L147-L178)
3. `ReadOnlyConnect`는 로그인 검사를 하지 않고, 연결 시 `uid: null`, `auth: READ`와 앞서 받은 채팅 토큰을 전달한다. 반면 쓰기용 `Connect`는 인증된 사용자 ID를 검사한다. [읽기·쓰기 진입점](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L74-L145), [핸드셰이크](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L270-L307)
4. `wss://kr-ss{1..9}.chat.naver.com/chat`에 연결하며 서버 번호는 채팅 채널 ID 문자값 합을 9로 나눈 나머지에 1을 더해 계산한다. 일반 WebSocket JSON 프로토콜이고 공식 세션 API의 Socket.IO 경로와 다르다. [연결](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L181-L195), [서버 선택](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L311-L320)

## 장시간 수집에 필요한 보완

| 항목 | 소스 사실 | 프로젝트에 미치는 영향 |
|---|---|---|
| Ping/Pong | `cmd=0`에 `10000`으로 응답한다. | 기본 heartbeat 응답은 구현되어 있다. |
| 재접속 | 읽기 오류가 나면 반환한다. 내부 재접속·토큰 갱신 루프가 없다. | 애플리케이션에서 재조회, 재연결, 지수 backoff, 수집 공백을 관리해야 한다. |
| 버퍼 압박 | 메시지 전달과 Ping 처리 모두 같은 읽기 루프에 있다. `recv` 전달이 막히면 다음 프레임을 읽지 못한다. | Kafka 송신 지연이 heartbeat 처리까지 막을 가능성이 있다. bounded buffer와 지연 관찰이 필요하다. |
| 최근 채팅 | 실시간 `93101`과 최근 채팅 `15101` 응답을 해석하지만 최근 채팅 요청·페이지네이션·resume offset 구현은 없다. | 과거 기록 복구나 누락 없는 backfill을 제공한다고 볼 수 없다. |
| 이벤트 구분 | 실시간·최근 채팅 모두 JSON 배열로 정규화해 전달한다. | 과거 데이터가 들어올 경우 수신 시각 기준으로 무조건 더하면 순위가 왜곡될 수 있다. |

근거: [수신·Ping·채널 전달 루프](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat.go#L197-L267), [배열 정규화](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat_codec.go#L107-L142).

HTTP 계층에도 상태 코드 기반 재시도나 429 처리·속도 제한이 없다. 응답 JSON을 읽고 호출자에게 돌려준다. 서버의 실제 접속·호출 한도는 이 소스로 알 수 없다. [HTTP 처리](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/transport/http/http.go#L42-L53)

## 검증 수준과 판단

- 라이브 읽기 연결 통합 테스트는 해당 커밋에서 주석 처리되어 있다. 실제 라이브 수신 성공을 자동 테스트가 보장한다고 말할 수 없다. [통합 테스트](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/chat_integration_test.go#L78-L132)
- 비공식 클라이언트는 기본적으로 로그인 쿠키를 붙이지 않는다. `WithCookie`는 별도로 호출하며 주로 메시지 전송 인증에 사용된다. [클라이언트 생성·쿠키 옵션](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/unofficial.go#L50-L101)
- Go 최소 선언 버전은 1.25.2, WebSocket 라이브러리는 `github.com/coder/websocket v1.8.15`다. [go.mod](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/go.mod#L1-L10)
- 읽기 전용 소규모 PoC의 참고 구현으로 사용할 수 있으나, 바로 전체 방송 수집기의 운영 기반으로 채택하기에는 재접속·제한 대응·집계 정확성·실수신 검증이 부족하다. 공개/성인 제한/로그인 요구 방송 간 동작 차이도 미확인이다.
- 라이브러리 자체가 이 기능을 공식 미지원으로 명시한다. 코드가 접근 가능하다는 것과 네이버가 서비스 운영·재배포를 허용한다는 것은 별개이며, 이 검토에서는 이용 허용 여부를 판단하지 않았다. [패키지 안내](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/unofficial.go#L1-L7)
