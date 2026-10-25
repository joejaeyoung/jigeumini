# 치지직 비공식 채팅 수집 구현 검토

확인일: 2026-10-25. GitHub 저장소를 내려받아 소스를 정적으로 검토했다. 실방송 연결, 쿠키 사용, 채팅 송신은 실행하지 않았다. 코드에 구현된 경로와 현재 서버에서의 동작 보장은 구분한다.

## 결론

공개 채팅을 익명 읽기 모드로 수신하는 코드가 실제 존재한다. 따라서 공식 API의 방송자 OAuth 동의 제한만으로 치지직 채팅 수집 프로젝트 전체를 제외할 필요는 없다. 비공식 접속의 현재 성공 여부와 장시간 안정성은 소규모 수신 PoC에서 확인해야 한다. 이용 조건 검토와 기술적 접속 가능 여부는 별개다.

## 확인한 버전

| 저장소 | 내려받은 HEAD | 해당 커밋 날짜 |
|---|---|---|
| kimcore/chzzk | `9208ad6901242c2854c44d6b13e219d7d023239d` | 2024-08-13 |
| sdkim96/chzzk-go | `3ac96faac23536fe99f5f519b0a68b280f8667e8` | 2026-08-30 |

Node 패키지는 1.10.4다. 날짜는 확인한 기본 브랜치 HEAD 기준이며 모든 브랜치나 배포판의 활동을 뜻하지 않는다.

## Node 구현의 연결 흐름

1. 채널 ID로 `/polling/v2/channels/{channelId}/live-status`를 조회해 `chatChannelId`를 얻는다. 영구 채널 ID와 방송 채팅방 ID는 구분한다. [live.ts](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/live.ts#L152)
2. 게임 API의 `/v1/chats/access-token?channelId={chatChannelId}&chatType=STREAMING`에서 채팅 접속용 토큰을 받는다. 이는 공식 개발자 OAuth Access Token과 다른 경로다. 코드에는 연령 제한 오류를 별도로 처리하는 분기가 있다. [api/chat.ts](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/chat.ts#L19)
3. 채팅방 ID의 문자 코드 합을 9로 나눈 나머지에 1을 더해 서버 번호를 구하고 `wss://kr-ss{번호}.chat.naver.com/chat`에 연결한다. [chat.ts](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L103)
4. 첫 연결 패킷에 토큰과 `uid`를 넣는다. 로그인 쿠키가 없으면 `uid: null`, `auth: "READ"`를 보낸다. 로그인 사용자면 `SEND`가 된다. 쿠키 헤더 자체도 인증 옵션이 있을 때만 추가된다. [연결](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L88), [HTTP 클라이언트](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/client.ts#L76)
5. 수신 JSON의 명령 번호로 연결 응답, ping, 일반 채팅, 후원, 최근 채팅을 분리한다. 채팅의 profile/extras에 들어 있는 JSON 문자열을 다시 파싱하고 내용·시각·최근 이력 여부 등을 이벤트로 전달한다. [메시지 처리](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L250)

명령 번호는 CONNECT=100, CONNECTED=10100, CHAT=93101, DONATION=93102, REQUEST_RECENT_CHAT=5101, RECENT_CHAT=15101이다. HTML 화면의 텍스트를 긁는 구조가 아니라 비공식 HTTP 엔드포인트와 채팅 WebSocket 프로토콜을 구현한다. [명령 정의](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/types.ts#L16)

## 재연결과 누락 처리의 실제 범위

- 기본 30초 주기로 채팅방 ID 변경을 확인하고 변경 시 재연결한다. 이것은 모든 네트워크 장애를 자동 복구하는 것과 다르다. 일반 socket close 경로에는 자동 백오프 재시도 루프가 없고 polling을 중단한다. [기본 주기](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/client.ts#L25), [종료 처리](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L131), [채팅방 변경 감지](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L361)
- 서버 ping에 pong으로 답하고 자체 ping 타이머도 사용한다. 현재 코드에는 pong 미응답에 대한 명시적인 재접속 감독 로직이 없다. [ping 처리](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L268)
- 최근 채팅 요청 함수가 있으며 기본 요청 건수는 50이다. 이는 서버가 항상 50건을 보장한다거나 임의 시점부터 전체 이력을 복원할 수 있다는 뜻이 아니다. 최근 이력은 `isRecent`로 표시된다. [최근 채팅 요청](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L175)
- 일반 채팅과 최근 이력을 같은 집계에 넣으면 중복될 수 있다. 이 소스의 정규화된 ChatEvent에는 고유 메시지 ID가 명시되어 있지 않아 실제 원본 필드 확인이 필요하다. 합성 해시는 충돌·오탐 가능성이 있다. [파싱](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/chat/chat.ts#L321)

## Kafka 수집기로 확장할 때의 제안

연결 관리 → 이벤트 파싱 → 제한된 내부 버퍼 → Kafka 발행으로 나눈다. 수집기에서 순위·키워드 분석까지 수행하지 않고 소비자별로 분리한다.

- 방송별 연결 상태, 마지막 수신 시각, 재접속 횟수와 수집 공백을 기록한다. 채팅이 조용한 상태와 연결 단절을 혼동하지 않는다.
- 재접속 시 지수 백오프와 지터를 적용하고 새 채팅방 ID/토큰이 필요한지 재확인한다.
- Kafka key는 우선 방송 채널 ID로 하여 동일 방송의 발행 순서를 유지한다. 인기 방송 한 곳에 부하가 집중되는 한계는 별도로 측정한다.
- 이벤트 발생 시각과 수집 시각, 실시간/최근 이력 구분을 함께 남긴다. 집계 로직은 늦은 이벤트와 재처리를 고려한다.
- Kafka 발행 실패 시 버퍼 한도와 디스크 임시 저장 여부를 정한다. 버퍼가 무한히 늘지 않도록 한다.
- 원천에서 받지 못한 이벤트는 Kafka로 복구할 수 없다. 최근 채팅 조회는 제한된 보완 수단이며 완전한 복구 계약이 아니다.

## 다음 검증 범위

일반 공개 방송 2~3개, 비로그인·읽기 전용으로 시작한다. 정상 수신, 강제 연결 종료 후 복구, 방송 재시작 시 채팅방 변경, 최근 이력 중복 여부, 초당 수신량과 버퍼 적체를 측정한다. 제한된 방송에 대한 접근 우회는 하지 않는다. 실제 검증이 끝나기 전에는 전체 방송 수집 가능, 무손실 수집, 대용량 처리 달성을 주장하지 않는다.

Go 구현의 세부사항은 [추가 검토](chzzk-go-source-review.md)에 정리했다.
