# 지금이니 — 서버·웹 모노레포

치지직 공개 채팅을 익명 READ WebSocket으로 수신해 **Spring에서 데이터 소스를 확보할 수 있는지** 검증한다.
Java 21 + Spring Boot 4.0.9 + Gradle 9.4.1로 구현했다. Kafka 4.1.2 → PostgreSQL 16.14의 10초 집계와 조회 화면까지 연결한다.

## 실제 웹 개발

Node.js 24(`.nvmrc`), Java 21, Python 3, Docker Compose를 사용한다. 최초 웹 의존성을 설치한다:

```bash
./scripts/web install
./scripts/web dev
```

제품 웹은 http://127.0.0.1:18082/ . `/api` 요청을 기본 `127.0.0.1:18080`의 Spring 조회 서버로 전달한다. 조회 서버와 Kafka·DB는 아래 파이프라인 명령으로 실행한다. 다른 조회 서버는 `API_PROXY_TARGET=http://127.0.0.1:포트 ./scripts/web dev`로 지정한다.

```bash
./scripts/check                 # 서버 테스트·JAR·문서 링크
./scripts/integration           # 실제 Kafka·PostgreSQL 통합 검증
./scripts/web check             # 웹 타입·단위 테스트·제품 빌드
./scripts/web browser-install   # 최초 Chromium 설치
./scripts/web test:e2e          # 합성 API 응답 기반 브라우저 검증
./scripts/web build
./scripts/web preview           # 빌드 확인: http://127.0.0.1:18083/
```

웹은 실제 집계 조회·1분/5분 선택·채널 ID 검색·관심 방송·상세 기록을 제공한다. 아직 서버에 없는 방송 메타데이터와 급상승 배수는 표시하지 않는다. 최근 수집 기록이 없으면 빈 화면이 정상이며 가상 방송으로 대체하지 않는다. 디자인 시안은 별도 포트로 보존한다.

[개발 구조·Git Flow](docs/DEVELOPMENT.md) · [GitHub Projects](https://github.com/users/joejaeyoung/projects/1) · [진행 상태](docs/STATUS.md)

## 웹 UI 디자인 시안

```bash
./scripts/ui-preview
```

브라우저에서 http://127.0.0.1:18081/ 에 접속한다. Python 3만 필요하며 `Ctrl+C`로 종료한다.
`web/design-preview/`의 플랫폼형 방송 탐색 서비스 시안이다. 모든 채널·수치는 합성 예시이며 썸네일은 AI로 제작했다. 서버 API와 연결하지 않는다.
홈·방송 탐색·반응 급상승·관심 방송을 공통 메뉴로 이동한다. 상단 검색, 카테고리 필터, 관심 방송 저장, 1분/5분 정렬, 상세 모달의 추이와 집계 안내를 확인할 수 있다.
화면별 주소는 `#/home`, `#/explore`, `#/surges`, `#/saved`이며 데스크톱과 모바일 모두 상단 메뉴로 이동한다.
관심 방송은 해당 브라우저의 로컬 저장소에만 저장한다. 가상 방송이므로 방송 보기 버튼은 실제 방송 대신 예시 안내를 표시한다.
모노레포의 웹 영역은 `web/`로 둔다. 서버는 `server/`, 제품 웹은 `web/src/`, 확정 시안은 `web/design-preview/`에 둔다.

## Kafka·PostgreSQL로 실행

```bash
./scripts/infra up
./scripts/pipeline analyzer --pipeline.offset-reset=earliest  # 새 그룹 최초 실행만
# 별도 터미널
./scripts/discover
./scripts/pipeline collector <채널ID> 120s 60s
```

화면은 http://127.0.0.1:18080/ . 이후 분석기 재시작은 `./scripts/pipeline analyzer`로 저장된 offset부터 진행한다.
[M1 실행·처리 계약](docs/pipeline/README.md) · [M1 검증 결과](docs/pipeline/VERIFICATION.md)

`./scripts/check`는 로컬 자동 테스트, `./scripts/integration`은 실제 Docker Kafka·DB 통합 테스트를 실행한다.
실행 스크립트는 JAR를 `work/runtime/`에 별도로 복사하므로 재빌드 중에도 실행 파일이 바뀌지 않는다.

## Kafka 없이 수신만 검사

```bash
./scripts/doctor
./scripts/check
./scripts/discover
# 위 결과의 현재 방송 중인 채널 ID를 넣는다.
./scripts/poc <채널ID> 60s 30s
# 공개 채널 2개, 30분 관측, 15분에 한 번 의도적으로 연결을 닫아 재접속 확인
./scripts/poc <채널ID1>,<채널ID2> 30m 15m
```

이 수신 전용 모드에는 Java 21, Python 3, curl이 필요하다. Gradle은 wrapper가 내려받는다. Docker·DB·로그인 쿠키는 필요하지 않다.
첫 빌드는 Maven Central과 Gradle 배포 서버에 접속한다. `scripts/check`의 테스트는 loopback 서버만 사용하며 치지직에 접속하지 않는다.
직접 JAR를 실행한다면 실행 중 재빌드하지 않는다. 지속 실행에는 위 스크립트의 별도 JAR 복사본을 사용한다:

```bash
java -jar server/build/libs/chat-pulse-poc.jar \
  --poc.channels=<채널ID> \
  --poc.duration=60s \
  --poc.reconnect-after=30s \
  --poc.output=work/my-result.json
```

수신 중에는 30초마다 채널별 건수·연결 상태를 로그에 표시한다. PoC는 정해진 시간 후 종료하며 결과를 `work/`의 JSON으로 남긴다. 공개 채널 1~3개, 기간 1초~30분으로 제한한다.
기간에는 최초 연결·재접속 대기 시간이 포함된다. 마지막 HTTP/handshake 정리로 종료가 다소 늦어질 수 있다.
`scripts/poc`는 저장소 루트로 이동한 뒤 파일 잠금으로 중복 실행을 막는다. 직접 JAR를 서로 다른 작업 디렉터리에서 실행하거나 다른 checkout/호스트에 띄우는 경우의 중복 방지는 구현하지 않았다.

## 결과 읽기

- 종료 코드 0 / `PASS`: 모든 채널에서 예정된 관측을 마치고 handshake, 일반 채팅, ping 응답을 확인했다.
  강제 재접속을 요청했다면 재접속 후 채팅도 있어야 한다.
- 종료 코드 2: 연결 실패·접근 제한·시간 초과·검증 불충족 또는 채팅이 없어 판정 보류.
  조용한 채널의 `INCONCLUSIVE_NO_CHAT`를 접속 실패로 해석하지 않는다.
- `chatCount`, `skipped`, `commands`, `connections`, `clientPings/serverPongs`, `reasons`로 근거를 확인한다.
- `connectionTimeRatio`는 소켓 승인부터 종료까지의 관측 시간 비율이다. heartbeat timeout 감지 지연을 포함할 수 있으며 실제 메시지 수신율이 아니다.
- `meanSourceToReceiveMillis`는 소스 시각과 로컬 시각의 차이다. DB 처리 지연이나 화면 지연이 아니다.
- `candidateIdFieldPresence`는 알려진 후보 필드의 존재 건수다. 비어 있어도 모든 가능한 고유 ID가 없다고 단정하지 않는다.

원문·닉네임·사용자 ID·토큰은 저장하지 않는다. 수신 전용 PoC에서는 정규화 이벤트도 디스크에 누적하지 않고 건수·시각 통계만 유지한다. pipeline 모드에서는 개인정보를 제거한 이벤트를 Kafka에 보관하고 DB에 처리 이력·집계를 저장한다.
일반 채팅은 `cmd=93101`과 type=1만 집계하며 최근 이력·후원·시스템 메시지는 제외한다.
채팅 송신이나 최근 채팅 요청 기능은 없다. 연결이 끊긴 동안의 채팅을 복원하지 않는다.

## 코드와 문서

| 위치 | 내용 |
|---|---|
| `server/src/main/java/io/chatpulse/poc` | HTTP 어댑터, 프로토콜, 연결 감독, 통계·실행기 |
| `server/src/test` | 합성 frame, 로컬 HTTP·실제 WebSocket 테스트 |
| `web/src/` | React·TypeScript 제품 웹, 서버 API 조회 |
| `server/src/main/java/io/chatpulse/pipeline` | Kafka 발행·집계·조회 API |
| `scripts/` | 환경 확인, 빌드·검증, 공개 채널 찾기, live 실행 |
| [설계 문서](docs/design/README.md) | PRD·아키텍처·HLD |
| [프로토콜과 구현 결정](docs/poc/PROTOCOL.md) | 입력 분류, timeout·재시도, 소스 근거 |
| [검증 기록](docs/poc/VERIFICATION.md) | 실제 실행 결과와 남은 검증 |
| [진행 상태](docs/STATUS.md) | 단계별 완료·후속 작업 |

`gromo/phone`의 문서 인덱스, Gradle wrapper, 로컬 검증과 live 스모크 분리 방식을 참고했다.
Jira·운영 배포·리뷰 호출 자동화는 이 개인 PoC의 범위에 포함하지 않았다.
