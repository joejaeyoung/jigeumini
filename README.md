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

웹은 실제 방송 제목·공개 채널명·썸네일·카테고리·방송 상태, 1분/5분 채팅 집계, 제목·채널명 검색, 카테고리 필터, 관심 방송과 상세 기록을 제공한다. 최근 수집 이력이 있는 20개 채널을 조회하며 전체 치지직 검색은 아니다. 방송 정보는 비동기로 조회하므로 첫 화면에서 잠시 확인 중으로 표시될 수 있다. 품질 관측이 없거나 불완전한 채널은 건수 `—`를 표시하고 정상 관측된 조용한 구간만 0건으로 표시한다. 순위는 완료된 1분/5분 구간 중 품질 조건을 통과한 방송만 포함한다. 반응 급상승은 6분의 정상 관측과 연속 판정이 쌓인 뒤 표시한다. 디자인 시안은 별도 포트로 보존한다.

기존 M1 분석기에 처음 M2를 적용할 때는 `./scripts/pipeline analyzer --quality.trend-offset-reset=earliest`로 새 trend 그룹의 시작 위치를 설정한다. 다음 재시작부터는 `./scripts/pipeline analyzer`를 쓴다. 기존 ranking 그룹은 reset하지 않는다. [수집 품질·순위·급상승 계약](docs/api/QUALITY_RANKINGS.md)을 참고한다.

방송 정보 조회를 끄려면 분석기를 `./scripts/pipeline analyzer --metadata.enabled=false`로 실행한다. 새 코드를 반영할 때는 실행 중인 분석기를 Ctrl+C로 종료한 뒤 `./scripts/pipeline analyzer`로 재시작한다. [방송 정보 API·캐시·원천 이용 범위](docs/api/BROADCAST_METADATA.md)를 참고한다.

[개발 구조·Git Flow](docs/DEVELOPMENT.md) · [구조 개선 결정](docs/REFACTORING.md) · [GitHub Projects](https://github.com/users/joejaeyoung/projects/1) · [진행 상태](docs/STATUS.md)

## 기술 목표와 추적 검사

[기술 목표·기능 매핑](docs/TECH_GOALS.md)은 오프셋·재처리, 파티션·부하 편중, 발행 버퍼·과부하 제어를
T ID로 관리한다. 기능/이슈/PR에서 관련 ID를 고르고, 검증 후 [STATUS](docs/STATUS.md)의 상태·근거·남은 문제를 갱신한다.
새 기능을 구현해도 연결된 기술 문제의 완료 조건을 입증해야 해결로 기록한다.

```bash
python3 scripts/check-tech-goals.py                # 목표·기능·상태·근거 연결 및 HEAD 대비 ID 삭제 검사
python3 scripts/check-tech-goals.py --base develop # 지정 기준 commit 대비 ID 보존 검사
python3 scripts/check-doc-links.py                # 문서와 템플릿의 파일 링크 검사
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests # 추적 검사기의 누락 방지 검증
./scripts/check                                  # 위 검사 + 서버 테스트·JAR 빌드
```

검사는 Python 3와 Git만 사용하며 네트워크를 호출하지 않는다. CI는 PR base/push 이전 commit과 비교한다.
기술 ID를 없애는 대신 보류 이유·재개 조건을 기록한다. 이 검사는 실제 장애 시험이나 결과 리뷰를 대신하지 않는다.

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
`replay`·`selection`·`fleet`는 내부 실행기 `scripts/lib/run-cli`를 공유한다. 기존 명령으로 실행하며, 자식 종료 후 해당 JAR 복사본을 정리한다. signal을 받으면 자식에 TERM을 전달하고 종료를 기다린다.

## 오류 격리·소비 복구 진단

분석기는 ranking/trend의 영구 계약 오류를 `<원본 topic>.dlq`에 격리한다. broker ACK와 DB 저장이 끝난 뒤에만 원본 offset이 진행한다. 일시 DB/Kafka 장애는 배치를 재시도하며, 격리된 입력은 순위·급상승·상세 기록의 수집 불완전에 반영한다.

기존 분석기 실행 후 `http://127.0.0.1:18080/`의 **소비 복구 진단**에서 group/partition lag, 첫 미처리 offset의 시각, 오류·격리 건수, 마지막 진행과 원본 보존 여부를 확인한다.

```bash
curl --fail http://127.0.0.1:18080/api/operations/recovery
```

미관측은 0으로 표시하지 않는다. DLQ에 원본 payload를 보관하거나 live로 자동 재투입하지 않으며, 수정 검증은 보존된 원본의 별도 replay run을 사용한다. [격리·품질·진단 계약](docs/pipeline/DLQ.md)을 참고한다.

## 관심 방송 알림

제품 웹 `/notifications`에서 저장한 관심 방송의 급상승 피드를 조회한다. 읽음 상태는 브라우저에 저장하며 10분 넘게 늦게 도착한 기록과 품질 정정은 새 반응으로 강조하지 않는다.

새 알림 그룹의 최초 시작만 다음 옵션을 지정한다. 이후에는 기존 분석기 실행 명령을 사용한다.

```bash
./scripts/pipeline analyzer --notifications.offset-reset=earliest
curl --fail http://127.0.0.1:18080/api/operations/notifications
```

급상승/outbox의 DB 원자 저장, ACK 뒤 전달 완료, 독립 소비와 사후 품질 반영은 [알림 처리 계약](docs/pipeline/NOTIFICATIONS.md)을 따른다. 외부 알림 전송은 없으며 기본 토픽은 `chat.received.v1.spikes`다.

## 카테고리 반응

`/categories`에서 완료5분의 수집 대상 내 카테고리별 채팅·비중·품질 충족 채널 수를 보고, 선택하면 최근1시간과 기여 방송을 확인한다. 카테고리 관측은 최대120초 유효하며 미상/만료를 분리한다. 최초 새 그룹은 `./scripts/pipeline analyzer --categories.offset-reset=earliest`로 시작하고 이후에는 기본none으로 재시작한다. 처음부터 전체 그룹을 만드는 경우 기존 `--pipeline.offset-reset=earliest`를 상속한다. [카테고리 계약](docs/api/CATEGORIES.md)을 따른다.

## 시청자 관측과 채팅 비교

방송 상세에서 최근1시간의 시청자 관측점과 완료된1분 채팅을 같은 시간축으로 비교한다. 시청자 표본은 기본60초, 누락/TTL 초과/방송 변경에서 선을 끊으며 실제0과 미관측을 구분한다. collector가 선택한 채널만 새 관측을 생성한다.

```bash
# 새 관측 그룹 최초 시작만. 다음에는 기존 analyzer 명령을 쓴다.
./scripts/pipeline analyzer --observations.offset-reset=earliest
# 관측 주기·TTL 조절 또는 끄기 (수집기 실행 예)
OBSERVATIONS_INTERVAL_SECONDS=120 OBSERVATIONS_TTL_SECONDS=240 ./scripts/pipeline collector <채널ID> 120s 0s
# OBSERVATIONS_ENABLED=false ./scripts/pipeline collector <채널ID> 120s 0s
curl --fail http://127.0.0.1:18080/api/operations/observations
```

비압축 Kafka 이력·DB7일 보존·원천 필드·관측 한계는 [시청자 관측 계약](docs/api/OBSERVATIONS.md)을 따른다. 과거에 쌓지 않은 시청자 수는 제공하지 않는다.

## 고정 Kafka 입력 재처리

[이슈 #9 재처리 계약](docs/pipeline/REPLAY.md)은 로컬 CLI로 사용한다. 운영 group offset·순위·알림을 바꾸지 않고 UUID run에 10초 bucket만 만든다. 기존 Kafka/PostgreSQL 접속 환경 변수를 사용한다.

```bash
./scripts/infra up
# 현재 보존된 전체 partition의 시작·끝 offset 고정. 파일은 새 경로여야 한다.
./scripts/replay migrate
./scripts/replay capture work/replay-input.json
# eventTime 필터를 쓸 경우 두 UTC 시각을 함께 지정한다. offset 범위는 보존 로그 전체다.
# ./scripts/replay capture work/replay-filtered.json 2026-10-25T00:00:00Z 2026-10-25T01:00:00Z
./scripts/replay create work/replay-input.json
# create JSON의 runId를 사용한다. worker는 대기 작업 하나를 처리하고 종료한다.
./scripts/replay worker
./scripts/replay status <runId>
./scripts/replay cancel <runId>
./scripts/replay reap
# 동일 manifest로 create → worker를 다시 실행한 두 COMPLETED run만 비교한다.
./scripts/replay compare <첫-runId> <두번째-runId>
```

표준 출력은 JSON이고 빌드 출력은 stderr다. 실패는 종료 코드 2와 고정 오류 코드로 알린다. 비교 불일치도 코드 2이며 결과의 `equal=false`와 bucket 차이를 확인한다. manifest의 끝 이후 입력과 이미 삭제된 입력은 포함하지 못한다. 실패·취소 run은 자동 재개하지 않으며 새 run으로 다시 실행한다. `status`/`reap`가 죽은 worker의 30초 heartbeat 만료를 확인한다. 자세한 종료 한도·부분 결과·비교 제한은 계약 문서를 따른다. `./scripts/integration`은 실행 JAR를 만들고 실제 Kafka/DB와 별도 JVM으로 이 경로를 검증한다.

## 수집 연결 메모리 진단

F01/F13/F14 → T02/T08/T11/T12/T14, 이슈 #64. 별도 Xmx64MiB JVM에서 합성 loopback READ/heartbeat 연결만 사용한다. Kafka·DB·API 부하·기대 이벤트 저장소 없이 연결 메모리와 종료 후 공유 IO group 등록 수를 검사한다. 공개 원천을 호출하지 않는다.

```bash
./scripts/collector-memory connections work/collector-connections.json
./scripts/collector-memory lifecycle work/collector-lifecycle.json
```

connections는32개 연결2회, lifecycle은8개 연결3회다. 고정 Tomcat 버전의 시험용 내부 계수, GC 관측 heap·스레드·FD 숫자만 기록한다. JVM heap 종료로 완성하지 못한 JSON은 성공 근거가 아니다. 전체 제품 용량은 `scripts/product-load`의 고정 자원 계획으로 별도 확인한다. [전후 검증](docs/COLLECTOR_MEMORY_VERIFICATION.md)에 실제180채널 실패 해소·450채널 저부하 지속·600채널 제외 근거를 기록했다.

실험 원본과 해당 Git history를 보유한 경우 #64 정제 결과를 다시 만들 수 있다. 새 출력 디렉터리를 지정하며 원본은 변경하지 않는다.

```bash
python3 scripts/collector-memory-summary.py work/collector-memory-64 work/collector-memory-summary
```

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

전체 제품 합성 부하(#43, F03/F04/F05/F06/F07/F10/F14 → T02/T07/T08/T11/T12)는 깨끗한 commit의 이미지로 실행한다. 분석기·Kafka·DB·생성기를 새 Compose에 만들고 종료 시 해당 자원과 생성 비밀만 정리한다. 공개 원천은 호출하지 않는다.

```bash
./scripts/deploy-images work/product-load-images.json
./scripts/product-load work/product-load-images.json smoke work/product-load-smoke
./scripts/product-load work/product-load-images.json uniform work/product-load-uniform
./scripts/product-load work/product-load-images.json hot work/product-load-hot
./scripts/product-load work/product-load-images.json steps work/product-load-steps
./scripts/product-load work/product-load-images.json soak work/product-load-soak
# docs/CAPACITY_VERIFICATION.md의 JSON을 work/capacity-plan.json에 저장한 뒤:
./scripts/product-load work/product-load-images.json work/capacity-plan.json work/capacity-run
# capacity 결과 root의 완료된 run과 environment.json을 숫자·hash 근거로 정제
python3 scripts/capacity-summary.py work/capacity-limits work/capacity-summary.json
# 현재 codec의 합성 이벤트 value 크기 (원문/식별자 값 출력 없음)
./gradlew --console=plain :server:testClasses
java -Dloader.path=server/build/classes/java/test \
  -Dloader.main=io.chatpulse.poc.CapacityEventSizeProbe \
  -cp server/build/libs/chat-pulse-poc.jar org.springframework.boot.loader.launch.PropertiesLauncher
```

채널 수·입력률·조회량·합성 WebSocket·자원 대조를 지정하는 용량 계획과 판정 기준은 [용량 경계 측정](docs/CAPACITY_VERIFICATION.md)을 따른다. 사용자 지정 계획은 `capacityAnalysis`의 생성기/계측 한계와 SLO 통과 여부를 별도로 확인한다.

병목 진단 계획에는 `measurementProfile`과 `diagnostics`를 선택할 수 있다. 기본값은 `FULL`·`false`이며 신규 계획의 `measurementSchemaVersion`은 2다.
`NO_COUNTS`는 입력 중 5초 주기 전체 COUNT만 끄고, `NO_COUNTS_OR_DU`는 container 내부 `du`도 끈다.
모든 모드에서 종료 후 ledger·bucket·offset 대조는 유지한다. `diagnostics: true`는 소유 시험 DB에만
`pg_stat_statements`를 켜고 고정 SQL 분류·실행 횟수·시간·블록·WAL 및 JVM 스레드 위치 개수를 남긴다.
SQL 텍스트·값·스레드 이름·원본 stack은 결과에 넣지 않는다. 진단 자체의 비용은 동일 설정 대조로 해석한다.
probe 벽시계/단조시계 잔차의 절댓값이 250ms를 넘거나 측정이 없으면 용량 통과로 판정하지 않는다.
부하 생성기의 `sideInputTimings`는 관측 발행 ACK/outbox 저장 시간, `maximumPacingLocation`은 최대 지연의 예정 초·tick을 기록한다.
합성 원천과 수집기는 숫자 close code와 고정 단계만 남긴다. 준비 중 연결 오류가 있으면 부하 입력 전에 거부한다.
[조사와 가설](docs/research/PROJECTION_PERFORMANCE.md)에 계측 범위와 해석 한계를 정리했다.
로컬 개발 DB의 임시 테이블로 최신 리포트 관측 조회를 대조하려면 `python3 scripts/report-index-probe.py work/report-index.json`을 실행한다. 20,160개 합성 행과 EXPLAIN 숫자만 기록하고 transaction을 rollback한다. 제품 테이블은 변경하지 않는다.

각 출력 경로는 새 디렉터리여야 한다. soak은50건/s·20채널·30분과 분석기20초 정지/재개를 포함한다. 관측/세션/heartbeat와 outbox는 합성 입력이다. 결과는 `result.json`과 `probe-output/load.json`, 독립1초 offset 표본은 `probe-output/offsets.jsonl`, 느린 계측 원본은 `probe-output/samples.jsonl`에 바로 남는다. API/DB 지연·별도 JVM 메모리는5초 주기로 따로 읽고, 종료 후 모든 집계를 대조한다. 복구 시간은 재개 뒤 첫 Kafka end를 고정해 해당 위치까지 따라잡은 표본 시각이다. 입력 종료 뒤 전체0lag도 확인한다. API 준비 회복 시각은 고정 offset 복구와 별도로 기록하며 둘 다 회복한 뒤2초까지를 장애/회복 구간으로 분류한다(시험 예산60초). 호스트와 VM의 시계 차이는 시작·재개 시 호출 전후 구간으로 남긴다. 정상 구간 계측 성공률90%·최소 표본 수·API200을 완료 조건으로 검사한다. API/DB/heap/RSS/버퍼는 정상10초 창의90% 이상을 함께 관측하고 정상 구간의 완전한 표본 간격이15초를 넘으면 실패한다. heap용 JMX는 시험 내부망에만 열고 자동 정리한다. 표본 시간은 실제 간격으로 해석하며 운영 용량/SLA를 뜻하지 않는다. 이미지·고정 복사한 시험 클래스는 로컬에 남는다.

| 위치 | 내용 |
|---|---|
| `server/src/main/java/io/chatpulse/poc` | HTTP 어댑터, 프로토콜, 연결 감독, 통계·실행기 |
| `server/src/test` | 합성 frame, 로컬 HTTP·실제 WebSocket 테스트 |
| `web/src/` | React·TypeScript 제품 웹, 서버 API 조회 |
| `server/src/main/java/io/chatpulse/pipeline` | Kafka 발행·집계·조회 API |
| `server/src/main/java/io/chatpulse/metadata` | 공개 방송 정보 조회·메모리 캐시·API |
| `server/src/main/java/io/chatpulse/quality` | 수집 품질 관측·완료 구간 순위·독립 급상승 판정 |
| `scripts/` | 환경 확인, 빌드·검증, 공개 채널 찾기, live 실행 |
| [설계 문서](docs/design/README.md) | PRD·아키텍처·HLD |
| [프로토콜과 구현 결정](docs/poc/PROTOCOL.md) | 입력 분류, timeout·재시도, 소스 근거 |
| [검증 기록](docs/poc/VERIFICATION.md) | 실제 실행 결과와 남은 검증 |
| [진행 상태](docs/STATUS.md) | 단계별 완료·후속 작업 |

`gromo/phone`의 문서 인덱스, Gradle wrapper, 로컬 검증과 live 스모크 분리 방식을 참고했다.
Jira·운영 배포·리뷰 호출 자동화는 이 개인 PoC의 범위에 포함하지 않았다.


재처리 조회의 후속 진단(#16): `./scripts/replay retention <manifest.json>`으로 현재 가용 offset 폭과 receivedAt 경계 표본을 확인한다. `status/compare`는 migration/watchdog을 수행하지 않으며 watchdog은 `reap`으로 실행한다. 생성/실행 빌드를 기록하고 다른 실행 빌드나 같은 run끼리의 비교를 거부한다. [진단 계약](docs/pipeline/REPLAY.md#조회와-migration빌드-이력-16)을 따른다.

## 방송 관측 리포트

채널 상세의 **방송별 요약 기록 보기**에서 최근7일 관측과 미종료 기록을 조회한다. 누적 참고 채팅·품질 충족 완료 분·분당 최대·시청자 관측 최고값·상위3개 피크를 구분한다. 실제 방송 시작/종료를 추정하지 않으며 내부 관측 세션과 미확정 상태를 표시한다. [리포트 계약](docs/api/REPORTS.md)을 따른다.

새 report group의 첫 실행에서는 `--reports.offset-reset=earliest`를 지정한다. 이후 기본none으로 저장된 offset을 이어간다. 잔존 입력 중 eventTime 최근7일만 반영하며 더 오래된 입력은 진행 위치만 갱신한다. 리포트에는 receivedAt48시간 차단을 적용하지 않는다. `GET /api/v1/channels/{채널ID}/reports`, `GET /api/v1/reports/{UUID}`, 운영 진행 `GET /api/operations/reports`를 제공한다.

## 수집 대상 선정 미리보기

공식 라이브 목록/팔로워 합계와 채팅 접근 권한을 구분하고, 팔로워 하한·접근 확인 목록·최대 채널 수로 정제된 선정 계획을 만든다. 기본은 합성 입력이며 수집기를 시작하지 않는다. [선정 계약·실행법](docs/discovery/README.md)·[공식 범위 조사](docs/research/CHZZK_SCALE.md)를 따른다.

```bash
./scripts/selection fixture docs/discovery/synthetic-policy.json docs/discovery/synthetic-catalog.json work/selection.json
```

## 격리된 합성 부하 실험

`./scripts/experiment smoke work/experiment-smoke.json`으로 먼저 실행한다. `baseline`은 직렬 ACK 기준선, `steady`는500건/초600초, `burst`는2,000건/초60초의 bounded async 실험 후보다. 운영 발행기 설정은 바꾸지 않는다. 출력 경로는 새 파일이어야 하며 JSON·진행JSON·HTML을 만든다. 목표 달성과 실제 입력/ACK/DB/replay 대조를 구분한다. 공개 플랫폼에 요청하지 않는다.

`uniform-1`~`uniform-4`, `hot-1`~`hot-4`, `recovery`(분석 group60초중단), `db-delay`, `db-failure`, `membership`, `poll-stall`, `hot-duplicates`, `sharded-3`를 비교한다. 동일 seed와 optional UTC eventTime 기준을 입력 manifest에 기록한다. 동시 실험은 자원 간섭을 만들므로 순서대로 실행한다.

샤딩 재검증 #67은 `shard-control-1`/`shard-control-3`와 `shard-explicit-1`/`shard-explicit-3`를 비교한다. 모두 3 partitions·2,000건/s·15초·80% 편향·두 producer·1% 재발행이며 끝 숫자는 consumer 수다. 명시 분할은 `channelId` 키를 유지하고 내부 eventId의 고정 해시로 실제 partition을 지정한다. `shard-smoke`는 100건/s·2초의 정합성 회귀다. `routingAudit`의 실제 hot 분포를 확인해야 하며, 과거 `sharded-3`은 세 suffix가 같은 partition에 충돌해 인기 방송 분산 효과의 근거로 사용할 수 없다. [공식 자료·설계 조건](docs/research/KAFKA_SHARDING_SOURCES.md).

기본 로컬 Kafka19092/DB15432의 UUID 전용 topic/group/schema만 만들고 종료 시 정리한다. host·Kafka/DB 파일시스템 각각2GiB 여유를 확인하고 실험 토픽+DB2GiB 상한을5초마다 관측한다. 생성 최대600초/400,000개·drain180초·replay300초, client heap768MiB이다. 삭제는 이 실행의 자원만 대상이며 다른 worktree나 live ledger를 수정하지 않는다. 강제종료로 cleanupComplete가false이면 결과의 정확한 UUID 자원만 확인해 정리한다.

합성 실험의 전체 프로필·자원 상한·계수/지연 정의는 [실험 계약](docs/pipeline/EXPERIMENTS.md), 실제 수치와 한계는 [검증 기록](docs/pipeline/EXPERIMENTS_VERIFICATION.md)을 따른다. `python3 scripts/summarize-experiments.py work/report <완료 결과.json>...`으로 표·정제 JSON·독립 HTML을 만든다.


수집기 #27의 기본 발행 한도는 전체1,000·채널500·비동기 창64다. 채널별 초과/거절·ACK 미확인·종료 정책은 [발행 계약](docs/pipeline/PUBLISHER.md)을 따른다.

## 다중 수집기 배정과 인계

`./scripts/fleet preview <선정.json> <유효초>`는 DB/원천 호출 없는 계획 확인이다. `migrate`, `apply`, `status`, `worker`의 실행 경계·공개 최대3개 제한·소유권 상실·미확인 입력은 [수집기 임대 계약](docs/fleet/README.md)을 따른다. 전체 치지직 수집을 자동 활성화하지 않는다.

## Kafka HA 로컬 장애 리허설

```bash
./scripts/kafka-ha work/ha-result.json
```

[HA 계약](docs/pipeline/KAFKA_HA.md)을 따른다. 합성 입력과 UUID별6노드 Compose project·소유 데이터 볼륨을 만들고 종료 뒤 정리한다. Docker 가용메모리6GiB 이상, 기존 사용량+실험 상한2.625GiB가90% 이내, 여유디스크2GiB가 필요하다. 기존 chat-pulse/다른 프로젝트를 정지하지 않는다. 외부 배포·실제 AZ 장애 검증은 아니다.

## ACK 수준과 offset commit 경계 비교

`./scripts/ack-commit work/ack-result.json`은 소유권이 있는 로컬6노드·합성 고정입력으로 AUTO/BATCH/MANUAL의 프로세스 종료와 ACK0/1/all의 복제 장애를 비교한다. 제품 기본 ACK/commit 설정은 유지한다. [HA 리허설](docs/pipeline/KAFKA_HA.md)의 자원 조건을 먼저 확인한다. 결과에서 API 성공·broker 확인·장애 후 보존·DB 반영을 구분한다.

## 로컬 배포 패키지

`./scripts/deploy-images work/images.json`은 clean commit의 비특권 서버/웹 이미지를 만들고 실제 image ID를 기록한다. 비밀 주입·내부망·migration·readiness·복구 시작 검사는 [배포 계약](deploy/README.md)을 따른다. 외부 배포나 registry push를 하지 않는다.

## Kafka 디스크 영구 손실 검증 #45

```sh
./scripts/kafka-disk-loss recover work/disk-recover.json
./scripts/kafka-disk-loss last-copy work/disk-last-copy.json
```

F15/F11 → T04/T10/T15. 각 실행은 신규 소유6노드·합성 입력으로만 진행한다. `recover`는 follower 디스크 삭제·빈 디스크 재복제·ISR1의 ACK 거부·DB 대조, `last-copy`는 stale baseline 디스크를 보존한 채 최신 tail이 있던 두 디스크를 실제 삭제하여 복구 불가 경계를 확인한다. 소유 label·service·volume mount·다른 container 참조를 확인한 뒤 해당 volume만 삭제한다. 공용/외부 broker에는 실행하지 않는다. class 고정/hash와 stopped replica CRC·offset·payload digest를 기록하며 종료 시 이번 실행 자원을 정리한다.

기존 HA 사전 자원 검사에 readonly probe128MiB를 추가한다. 결과 JSON과 `.probe-classes` 디렉터리는 새 경로여야 한다. [조사·실험 판정](docs/research/KAFKA_DISK_LOSS.md)을 따른다. [실제 결과](docs/pipeline/KAFKA_DISK_LOSS_VERIFICATION.md)는 복제본 재구성7단계와 마지막 copy 손실6단계다. 단일 호스트 volume 삭제를 AZ/power-loss 보장으로 확장하지 않는다.

## 새 migration 릴리스 복구 시험

#42 / F16/F08 → T04/T16. clean commit에서 현재 제품 schema 이미지와 그 다음 번호의 시험 전용 migration 이미지를 만든다. 두 실패 후보는 DDL/backfill 뒤 SQL 실패 또는 프로세스 중단을 주입한다. 제품 migration에는 시험 DDL을 넣지 않는다.

```bash
./scripts/deploy-images work/migration-baseline.json
./scripts/migration-images work/migration-baseline.json work/migration-images
./scripts/migration-rehearsal work/migration-images work/migration-recovery
```

각 출력 경로는 새 경로를 사용한다. 리허설은 자체 Compose/DB/Kafka만 만들고 기존 개발 인프라를 변경하지 않는다. 현재 schema 백업→실패/중단 rollback→다음 시험 migration 적용/새 입력→구 이미지 거부→새 DB 이전 schema 복원/새 group 재생→실제 웹 대조 후 소유 자원·secret·dump를 정리한다. 같은 코드 commit의 다른 migration 이미지이며, build ID·SQL hash·실제 image ID로 구분한다. 실행 결과와 미검증 범위는 [진행 상태](docs/STATUS.md)에 기록한다. 생성한 Docker 이미지는 manifest와 함께 로컬에 남고 자동 정리 대상에 포함되지 않는다.

## PostgreSQL physical 시점 복구 #46

`./scripts/postgres-pitr <같은 commit의 이미지 manifest.json> <새 결과 디렉터리>`는 base backup/WAL→named target pause→쓰기 재개→Kafka 위치 정렬·재생, 필수 WAL 누락과 retention gap을 확인한다. `./scripts/deploy-images`로 이미지 manifest를 먼저 준비한다. [실행·검증 범위](docs/POSTGRES_PITR_VERIFICATION.md)를 따른다. 2026-10-26의 로컬 실제12단계와 정리가 통과했다. named target·동일 호스트 범위이며 외부 운영 RTO/RPO는 별도 검증이다. 생성한 소유 volume과 비밀은 종료 시 정리한다.

### 전체 제품 부하 정제 근거

[부하 검증](docs/PRODUCT_LOAD_VERIFICATION.md)은 단일 순차 API probe·transaction 시작 시각 지연·단회 관측의 한계를 구분한다. 실행 원본에서 경로별 지연·partition별 입력/처리율·분단위 메모리·원본 hash를 다시 계산하려면 다음 명령을 사용한다. 공개 원천 데이터에는 사용하지 않는다.

```sh
python3 scripts/product-load-summary.py work/product-load-soak work/product-load-summary.json work/product-load-images.json
```
