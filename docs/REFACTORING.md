# 코드 구조 검토와 리팩토링 결정

2026-10-25 · 기준 `707230a` · 이슈 #17 · 최종 코드 commit `f6a4d63`.
진행 상태는 [STATUS](STATUS.md)에서 관리한다. 기능 추가 대신 기존 동작을 유지하면서 책임과 중복을 정리했다.

## 검토 범위와 선택

서버 `poc`, `pipeline`, `metadata`, `quality`, `replay`와 웹 `app`, `features`, `shared`, 실행 스크립트·CI의 파일 구성, 주요 호출 관계, 상태/트랜잭션 소유, 중복 설정을 검토했다. 전수 보안·성능 감사나 모든 분기의 새 검증을 의미하지 않는다.

| 영역 | 발견한 점 | 결정 |
|---|---|---|
| 품질·순위 | RankingStore가 세션·버퍼·소비 관측 SQL과 순위/급상승 저장을 함께 소유 | 읽기·품질 판정을 CollectionQuality에 모으고 RankingStore는 기존 트랜잭션과 결과 저장을 유지 |
| DB 초기화 | PipelineConfiguration과 ReplayCli에 같은 Hikari 한도·timeout·Flyway 초기화 중복 | PostgresSupport.openMigrated로 통합. 성공 시 호출자가 pool 소유, 초기화 실패 시 즉시 정리 |
| 웹 API | client.ts에 HTTP·집계·방송 정보 계약이 혼재하고 quality.ts에 공통 필드 중복 | transport/fields/buckets/metadata로 분리. 각 호출부가 실제 사용하는 모듈을 직접 import |
| 수집·발행 | 프로토콜/HTTP 어댑터·연결 감독·EventSink가 이미 분리됨. 발행 큐는 동시성·종료 규칙 보유 | 이 범위에서 구조 변경하지 않음. 버퍼/발행 병렬성은 #11의 부하·장애 근거와 함께 판단 |
| replay | 상태 쓰기·비교 SQL이 한 Store에 있고 조회 migration/임시 JAR/진단 정책 개선 후보 존재 | #16은 관찰 가능한 정책 변경을 포함하므로 이번 책임 분리와 혼합하지 않음. Store 추가 분리는 후속 기능의 실제 변경 지점으로 판단 |
| 메타데이터 | 캐시와 원천 HTTP 어댑터가 분리되고 테스트에서 시계·실행기를 대체함 | 구현체 하나뿐인 새 인터페이스나 범용 캐시 계층을 추가하지 않음 |
| 웹 화면·하네스 | 짧은 화면 컴포넌트와 polling 공통화가 존재. launch JAR 복사 코드가 중복됨 | 화면 구조는 유지. 임시 파일 수명·signal 전달의 변경은 #16에서 다룸. refactor 브랜치는 CI 대상에 추가 |

## 보존한 처리 계약

CollectionQuality는 package-private 내부 모듈이다. RankingStore와 같은 JdbcTemplate/DataSource를 사용하고 스스로 트랜잭션을 시작하지 않는다. 따라서 기존 REPEATABLE READ·advisory lock 안에서 관측을 조회하고 순위 snapshot·급상승 상태를 확정한다. SQL, clock 호출 위치, 채널→partition 계산, 창 뒤 첫 안전한 drain과 caught-up 대조를 그대로 옮겼다. 사용하지 않던 Health의 intervals/loss 포장만 제거하고 QualityPolicy.Result를 반환한다.

`io.chatpulse.db.PostgresSupport`는 서버와 CLI의 pool=4, 연결 대기 3초, 검증 2초, PostgreSQL connect/socket 3초/10초를 유지한다. 각 호출은 별도 pool을 소유하며 JVM 전역 pool을 만들지 않는다. migration의 RuntimeException/Error 모두 pool을 정리하고, 정리 실패는 원래 오류의 suppressed 예외로 보존한다. replay의 조회 명령도 migration을 실행하는 기존 정책은 #16에서 별도로 개선한다.

웹 transport는 기존 8초 timeout, 상위 취소 신호, HTTP/네트워크/계약 오류 분류를 유지한다. fields는 channel ID·timestamp·안전한 정수 검증의 정본이다. buckets와 metadata는 각 응답 스키마와 요청-응답 일치 검사를 소유한다. 기존 client.ts 재수출 계층은 남기지 않았으며 테스트도 실제 모듈을 호출한다. HTTP 경로·JSON·사용자 화면은 바꾸지 않았다.

## 회귀 검증

[실행 근거](poc/evidence/refactor-2026-10-25.json)에 코드 commit, JAR SHA-256, 환경, suite별 개수를 기록했다. 입력은 합성 HTTP/WS/API와 격리된 실제 로컬 Kafka topic·DB schema다.

| 명령 | 실제 결과·대조 범위 |
|---|---|
| `./scripts/check` | Java 단위 55개·하네스 16개·JAR 빌드, 문서/기술 ID 연결 통과 |
| `./scripts/integration` | 실제 Kafka/PostgreSQL 29개 통과. 품질 제외·창 뒤 drain·소비 따라잡음·사후 정정·독립 소비·중복/rollback·고정 replay/운영 불변·실행 JAR CLI |
| `./scripts/web check` | 타입 검사·기존 단위 17개·제품 빌드 통과. API 계약·metadata 만료·요청 실패/취소 검증 |
| `./scripts/web test:e2e` | Chromium 22개 통과. 관심·상세·기간 전환·집계 미확인·품질/리본·반응형 표시 |

내부 메서드 호출 횟수나 파일 배치를 복제하는 테스트는 추가하지 않았다. 기존 외부 interface를 통한 기대 동작으로 회귀를 확인했다. T01/T02/T04/T08/T11의 기존 범위를 보존한 근거이며 새 기술 목표 해결이나 처리량 향상 근거는 아니다. 실방송·새 플랫폼 프로토콜·HA·고부하·실기기는 이번 검증에 포함하지 않는다.

## 2026-10-26 전체 코드 검토 #39

기준 `c48a190dd8ffe122a1dac216bbf2f2ff76c8dc8c` · [이슈 #39](https://github.com/joejaeyoung/jigeumini/issues/39).
서버 main Java 102개(6,346줄), 웹 src 38개(1,448줄), scripts 26개(1,009줄)의 구성과 호출 관계를 점검했다.
각 영역의 interface, 상태·트랜잭션 소유, 반복되는 규칙을 검토하고 아래 네 영역을 변경 대상으로 정했다.
전체 파일의 모든 분기를 새로 검증하거나 성능·보안 감사를 수행했다는 뜻은 아니다.

### 영역별 결정

| 영역 | 검토 결과와 결정 |
|---|---|
| `poc` | HTTP/프로토콜/연결 감독/EventSink/CollectionObserver 경계와 합성 대체 구현이 있다. 재접속과 heartbeat·공개3채널 제한을 유지한다. |
| `pipeline` 소비 | 네 소비 구현에 같은 BATCH·동기 commit·1초 무제한 재시도·초기 earliest가 중복된다. `BatchConsumers`가 컨테이너 계약을 소유한다. 두 소비 구현의 poll proxy는 내부 `ConsumerPollTracker`로 모은다. |
| `pipeline` 발행·DLQ | BufferedKafkaPublisher의 큐/ACK/종료 계수는 같은 lock의 불변식이다. 분할하면 여러 객체가 같은 상태를 조정해야 하므로 유지한다. DLQ의 ACK/DB 경계와 오류 분류도 각 listener에 둔다. |
| `db`·`quality` | PostgresSupport·CollectionQuality는 앞선 리팩토링에서 경계가 마련됐다. migration/validate의 오류 의미, 품질 평가와 snapshot의 동일 transaction·clock 위치를 유지한다. |
| `observations`·`notifications` | 공통 소비 설정만 공유한다. 관측의 시각/TTL 계약, 알림 outbox의 commit/ACK와 payload 충돌 정책은 별개다. 범용 Store 상속을 추가하지 않는다. |
| `categories`·`reports` | DualTopicConsumer는 이미 공유한다. 귀속 보정, 활성 리포트 보존, cleanup lock·freshness 기준은 다르므로 SQL을 하나의 범용 projection으로 합치지 않는다. |
| `replay` | manifest·source identity/retention·worker 상태·DB 저장이 나뉘어 있다. 조회/수정 명령의 DB 권한과 replay namespace를 유지한다. JAR 실행 수명 중복만 제거한다. |
| `discovery`·`metadata` | 목록 pagination/부분 결과/요청 예산과 단일 방송 캐시/관측은 수명·오류 계약이 다르다. 임의의 공통 HTTP 프레임워크를 만들지 않는다. |
| `fleet` | FleetCli가 명령 해석과 producer/telemetry/관측기 조립·종료·결과 저장까지 함께 소유했다. package-private `FleetRuntime`으로 실행 자원을 옮긴다. CollectorWorker의 임대 갱신·drain·SIGTERM 저장 동기화는 그대로 둔다. |
| `operations` | RecoveryPlan은 복원 DB 위치·기존/새 group 검사를, DeploymentHealth는 비동기 표본·readiness를 소유한다. 두 책임을 합치지 않는다. 비밀 파일 처리·고정 진단·외부 JSON도 유지한다. |
| 웹 `app`·`features` | 경로와 조회 상태는 화면별 의미가 있다. 반복된 분당 막대 geometry/null 음영만 `MinuteBars`에 모으고, 시청자 선 연결은 API 모듈에서 `observationModel`로 옮긴다. |
| 웹 `shared/api` | 네 API의 연속 분 구간 검증을 `isMinuteTimeline`으로 모은다. 요청 기간·응답 identity·asOf·품질·최대360분 절단은 각 schema에 남긴다. transport/polling/로컬 저장 정책은 유지한다. |
| 실행 스크립트·배포·CI | replay/selection/fleet의 동일한 임시 JAR·signal·정리 코드를 `scripts/lib/run-cli`가 소유한다. `exec java`로 동작하는 PoC/analyzer, 소유 자원만 정리하는 배포/HA 실험은 수명이 달라 유지한다. |

### #39에서 보존할 계약

- `BatchConsumers`는 재사용 가능한 소비 설정만 가진다. listener가 DB/DLQ 처리를 완료한 뒤 반환하며, 오류 코드는 기존 소비자가 정한다. 첫 earliest 배정에서 DB progress 초기화가 모두 성공한 뒤 미커밋 partition만 초기 commit한다. 기존 commit과 none 정책은 바꾸지 않는다.
- `ConsumerPollTracker`는 성공한 빈 poll도 기록하고, 실패한 poll은 이전 snapshot을 갱신하지 않는다. revoke 시 시각/할당을 함께 비운다. ProjectionConsumer의 공개 PollState는 유지한다.
- `FleetRuntime`은 같은 factory·telemetry·선택적 시청자 관측을 만든다. producer 준비 → worker 임대/수신 → 수신 종료/drain/임대 해제 → 관측기/발행기/telemetry/factory 종료 → 결과 저장의 기존 순서를 유지한다. 출력 파일 선점과 준비 실패 시 빈 파일 보존도 동일하다.
- 웹은 같은 기간·응답과 동일한 DOM/SVG geometry를 사용한다. null 음영과 관측된0건을 구분하며, 채널/방송 identity·TTL에서 시청자 선을 끊는다. 표시 시각의 locale·시간대·문구를 바꾸지 않는다.
- 관리 CLI는 같은 인자·stdout·exit code를 유지한다. build 출력은 stderr, 각 실행은 읽기 전용 JAR 복사본, signal은 자식에 TERM 전달·종료 대기 뒤 정리·130 반환이다. 동시 실행은 서로의 복사본을 삭제하지 않는다.

### #39 검증 기준과 한계

F01/F03/F04/F05/F06/F07/F08/F13/F16 → T01/T02/T03/T04/T05/T08/T11/T14/T16의 회귀 범위다. 처리량·HA·실방송 접근이나 기술 목표의 해결 범위를 새로 승격하지 않는다.

기본 검증은 `./scripts/check`, `./scripts/integration`, `./scripts/web check`, `./scripts/web test:e2e`다. 추가 경계 시험은 초기 DB 실패 후 무커밋/기존 offset 보존, 성공한 빈 poll과 실패한 poll의 freshness 차이, 실행 JAR의 정상/관측 초기화 실패 종료, 관리 스크립트의 signal·동시 실행·build 실패를 확인한다. DB schema·SQL·부하 실험·배포 구성은 변경하지 않으며, 새 live·고부하·HA·이미지 배포 리허설을 실행한 근거로 사용하지 않는다.


### #39 실제 회귀 결과

[정제 근거](poc/evidence/refactor-2026-10-26.json)에 코드 commit, 현재 JAR/buildId, XML/log hash와 환경을 기록했다.

| 실행 | 결과와 코드 범위 |
|---|---|
| `./scripts/check` | 최종 코드07021e4의 단위147·하네스20·JAR 통과 |
| `./scripts/integration` | 최초 리팩토링e5222e0의 실제 Kafka/PostgreSQL94개 통과 |
| `:server:integrationTest --tests io.chatpulse.fleet.FleetRuntimeIntegrationTest` | 07021e4의 실제3개 통과. 관측 활성/비활성 JAR, 초기화 실패, System.exit 없는 정상/실패 자연 종료 |
| `fnm exec --using 24.20.0 ./scripts/web check` | 타입·단위29·빌드 통과 |
| `fnm exec --using 24.20.0 ./scripts/web test:e2e` | Chromium31 통과. null/0·품질·기간 변경·모바일/데스크톱 표시 |

최초 전체 실행 뒤 제품 코드의 후속 차이는 factory 속성 강제형변환 제거와 주석이다. 전체 최종 회귀와 병합 게이트의 정확한 HEAD·run은 [PR #40](https://github.com/joejaeyoung/jigeumini/pull/40)의 기록을 따른다. 최초94개 실행 JAR hash는 보관하지 않았고 현재 hash는07021e4의 실제 실행 JAR다. 초기 Node26 결과와 지정 Node24 재검사를 구분한다. TERM 외 shell 신호·실제 source 활성화는 새로 시험하지 않았다.

### Standards

고정 diff `c48a190…e5222e0`의 규칙 위반0·구조 차단0·별도 heuristic 권고0건. 공통 소비 계약과 실제 변형, DB/worker 자원 소유, API/표현 경계를 읽기 전용으로 검토했다.

### Spec

같은 diff의 지적0·차단0건. 초기 commit/none, poll freshness, Fleet 종료·결과 저장, null/0·360분 절단·TTL 선 연결, shell 인자/종료의 보존을 확인했다. 두 리뷰는 정적 검토이며 실제 실행이나 최종 CI를 대신하지 않는다.

Claude 초기 승인의 비차단 의견에서 강제형변환·null initializer 설명·자연 종료 시험·README를 보완했다. 반복 close와 지역 JsonMapper는 기존 실패/재시도 계약을 보존하기 위해 유지했다. 최종 HEAD 승인은 PR40 게이트에 연결한다.


## 2026-10-26 · 샤딩 실험 경계 #67

F01/F10 → T02/T04/T06/T07/T08. 기존 suffix 배정의 hash 충돌을 발견해 실제 분산 검사와 배정 책임을 `ExperimentRouting`에 모았다. `ExperimentPublisher`는 배정된 record를 유한 발행하고, `LoadExperiment`는 생성·소비·replay·정리를 계속 소유한다. 기존 suffix 프로필만 decode 전 정규화하며 명시 분할은 제품의 channelId 업무 key를 유지한다. 시작 준비 검사는 소비자 수에 맞는 실제 할당을 기다린다.

제품 전용 routing 추상화나 부분 bucket은 이번 단순 실험에 도입하지 않았다. [제품 계약 검토](pipeline/SHARDING_REVIEW.md)의 `ChatRouting` 소유권·품질 기대 집합·정책 이력과 consumer 수명은 #68의 후속 설계다. [6회 근거](poc/evidence/sharding-2026-10-26/report/summary.json)에서 동일 eventId 배정·실제 분포·중복 제거·DB/replay 일치를 확인했다. 제품 `server/src/main`·web·deploy 파일은 변경하지 않았다.


## 2026-10-27 · 수집 메시지와 transport 수명 #64

F01/F13/F14 → T02/T08/T11/T12/T14. `IncomingText`는 UTF-8 전체 한도·surrogate 경계·완료/실패 초기화를 소유한다. `ChatConnection`은 완성 메시지만 protocol/sink/관측에 전달한다. 공개 `ChatProtocol.decode(String, ...)`도 같은 byte 검사를 사용하고 내부 Complete 경로는 중복 순회를 피한다. 실제 UTF-8 codec은 transport가 담당하며 별도 JSON parser를 만들지 않는다.

`ChatSocketClient`는 고정 Tomcat container의 설정·연결·종료를 소유하는 작은 내부 모듈이다. `ChannelProbe`는 재접속 정책과 scheduler 수명을 유지한다. 소켓 close와 container destroy의 차이를 이 모듈에 감추고 late handshake도 정리한다. 공유 singleton client·범용 transport interface는 도입하지 않는다. 정확히1MiB·wire fragment·재접속·다른 owner 보존과 실제 heap/자원 반복 시험으로 경계를 검증한다. [조사](research/COLLECTOR_TEXT_MEMORY.md).

최종 리뷰에서 실험 태스크의 속성 평가는 실행 인자가 제공될 때만 하도록 보완해 일반 Gradle 탐색과 실험 실행의 설정 경계를 분리했다. 정제기의 실제/기대 종료값·출처도 구분하며 전체 메시지 한도를 전역 조립 예산으로 설명하지 않는다.

## 2026-10-27 · 집계 SQL와 진단 책임 #65

F05/F06/F10 및 F01/F13/F14 → T02/T07/T08/T11/T12/T14, F16 → T16. [검증 문서](PROJECTION_PERFORMANCE_VERIFICATION.md)에 입력·실행 commit·정제 근거·한계를 연결한다.

- CategoryStore는 관측 시점 결합과 입력 저장을 같은 SQL에 두고 배치별 partition 진행만 합친다. 멱등 ledger와 bucket·offset은 기존 transaction을 공유한다.
- ReportStore는 새 관측의 기존 기록 보정, 새 채팅 귀속, 채팅만 있는 경우의 TTL 갱신을 구분했다. 전역 generic repository나 store 사이 공유 상태를 추가하지 않았다. 순서가 필요한 채널 잠금과 별칭/만료 방어는 기존 경계에 둔다.
- ChatConnection은 최초 실패 확정을 한 곳에서 원자적으로 소유한다. 예외·ready·observer가 같은 완료 값을 사용하게 해 cleanup/transport 경합에 의한 다른 실패 코드를 제거했다. 예외만 바꾸고 실제 원인을 고쳤다고 주장하지 않는다.
- 부하 원본과 판정·정제를 구분한다. 가시성 미관측을 성공 표본 quantile에 숨기지 않고, 미완료·시계 오류·중첩 실행도 hash와 제외 이유를 보존한다. 정제기의 원본/보조 코드 hash를 버전 관리한다.
- migration 하네스는 다음 번호를 계산하되 제품 버전·이미지 revision/buildId·이전 schema를 검증한다. 복원 계약을 우회하는 번호만의 수정은 하지 않았다.

계약 회귀·실제 DB·부하 최종 대조와 구현 CI 179/142/38/29/31을 확인했다. 초기 Standards/Spec 지적0, 연결 진단 Spec P2는 회귀 후 해결했다. 비차단 watermark 토픽 표시는 문서에 반영했다. 최종 HEAD의 Claude 승인과 CI는 PR71 병합 게이트로 남긴다.

최종 a536259의 [Standards/Spec 검토](poc/evidence/projection-performance-2026-10-27/review-final.json)는 각각 차단0·비차단1이었다. 세션 TTL 색인 설명과 미실행 계측의 #49 재개 조건을 보완했다. 제품·하네스 실행 코드는 유지했다.


## 2026-10-27 보존 경계 #63

F01/F08/F10/F16 → T02/T04/T08/T16. `RetentionWindow`가 하한·잠금 순서를 소유하고 `ProjectionRetention`/`ReplayRetention`/`HistoryRetention`이 서로 다른 보호 계약을 맡는다. `RetentionMaintenance`는 실행 예산·재시도·진단만 관리하며 도메인 DELETE를 알지 않는다. 기존소비인터페이스에서cleanup을제거하고worker한곳으로수명주기를모았다.

리뷰에서report부모1,000행한도가int합계에서소실되는문제를확인해`CleanupBatch`로삭제량과한도도달을함께전달했다. `KafkaSupport.awaitTopic`의고정기한·예외분류를설정validator와분리해실험도공유하며제품정책최소값은유지한다. 임의시간partition이나범용정리SQL생성기는현재멱등PK·보호참조가달라추출하지않았다.

검증은[보존 경계·실제저장량](pipeline/RETENTION_VERIFICATION.md)에연결한다. Standards최종8624c04지적0, Spec93cf16f이전P2해소·추가지적0. 단기삭제처리율을장기운영용량으로해석하지않는다.
