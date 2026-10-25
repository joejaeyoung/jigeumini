# 관심 방송 알림 검증

2026-10-25 · F07 / #8 · T02/T03/T08/T10. 구현 commit·실행 JAR SHA-256·suite 실행 시각은 [정제 근거](../poc/evidence/notifications-2026-10-25.json)에 고정한다.

## 실행

`./scripts/check`: Java 단위 67, 하네스 16, 실행 JAR 통과. `./scripts/integration`: 실제 Kafka/PostgreSQL 47개(알림 5·DLQ 10·토픽 준비 2·기존 파이프라인 9·품질 11·replay 10) 통과. `./scripts/web check`: 타입·단위 21·빌드 통과. `./scripts/web test:e2e`: Chromium 25 통과.

서버 최종 검증은 `d8f3955`, 웹 검증은 이후 웹 변경이 없는 `c031171` 기준이다. 토픽 조회 간헐 실패의 제한 재시도와 재현 범위는 [별도 기록](TOPIC_READINESS.md)을 따른다.

입력은 합성 채널과 UUID, 1분 100건/기준 50건/비율 2의 확정 spike다. 각 시험은 UUID topic/group/DB schema를 사용하고 해당 자원을 정리한다. Kafka 4.1.2 단일 broker/RF=1, PostgreSQL 16.14, Java 21 환경이다.

## 실패 경계와 결과

| 경계 | 실제 주입·대조 |
|---|---|
| spike/outbox 원자성 | 기존 실제 급상승 판정 시험에서 outbox INSERT를 SQL trigger로 거부. spike=0, alert state는 이전 tick 유지. 제거 후 spike/outbox 각각1 |
| outbox commit 직후 | 별도 JVM halt(95). spike/outbox 각각1, Kafka0. 다음 publisher가 미전달 건을 발견 |
| Kafka ACK 직후 | 별도 JVM halt(96). Kafka1, published_at=null. 재기동 발행 후 Kafka2, feed1, group commit2. 소비자 재시작 후에도 feed1 |
| broker 거부 | 격리 토픽 max.message.bytes=64. 발행 실패 동안 Kafka0·published_at=null. 복구 후 Kafka1·전달 완료 |
| feed DB 장애 | notification_progress INSERT trigger 실패 시 feed·진행 위치 함께 rollback. 복구·중복 입력 후 이벤트2·next_offset2 |
| 내용 충돌 | 같은 spikeId의 다른 근거는 SPIKE_ID_CONFLICT. 기존 두 기록과 offset2 보존 |
| 독립 소비 | feed를 실제 60초 중단. 그동안 같은 DB의 ranking은 합성 채팅3건 처리, feed0. 재시작 후 feed3·commit3 |
| 조회 | 관심 필터, 시각/UUID cursor, 601초 지연, source 품질 정정 valid=false, 빈 관심 목록, 잘못된 인자400·DB 장애503·no-store |
| 정리 | 전달 후7일·feed 있음만 삭제. pending와 미소비, feed 멱등 ledger 보존 |
| 실제 실행 JAR | analyzer HTTP의 빈 알림 피드, 운영 API와 알림 group 3 partition 할당 확인. 기존 ranking/trend 복구 진단 회귀 유지 |

웹은 관심 채널 필터 변경 시 cursor 초기화, 읽음 reload 유지, 지연 항목/정정/조회 실패의 새 반응 억제, 페이지 이동, 중복 응답 거부, 320px 가로 넘침을 검증했다. 최초 fixture가 요청하지 않은 채널을 추가 반환해 메타데이터 계약 검사에 실패했으며 요청 ID 범위로 수정 후 통과했다.

이벤트의 숫자 문자열·소수 건수는 자동 변환하지 않고 거부한다. 입력 본문/SQL 합성 canary가 서버 XML 결과에 나타난 파일은 0개다.

Claude 리뷰 이후 600초+1마이크로초의 정상 지연 응답을 웹이 거부하는 문제를 재현했다. Date의 밀리초 절삭 아래 소수부를 보존하고 경계 앞/일치/뒤·시간대 차이를 단위 검증했다. TrendPolicy의 101/30=3.36666667 근거 round-trip, 저장 payload 계약 오류의 고정503도 검증했다. 명시적 topic bean 주입 후 실행 JAR를 포함한45개 통합 검증을 다시 통과했다.

## 해석과 한계

outbox ACK와 PostgreSQL 저장은 다른 경계이며 물리 재발행은 허용한다. 같은 spikeId의 feed 중복은 DB에서 막는다. Broker ACK가 feed 저장 또는 복제 HA를 뜻하지 않는다. 정상 DB 재시도·정정은 기존 공개 spike를 보존하며 과거 replay는 알림을 발행하지 않는다.

별도 [실방송 화면 확인](../poc/evidence/runtime-smoke-2026-10-25.json)은 공개3채널의 약5분30초 수집·웹/API를 확인했다. 6,965건 수신=Kafka ACK=DB ledger/bucket 증가였다. 사용자가 확인 후 종료를 요청해600초 계획 전에 중단했으며 collector 판정은 CANCELLED/FAIL_OR_INCONCLUSIVE다. 완료 시간 시험의 PASS로 바꾸지 않는다. 실제 급상승이 관측되지 않아 live outbox 전달 근거에는 포함하지 않는다.

broker 전체 중단, 네트워크 단절, 복제 HA, 다중 publisher 처리량·고부하는 이번 검증에 포함하지 않는다. 독립 group도 DB와 broker 자원을 공유한다. 10분 지연은 고정 clock으로 검증했고 실제10분 대기 시험이라고 주장하지 않는다. feed/live ledger 정리는 별도이며 조회7일 제한과 저장삭제를 혼동하지 않는다. PR #18의 describeConfigs 간헐 실패는 원인이 확정되지 않았다.

진행·리뷰·병합 상태의 정본은 [STATUS](../STATUS.md)와 연결된 PR이다.
