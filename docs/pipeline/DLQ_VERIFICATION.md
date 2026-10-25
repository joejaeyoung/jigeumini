# 오류 격리·소비 복구 검증

2026-10-25 · F09 → T01/T03/T05/T08. 계약은 [DLQ](DLQ.md), 목표별 상태 정본은 [STATUS](../STATUS.md)다.

## 환경과 입력

Java 21, Kafka 4.1.2 단일 broker/RF=1, PostgreSQL 16.14, Node.js 24와 Chromium을 사용한다. 각 통합 테스트는 UUID topic·group·DB schema를 만들고 그 자원만 정리한다. 실제 방송이나 다른 worktree의 입력을 사용하지 않는다.

합성 입력은 정상 2건 앞뒤의 malformed 1건, 중복 eventId, 같은 배치의 서로 다른 payload, 이미 DB에 반영한 ID의 충돌, schema 위반으로 정의한다. 장애 시험은 partition 0의 정상 1건→오류 1건→정상 1건과 시작 offset 0을 사용한다. 기대값은 정상 ledger/bucket 2건·격리 1건·다음 offset 3이다. 두 독립 소비자는 각각 같은 기대값을 가진다.

## 장애와 대조 항목

| 시험 | 실패 주입과 대조 |
|---|---|
| 영구 오류·독립 소비 | ranking/trend 각각 정상 집계·failureId별 격리 수·Kafka offset, 동일 배치 충돌 후보 모두 제외, 기존 ledger 충돌 보존 |
| DLQ broker 거부/복구 | DLQ 토픽의 max.message.bytes=64, 실제 broker의 발행 거부, DB/원본 offset 0 유지; 설정 복구 후 기대값 2/1/3 |
| ACK 직후 JVM 종료 | halt(93), DLQ 1건·DB 0건·offset 0; 재시작 후 같은 failureId의 DLQ 2건, DB 격리는 1건·집계 2건 |
| DB commit 직후 JVM 종료 | halt(94), DB 집계 2건·격리 1건·offset 0; 재시작 후 원본 offset 3, DLQ 추가 발행 없이 1건 유지 |
| DB 장애 | processed_event trigger 오류 중 정상 입력의 DLQ/DB/offset 0; trigger 제거 뒤 정상 입력 1건·DLQ 0 |
| ACK 후 DB rollback | 진행 위치 UPDATE trigger 오류: ACK가 있어도 정상 ledger/bucket·격리·진행 모두 0 유지, 복구 후 2/1/3 |
| 확정 정상 입력 재전달 | DB commit 직후 halt(94) 후 48시간 경계 초과·새 충돌 입력 추가를 각각 재현. 기존 offset 0의 격리는 0건; 새 충돌 offset 1만 격리 |
| 원본 보존·identity | deleteRecords 후 EXPIRED, topic 재생성 후 TOPIC_CHANGED·동일 namespace 진행 거부 |
| 품질 | 격리 구간 제외, 이미 공개된 snapshot/spike의 경고, 상세 count=null, 채널 미확인 오류가 신규 채널에도 적용 |
| 실행 JAR | Spring analyzer의 양쪽 group·실제 HTTP 진단 응답·운영 HTML 제공 |
| 브라우저 | 합성 운영 응답의 0/null lag, 오류 수, 보존 만료, 관측 불가와 HTTP 실패 시 이전 표본 표시 |

## 실제 실행 결과

[정제 근거](../poc/evidence/dlq-2026-10-25.json)에 구현 commit `cb4117a`, 실행 JAR SHA-256, suite별 시각·건수와 장애 대조값을 기록했다.

| 명령 | 결과 |
|---|---|
| `./scripts/check` | 서버 단위 58개·하네스 16개·JAR, 기술 목표/문서 링크 통과 |
| `./scripts/integration` | 실제 Kafka/PostgreSQL 37개 통과: DLQ 7개, 기존 파이프라인 9개, 품질 11개, replay 10개 |
| `./scripts/web check` | 타입·웹 단위 17개·제품 빌드 통과 |
| `./scripts/web test:e2e` | Chromium 23개 통과, 운영 진단 사례 1개 포함 |

서버 테스트 결과 파일에서 합성 원본 canary 표식이 남은 파일은 0개였다. 이는 해당 합성 경로의 출력 검사이며 모든 운영 입력의 전수 개인정보 감사는 아니다.

미확인 오류의 품질 영향 범위는 수신 시각 +5초와 eventTime의 추가 +5초 허용치를 함께 고려해 마지막 가능 bucket 끝까지 포함한다. 고정 시각 fixture로 이 경계를 확인했다.

## 시험 설계에서 확인한 점

최초 테스트 준비에서 Kafka Admin의 default.api.timeout.ms=10000에 기본 request.timeout.ms=30000이 충돌했다. 요청 timeout을 명시해 수정했다. 이 실패는 DLQ 처리 시험에 진입하기 전의 설정 오류였다.

RF=1에서 min.insync.replicas=2로 ACK 실패를 유도하려던 시험은 정상 발행됐다. 설정 조회를 추가해도 재현됐으며 소비 상태는 RUNNING·실패 시도 0이었다. [Kafka 4.1.2 Partition 소스](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/cluster/Partition.scala#L354-L361)는 유효 최소 ISR을 복제 수 이하로 제한한다. 따라서 이 방법을 장애 근거로 쓰지 않고, DLQ 토픽의 허용 크기를 내려 실제 broker가 발행을 거부하는 시험으로 교체했다. 이는 broker 전체 중단·네트워크 단절·복제 HA 시험이 아니다.

Claude 최초 리뷰는 확정된 정상 offset을 다시 분류하는 문제로 REQUEST_CHANGES였다. 시간 경과·배치 구성 변경의 두 JVM 재시작 시험이 수정 전 각각 실패했다. DB 진행 위치를 같은 트랜잭션에서 잠그고 미확정 입력만 분류하도록 변경했다. 테스트 fixture도 ledger와 격리 저장이 같은 DataSource의 트랜잭션에 참여하도록 맞췄으며 ACK 뒤 SQL 실패를 추가해 원자 rollback을 대조한다. 운영 Spring 경로는 원래 같은 DataSource bean을 공유한다.

## 해석의 한계

동일 내부 eventId와 failureId의 DB 중복 방지, 특정 ACK/commit 경계의 재전달을 확인한다. 전체 시스템 Exactly-once·무손실·성능·장시간 복구를 입증하지 않는다. 시각을 확인할 수 없는 입력은 전체 수집 범위를 보수적으로 제외한다. DLQ envelope로 원본을 복원하지 않으며 자동 재투입도 없다.

PR #18의 토픽 생성 직후 describeConfigs 간헐 오류는 별개의 알려진 위험이다. 이번 시험이 통과해도 해당 원인을 수정한 것으로 해석하지 않는다.
