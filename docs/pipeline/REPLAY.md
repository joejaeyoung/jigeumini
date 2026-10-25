# 고정 offset 재처리·집계 비교

기능 F08 · 이슈 #9/#16 · 기술 T01/T02/T03/T04. 운영자가 Kafka에 **현재 보존된 고정 입력**을 별도 run으로 다시 읽어 채널·10초 bucket을 비교하는 로컬 관리 CLI다. 웹 서버나 collector/analyzer를 함께 실행하지 않는다. 실행법은 [루트 README](../../README.md), 상태·검증 근거는 [STATUS](../STATUS.md)가 정본이다.

## 입력 계약

`capture`는 허용된 `PIPELINE_TOPIC`(기본 `chat.received.v1`)의 topic ID, 전체 partition, 각 `[start,end)`, eventTime 필터, schemaVersion=1, algorithmVersion=`bucket-10s-v1`, 생성 시각을 JSON manifest로 고정한다. 기존 파일을 덮어쓰지 않는다. 누락 partition·알 수 없는 버전·잘못된 범위는 거부한다.

현재 보존된 earliest부터 end까지 읽는다. Kafka timestamp는 receivedAt이며 eventTime이 아니므로 timestamp lookup으로 범위를 좁히지 않는다. 선택 기간은 디코딩 후 `[eventFrom,eventTo)`로 필터한다. 필터 밖 입력도 스키마·키·이벤트 시각의 상대 계약을 검사한다. 필터를 통과한 이벤트의 중복은 eventId·payload hash로 검사한다. `read = filtered + unique + duplicate`는 성공적으로 commit한 배치 기준이다. 필터 밖 이벤트는 ledger에 넣지 않으므로 중복을 별도 계산하지 않으며, 같은ID의 필터 안/밖 사본 사이 payload 충돌도 탐지하지 않는다. 이 제한은 read/filtered 계수와 결과 비교 해석에 남는다.

**삭제된 입력과 capture 이후 도착한 입력은 재현할 수 없다.** 보존 설정 24h·partition당 256MiB는 24시간 전체 가용성도 보장하지 않으며, DB의 7일 통계를 모두 재생할 수 있다는 뜻이 아니다. manifest 생성 직후 추가 발행돼도 end는 늘리지 않는다.

## 처리·격리

- `projection=replay`, UUID runId와 `replay-<runId>` group ID를 사용한다. assign/seek만 하고 Kafka group offset을 커밋하지 않으므로 broker에 replay group이 등록되지 않을 수 있다.
- 운영 ranking/trend의 ledger·bucket·group offset·순위 snapshot·급상승/알림을 쓰지 않는다. 과거 관측 품질·알림 재현은 범위 밖이다.
- 원래 이벤트 계약은 유지한다. replay 경로만 현재 시점 기준 live-age 제한을 적용하지 않는다. live `apply`는 기존 제한을 유지한다.
- 새 이벤트 ledger·bucket·필터 밖을 포함한 DB 진행 offset·계수는 같은 트랜잭션에 반영한다. 같은 eventId의 같은 payload는 재증가하지 않으며 충돌/SQL 실패는 배치 전체 rollback이다. 원천이 같은 채팅을 새 eventId로 보내는 중복은 제거하지 않는다.
- 정상 입력 topic의 연속 offset을 요구한다. control record·compaction 등으로 데이터 record 사이 offset이 건너뛰면 `OFFSET_GAP`이다. 제어 record만 꼬리에 남아 다음 데이터 record가 없다면 progress가 end에 도달하지 못하고 기본30초 무진행 뒤 `NO_PROGRESS`다. 자동으로 offset을 채우거나 성공 처리하지 않는다. topic을 만들거나 설정을 변경하지 않는다.
- 실행 전·배치 전후·완료 전에 topic identity/partition/earliest/end를 다시 검사한다. topic 재생성·partition 변경·retention 만료·끝 offset 절단은 실패다. 이미 읽은 prefix가 실행 중 삭제된 경우도 보수적으로 실패한다. 이 검사는 Kafka와 DB 사이의 분산 원자적 스냅샷을 의미하지 않는다.

## 작업 상태와 중단

`QUEUED → RUNNING → COMPLETED | FAILED | CANCELLED`. 대기 중 취소도 가능하다. `worker`는 대기 작업 하나를 처리하고 종료하며, 실행하지 않으면 QUEUED로 남는다.

worker는 DB session advisory lock으로 같은 schema에서 하나만 실행한다. 짧은 `FOR UPDATE SKIP LOCKED` 트랜잭션으로 작업을 가져오고, 실제 처리는 그 밖에서 한다. 각 쓰기 배치·완료는 RUNNING과 owner UUID를 잠근 행에서 확인한다. 모든 partition의 진행 offset이 고정 end에 도달해야 COMPLETED다.

heartbeat는 루프에서 5초마다 및 DB 배치 commit 때 갱신한다. poll 0.5초, Kafka API별 최대 5초, DB statement 5초·transaction 20초이며 외부 작업 호출이 진행 중이면 heartbeat·취소 관측도 지연될 수 있다. 취소 요청은 다음 쓰기 트랜잭션/완료 전에 다시 확인한다. 잠금/연결 timeout으로 취소 명령 자체가 실패하면 오류를 출력하므로 재시도해야 한다. 무진행 30초·전체 30분 한도는 다음 루프 경계에서 검사하며 진행 중 API/DB 호출 한도만큼 더 걸릴 수 있다.

`cancel`, `reap`, `worker`는 필요할 때 watchdog을 실행한다. `status`와 `compare`는 읽기 전용이며 watchdog·migration을 실행하지 않는다. heartbeat가 30초 넘게 없고 worker session lock도 해제된 작업을 WORKER_LOST로 실패 처리한다. 살아 있는 worker의 긴 처리만으로 실패시키지 않는다. 전역 schema lock이므로 다른 정상 worker가 실행 중이면 먼저 죽은 run도 reap되지 않는다. 그 worker의 잠금 해제 뒤 다음 `reap`/`cancel`/`worker`에서 판정한다. 상시 daemon은 아니며 조회만 반복해도 죽은 worker의 RUNNING 상태를 바꾸지 않는다. **자동 재개는 하지 않는다.** 부분 ledger/bucket/offset은 진단용으로 보존하고 같은 manifest로 새 run을 만들어 다시 시작한다. FAILED/CANCELLED는 비교 기준으로 쓸 수 없다.

## 결과 비교·출력

서로 다른 두 run ID이며 동일 topic ID·offsets·필터·schema/algorithm·확인된 worker build ID를 가진 COMPLETED run끼리만 비교한다. 동일 run 자기 비교와 서로 다른 빌드, 빌드를 모르는 이전 run은 `INCOMPATIBLE_RUNS`다. 생성 시각은 비교 동일성에서 제외한다. 다른 입력·미완료 run은 `INCOMPATIBLE_RUNS`다. 계속 변하는 live 결과와는 직접 비교하지 않는다.

비교 일치 시 `equal=true`·종료 코드 0, 불일치 시 `equal=false`·코드 2다. 출력은 matched/mismatched bucket 수, 양쪽 합계, 최대 100개 bucket 차이, 각 run의 읽음/필터 제외/고유/중복/오류 수, manifest와 실제 진행 위치다. 두 run 일치는 같은 버그를 공유할 수 있으므로 합성 고정 기대값 대조 시험도 함께 사용한다. 원문·사용자 정보·토큰·원본 예외는 출력하지 않는다. 실패 run의 계수는 마지막 성공 commit까지이며 실패 배치의 읽기 시도 건수를 포함하지 않는다.

기본 접속은 loopback Kafka/PostgreSQL이다. 기존 `PIPELINE_BOOTSTRAP_SERVERS`, `PIPELINE_TOPIC`, `PIPELINE_JDBC_URL`, `PIPELINE_DB_USER`, `PIPELINE_DB_PASSWORD`로 접속을 설정한다. 명령 실행 권한이 있는 로컬 운영자용이며 HTTP 관리 API를 열지 않는다. 단일 호스트·단일 broker 검증이며 HA·네트워크 분할 복구는 주장하지 않는다. replay 결과의 자동 보존 삭제·운영 승격·알고리즘 버전 간 비교는 후속 범위다.


## 조회와 migration·빌드 이력 (#16)

`./scripts/replay migrate`만 명시적으로 Flyway를 실행한다. 최초 create/worker 전에 실행한다. 서버 기동의 기존 migration은 유지하지만 replay의 create/cancel/worker/reap도 자동 migration을 하지 않는다. `status/compare`는 Hikari 읽기 전용 연결과 읽기 트랜잭션으로 조회만 수행한다. schema가 없거나 이전 버전이면 조회가 실패하며 새 테이블을 만들지 않는다.

생성 빌드 `created_build`와 실행을 claim한 빌드 `worker_build`를 V10에 기록한다. 빌드 ID는 정렬한 서버 main 소스/리소스·server build.gradle·settings.gradle·실제 runtime 의존 JAR들의 이름과 바이트를 SHA-256으로 계산해 JAR 리소스에 포함한다. 시각·로컬 절대 경로는 제외한다. 실행 JAR 파일 자체의 hash나 서명 검증을 대신하지 않으며 JVM/OS 차이까지 동일성을 보장하지 않는다. 정제 실행 근거에는 별도 JAR hash를 기록한다. 같은 algorithmVersion이어도 빌드 ID가 다르면 의미적 동일성을 자동 승인하지 않는다. 새 빌드에서 같은 manifest로 두 run을 다시 실행해야 한다. V10 이전 완료 run은 worker_build가null이라 비교하지 않으며 상태 조회는 가능하다.

`./scripts/replay`의 실행용 JAR 복사본은 정상 종료·오류 반환 및 INT/TERM 처리 뒤 지운다. JVM이 쓰는 동안 지우지 않고 종료를 기다린다. 강제 SIGKILL/호스트 종료로 남은 `work/runtime/replay.*`는 해당 replay 프로세스가 모두 종료됐음을 확인한 뒤 로컬에서 정리한다. 실행 중 JAR나 다른 pipeline 복사본을 자동 청소하지 않는다.

## 보존 상태를 수치로 확인

`./scripts/replay retention <manifest.json>`은 DB를 열지 않고 현재 topic ID/partition과 보존 bounds를 조회한다. capturedStart/End·currentEarliest/End, capturedOffsetSpan·availableCapturedOffsetSpan·missingPrefixOffsetSpan과 AVAILABLE/RETENTION_EXPIRED/OFFSET_TRUNCATED를 반환한다. **offset 폭은 record 개수가 아니다.** 삭제된 prefix의 원래 eventTime이나 삭제 건수는 알 수 없다.

각 partition의 가용 capture 범위 처음/마지막 최대200 offset을 각1초 안에 표본 조회한다. `firstProbe`/`lastProbe`에 실제 probe 범위·관측 offset·Kafka timestamp를 넣는다. 비어 있는 범위는null, 제한 시간 내 데이터가 보이지 않으면NOT_OBSERVED다. 제어 record와 접근 지연 때문에 경계 표본이 없을 수 있다. Kafka timestamp는 발행 receivedAt이며 eventTime과 같지 않고 offset 순서대로 증가한다는 보장도 없다. 이 표본은 기간의 모든 채팅을 포함했다는 증거가 아니다. 표본 및 bounds 조회 사이에 보존 상태가 바뀔 수 있으므로 worker는 기존대로 재검증한다.

토픽이 존재하지 않으면 `TOPIC_NOT_FOUND`, broker 접속/조회 실패는 `SOURCE_UNAVAILABLE`, 날짜 문자열 파싱 실패는 `INVALID_DATE`를 반환한다. 권한 오류를 토픽 삭제로 추정하지 않는다. 원본 예외·원문·토큰은 출력하지 않는다.

#16 검증 근거와 현재 상태는 [후속 검증](REPLAY_DIAGNOSTICS_VERIFICATION.md)·[STATUS](../STATUS.md)에 연결한다.
