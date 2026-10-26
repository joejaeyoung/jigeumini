# 보존·정리·복원 경계

#63 · F01/F08/F10/F16 → T02/T04/T08/T16. 구현 검증 중이며 완료 여부는 [STATUS](../STATUS.md)가 정본이다. [공식 자료 조사](../research/DATA_RETENTION.md)는 구현 전 후보와 사실을 구분한다.

## 기본 정책

삭제 조건은 기준 시각 **미만**이며 같은 시각은 보존한다. PostgreSQL에 저장한 입력 하한은 시계가 뒤로 움직여도 낮아지지 않는다.

| 자료 | 보존 기준 | 보호 조건 |
|---|---|---|
| ranking/trend processed_event | 원래 receivedAt 72시간 | namespace별 원천 identity·확정 offset·입력 하한 영구 유지 |
| ranking/trend chat_bucket_10s | eventTime bucket 8일, 10초 경계 | 수락 가능한 eventTime보다 과거만 정리 |
| 시청자 관측 | observedAt 7일 | 전체 run에 공통 단조 하한 |
| category event/bucket | eventTime 7일, bucket 경계 | 관측은 추가120초 보존 |
| report event/관측/bucket | 종료 확인 뒤7일 | 미종료 세션의 전체 이력·세션 identity/anchor/alias 보호 |
| report 미귀속 입력 | eventTime 7일, 관측 추가120초 | 오래된 부분 정리 결과를 새 세션으로 합치지 않음 |
| 알림 수신 이력 | evaluatedAt 7일 | 연결된 outbox가 남아 있으면 보호 |
| outbox | publishedAt 7일 | 미발행·수신 이력 미확인 항목 보호 |
| spike 이벤트 | evaluatedAt 8일 | outbox가 남아 있으면 보호 |
| 순위 snapshot/품질 정정·종료된 수집 세션·loss | 8일 | 보존 중 report의 가장 오래된 observedFrom까지 과거 품질 보호 |
| projection_failure | affectedTo 8일 및 quarantinedAt 8일 | 위 report 품질 보호 범위 유지 |
| replay 결과 | terminal finishedAt 7일 | QUEUED/RUNNING은 나이만으로 삭제하지 않음 |
| replay manifest·상태·계수 | terminal finishedAt 30일 | 결과 자식 정리 완료 이후만 삭제 |

source identity, DB progress, alert state, lease epoch, live namespace의 입력 하한은 시간 삭제하지 않는다. 과거 namespace/채널/worker의 식별 상태는 규모에 비례해 남는다. 이미 사용한 run ID를 재사용하거나 guard만 삭제하면 보호 계약이 깨진다. namespace 폐기는 소비자·수집기 정지, 백업, 관련 상태의 일괄 폐기와 새 run ID/새 group 생성이 필요한 별도 관리 작업이다.

## 멱등성의 유효 범위

- 같은 원천 topic identity/partition/offset은 기존 DB progress로 먼저 제외한다. ledger 정리가 이 위치를 후퇴시키지 않는다.
- 같은 UUID와 불변 payload가 새 offset에 다시 오면, 보존 중에는 hash/UUID로 중복 제거한다. ledger 삭제 뒤에는 원래 receivedAt 입력 하한으로 거부한다. live 복구 경로는 기존 OUTSIDE_LIVE_RETENTION DLQ·품질 계약을 사용한다.
- plain apply에도 입력 하한을 적용한다. ledger INSERT와 bucket·progress는 같은 transaction이다.
- **삭제한 UUID에 최근 receivedAt/다른 payload를 붙인 재발행의 영구 충돌 탐지는 보장하지 않는다.** 현재 입력 계약은 eventId와 payload가 불변인 재전달이다. 무한 충돌 탐지를 위해 ID/hash를 영구히 보관한다고 주장하지 않는다.
- category/report/관측/알림의 기존 시간 필터도 단조 하한과 함께 적용한다. category/report/관측의 오래된 입력은 기존대로 skip하고 progress를 전진시킨다. 알림은 7일 조회 범위 밖이라도 해당 outbox가 남아 있으면 수신 확인을 저장한다. 오래 밀린 첫 발행도 outbox 정리가 완료될 수 있으며, 피드에는 다시 노출하지 않는다. outbox가 사라진 오래된 재전달은 저장하지 않는다.

`retention_window` 행을 집계 transaction은 FOR SHARE, 하한 전진은 UPDATE로 잠근다. 따라서 전진 전에 시작된 집계가 끝나거나 정리 잠금 예산이 끝난 뒤 다음 주기에 재시도한다. ranking/trend는 먼저 하한을 commit하고 ledger와 bucket을 각각 제한된 transaction에서 지운다. 중간 실패 후에도 하한은 남아 과거 재집계를 막고 다음 batch가 남은 행을 다시 찾는다.

live `/api/status`의 ledger 총수와 bucket 총수는 **서로 다른 보존 창**의 값이다. 자동 정리 후 전체 누적 합계가 같아야 한다는 검사를 적용하지 않는다. 응답의 retention.totalsScope=DIFFERENT_RETAINED_WINDOWS와 두 floor를 함께 읽는다. fresh DB의 고정 입력 정합 대조는 기존대로 가능하다.

## replay 만료

terminal 결과가 만료되면 부모 행을 잠가 result_expired_at을 먼저 commit한다. 이후 ledger/bucket/progress를 나눠 삭제한다. `status`의 작업 state(COMPLETED/FAILED/CANCELLED)는 유지하고 `resultState=EXPIRED`, totals/progress=null로 반환한다. 완료 당시 계수는 metrics에 남지만 현재 결과 조회 합계가 아니다.

- 비교는 RESULT_EXPIRED로 거부한다. Kafka 원본 보존 실패 RETENTION_EXPIRED와 다른 코드다.
- metadata까지 정리되면 RUN_NOT_FOUND다. 영구 tombstone을 남기지 않는다.
- applyReplay는 RUNNING 부모 존재·미만료를 FOR SHARE로 확인한다. progress 초기화도 활성 부모가 필요하다. 오래된 Java 호출이 삭제된 run의 namespace를 다시 만들 수 없다.
- status/compare의 REPEATABLE READ는 만료 전 snapshot이면 완전한 이전 결과, 만료 이후면 명시 만료를 보게 한다.
- create는 DB 잠금 아래 active QUEUED/RUNNING 최대32개를 검사한다. 초과하면 QUEUE_FULL이며 active 자료를 강제로 지우지 않는다. terminal·보호 자료의 bytes 상한을 의미하지 않는다.
- worker heartbeat가 오래됐다는 이유로 retention이 지우지 않는다. 기존 worker lock과 reap의 실패 확정 후에만 terminal 보존 기간이 시작된다.

## 정리 예산과 진단

분석기는 정리 전용 worker에서10초 간격으로 작업을 순회한다. 한 회차는8초가 지난 뒤 새 작업을 시작하지 않고 다음 회차에 cursor부터 재개한다. 진행 중 작업은 자체 transaction 예산까지 실행할 수 있으므로 회차 전체가8초 안에 끝난다는 보장은 아니다.

- ranking/trend: namespace별 최대 ledger10,000·bucket10,000행. 하한과 두 삭제는 각각2초 transaction, lock100ms·statement1초.
- replay: 한 회차 최대 terminal run1개를 선택하고 각 자식10,000행, metadata100행. 각 transaction2초·lock100ms·statement1초.
- category/관측/알림/outbox: 각 삭제 최대10,000행. report의 session/alias 부모도 각각1,000행 상한. 이 경로는 전체 transaction8초·lock100ms·statement1초.
- 품질 이력: snapshot 최대100개, 자식·각 이력10,000행, 전체8초·각 statement1초.
- SKIP LOCKED의0행이나 삭제0행을 전체 정리 완료로 보고하지 않는다. limited는 추가 backlog 가능성이고 실제 남은 행수는 아니다.
- 수신500/s이면 projection당 분당30,000개 ledger가 늘 수 있다. 명목10초×10,000행 예산만으로 처리율을 보장하지 않는다. 실제 성공 주기·삭제 처리율·가장 오래 남은 자료·live lag를 함께 확인한다.

`ops retention-status`는 마지막 시도/성공·RETRYING·실패 횟수·마지막 성공 삭제량·소요 시간, table/index bytes·dead tuple 추정·클러스터 WAL 생성 누계와 protected 자료를 반환한다. 원문/사용자 식별자/SQL 값/예외 본문은 내보내지 않는다. DB 단절 중에는 상태 저장도 실패하므로 오래된 attempted_at 자체가 경보다. relation bytes는 OS 디스크 여유가 아니며 WAL 생성 누계는 현재 WAL 디렉터리 점유량이 아니다.

```sh
./scripts/ops retention-status
./scripts/replay cleanup  # 종료 replay의 한정 batch만 수동 실행, 반복 가능
```

읽기 진단에는 runtime DB 권한을 사용한다. replay cleanup은 DB 삭제 작업이며 상태의 보존 계약을 적용한다. 공개 원천 요청이나 Kafka group reset을 실행하지 않는다.

## 용량과 운영 조치

다음 수치는 초기 경보 후보이며 실제 디스크/입력량·복구 창을 검증한 운영 SLO가 아니다.

| 조건 | 조치 |
|---|---|
| 사용률70% 또는 최근24시간 증가 추세로7일 이내 고갈 예상 | 정리 성공·dead tuple·WAL/백업 크기·보호 이력·유입률 확인, 증설 준비 |
| 사용률80% 또는48시간 이내 고갈 예상 | 신규 채널/재처리 admission 중단, 검증된 수집 상한 축소, 정리 backlog 원인 해결 |
| 사용률90% 또는12시간 이내 고갈 예상 | 공개 수집 중단·품질 gap 표시, 보존 정책을 임의로 줄이기 전에 복구 가능 창 확보와 디스크 증설 |
| 정리 성공 없음60초·연속 실패3회·유입보다 삭제 가능 처리율이 낮음 | 원인 코드·DB lock/부하·protected 자료 확인. 자동 강제 삭제 금지 |
| Kafka earliest가 필요한 DB/PITR/replay offset에 접근 | 수집/복구 계획 재검토. 부족한 로그를 복원 완료로 표시하지 않음 |

이 조치들은 운영 절차이며 디스크 포화에 따른 자동 수집 차단기를 구현한 것은 아니다. 실제 경보/차단 연결은 #49 운영 검증에서 확인한다. 장기 active report·미발행 outbox·active replay는 보호 때문에 TTL을 넘을 수 있다. 따라서 이번 정책을 절대 byte cap 또는 전체 플랫폼 운영 용량으로 설명하지 않는다.

DELETE 성공 직후 파일 크기가 줄지 않을 수 있다. VACUUM으로 재사용 가능한 공간, 실제 파일 크기·WAL·OS 여유를 구분한다. 정상 작업에서 VACUUM FULL을 자동 실행하지 않는다. V14 색인 생성은 writer를 정지한 명시 migration 구간에서 수행한다. 큰 기존 DB의 적용 시간은 별도 리허설이 필요하다.

PITR/백업은 ledger·bucket·progress·원천 identity·retention_window를 같은 시점으로 복원한다. 일부 guard만 최신/과거 값으로 섞지 않는다. Kafka가 필요한 prefix를 이미 지웠다면 #46의 복구 절차도 무손실 완료를 보장하지 못한다. retention.ms=24시간과 retention.bytes=256MiB/partition은 최소24시간 복구 창의 약속이 아니다. [실제 segment/bounds/파일 삭제 측정](RETENTION_VERIFICATION.md)에서 24시간 설정과 256MiB/partition의 크기 만료를 구분했다. 가속 시험의 관측 시간을 운영 복구 창으로 사용하지 않는다.
