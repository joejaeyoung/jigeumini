# 처리 이력·집계 보존과 저장 용량 조사

조회일: **2026-10-27 KST**. 대상: [#63 처리 이력·집계 보존과 저장 용량 상한](https://github.com/joejaeyoung/jigeumini/issues/63).
기술 연결: **F01/F08/F10/F16 → T02/T04/T08/T16**. 코드 확인 기준: `8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea`.
PostgreSQL **16** 공식 문서와 Kafka **4.1 문서·4.1.2 소스**를 조회했다. PostgreSQL 실제 프로젝트 이미지는 16.14다.
이 문서는 #65 실행 중 작성한 구현 전 조사다. 아래 후보·미실행 표기는 조사 당시의 범위다. 이후 구현 선택은 [보존 계약](../pipeline/RETENTION.md), 검증 진행은 [STATUS](../STATUS.md)에서 관리한다.

### 조사 버전과 프로젝트 대조

루트 `compose.yaml:4`와 `deploy/compose.yaml:50`은 모두 `apache/kafka:4.1.2`, PostgreSQL은 `16.14-alpine`을 지정한다. `scripts/product-load.py:54`는 배포 Compose에 시험 overlay를 더한다. 기존 `server/build/libs/chat-pulse-poc.jar`의 ZIP 항목만 읽어 `kafka-clients-4.1.2.jar`와 `spring-kafka-4.0.8.jar`를 확인했다. `server/build.gradle`은 Spring Boot BOM 4.0.9를 사용한다.
따라서 여기서 읽은 Kafka 4.1.2 소스는 현재 선언된 broker 이미지와 로컬 빌드 산출물의 Kafka client 버전에 맞는다. **현재 실행 중인 #65 컨테이너·이미지 digest·실행 JAR hash를 이 조사에서 재검증하지는 않았다.** 실제 retention 시험은 해당 실행의 이미지 ID·broker/client 버전·JAR hash·effective config를 함께 남겨야 한다. 다른 버전·tiered storage 구성으로 이 동작을 일반화하지 않는다.

## 1. 현재 코드에서 확인한 사실

| 대상 | 확인한 현재 동작 | 보존 설계와 연결할 점 |
|---|---|---|
| `processed_event` | PK는 `(projection, run_id, event_id)`. 원천 topic/partition/offset과 payload hash, event/received/processed 시각 보유 | 멱등성 기억을 삭제하는 작업이므로 일반 로그 TTL과 구별 |
| `ProjectionStore` | ledger 삽입·새 이벤트 bucket 증가·DB progress 저장이 한 트랜잭션 | 정리와 진행 경계를 함께 검증해야 함 |
| live `applyRecovering` | source identity 확인 후 progress 행을 `FOR UPDATE`로 잠그고 `offset < next_resolved_offset` 제외 | 같은 offset 재전달을 ledger 없이 차단할 기반이 있음 |
| `apply`, `applyReplay` | 위 `unresolved` 필터를 사용하지 않음 | 모든 호출 경로가 동일 보호를 받는다고 가정하면 안 됨 |
| `EventCodec.validateLiveAge` | receivedAt은 현재 기준 48시간 전보다 오래되면 거부, eventTime은 7일 전보다 오래되면 거부 | 삭제 기준을 고를 때 두 시각을 섞지 않음 |
| `ReplayStore` | run 행 잠금·owner/state 확인, 별도 `replay/<UUID>` namespace, apply와 계수·progress를 같은 트랜잭션에 저장 | QUEUED/RUNNING 보호 및 종료 run 결과 보존을 별도로 설계 |
| 관측·카테고리 | 7일 기준, 최대 10,000행 단위 정리가 일부 존재 | 이력·집계·기준 관측의 경계와 실제 정리 처리율 확인 |
| 리포트 | 종료 확인된 오래된 세션의 자식을 우선 정리. 미확정/활성 세션 보호 | 오래 지속되는 active 상태 때문에 크기 상한은 자동 보장되지 않음 |
| Kafka 정책 | delete/CreateTime, 24시간, partition당 256MiB, segment 32MiB, partition 3개 | 24시간 전체 로그 보존이나 파일 크기 hard cap이 아님 |

코드 근거: [V1 스키마](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/resources/db/migration/V1__chat_projection.sql), [ProjectionStore](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/pipeline/ProjectionStore.java), [EventCodec](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/pipeline/EventCodec.java).
보호·정리 근거: [ReplayStore](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/replay/ReplayStore.java), [CategoryStore](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/categories/CategoryStore.java), [ReportStore](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/reports/ReportStore.java), [KafkaTopicPolicy](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/pipeline/KafkaTopicPolicy.java).

## 2. PostgreSQL: 행수 제한, 실행시간 제한, 파일 회수는 별개

**공식 사실:** PostgreSQL 16 `DELETE` 문법에는 직접적인 `LIMIT` 절이 없다. `WITH`/부질의로 삭제할 기본키를 한정한 뒤 `DELETE ... USING`으로 지우는 형태는 가능하다. [PostgreSQL 16 DELETE](https://www.postgresql.org/docs/16/sql-delete.html)
**공식 사실:** `SELECT ... FOR UPDATE SKIP LOCKED`는 즉시 잠글 수 없는 행을 건너뛴다. 결과는 전체 일관된 목록이 아니며, 필요한 table lock까지 생략하는 기능도 아니다. 잠긴 행만 남아 0건이 반환됐다고 정리 완료를 판정할 수 없다. [PostgreSQL 16 SELECT 잠금](https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE)
**설계 후보:** `(run/projection, cutoff, 진행 경계)` 조건에 맞는 PK를 안정적인 순서로 최대 N개 고르고 같은 짧은 트랜잭션에서 삭제한다. N, 실행당 batch 수, 총 실행 시간, `lock_timeout`, `statement_timeout`을 각각 제한한다. 인덱스 없이 N개를 찾으려고 큰 테이블을 훑으면 N 제한만으로 비용이 제한되지 않는다.
**설계 후보:** 정리 worker 간 경쟁은 단일 실행 임대/try-lock으로 줄이고, 같은 테이블의 수정과 정리가 여러 잠금을 잡을 때 순서를 통일한다. 트랜잭션이 실패하면 삭제·진행 marker도 함께 rollback한다. 오류 본문 대신 고정 코드·행수·소요 시간만 진단한다.
잠금은 보통 트랜잭션 끝까지 유지되고 여러 행의 잠금 순서가 다르면 deadlock이 생길 수 있다. 위 후보는 [PostgreSQL 잠금·deadlock 계약](https://www.postgresql.org/docs/16/explicit-locking.html)을 적용한 설계 추론이다.

**공식 사실:** DELETE는 MVCC의 과거 행 버전을 곧바로 파일에서 제거하지 않는다. 표준 VACUUM은 더 이상 필요한 snapshot이 없는 dead tuple 공간을 재사용 가능하게 만든다. 보통 OS에 파일 공간을 반환하지 않으며, 완전히 빈 끝부분을 잘라낼 수 있을 때는 예외가 있다. [PostgreSQL routine vacuuming](https://www.postgresql.org/docs/16/routine-vacuuming.html#VACUUM-FOR-SPACE-RECOVERY)
**공식 사실:** `VACUUM FULL`은 새 파일로 다시 쓰고 `ACCESS EXCLUSIVE` 잠금과 추가 디스크가 필요하다. 표준 VACUUM의 tail truncate도 `ACCESS EXCLUSIVE`를 사용할 수 있어 `TRUNCATE false`로 이를 피하는 선택지가 있다. VACUUM은 DB 트랜잭션 블록 안에서 실행할 수 없다. [PostgreSQL 16 VACUUM](https://www.postgresql.org/docs/16/sql-vacuum.html)
**설계 추론:** 정상 운영 정리의 성공 기준은 "행수 감소"와 "재사용 공간 확보/장기 크기 안정화"를 포함해야 한다. DELETE 직후 `pg_database_size`가 줄지 않는 것은 곧바로 실패가 아니다. 반대로 행수만 줄고 dead tuple·WAL·파일 사용량이 계속 증가하면 용량 상한을 달성했다고 판정할 수 없다.

## 3. 시간 partition의 이익과 멱등성 제약

**공식 사실:** 오래된 partition을 drop/detach하면 대량 행별 DELETE와 그에 따른 VACUUM 비용을 피할 수 있다. 일반 drop/detach는 부모의 강한 잠금을 요구한다. [PostgreSQL partition maintenance](https://www.postgresql.org/docs/16/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-MAINTENANCE)
**공식 사실:** `DETACH PARTITION CONCURRENTLY`는 부모에 낮은 잠금 수준을 쓰지만 내부 두 단계, 진행 중 트랜잭션 대기, 자식의 `ACCESS EXCLUSIVE` 잠금이 있다. 명시 트랜잭션 블록 안에서 실행할 수 없고 default partition이 있으면 사용할 수 없다. 중단 후 `FINALIZE`가 필요할 수 있다. detach 자체는 독립 테이블을 남긴다. [PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/16/sql-altertable.html)
**공식 사실:** partitioned table의 UNIQUE/PK에는 모든 partition key 열이 들어가야 한다. 각 자식 인덱스만으로 다른 partition 사이의 중복을 막을 수 없기 때문이다. [PostgreSQL partition 제약](https://www.postgresql.org/docs/16/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-LIMITATIONS)
**설계 추론:** `processed_at`/`event_time`으로 ledger를 나누고 현재 PK에 시각을 추가하면 같은 eventId가 다른 날짜에 다시 들어오는 것을 허용할 수 있다. 현재 `(projection, run_id, event_id)` 멱등성을 그대로 유지했다고 설명하면 안 된다.
**후보 비교:** 기존 ledger에는 먼저 인덱스와 bounded DELETE를 적용하는 방안, 시간 partition과 별도 전역 dedup 저장소를 결합하는 방안, 종료 replay를 run 단위로 제거하는 방안을 비교한다. 별도 dedup 저장소도 무기한이면 전체 저장량 문제가 남는다.
**입증 필요:** migration 중 읽기/쓰기 잠금, 과거 unique 충돌, 늦은 입력의 partition 부재, 자동 partition 생성 실패, detach 중단·재개, 실제 파일 회수까지 검증해야 한다. 이 조사에서는 partition 방식을 채택하지 않는다.

## 4. ledger 삭제의 안전조건 — 설계 후보

### 구별해야 할 세 종류의 재입력

| 재입력 | 지울 때 사라지는 보호 | 필요한 조건 |
|---|---|---|
| 같은 topic identity·partition·offset 재전달 | eventId ledger 조회 | 영구 DB progress와 source identity를 보존하고 확정 offset을 먼저 거부 |
| 같은 eventId/payload가 새 offset에 다시 발행 | 새 offset은 progress guard를 통과 | ledger의 보존 창 또는 영구 admission cutoff로 별도 처리 |
| 같은 eventId에 변경된 payload·최근 시각 | 삭제된 hash를 비교할 수 없음 | 유한 보존 뒤 충돌 탐지 범위를 명시하거나 별도 영구 기억 필요 |

이 표는 현재 코드의 조건문과 DB 상태를 바탕으로 한 추론이다. Kafka offset이 이미 지나갔다는 사실만으로 새 offset의 UUID 중복을 방지할 수 없다.

### live 후보: offset guard와 입력 수락 하한을 함께 보존

1. namespace별 source identity와 partition progress를 TTL 삭제 대상에서 제외한다. 토픽 재생성은 이전 offset 공간으로 취급하지 않는다.
2. 모든 live 소비 경로가 같은 offset guard를 사용하도록 명확히 제한한다. 현재 `applyRecovering`만으로 전체 호출 경로가 보호된다는 주장은 하지 않는다.
3. ledger 만료 기준과 입력 수락 기준을 함께 정의한다. **eventTime이 오래됐다는 이유만으로 아직 수락 가능한 새 receivedAt의 ledger를 지우지 않는다.**
4. `processed_at` 72시간 등 기간 후보는 현재 48시간 live 수락 창보다 여유를 주지만, 숫자만으로 안전성이 증명되지는 않는다. DB/app 시계 차이·시계 후퇴·지연 batch를 고려해야 한다.
5. 더 명시적인 후보는 namespace별 단조 증가하는 `admission_received_at_floor`를 DB에 보존하고 그보다 오래된 새 입력을 집계하지 않는 것이다. cutoff 발행과 정리 대상 확정의 경쟁을 트랜잭션/공유 잠금 계약으로 제어한다.
6. cutoff를 넘어선 동일 payload 재발행은 재집계 대신 만료 처리하고, 이미 확정된 같은 offset은 기존 결정 그대로 건너뛴다. 이 둘의 품질·DLQ 표현은 구분한다.
7. ledger가 사라진 뒤 최근 시각으로 바뀐 같은 UUID의 충돌까지 검출하려면 더 오래 유지하는 hash/ID 기억이 필요하다. 이를 구현하지 않으면 멱등성·충돌 탐지 보장은 보존된 범위로 명시한다.

**입증 필요:** 정리·소비가 서로 다른 기준 시각을 보는 경쟁, cutoff 갱신 후 worker 종료, 삭제 후 DB 복원, group offset rewind, 여러 partition의 같은 UUID, 코드의 plain apply 경로 우회를 실제 DB로 검증한다.
현재 live age와 source binding은 [EventCodec](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/pipeline/EventCodec.java), [DeadLetterRecovery](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/pipeline/DeadLetterRecovery.java)에 구현되어 있지만 위 단조 cutoff는 아직 구현되지 않았다.

### replay·집계·진단의 후보

- QUEUED/RUNNING replay의 ledger·bucket·progress·manifest는 정리하지 않는다. 만료 정책은 heartbeat만으로 실행 작업을 지우지 않고 기존 owner/state/worker lock 계약을 따라야 한다.
- 완료·실패·취소 run을 만료시킬 때 run 행 잠금 아래 별도 만료 상태를 확정하고, 자식 자료를 여러 짧은 batch로 제거하는 후보가 있다. compare/status는 만료 중 결과를 완전한 결과로 표시하지 않아야 한다.
- live bucket 보존은 ledger 보존과 독립일 수 있다. 그러면 기존 `processedEvents == bucketTotal` 대조는 전체 누적 정합성 검사가 아니므로 retained 범위·누적 counters·만료 계수를 명확히 바꿔야 한다.
- 관측 TTL 경계의 이전 표본, 카테고리 보정 구간, 종료 전 리포트, 미발행 outbox, 품질 실패 기록을 단순 테이블별 TTL로 서로 끊지 않는다.
- DB 복원은 ledger·bucket·progress·source identity·admission floor의 같은 시점 계약을 복원해야 한다. Kafka가 이미 필요한 입력을 지웠으면 복원 완료로 표시하지 않는다.

현재 복구는 DB progress와 Kafka begin/end·committed offset을 대조하며 `RETENTION_GAP` 등을 구분한다. 새 purge floor를 포함한 검증은 추가 설계다. [RecoveryPlan](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/operations/RecoveryPlan.java)

## 5. Kafka 보존 창과 active segment의 실제 제약

**공식 사실:** `retention.bytes`는 partition별 한도이며 `retention.ms`와 함께 적용된다. delete 정책은 segment 파일 단위로 처리한다. 따라서 24시간 설정이 있어도 바쁜 partition의 크기 한도가 먼저 적용될 수 있다. [Kafka topic retention·segment 설정](https://kafka.apache.org/41/configuration/topic-configs/)
**4.1.2 소스 사실:** 시간 정리는 `now - segment.largestTimestamp > retentionMs`를 검사한다. 크기 정리는 전체 초과분에서 segment 크기를 빼는 방식이며, `deleteOldSegments()`가 크기·시간 정리를 각각 호출한다. [UnifiedLog retention 구현](https://github.com/apache/kafka/blob/4.1.2/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java#L1756-L1825)
**4.1.2 소스 사실:** 마지막 빈 segment는 지우지 않고 high watermark보다 앞선 범위인지 확인한다. 모든 segment가 삭제 대상이면 먼저 새 segment를 roll한다. **active segment는 언제나 retention 삭제에서 제외된다고 단정할 수 없다.** [삭제 후보·roll 구현](https://github.com/apache/kafka/blob/4.1.2/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java#L1629-L1753)
**공식 사실:** 삭제 자격 확인 주기는 broker `log.retention.check.interval.ms`, 파일 실제 삭제 지연은 topic `file.delete.delay.ms`다. 문서 기본값은 각각 5분·1분이지만 실제 배포의 effective config를 기록해야 한다. [broker 확인 주기](https://kafka.apache.org/41/configuration/broker-configs/#log.retention.check.interval.ms), [파일 삭제 지연](https://kafka.apache.org/41/configuration/topic-configs/#file.delete.delay.ms)
**설계 추론:** 논리적 earliest offset 진행과 파일시스템 여유 회복 시각을 따로 측정한다. 시간·용량 계산식은 계획용이며 segment 경계·압축·record overhead·복제·디스크의 다른 파일까지 반영한 hard limit이 아니다.

### 실측 보존 창에 남길 값

1. 코드/JAR/image 식별, Kafka version, topic identity, partition별 begin/end, 실제 retention/segment/check/delete-delay 설정과 RF.
2. 각 partition의 합성 입력 offset·Kafka timestamp·발행 확인 시각, 실제 log/index 크기와 삭제 시각. 공개 payload를 덤프하지 않는다.
3. 매 관측 시점의 earliest/끝 경계, 고정 manifest에서 사라진 prefix, 복구할 DB progress가 남은 범위 안인지 여부.
4. 같은 총 입력률에서 균등·hot 편중·idle 채널을 비교하고, 마지막 입력 후에도 시간 만료와 active roll을 관측한다.
5. 시간 제한 먼저/크기 제한 먼저/둘 다 활성/소비 중 만료/짧은 broker 중단을 분리한다. Kafka가 소비자 commit까지 기다려 로그를 보존한다고 가정하지 않는다.

`beginningOffsets`는 현재 읽을 수 있는 첫 offset이고 `offsetsForTimes`는 주어진 Kafka timestamp 이상인 첫 offset을 찾는다. 모든 eventTime의 연속 관측을 증명하는 API가 아니다. [KafkaConsumer API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
현재 `ReplaySource.retention`도 offset 폭이 record 수가 아님, 경계 표본이 eventTime 완전성을 뜻하지 않음, 최대 200 offset·1초 표본 제한을 명시한다. 이 기능은 보존 창 연구의 출발점이며 전체 원천 무누락 증거가 아니다. [ReplaySource](https://github.com/joejaeyoung/jigeumini/blob/8a0fb7609ebd4062c77832b4cc53209ea1f1d2ea/server/src/main/java/io/chatpulse/replay/ReplaySource.java)

## 6. #63에서 입증할 측정과 실패 조건

| 축 | 반드시 비교할 값·조건 | 성공을 과장하지 않을 경계 |
|---|---|---|
| 삭제 처리율 | 선택/삭제 행수, batch/전체 시간, timeout, retry, 남은 만료 자료 | 0행은 잠금 skip·보호 중일 수 있음 |
| PostgreSQL 공간 | table/index/TOAST 크기, dead tuple 추정, VACUUM 시각, OS 여유 | 삭제 행수와 즉시 파일 회수는 다름 |
| WAL | 정리 전후 생성 bytes, checkpoint·재시작·stats reset | 생성량과 현재 WAL 디렉터리 점유량은 다름 |
| 부하 영향 | API p95/error, 각 projection lag, lock/pool 대기, 정리 backlog | 정리만 빠르고 live가 막히면 통과 아님 |
| 재전달 | cutoff 직전/같음/직후, 같은·새 offset, 다른 payload | 유한 ledger 이후 보장 범위를 명시 |
| 장애 | batch 중 SQL rollback, commit 직후 종료, cleanup 재시작, PITR | 부분 정리 상태를 완료로 표시하지 않음 |
| 보호 | active replay/report, stale heartbeat, 미발행 outbox, namespace 분리 | 보호된 자료가 영구 성장하면 경보·입력 제한 필요 |

공식 측정 정의: `pg_total_relation_size`는 index·TOAST를 포함하고 `pg_table_size`는 일반 index를 제외한다. partitioned table은 실제 자식 관계도 함께 합산하도록 조사한다. [PostgreSQL 크기 함수](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
`pg_stat_wal.wal_bytes`는 **클러스터 전체** WAL 생성 누계다. 다른 작업과 reset을 구분한 격리 시험에서 차이를 사용한다. `n_dead_tup` 같은 통계는 추정치로 해석한다. [PostgreSQL 누적 통계](https://www.postgresql.org/docs/16/monitoring-stats.html)
**설계 후보:** 유입과 정리가 함께 진행되는 여러 보존 주기를 합성 데이터로 확인한다. 정리 가능한 자료의 유입률보다 삭제 처리율이 낮으면 주기/N/인덱스 조정 또는 입수 상한이 필요하다. 기간 설정 하나로 byte 상한이 생기지는 않는다.
**설계 후보:** 경보는 최소 디스크 여유, 최근 증가율, 추정 고갈 시간, cleanup 실패·backlog, 최단 Kafka 복구 가능 창을 함께 사용한다. 숫자 임계값은 #65 및 보존 실험으로 확정한다.

## 7. 구현 전에 확정해야 할 계약

- live ledger·bucket·관측·품질·리포트·outbox·replay 결과의 보존 기준 시각과 기간, 시작/끝 경계의 포함 여부.
- 같은 offset 재전달, 새 offset UUID 중복, 오래된 UUID의 변경 payload를 각각 어느 범위까지 검출할지.
- source identity·progress·admission floor를 영구 보존할 단위와 운영 namespace 종료 절차.
- 보호 때문에 자동 정리가 불가능할 때의 상태·경보·입력 제한 및 운영자 조치.
- 기존 데이터 migration, 만료 결과 API 표현, PITR·고정 replay와의 상호작용.

이번 조사에서는 보존 기간·DB partition 도입·새 admission floor를 확정하지 않았다. 구현 선택과 실제 Kafka/PostgreSQL 검증 결과가 있어야 #63 완료를 판정할 수 있다.
