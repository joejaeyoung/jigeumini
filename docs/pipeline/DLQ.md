# 오류 격리와 소비 복구 진단

F09 · [이슈 #10](https://github.com/joejaeyoung/jigeumini/issues/10) → T01/T03/T05/T08. 진행 상태와 검증 판정은 [STATUS](../STATUS.md)에 둔다. 아래 계약은 ranking/trend live 소비자에 적용한다. [replay](REPLAY.md)는 영구 오류를 실패로 처리하는 기존 동작을 유지한다.

## 처리 경계

1. poll의 모든 입력을 검증한다. 형식·schema·키·시각·live 보존 범위 위반과 eventId/payload 충돌만 영구 오류다. 같은 배치의 동일 ID에 서로 다른 payload가 있으면 모든 후보를 격리한다. 이미 DB에 확정한 ID와 충돌하면 기존 집계를 보존하고 새 위치의 입력을 격리한다.
2. 원본 topic ID를 확인하고 DB namespace의 `projection_source`와 대조한다. 토픽을 재생성했으면 같은 live namespace로 진행하지 않는다.
3. 하나의 DB 트랜잭션에서 정상 입력의 ledger를 삽입한다. 영구 오류는 별도 DLQ에 발행하고 broker ACK를 기다린 뒤 `projection_failure`에 저장한다. 이미 해당 namespace에 격리 기록이 있으면 재분류해 반영하거나 재발행하지 않는다.
4. 새 정상 이벤트만 bucket에 합산하고, 모든 정상/격리 입력의 다음 offset을 `projection_progress`에 기록한다. ledger·bucket·격리 기록·진행 위치는 함께 commit하거나 함께 rollback한다. 오류만 있는 배치도 진행 위치를 기록한다.
5. DB commit 후 listener가 반환해야 Spring의 BATCH 동기 offset commit이 진행된다. 직접 ACK나 Kafka/DB 분산 트랜잭션이 아니다.

DLQ ACK 대기는 배치의 DB 처리 진입부터 10초의 공통 기한을 쓴다. producer의 동기 `send` 진입은 `max.block.ms=2000` 제한으로 기한을 최대 2초 더 넘길 수 있지만 이 경우에도 성공으로 반환하지 않는다. DB transaction 20초·statement 5초·lock 3초·연결/소켓 제한을 유지한다. 기한 안에 처리하지 못한 배치는 1초 간격으로 재시도한다. 처리량과 큰 배치의 시간 한도는 #11에서 실측한다.

DB 오류, DLQ 전송 실패, topic identity 확인 실패와 기타 미분류 실행 오류는 전체 배치 재시도 대상이다. DB 장애를 영구 입력 오류로 바꾸지 않는다. listener 밖으로 전달하는 예외에는 고정 코드만 넣는다. 한 partition의 장애는 같은 consumer에 할당된 다른 partition도 늦출 수 있다.

| 장애 위치 | DB·원본 offset | 다음 실행 |
|---|---|---|
| DLQ ACK 전 실패 | 해당 배치 DB rollback, 원본 offset 미진행 | 배치 재전달 |
| ACK 후 DB commit 전 실패/종료 | DLQ만 남을 수 있음, DB rollback, offset 미진행 | 같은 failureId로 DLQ 중복 가능 |
| DB commit 후 offset commit 전 종료 | 정상 집계·격리·DB 진행은 저장, Kafka offset 미진행 | ledger와 격리 기록을 확인해 이중 반영·불필요한 DLQ 발행 방지 |
| DB 일시 장애 | 실패한 배치의 DB·offset 미진행 | DB 복구 후 처리; 정상 이벤트를 DLQ로 보내지 않음 |

## 안전한 DLQ 계약

토픽은 `<원본 topic>.dlq`다. 3 partitions, RF=1, CreateTime, 24시간·partition당 256MiB·segment 32MiB로 원본과 같은 개발 보존 설정을 사용한다. DLQ는 운영 작업 목록의 정본이 아니며, commit된 격리 이력은 PostgreSQL에 남는다. 자동 DB 정리는 아직 없다. topic 설정을 별도로 바꾼 운영 환경의 보존·복제 조건은 이 개발 검증으로 입증하지 않는다.

- key=`SHA-256(topicId + ':' + partition + ':' + offset)`. topicId는 broker가 제공하는 Kafka UUID다. 같은 이름의 재생성 topic과 구분한다. ranking/trend가 같은 원본 오류를 발견하면 같은 failureId를 사용하며 DB는 `(projection,run_id,failure_id)`별 1건이다.
- JSON은 `schemaVersion`, `failureId`, `topicId`, `sourceTopic`, `partition`, `offset`, `errorCode`, `channelId`, `affectedFrom`, `affectedTo`만 포함한다. 원본 bytes/key/headers·예외 본문·eventId·사용자 정보·원문 hash를 복사하지 않는다.
- 허용 코드는 `EVENT_SIZE`, `INVALID_EVENT_FIELDS`, `INVALID_EVENT_JSON`, `INVALID_EVENT_CONTRACT`, `KEY_MISMATCH`, `INVALID_EVENT_TIME`, `OUTSIDE_LIVE_RETENTION`, `EVENT_ID_CONFLICT`다.
- 완전히 검증된 충돌 이벤트만 channelId와 해당 10초 eventTime bucket을 사용한다. 나머지 오류의 channelId는 null이며, 감지 시각 기준 최근 7일부터 5초 뒤까지 전체 수집 채널의 품질을 불완전하게 만든다. 정상 live 입력이 기여할 수 있는 시간 범위를 보수적으로 덮는 정책이다. 잘못된 입력의 시각이나 key로 범위를 추측하지 않는다.
- ACK 뒤 crash/DB 실패가 있으면 Kafka DLQ의 물리적 건수가 중복될 수 있다. 운영 건수는 DB의 failureId별 격리 건수다. producer idempotence가 재시작·애플리케이션 재발행을 중복 제거한다고 가정하지 않는다.

## 품질 반영

`projection_failure`의 영향을 수집 손실과 함께 `KNOWN_LOSS`로 판정한다. 범위가 겹치면 정상 rank/count 대신 `INCOMPLETE`와 참고 건수를 제공한다. 최근 7일의 이미 공개된 정상 snapshot과 급상승에는 `COLLECTION_REVISED`를 붙인다. 기존 건수·순위를 새 값으로 덮어쓰지 않는다. 상세 분별 기록과 새 채널도 같은 범위 검사를 한다.

ranking 또는 trend 어느 쪽에서 발견해도 같은 run/topic의 source 품질에 반영한다. 채널·시간을 알 수 없는 격리는 전체 수집 범위에 영향을 주며 1분/5분 순위와 6분 급상승 창이 그 영향 범위를 벗어난 뒤 정상 관측 조건을 다시 만족해야 한다. 이것은 오류 데이터의 복구 완료를 뜻하지 않는다.

## 로컬 운영 조회

분석기를 기존 명령으로 실행하고 `http://127.0.0.1:18080/`의 **소비 복구 진단**을 확인한다. HTTP JSON은 `GET /api/operations/recovery`다. 루트 [사용법](../../README.md)을 따른다.

- 두 group 각각의 partition별 committed/beginning/end offset·lag·DB next resolved offset·마지막 DB 진행 시각.
- 첫 미처리 offset의 Kafka CreateTime. eventTime 전체 backlog의 최소값이 아니며, 조회하지 못한 값은 null(`—`)이다. 같은 group의 offset을 변경하지 않는 별도 assign 소비자로 시각만 확인한다. payload는 저장하지 않는다.
- 현재 소비 상태·고정 마지막 오류 코드·프로세스 시작 이후 실패 시도 수, DB에 영속된 partition/오류 코드별 격리 건수.
- 최근 격리 50건과 `AVAILABLE`(현재 보존 범위 안), `EXPIRED`(삭제됨), `TOPIC_CHANGED`(원본 identity 변경), `UNKNOWN`(확인 불가).

진단은 5초 간격으로 표본을 수집한다. Kafka/DB 조회 실패 시 `UNAVAILABLE`, 20초 넘은 표본은 `STALE`, 최초 표본 전은 `UNKNOWN`이다. 이전 표본을 남기되 0 lag나 복구 가능으로 새로 확정하지 않는다. 각 RPC·group 조회는 한 순간의 원자 snapshot이 아니며 화면은 운영 참고용이다. 소비자의 poll 생존과 source completeness는 lag=0과 별도다.

offset이 초기화되지 않았으면 `OFFSET_UNSET`, 보존 범위 밖이면 `OFFSET_EXPIRED`, end보다 앞서면 `OFFSET_AHEAD`다. 이런 상태의 lag는 null이다. topic identity가 바뀐 namespace는 `SOURCE_IDENTITY_CHANGED`다. 자동 offset 이동은 없다.

DLQ를 live 토픽으로 재투입하는 API나 동작은 없다. 코드를 수정한 뒤 보존된 원본으로 별도 replay run을 만들고 결과를 비교한다. `AVAILABLE`은 원본 위치의 보존 여부만 뜻하며 현재 codec으로 replay가 성공한다는 뜻이 아니다. 원본이 만료되면 이 DLQ envelope로 채팅 이벤트를 복원할 수 없다.

## 범위와 근거

실제 실행 결과는 [검증 기록](DLQ_VERIFICATION.md)에서 확인한다. 단일 로컬 broker/DB와 합성 입력의 장애 경계 검증이며 실방송·고부하·복제 HA·자동 운영 복구는 포함하지 않는다. 초기 토픽 설정 조회의 알려진 간헐 실패는 원인 확인 없이 수정됐다고 주장하지 않는다.

기술 근거: [Spring Kafka 전체 배치 재시도](https://docs.spring.io/spring-kafka/reference/4.0/kafka/annotation-error-handling.html), [Kafka Admin의 topic identity·offset 조회](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/Admin.html). 구현은 저장소 BOM 버전을 사용한다.
