# 관심 방송 알림과 outbox 계약

F07 · 이슈 #8 · T02/T03/T08/T10.

## 처리와 장애 경계

`RankingStore`가 확정한 spike와 `spike_outbox` 행을 같은 PostgreSQL 트랜잭션에서 저장한다. outbox 실패면 snapshot·급상승 상태·spike도 rollback한다. 기존 spike는 migration에서 소급 발행하지 않는다. replay는 이 경로를 호출하지 않는다.

publisher는 `(created_at, spike_id)` 오름차순으로 미전달 행 하나를 잠그고 Kafka ACK를 기다린 뒤 `published_at`을 기록한다. ACK 전 실패는 전달 미완료로 남고 1초 간격으로 재시도한다. 트랜잭션 한도 20초, SQL 5초, ACK 대기 10초다. ACK 직후 종료나 DB commit 실패는 같은 spike를 다시 보낼 수 있다. 발행은 at-least-once이고 전체 Exactly-once를 보장하지 않는다. `SKIP LOCKED`를 쓰므로 여러 publisher 사이의 전역 발행 순서는 보장하지 않는다. 피드는 `evaluatedAt, spikeId`로 정렬한다.

단일 publisher에서 선두 행의 영구 계약 오류도 계속 재시도하며 이후 행을 막는다. 임의로 버리거나 완료로 표시하지 않는다. 운영자는 `outbox.state=RETRYING`, pending 증가·마지막 ACK 정체로 확인하고 원본 판정/코드를 수정한 뒤 다시 실행한다. 행별 오류 코드·연속 실패 횟수와 cleanup 전용 상태는 아직 없다. ACK 대기 중 DB 연결 하나와 행 잠금을 유지하므로 공유 풀/DB의 자원 비용이 있다.

기본 토픽은 `<pipeline.topic>.spikes`(기본 `chat.received.v1.spikes`), 이벤트 종류는 `spike.detected.v1`이다. `notifications.topic`으로 전용 토픽명을 지정할 수 있다. 채팅 토픽과 같을 수 없다. key=channelId, CreateTime=evaluatedAt, 3 partition, RF=1, ACK=all, idempotent producer이며 보존은 24시간/partition당 256MiB/segment 32MiB다. ACK는 복제 HA나 feed 저장 완료를 뜻하지 않는다.

알림 소비 그룹은 `<pipeline.group>-notifications`다. `notification_event`가 `(run_id, spike_id)` 멱등 ledger이자 피드 projection이며 불변 payload가 다르면 오류로 중단한다. feed·진행 위치는 같은 트랜잭션에서 저장하고 Spring BATCH sync offset commit은 그 뒤다. 다른 run의 유효한 이벤트는 이 run 피드에 넣지 않는다. 잘못된 알림 계약·DB 실패는 고정 오류 코드로 무제한 배치 재시도하며 원본을 출력하지 않는다. 채팅 DLQ는 별도 계약이다.

## 안전한 이벤트와 정정

schemaVersion=1, spikeId, runId, channelId, evaluatedAt, algorithmVersion, count, baseline, ratio, quality=`HEALTHY_AT_DETECTION`만 전달한다. 원문·닉네임·사용자 ID·토큰·예외 body는 없다. 품질 필드는 판정 당시 사실로 고정해 재전달 payload가 바뀌지 않는다.

현재 지원하는 알고리즘은 `chat-spike-v1` 하나다. 소비 검증은 run 필터보다 먼저 수행하며 다른 버전은 배치를 중단한다. 알고리즘을 올릴 때는 기존 미전달 outbox·보존 토픽·feed ledger와 웹 decoder를 함께 이행해야 한다. 버전 상수를 바꾸는 것만으로 구버전 이력을 읽을 수 있다고 가정하지 않는다. 저장된 payload 계약 오류의 조회 응답은 원문 없는 503 `QUERY_UNAVAILABLE`이다.

사후 품질은 feed 조회에서 같은 run의 원본 `spike_event.quality_warning`과 결합한다. COLLECTION_REVISED면 valid=false이며 과거 알림도 즉시 보류 표시한다. 원본 spike가 없으면 SOURCE_UNAVAILABLE이다. 정정 시 새 알림을 발행하지 않는다. 공유 DB를 사용하는 조회이며 서비스 간 완전한 자원 격리를 주장하지 않는다.

## 조회와 브라우저 상태

`GET /api/v1/notifications?channels=<쉼표로 구분한 ID>&cursor=<시각|UUID>&limit=20`.

- 채널 0~20개, limit 1~100. 빈 채널 목록은 빈 피드다. 최근 7일의 evaluatedAt만 반환한다.
- cursor는 `(evaluatedAt, spikeId)`의 엄격한 이전 위치다. 중간에 새 항목이 들어와도 이전 페이지에 중복되지 않는다.
- receivedAt은 feed의 첫 DB 반영 시각이며 재전달로 바뀌지 않는다. evaluatedAt보다 600초를 초과해 늦으면 delayed=true이다.
- 웹은 지연 경계 비교에서 서버 시각의 소수부를 유지한다. 600초+1마이크로초를 밀리초로 절삭해 정상 응답을 거부하지 않는다.
- 웹 `/notifications`는 기존 로컬 관심 목록을 필터로 보내고 읽음 spikeId를 브라우저에 저장한다. 7일·최근 2,000개 읽음 상태만 유지하며 저장 실패를 알린다. 계정이나 외부 전송은 없다.
- 새 반응 강조는 미읽음·유효·지연 아님·발생 후 10분 이내·조회 갱신 성공일 때만 한다. 이전 페이지의 오래된 기록과 정정은 강조하지 않는다.
- 빈 피드는 도착한 기록 없음이며 수집 전체의 완전성이나 알림 누락 없음을 뜻하지 않는다.
- 운영 진단 `GET /api/operations/notifications`는 pending outbox·마지막 ACK·소비 상태·실패 횟수·할당 수를 표시한다. 비어 있는 group의 초기 시작은 `notifications.offset-reset=earliest`를 명시하고 이후 `none`을 유지한다. 만료된 offset은 자동 이동하지 않는다.

## 정리와 한계

전달 완료 후 7일이 지났고 feed에 반영된 outbox만 매시간 삭제한다. pending·미소비 행과 feed ledger·원본 spike는 삭제하지 않는다. 피드 조회는 7일로 제한하지만 멱등 ledger 자동 정리는 별도 용량 정책이 필요하다. Kafka 보존 만료 뒤에 ACK된 미소비 이벤트를 outbox가 자동 재발행하지 않으며 운영자가 손실을 확인해야 한다. 실험과 정제 실행 근거는 STATUS에 연결한다.

CreateTime은 발행 시각이 아니라 evaluatedAt이다. 24시간을 넘긴 outbox 적체를 뒤늦게 발행하면 보존 기한이 이미 지난 시각으로 들어가며 segment 정리 때 일찍 삭제될 수 있다. ACK만으로 소비 가능한 24시간이 새로 확보되지는 않는다.

## 근거

- [PostgreSQL SELECT 잠금](https://www.postgresql.org/docs/16/sql-select.html): SKIP LOCKED의 queue 처리 용도와 순서 제한.
- [Spring Kafka offset 처리](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html): BATCH listener 완료 뒤 commit.
