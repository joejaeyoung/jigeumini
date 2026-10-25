# 수집 품질 기반 순위·급상승 계약

[HLD 7장](../design/HLD.md)의 완료 구간·품질·급상승 규칙을 구현한다. 기존 `/api/buckets`는 운영 진단용 원시 조회로 유지하고 제품 순위는 이 문서의 snapshot API를 사용한다. 진행 기록은 [STATUS](../STATUS.md), 실행 명령은 [루트 README](../../README.md)에 둔다.

## 수집 품질 관측

`CollectionObserver`는 WebSocket callback에서 공개 채널 ID·세션 UUID·시각·진단 코드만 메모리에 기록한다. `CollectionTelemetry` worker가 5초마다 PostgreSQL에 반영한다. 원문·시청자 닉네임·사용자 ID·토큰은 전달하지 않는다. Kafka 이전의 관측도 필요하므로 품질 관측은 채팅 이벤트와 별도로 DB에 쓴다. DB 장애 시 heartbeat가 갱신되지 않아 UNKNOWN/INCOMPLETE로 내려간다.

- collector 시작 시 대상 채널의 관측 시작 시각과 enabled를 기록한다. 정상 종료 시 disabled, 원천 종료 확인 시 OFFLINE, 접근 제한·발행 실패 등은 UNAVAILABLE다. PoC의 최대 3채널·30분 제한과 단일 checkout 파일 잠금은 유지한다. 다중 호스트 fencing과 관리자 등록 UI는 구현 범위에 포함하지 않는다.
- 프로토콜 승인부터 세션 정상 구간을 시작하고 frame/pong을 원천 생존으로 기록한다. 열린 세션은 collector heartbeat가 15초 미만이고 원천 생존이 30초 미만일 때만 현재까지 잠정 정상으로 본다.
- 정상 종료는 종료 시각까지, 비정상 종료는 마지막 원천 생존까지만 확정한다. heartbeat timeout은 마지막 확인 이후를 공백으로 정정한다. 여러 세션의 겹치는 시간을 합집합으로 계산한다.
- 잘못된 메시지·frame 처리 실패·발행 실패·미확인 버퍼는 알려진 손실 구간으로 남긴다. 손실 큐가 커지면 같은 채널 구간을 넓게 합친다. callback에서 DB I/O를 기다리지 않는다.
- 발행 대기와 in-flight를 포함한 채널별 건수·가장 오래된 미확인 시각을 5초마다 기록한다. 관측이 15초 이상 오래되면 미확인, 미확인 이벤트가 10초를 넘으면 지연이다.

연결 시간은 수신 시각 기준이고 채팅 집계는 원천 eventTime 기준이다. **연결 관측률 100%는 원천 채팅의 완전한 수신율을 뜻하지 않는다.** 품질 정보가 없던 기존 기록을 정상 0건이나 검증된 순위로 승격하지 않는다.

## 독립 분석과 freshness

기존 ranking 소비자와 별도로 `{pipeline.group}-trend` 그룹, `{pipeline.projection}-trend` ledger/bucket을 사용한다. 한 소비자가 멈춰도 다른 소비자의 입력 처리는 독립적이다. 두 평가 로직은 현재 analyzer 프로세스 안의 한 scheduler에서 수행한다. 프로세스 자체 장애의 독립성·부하 기준은 별도 운영 단계다.

`ProjectionConsumer`는 빈 poll에도 실제 poll 시각과 소유 partition을 기록한다. 최초 할당 시 DB에 시작 위치를 `ON CONFLICT DO NOTHING`으로 기록해 입력이 없는 partition의 처리 위치도 관측할 수 있다. 실패 배치는 기존처럼 재시도하고 offset을 넘기지 않는다. DB 시작 위치의 존재는 이벤트 반영 성공을 뜻하지 않는다.

`ProjectionObserver`는 5초마다 소유 partition의 Kafka end offset을 조회한다. 최근 3개 표본 중 DB 진행 위치가 따라잡은 **표본의 관측 시각**을 lastCaughtUpAt으로 기록한다. 뒤늦게 따라잡았다고 현재 시각으로 바꾸지 않는다. 조용한 정상 partition도 통과할 수 있다.

- observer와 poll 관측은 15초 미만, lastCaughtUpAt은 10초 이내여야 한다.
- 발행 버퍼 지연·미관측, 소비 retry/계약 오류·중지, partition 관측 부재는 정상으로 판정하지 않는다.
- 채널의 partition은 현재 계약인 3 partitions와 Kafka String key의 murmur2 분배로 계산한다. KafkaSupport가 실제 토픽의 3 partitions를 검사한다.
- 새 trend 그룹의 최초 실행은 `--quality.trend-offset-reset=earliest`를 명시한다. 이후 기본은 none이다. ranking 그룹을 자동으로 reset하지 않는다. 새로운 projection/run을 쓰려면 별도 새 group과 입력 시작 위치를 함께 정해야 한다.

## 완료 구간과 순위

평가 scheduler는 각 10초 경계의 1초 뒤에 고정 주기로 실행한다. 창 끝 T 이후의 버퍼 표본이 있어야 하며, 창 이전 receivedAt의 미확인 발행이 있으면 LAGGING이다. Kafka 처리 완료 표본은 그 버퍼 표본 시각 이상이어야 한다. 표본 시각은 broker RPC 요청 전에 기록한다. 이 조건이 확인되지 않으면 참고 건수만 남기고 순위·급상승에서 제외한다. 관측 주기와 consumer 지연 때문에 정상 연결도 보수적으로 제외될 수 있다.

이는 관측한 입력의 처리 완료 조건이며 eventTime watermark나 원천 전체 입력의 완전성을 보장하지 않는다. 이후 도착하는 과거 eventTime 입력은 bucket·추이만 보정한다. 무한 지연 입력까지 불변 순위에 반영하려면 별도 watermark·정정 설계가 필요하다.


10초 tick의 UTC 시각 T에서 `[T−60,T)`, `[T−300,T)`만 집계한다. 현재 진행 중인 10초 bucket은 포함하지 않는다. scheduler가 놓친 tick은 소급 생성하지 않는다.

`REPEATABLE READ` 트랜잭션과 projection/run별 `pg_try_advisory_xact_lock`으로 header·entries·trend 상태를 저장한다. 동일 projection/run/window/T/algorithm의 unique 제약으로 중복 snapshot을 막는다. 채팅 건수가 없더라도 등록·정상 관측 조건을 통과한 채널은 0건으로 포함한다.

상태 우선순위는 DISABLED/OFFLINE → 알려진 손실 또는 전체 window의 커버리지 95% 미만 → WARMING_UP → LAGGING/UNKNOWN → HEALTHY다. 시작 후 window를 채우지 못했으면 WARMING_UP이며, 채널별 대표 status와 모든 reasons를 함께 준다. 제외 채널의 정식 `rank`/`count`는 null, 원시 참고값은 `referenceCount`다.

동률은 같은 순위, 다음 순위는 건너뛰는 competition rank를 사용하고 동률 표시 순서는 채널 ID다. 이전 순위는 정확히 T−60의 같은 window snapshot이다. 없으면 `NO_PREVIOUS_SNAPSHOT`, 불완전하거나 품질 정정된 경우 `PREVIOUS_INCOMPLETE`로 표시한다. 사후 disable/OFFLINE은 조회에서도 즉시 제외한다.

## 급상승 v1

trend 전용 bucket에서 C=`[T−60,T)` 합계, B=`[T−360,T−60)` 합계/5를 계산한다. BigDecimal로 기준선을 유지하고 정수로 버리지 않는다.

1. 6분 품질 조건을 통과한다.
2. C≥100, C−B≥50, C/max(B,10)≥2를 모두 만족한다.
3. 정확히 10초 간격으로 두 번 연속 충족한다. 품질 불량·불충족·tick 누락은 연속 횟수를 초기화한다.
4. 이벤트와 마지막 알림 시각을 같은 DB 트랜잭션에 기록한다. 동일/이전 T의 재평가는 무시한다.
5. 알림 후 300초 미만은 cooldown이다. 300초 경계부터 다시 첫 번째 충족을 시작하고 310초에 두 번째 조건을 채워야 다음 알림을 만든다.

알고리즘 버전은 `chat-spike-v1`이다. 리본은 유효한 이벤트만 사용하며 2배대 청록·3배 이상 코랄로 구분한다. 피드가 갱신되지 않거나 품질 경고가 붙으면 리본을 숨긴다. 배수는 인기·시청자 수·만족도의 배수가 아니다.

## 사후 정정과 추이

최근 7일의 snapshot/급상승 중 새 손실 또는 생존 만료가 영향을 줄 수 있는 후보를 검사한다. 영향 범위의 커버리지 하락·손실이 확인되면 snapshot_quality_revision 또는 spike의 qualityWarning을 남긴다. 저장된 기존 rank/count를 덮어쓰거나 과거 알림을 새로 발송하지 않는다. 조회에서 해당 순위를 제외하고 이전 순위 비교에서도 정정된 결과를 사용하지 않는다.

추이는 최대 최근 1시간의 완료된 1분 구간이다. 해당 시점 snapshot의 품질과 현재 확인된 세션/공백을 함께 검사한다. 정상 구간은 0건을 포함하며, 품질을 확인할 snapshot이 없거나 공백이 있으면 count=null과 참고 건수를 준다. 늦은 이벤트는 원시 bucket과 추이를 보정할 수 있지만 이미 확정한 snapshot은 재계산하지 않는다.

## API

모두 `Cache-Control: no-store`, DB 조회 불가는 상세 SQL 없이 503 `QUERY_UNAVAILABLE`을 반환한다. limit은 1~100, 채널 ID는 32자리 소문자 hex다. 시간·cursor·범위 위반은 400이다.

| 경로 | 계약 |
|---|---|
| `GET /api/v1/rankings?window=60&limit=20` | window=60 또는 300. asOf/evaluatedAt/snapshotId/windowSeconds/algorithmVersion/status, 정상 entries와 excludedChannels |
| `GET /api/v1/spikes?cursor=...&limit=20` | asOf/status/events/nextCursor. cursor는 마지막 evaluatedAt과 spikeId의 고정 위치. 최신순, 다음 페이지는 엄격히 이전 위치부터 |
| `GET /api/v1/channels/{id}/activity?seconds=1800` | seconds=1800 또는 3600. 서버 시계로 완료된 분 범위를 정한다. 생략 시 30분. 명시적 from/to는 최근 1시간·UTC 분 경계만 허용하며 seconds와 함께 쓰지 않는다. 각 point의 from/to/count/referenceCount/coverage/status/reasons |
| `GET /api/v1/collection-status` | 1분 snapshot의 전체 상태와 채널 상태별 개수. 공개 ID 외 내부 진단/원문은 노출하지 않음 |

순위 entry에는 channelId/rank/count/referenceCount/coverage/status/reasons/previousRank/previousReason을 둔다. snapshot이 없으면 WARMING_UP과 빈 결과다. 평가 시각이 20초 이상 오래되면 STALE이다. 웹도 이 경계에서 최신 순위·건수를 보류한다.

급상승 event에는 spikeId/channelId/evaluatedAt/count/baseline/ratio/algorithmVersion/valid/qualityWarning을 둔다. 피드는 평가가 없으면 WARMING_UP, 마지막 trend 평가가 20초 이상 오래되면 STALE이다. 이전 이벤트를 현재 방송의 채팅 건수로 표시하지 않고 기록 시각과 당시 기준선을 함께 표시한다.

## 검증 범위

기능 F03은 T02(독립 projection 멱등 소비), T08(partition freshness), T11(미확인 버퍼 품질 제외)에 기여한다. 동일 eventId를 두 번 발행해 ranking/trend 각각 1건만 집계하는 것을 검증한다. 신규 trend의 충돌·rollback 전용 장애 시험, consumer 수별 부하·DB 병목 비교와 실제 메모리·채널 공정성은 남아 있다.

- 고정 시간 단위 시험: 57/60과 56.999/60 경계, 세션 합집합, warmup·손실·지연, 250/5와 251/5 기준선, 세 임계값, 연속/누락 tick, cooldown 경계.
- 실제 Kafka/PostgreSQL: 완료 창 경계, 정상 0건·동률, snapshot 멱등성과 지연 bucket 보정 분리, 제외 조건, heartbeat 사후 정정, 독립 trend 판정, 중복 알림 방지, collector 관측 저장, 이전 순위·cursor·즉시 disable, 조용한 partition poll·end offset, ranking 소비 중지 중 trend 계속 처리.
- 합성 HTTP/WS: 읽기 전용 프로토콜 유지, 관측 callback, invalid 메시지의 품질 손실, API 범위·cursor·503 비노출.
- 웹: 품질과 숫자의 런타임 계약, 정상 0건/공백 구분, 20초 만료, 두 리본 색상, 피드 페이지 이동·사후 정정·갱신 실패, 상세 구간 표시.

실방송의 6분 급상승 발생을 강제로 유도하거나 검증했다고 주장하지 않는다. 자동 테스트는 합성 데이터와 격리한 topic/group/schema를 사용한다. 보존 자료 자동 삭제·관리자 UI·DLQ·재처리·장시간 부하·다중 호스트 수집은 후속 범위다.
