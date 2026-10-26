# 방송 내부 샤딩 도입 검토

2026-10-26 · [#67](https://github.com/joejaeyoung/jigeumini/issues/67) · 기준 `6e0e8b2`.
F01/F05/F06/F08/F10 → T02/T04/T06/T07/T08. 진행 상태는 [STATUS](../STATUS.md)에 둔다.

## 출발점과 기존 근거 정정

공용 채팅 토픽을 유지하고 인기 방송의 입력을 여러 partition으로 보내는 후보를 검토한다. 제품 채택은 전체 projection의 정확성·품질·복구와 같은 자원에서의 처리 개선을 함께 입증한 뒤 결정한다.

기존 `sharded-3`의 p95 270.585ms와 대조군 2,822.982ms는 hot 방송 분산 효과의 근거로 사용할 수 없다. 두 실행 모두 hot 방송이 partition2에만 기록됐다. 세 suffix 키가 기본 hash에서 같은 partition으로 충돌했다. 전체 물리 분포는 대조군1,580/1,896/26,824, suffix 후보1,790/1,999/26,511이었다. 원본은 보존하며 차이 원인은 미분리다. [기존 실험 정정](EXPERIMENTS_VERIFICATION.md), [공식 자료와 원본 감사](../research/KAFKA_SHARDING_SOURCES.md).

## 현재 코드의 제약

기준 commit의 정적 검토이며, 발생하지 않은 장애를 재현했다고 표현하지 않는다.

| 책임 | 현재 구현 | 샤딩 도입에 필요한 계약 |
|---|---|---|
| 발행·decode | [BufferedKafkaPublisher](../../server/src/main/java/io/chatpulse/pipeline/BufferedKafkaPublisher.java)는 channelId 키, [EventCodec](../../server/src/main/java/io/chatpulse/pipeline/EventCodec.java)은 key/payload 불일치 거부 | 업무 key 유지·명시 partition 후보. routing version/배정 검증 추가 |
| 소비 병렬성 | [BatchConsumers](../../server/src/main/java/io/chatpulse/pipeline/BatchConsumers.java)는 단일 container. 현재 각 projection에 consumer1명 | 같은 group의 여러 consumer가 서로 다른 partition을 맡는 실행·관측·종료·rebalance 검증 |
| 품질 | [CollectionQuality](../../server/src/main/java/io/chatpulse/quality/CollectionQuality.java)는 hash(channelId)%3 한 partition만 조회 | 기대하는 모든 partition의 freshness·blocked·진행 확인. 관측된 shard만 모아 정상 판정하지 않음 |
| 단순 count | [ProjectionStore](../../server/src/main/java/io/chatpulse/pipeline/ProjectionStore.java)는 ledger·bucket delta·progress 원자 저장 | 전역 eventId 멱등 범위 유지. 같은 bucket 행 경합과 다중 bucket 잠금 순서·deadlock/retry 측정 |
| 카테고리 | [CategoryStore](../../server/src/main/java/io/chatpulse/categories/CategoryStore.java)는 채널 advisory lock 아래 시각 결합·관측 보정, run별 revision 갱신 | 같은 방송은 DB에서 직렬 처리됨. 잠금 삭제 없이 배치/SQL 진단부터 수행 |
| 리포트 | [ReportStore](../../server/src/main/java/io/chatpulse/reports/ReportStore.java)는 채널 advisory lock 아래 세션·귀속 재계산 | shard별 count 합산만으로 세션/alias·늦은 관측 보정을 대체할 수 없음 |
| 관측·알림 | 관측은 별도 채널 key topic, 알림은 outbox/독립 group | 처음에는 관측을 샤딩하지 않음. 채팅 전체 shard 완료 전에 정상 급상승/알림을 확정하지 않음 |
| partition 수 | [DualTopicConsumer](../../server/src/main/java/io/chatpulse/pipeline/DualTopicConsumer.java)는 두 topic 각P3, category/report freshness는6개 관계 기대 | 첫 후보는 P3 고정. P 증설 시 정책·진단·기대 집합을 함께 변경 |
| replay·복원 | [ReplayManifest](../../server/src/main/java/io/chatpulse/replay/ReplayManifest.java)·[ReplaySource](../../server/src/main/java/io/chatpulse/replay/ReplaySource.java)는 identity와 partition별 범위 고정. [RecoveryPlan](../../server/src/main/java/io/chatpulse/operations/RecoveryPlan.java)은 group6개/입력관계8개 정렬 | 과거 routing 계약도 보존. partition 간 offset을 max 하나로 환산하지 않음. DB 진행/보존 대조 유지 |

최신 용량 시험에서 적체된 category/report가 샤딩만으로 개선된다고 판단할 근거는 없다. [#65](https://github.com/joejaeyoung/jigeumini/issues/65)의 SQL·잠금·공유 pool·계측 비용 진단을 병행한다. API와 여섯 group·scheduler도 한 JVM이므로 API 복제로 consumer만 늘리지는 않는다.

## 후보와 우선순위

| 후보 | 기대 이익 | 비용·판단 |
|---|---|---|
| 현재 key + 배치/SQL 개선 | 기존 품질·복구 유지, 변경 작음 | hot partition 한계는 남음. #65 기준선으로 먼저 측정 |
| channelId+suffix hash | 짧은 구현 | 실제 partition 충돌·key 검증 변경. 과거 재현용 유지 |
| channelId + eventId 기반 명시 partition | 같은 eventId 배정 고정, 서로 다른 shard를 다른 partition에 배치 | routing·품질·consumer 병렬화·DB 경합 검증 필요. 우선 실험 후보 |
| shard별 부분 bucket + SUM | 같은 bucket 행 경쟁 완화 가능 | 행/인덱스·읽기/병합 비용 증가. 실제 경합 확인 뒤 검토 |
| 인기 방송 전용 topic | 보존·권한·운영 정책 분리 | 동일 key 집중·공유 broker/DB는 남음. 독립 정책 필요 시 선택 |

별도 병합 topic/Kafka Streams는 기본안에 추가하지 않는다. 현재 병목을 확인한 뒤 두 번째 상태 저장·재처리 경계의 비용을 비교한다. [공식 근거](../research/KAFKA_SHARDING_SOURCES.md).

## 명시 배정의 제품 후보 계약

제안이며 현재 제품 기능이 아니다. 처음에는 P3을 유지하고 선정된 hot 방송만 S3, 일반 방송은 S1로 고정한다. 자동 승격/축소는 첫 도입 범위에서 제외한다.

```text
key = channelId
base = positiveMurmur2(UTF8(channelId)) % P
shard = positiveMurmur2(UTF8(canonicalEventId)) % S
partition = (base + shard) % P    # 1 <= S <= P
```

서로 다른 shard 번호는 다른 partition을 가리키지만 입력의 균등성은 따로 검사한다. 프로세스 순번·재발행 횟수를 배정 기준으로 쓰지 않는다. 원천 재수신이 새 eventId로 들어오는 중복은 해결하지 못한다.

`ChatRouting` module이 발행 위치와 품질의 기대 partition 집합을 소유하는 방향을 제안한다. Interface에는 routingVersion, topic identity/P, 채널별S, hash·UUID 바이트 규칙과 허용 과거 버전이 포함된다. publisher·quality·replay·복구 CLI에 hash/%3를 복제하지 않는다. 업무 payload hash/전역 eventId 멱등성과 routing 검증은 별개로 유지한다.

routing version·shard를 전달하는 header와 불변 정책 이력을 후보로 둔다. header 없는 과거 기록은 기존 key/P3 계약으로 해석하고 미지원 버전·잘못된 배정을 명시적으로 처리한다. prefix 제거로 decoder를 우회하는 실험 어댑터를 제품에 넣지 않는다. DB progress는 projection/run/topic/partition 단위를 유지한다.

품질은 창에 걸친 모든 routing 정책의 기대 partition 합집합을 검사한다. 모든 대상의 collector drain 이후 broker 표본이 DB에 반영돼야 한다. 한 곳이라도 미관측·적체·blocked이면 정상 순위/급상승 확정을 보류하고, 조용한 shard도 기대 집합에서 빼지 않는다. 같은 partition의 다른 방송 때문에 보류될 수 있는 보수적 범위도 기록한다.

## 전환·복귀 제안

1. S1 상태에서 기존 의미를 유지하는 새 reader/quality/replay/관리 도구를 먼저 검증한다. 모든 소비 경로가 과거·신규 정책을 해석해야 한다.
2. 첫 전환은 수집 중지→발행 drain→고정 end까지 모든 DB/consumer 배출→이전 writer 종료 확인 순서다. 실패·미확인이 있으면 전환하지 않는다. 원천 중지 공백은 복원할 수 없다.
3. topic/P는 유지하고 불변 routing 정책·전환 offset 벡터를 기록한다. 외부 WS/Kafka/DB 원자 transaction이 아니므로 명시 관리 절차·중간 실패 검사가 필요하다.
4. 선정한 hot 방송만 S3으로 시작한다. group offset reset이나 과거 로그 재분배를 하지 않는다. 창이 정책 전환을 걸치면 두 기대 집합과 준비 중 표시를 유지한다.
5. 복귀도 정지/배출 후 새 정책의 S1으로 전환한다. 보존된 S3 기록을 이해하는 reader/품질은 유지한다. 구형 바이너리로 즉시 교체하지 않는다. 새DB/새run 복원은 [기존 복구](../../deploy/README.md)와 함께 검증한다.

정책 저장 형식·명령·실패 주입은 후속 구현 범위다. 이 절은 실행 가능한 전환 도구가 완성됐다는 뜻이 아니다.

## 실험과 제품 채택 조건

[정본 명령](EXPERIMENTS.md)의 shard-control-1/3과 shard-explicit-1/3을 사용한다. 두 producer·20채널·80% 편향·1% 중복·2,000건/s·15초·P3/RF1·동일 eventTime 기준이다. C3 비교는 실행 순서를 반대로 한 반복을 포함한다. 이 실험은 모든20채널에 S3을 적용하며 제품 후보인 hot S3/일반 S1은 별도 검증 대상이다. 실제 hot 분포·배정 불일치·고유/물리/DB/replay·p95/p99·표본 lag/자원·정리를 기록한다. 품질/category/report/API가 없는 단순 projection 실험이며 전체 제품·실원천·HA·지속 용량을 검증하지 않는다.

### 2026-10-26 재검증 결과

깨끗한 `96d2cf4592bb8c1b7142e12c965dfaf6c8674fcd`에서 14:49:58~14:54:08 UTC에 순차 실행했다. 여섯 실행의 runtime class SHA256은 `847e9430c6e34ccb05db49b8f4a7e34f2746d1e1aa9936a9f8696551ac1fdf76`으로 같다. [실행 명령·환경·정리](../poc/evidence/sharding-2026-10-26/execution.json), [정제 근거](../poc/evidence/sharding-2026-10-26/report/summary.json), [독립 비교 화면](../poc/evidence/sharding-2026-10-26/report/index.html).

| 순서 | 배정 | consumer | p95 / p99 (ms) | 표본 최대 lag | 입력 후 drain (s) |
|---|---|---|---|---|---|
| 1 | channelId 기본 hash | 3 | 331.079 / 386.808 | 291 | 0.109 |
| 2 | eventId 명시 분할 | 3 | 45.447 / 98.730 | 0 | 0.106 |
| 3 | eventId 명시 분할 | 3 | 52.016 / 108.760 | 41 | 0.002 |
| 4 | channelId 기본 hash | 3 | 660.135 / 703.799 | 699 | 0.215 |
| 5 | channelId 기본 hash | 1 | 1,259.047 / 1,295.963 | 2,196 | 1.184 |
| 6 | eventId 명시 분할 | 1 | 3,239.650 / 3,751.877 | 4,832 | 2.806 |

- 각 실행은 고유30,000·물리/ACK30,300·DB30,000건, 중복300건 제외, 고정 범위 replay 차이0·계수 보존·입력 목표·소유 topic/schema/group 정리를 통과했다. 기존 실행 container8개 ID/이름도 보존했다.
- hot 물리24,300건은 기존 배정에서 `[0, 0, 24300]`, 명시 분할에서 `[8047, 8117, 8136]`이었다. 명시 분할의 최다 partition 비중33.4815%·배정 불일치0이며 전체 물리 분포는 `[10072, 10082, 10146]`이다. 업무 key를 변경하거나 replay에서 제거하지 않았다.
- 소비자3명은 시작 전 각1partition 할당을 기다렸고 모든 관측 표본도 `[1,1,1]`이었다. 소비자1명은 `[3]`이었다. 시작 전 검사와 드문 표본이 실행 전체의 모든 순간을 보증하지는 않는다.
- 생성30,000건은15.000~15.006초에 완료됐다. 생성 스케줄 지연 최대20.919ms다. 지연은 전체 고유30,000개의 receivedAt부터 DB commit 뒤 계측까지이며 HTTP 가시성 지연과는 다르다.

**판단: 명시적 분산과 병렬 소비를 후속 제품 실험 후보로 채택한다. 제품 기본 설정 변경은 보류한다.** C3의 두 쌍 모두 명시 분할 지연이 낮았지만 C1 단회에서는 오히려 악화됐다. partition만 분산하면 개선된다고 해석할 수 없다. C1 악화의 fetch/batch/SQL 원인은 이번에 분리하지 않았다. [#68](https://github.com/joejaeyoung/jigeumini/issues/68)에서 #65의 병목 진단과 함께 제품 도입을 판단한다.

클라이언트 JVM은 macOS/aarch64·Java21·Xmx768MiB·DB pool4, broker/DB는 Docker VM8CPU·약7.65GiB의 기존 공유 서비스다. 호스트와 다른 서비스의 부하는 분리 계측하지 않았다. 기본 배정 C3끼리도 p95가 약2배 달라, 고정 개선 배수나 운영 최대 채널 수로 환산하지 않는다. lag/자원 표본은 실행별3~4개이며 표본 수집 자체에 최대2.73~2.81초가 걸렸다. 표본 lag0은 실행 내내 적체0이라는 뜻이 아니다. 15초·fresh schema·C3 두 반복/C1 단회의 한계를 유지한다.

### 전체 제품 채택 게이트

제품 채택에는 다음이 모두 필요하다.

- 동일 자원·DB 데이터량·생성기에서 현행/병렬 consumer만 추가/샤딩/필요 시 부분 집계를 대조한다. 개선 목표·SLO는 실행 전에 고정한다.
- 한 shard 지연/정지·미관측, DB commit 직후 종료·rebalance·재전달, payload 충돌·cross-partition 중복에서 과소/중복 집계와 잘못된 정상 판정이 없어야 한다.
- 관측 지연·카테고리 전환·리포트 보정·outbox를 포함한 전체 결과/품질을 대조한다. 현재 replay CLI는10초 bucket 전용이므로 전체 제품 oracle을 별도로 둔다.
- routing 전환·복귀·보존 만료·P 변경·PITR 복원에서 group 불변과 DB/offset 정렬을 확인한다.
- lag 기울기·최악 partition·DB lock/풀/WAL·API p95·수집 buffer·비용을 함께 판정한다. 경쟁 증가로 악화되면 채택하지 않는다.

## 리팩토링 점검

이번에는 키/배정 선택과 실제 분포 검사만 ExperimentRouting에 모으고 생성·commit·replay·정리를 재사용한다. 제품 Interface는 변경하지 않았다. 후속에서는 routing 규칙, consumer 실행/관측 수명, category/report의 시각 결합을 각각 소유하는 module을 유지한다. 잠금 삭제나 범용 처리 프레임워크를 샤딩에 묶지 않는다.
