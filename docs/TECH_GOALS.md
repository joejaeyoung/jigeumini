# 지금이니 기술 목표와 기능 매핑

기술 목표는 **① 오프셋 커밋·재처리 ② 파티션 키·부하 편중 ③ 발행 버퍼·과부하 제어**다.
기능을 늘리는 것보다 각 문제의 실패 조건을 재현하고 선택한 처리 방식의 효과와 한계를 증명한다.
현재 가장 강한 근거는 M1의 DB commit 직후 종료·재전달 시험이다.

이 문서는 기술 문제·완료 조건·기능 연결의 정본이다. 현재 상태와 실행 근거는
[STATUS의 기술 목표 추적표](STATUS.md#기술-목표-추적표)에만 기록한다.
아래 **기준선**은 `44b6321`의 [처리 계약](pipeline/README.md)과 [검증 기록](pipeline/VERIFICATION.md)을
토대로 한 2026-10-25 M1 설명이다. 이후 코드나 작업 중 파일을 새로 검증했다는 뜻이 아니다.

## 갱신 절차

1. 기능·실험을 시작할 때 아래 기능 표에서 관련 기술 ID를 고른다. 새 문제는 다음 T 번호로 추가한다.
2. 이슈·PR에 기술 ID, 재현할 실패 조건, 기대 결과, 검증 명령을 적는다. 기술 목표와 무관한 변경은 `해당 없음`과 이유를 쓴다.
3. 구현 후 STATUS의 해당 행에 상태, 정제된 근거 링크, 검증 범위, 남은 문제/다음 행동을 갱신한다.
4. `python3 scripts/check-tech-goals.py`와 `python3 scripts/check-doc-links.py`를 실행한다. 전체 기본 검증은 `./scripts/check`다.

기능 완료와 기술 문제 해결은 별개다. 하나의 기능이 여러 ID에 기여해도 각 완료 조건을 따로 판정한다.
기술 ID는 삭제·재사용하지 않는다. 범위에서 제외하면 STATUS에 `보류`와 이유·재개 조건을 남긴다.
`검증됨`은 해당 ID의 명시된 범위에 한정되며 전체 시스템의 무손실·Exactly-once·HA를 뜻하지 않는다.

## ① 오프셋 커밋·재처리

### T01 커밋 주체와 시점 — Auto / BATCH / Manual

- 문제: DB 반영 전에 offset이 진행되거나 커밋 주체를 잘못 이해하면 장애 시 입력이 누락된다.
- 기준선: auto commit=false, AckMode.BATCH, 동기 커밋, poll 최대 200건. 정상 배치의 DB 반영 이후 Spring 컨테이너가 커밋한다. 직접 수동 ACK 구현이 아니다.
- 완료 조건: 격리된 실험에서 Auto/BATCH/MANUAL의 커밋 시점과 DB 지연·실패 시 offset을 비교한다. MANUAL도 즉시 커밋을 뜻하지 않으며 MANUAL_IMMEDIATE와 구분한다. 생산 설정 변경 여부는 결과로 결정한다.
- 근거: [Spring 컨테이너의 offset 처리](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html).

### T02 재전달과 DB 멱등 소비

- 문제: at-least-once 재전달로 같은 입력을 집계에 두 번 더할 수 있다.
- 기준선: 처리 이력·10초 bucket·진행 위치를 같은 DB 트랜잭션에 저장하고 같은 내부 eventId의 재반영을 막는다. payload 충돌은 오류다.
- 완료 조건: 배치 내부 중복, 재전달, payload 충돌, SQL rollback에서 기대 bucket·ledger·offset을 대조한다. 추가 projection에도 독립 namespace와 같은 검증을 적용한다.
- 한계: 원천이 같은 채팅을 새 eventId로 보내는 중복은 제거하지 않는다. Kafka와 PostgreSQL 전체를 포괄하는 Exactly-once 보장은 아니다.

### T03 DB commit과 offset commit 사이 장애

- 문제: DB 성공 후 offset 기록 전에 죽으면 이미 반영한 입력이 다시 온다.
- 기준선: DB commit 직후 별도 JVM을 halt(91)로 종료하고 동일 group 재시작 후 집계 1건 유지를 검증했다.
- 완료 조건: 해당 장애 위치에서 offset 미완료와 재전달을 확인하고 재시작 전후 DB 정합성을 비교한다. 실패 주입 위치·실제 종료·입력·실행 버전을 근거에 남긴다.
- 한계: 이 완료 조건은 특정 장애 지점이다. broker 장애, 디스크 손상, 대용량 복구는 별도 시험이며 알림 outbox도 별도 장애 경계로 검증한다.

### T04 시작 offset·retention·고정 범위 replay

- 문제: 시작 위치를 자동 이동하거나 삭제된 입력을 있다고 가정하면 불완전한 재처리를 성공으로 오인한다.
- 기준선: 기본 reset=none, 최초 그룹만 명시적 earliest. 24시간·파티션당 256MiB 보존 설정이며 용량 제한과 segment 삭제 때문에 24시간 전체 보존을 보장하지 않는다. earliest는 유효한 커밋이 있는 매 실행을 되감지 않는다.
- 완료 조건: topic identity·partition별 시작/끝 offset을 고정한 replay를 별도 run으로 수행한다. 두 실행의 bucket 일치, live 불변, offset 만료·취소·중단 복구를 검증한다. Kafka receivedAt과 분석 eventTime의 차이도 기록한다.
- 한계: 보존 범위 밖의 원천 입력과 수집 공백은 복원하지 않는다.

### T05 배치 재시도·poison message·DLQ

- 문제: 잘못된 한 건이 정상 이벤트와 같은 consumer에 할당된 다른 partition도 지연시킬 수 있다.
- 기준선: 1초 간격 무제한 batch 재시도, 오류 상태 노출, offset 차단. DLQ는 M1에 없다.
- 완료 조건: 영구 계약 오류와 일시 DB 장애를 구분한다. 안전한 DLQ ACK 이전 offset 진행을 막고, ACK 직후 종료·중복 격리·정상 이벤트 진행·DB 복구를 실제 broker/DB로 검증한다.
- 한계: DLQ 발행과 원본 offset commit은 별개다. failureId로 중복을 다루고 원문을 저장하지 않는다. 오류 수정 검증은 별도 replay에서 한다.

## ② 파티션 키·부하 편중

### T06 channelId 키와 순서의 범위

- 문제: 키 개수와 partition 수를 혼동하거나 채널 전체의 전역 순서를 가정할 수 있다.
- 기준선: key=channelId, partition 3개. 동일 키의 partition 내 append 순서와 eventTime 순서는 다른 개념이다.
- 완료 조건: 고정 partition 구성에서 같은/다른 키의 배치 위치, 다중 producer·재전달·늦은 eventTime 입력의 결과를 확인한다. partition 증설 시 키의 배치가 달라질 수 있는 영향과 집계 정책을 기록한다.
- 한계: 서로 다른 채널의 전역 순서를 보장하지 않는다. 카테고리 전환은 시각 기준 결합 문제도 포함한다.

### T07 hot partition과 키 분산의 대가

- 문제: 인기 채널 한 곳의 입력이 특정 partition에 몰리면 consumer 추가만으로 해결되지 않는다.
- 완료 조건: 균등 분포와 단일 채널 80% 입력을 비교해 partition별 입력률·처리율·lag를 측정한다. channelId와 channelId+shard 후보를 비교하고, 분산 후 채널 집계 병합·중복·순서·지연의 비용을 측정해 채택/기각 결정을 남긴다.
- 한계: 키 분산은 아직 채택한 설계가 아니다. 합성 데이터·별도 topic으로 검증한다.

### T08 consumer lag와 확장 판단

- 문제: 전체 lag 하나만으로 증설하면 hot partition이나 공유 DB 병목을 놓친다.
- 완료 조건: partition별 lag·입력률·처리율·DB 지연·수집기 미확인 버퍼 나이를 함께 관측한다. 같은 group consumer 수 1/2/3/4에서 처리량·유휴 할당·회복 시간을 비교하고 확장/축소 판단 기준을 적는다.
- 한계: 일반 consumer group에서 partition 3개면 네 번째 consumer는 추가 partition을 받지 못한다. 자동 확장 운영까지 구현했다는 주장은 별도다. 입력 없는 정상 상태와 지연/관측 불가도 구분한다.

### T09 리밸런싱과 처리 시간 제한

- 문제: 긴 DB 처리와 consumer 종료/추가가 재할당·재전달에 주는 영향을 구별해야 한다.
- 기준선: classic group, max.poll.interval.ms=300000, heartbeat=2000ms, session timeout=6000ms. 설정 존재는 장애 시험 통과가 아니다.
- 완료 조건: 긴 처리로 poll 제한 초과, heartbeat 단절, consumer 종료·추가를 각각 재현한다. 재할당 시간·중복·offset·집계 정합성을 비교하고 timeout을 바꾸면 무엇이 달라지는지 기록한다.
- 근거: [Kafka consumer 설정](https://kafka.apache.org/41/configuration/consumer-configs/). heartbeat/session은 생존, max.poll.interval은 poll 진행 제한과 관련되며 protocol·membership 설정에 따른 차이를 함께 기록한다.

## ③ 발행 버퍼·과부하 제어

### T10 Producer ACK와 멱등 발행

- 문제: broker ACK를 DB 집계 완료 또는 복제 HA로 오인할 수 있다.
- 기준선: acks=all, enable.idempotence=true, max.in.flight.requests.per.connection=1. broker 1대·RF=1 기능 검증이다.
- 완료 조건: 격리된 환경에서 ACK·재시도·실패의 관측값과 DB 반영을 구분한다. ACK 수준 비교 시 RF·ISR·min.insync.replicas를 함께 기록하고, 단일 broker에서 입증할 수 없는 범위를 명시한다.
- 표현: ACK 값은 `0`, `1`, `all(-1)`이다. `acks=2`는 없다. all은 ISR 확인을 기다리는 설정이며 단독으로 HA를 보장하지 않는다. 근거: [Kafka producer 설정](https://kafka.apache.org/41/configuration/producer-configs/).

### T11 유한 발행 버퍼와 과부하 정책

- 문제: broker/DB 지연이 누적될 때 메모리 증가나 조용한 손실을 막아야 한다.
- 기준선: ACK 대기 이벤트까지 최대 1,000개, 이벤트당 16KiB 상한, frame 전체 수락/거절, 초과·실패 시 수집 중단, 종료 drain 10초와 미확인 건수 기록. JVM 강제 종료 시 메모리 입력은 복원하지 못한다.
- 완료 조건: 느린 ACK·broker 단절·급격한 입력·동시 채널·종료 중 실패를 주입한다. queued+in-flight 상한, 실제 메모리, accepted/ACK/rejected/unconfirmed 보존 관계를 대조한다. 전역/채널별 공정성과 중단·복구 정책의 결정을 남긴다.
- 한계: payload 상한과 실제 JVM 메모리 상한은 다르다. 상류 READ WebSocket을 신뢰성 있게 되감거나 모든 입력을 보존하는 backpressure라고 설명하지 않는다.

### T12 발행 병렬화와 배치 효율

- 문제: 현재 worker 한 개가 한 건의 ACK를 기다린 뒤 다음 건을 보내 처리량을 제한할 수 있다.
- 완료 조건: 동일 입력으로 기존 직렬 발행과 bounded async concurrency 후보를 비교한다. producer batch 크기·요청률·처리량·p95/p99·메모리·채널 공정성을 측정하고 실패 callback/종료 drain에서 건수와 순서를 검증한다.
- 한계: 애플리케이션 동시 발행 수, Kafka client의 연결당 in-flight 요청 수, broker batch는 서로 다르다. T11 상한·실패 추적과 T02 정합성을 유지한 후보만 채택한다.

## 기능별 기술 매핑

표의 ID는 해당 기능이 **기여하거나 검증할 문제**다. 해결 여부는 STATUS의 근거로 판단한다.
신규 기능은 아래 F 번호를 추가한다. 기술 학습과 무관한 기능은 기술 ID를 `없음`으로 쓰고 기여/검증 열에 이유를 적는다.
기존 기능을 없애거나 합치면 행과 ID는 유지하고 대체 기능·이슈를 적는다.

<!-- tech-features:start -->
| 기능 ID | 기능·이슈 | 기술 ID | 기여/검증 |
|---|---|---|---|
| F01 | M1 수집→Kafka→DB→조회 | T01, T02, T03, T04, T05, T06, T10, T11 | 기존 수직 경로와 장애 검증의 기준선 |
| F02 | [웹 모노레포 #1](https://github.com/joejaeyoung/jigeumini/issues/1)·[메타데이터 #2](https://github.com/joejaeyoung/jigeumini/issues/2) | 없음 | 제품 기반이며 Kafka 기술 해결 실적으로 계산하지 않음 |
| F03 | [품질 기반 순위·급상승 #3](https://github.com/joejaeyoung/jigeumini/issues/3) | T02, T08, T11 | 신규 projection 정합성, lag/버퍼 freshness와 품질 제외 |
| F04 | [시청자·채팅 타임라인 #5](https://github.com/joejaeyoung/jigeumini/issues/5) | T02, T04, T06 | 별도 이벤트 이력, 시각 기준 결합, 늦은 데이터·재전달 |
| F05 | [카테고리 반응 #6](https://github.com/joejaeyoung/jigeumini/issues/6) | T02, T06, T07, T08 | 집계 키 변경·편향·독립 소비와 공유 DB 비용 |
| F06 | [방송 리포트 #7](https://github.com/joejaeyoung/jigeumini/issues/7) | T02, T04, T08 | 독립 projection·보존 범위·부분 관측 리포트 |
| F07 | [관심 방송 알림 피드 #8](https://github.com/joejaeyoung/jigeumini/issues/8) | T02, T03, T08, T10 | outbox의 DB/Kafka 장애 경계, 중복 피드 방지·독립 소비 |
| F08 | [고정 offset replay #9](https://github.com/joejaeyoung/jigeumini/issues/9) | T01, T02, T03, T04 | 운영 offset 불변·재처리 비교; ACK 방식 비교는 별도 실험 필요 |
| F09 | [DLQ·복구 진단 #10](https://github.com/joejaeyoung/jigeumini/issues/10) | T01, T03, T05, T08 | 영구 오류 격리·ACK/offset 경계·재시도 영향 |
| F10 | [부하·적체 회복 #11](https://github.com/joejaeyoung/jigeumini/issues/11) | T06, T07, T08, T09, T11, T12 | 분포·consumer 수·처리 지연·버퍼·발행 병렬화 실험; 기존 이슈 범위를 이 표로 보완 |
| F11 | ACK·커밋 방식 비교 실험 (로컬 계획) | T01, T10 | Auto/BATCH/MANUAL, ACK 수준 비교; 원격 이슈는 아직 없음 |
<!-- tech-features:end -->

## 검증 기록과 완료 판정

각 새 근거에는 기술 ID, 코드 commit/JAR hash, 실행일·환경, 입력 seed/manifest 또는 합성 입력 정의, 명령, 기대값·실제값,
실패 주입 위치, 대조 결과, 미검증 범위를 적는다. 실행 전의 시험 설계는 검증 근거로 승격하지 않는다.
공유 근거는 정제해 `docs/pipeline/evidence/` 또는 `docs/poc/evidence/`에 두고 검증 문서에서 해석한다.
진행 상태를 근거 파일에 별도로 관리하지 않는다.
기존 M1 기록은 당시의 검증 문서·suite 요약·실행 메타데이터를 함께 참조하는 기준선이다.
당시 남기지 않은 값을 사후에 만들어 채우지 않으며, 새 기술 실험의 완료 조건으로 확장하지 않는다.

STATUS 상태 값은 `미착수`, `진행 중`, `부분 검증`, `검증됨`, `보류`다.
`부분 검증`과 `검증됨`에는 저장소 안의 근거 링크와 실제 검증 범위가 필요하다.
`미착수`/`보류`라도 다음 행동을 비워두지 않는다. 실패한 실험도 결과를 남기고 미해결 이유를 적는다.

검사기는 목표·기능·상태 ID의 누락/중복/알 수 없는 참조, 기능 연결 누락, 근거 없는 검증 상태를 검사한다.
기본은 Git HEAD와 비교해 기존 ID 삭제도 검사한다. CI/커밋 간 검토에서는 `--base <기준 commit>`을 사용한다.
CI는 checkout에 없는 이전 SHA를 추가 fetch한다. PR base 확보 실패는 검사 실패다.
force-push 등으로 push 이전 SHA를 가져올 수 없으면 경고와 함께 삭제 비교만 생략하고 연결 검사를 수행한다.
이 경우 삭제 보존 판단은 PR base 검사로 확인하며 push 검사만으로 입증했다고 기록하지 않는다.
문장과 실험 결과의 진위·기술적 충분성은 리뷰 대상이며 이 검사 통과만으로 기술 해결이 입증되지는 않는다.
