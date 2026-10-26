# 방송 내부 샤딩의 공식 자료 검토

조회일: **2026-10-26 KST**. Apache Kafka **4.1 문서·4.1.2 태그 소스**, PostgreSQL **16 문서**를 확인했다. 기술 연결: **T02/T04/T06/T07/T08/T10/T15/T16**. 기능 연결은 [기술 목표](../TECH_GOALS.md)의 F01/F05/F06/F08/F10/F14/F15/F16을 따른다.

이 문서는 공식 계약과 그 계약에서 도출한 설계 검토다. 샤딩의 제품 적용, 새 성능 시험, 실제 운영 인프라 구축을 완료했다는 근거가 아니다. 현재 상태와 검증 범위는 [STATUS](../STATUS.md), 기존 합성 실험은 [파이프라인 실험 검증](../pipeline/EXPERIMENTS_VERIFICATION.md)을 따른다. 외부 자료는 Apache Kafka 소스·문서와 PostgreSQL 공식 문서만 사용했다.

## 1. 파티션 수·샤드 수·복제 수의 역할

| 항목 | 공식 계약에서 확인한 역할 | 이 프로젝트에서 구별할 것 |
|---|---|---|
| 토픽 | 이벤트를 묶고 구독·보존·권한 정책을 적용하는 단위 | 방송 수와 같은 수로 만들 필요는 없음 |
| 파티션 | 로그의 순서와 일반 consumer group의 할당 단위 | 파티션이 여러 개여도 한 키의 부하가 자동 분산되지 않음 |
| 애플리케이션 샤드 | Kafka가 정의한 별도 자원이 아니라 앱의 메시지 분할 규칙 | 샤드 번호와 실제 파티션 번호가 같다는 보장은 없음 |
| replica | 같은 파티션 로그의 복제본 | 복제본 수와 같은 수의 일반 소비자가 한 파티션을 나눠 처리하는 구조는 아님 |

토픽·파티션·복제의 구분은 [Kafka 기본 개념](https://kafka.apache.org/41/getting-started/introduction/#main-concepts-and-terminology), 일반 소비자 할당은 [KafkaConsumer 4.1.2 API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)를 확인했다. 애플리케이션 샤드의 정의와 오른쪽 열은 이 계약을 적용한 설명이다.

## 2. 키에 접미사를 붙이는 것만으로는 분산을 확정할 수 없다

### 공식 소스에서 확인한 사실

Kafka 4.1.2 Java producer는 아래 순서로 파티션을 선택한다.

1. `ProducerRecord`에 파티션이 명시됐으면 그 번호를 사용한다.
2. 사용자 지정 partitioner가 있으면 그 결과를 사용한다.
3. 직렬화된 키가 있고 키 무시 설정이 아니면 기본 키 해시를 사용한다.
4. 그 밖에는 키 없는 메시지의 기본 분배 경로를 사용한다.

근거: [KafkaProducer.partition 소스](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1395-L1412).

기본 키 분배식은 `toPositive(murmur2(serializedKey)) % numPartitions`다. 같은 직렬화 바이트와 같은 파티션 수를 쓰는 producer들은 같은 결과를 얻는다. `channelId + shard`로 키를 바꾸면 서로 다른 키가 되지만, 나머지 연산 결과까지 서로 다르다는 뜻은 아니다. 세 샤드가 두 파티션에만 배치되거나 한 파티션에서 충돌할 수 있다. 이는 소스의 분배식에서 바로 도출되는 성질이다. [BuiltInPartitioner.partitionForKey 소스](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L299-L304)

### 우리 설계에서 검토할 후보

**설계 추론:** 키 접미사 방식을 선택한다면 실제 파티션 배치를 미리 계산·검증해야 한다. 다른 후보는 원래 `channelId`를 키에 유지하면서 `eventId`로 논리 샤드를 정하고 `ProducerRecord`에 파티션을 명시하는 방식이다. 명시 파티션 사용 자체는 위 공식 API 동작이며, 아래 배정 규칙은 아직 채택하지 않은 예시다.

```text
전제: 고정 routingVersion, 파티션 수 P, 샤드 수 S, 1 <= S <= P
shard = stableHash(canonicalEventIdBytes) mod S
partition = (stableHash(canonicalChannelIdBytes) mod P + shard) mod P
```

같은 방송의 서로 다른 샤드 번호는 이 식에서 서로 다른 파티션을 가리킨다. 모든 샤드에 충분한 이벤트가 배치되는지는 입력과 해시 분포의 문제이므로 별도 검증이 필요하다. 서로 다른 방송의 샤드는 여전히 같은 파티션을 공유한다. 또한 파티션 번호가 다르더라도 leader가 같은 broker에 있으면 broker 자원은 공유한다. 이것은 위 식과 Kafka의 파티션 배치를 적용한 추론이며 성능 보장은 아니다.

안정성을 얻으려면 해시 알고리즘, ID의 바이트 표현, 샤드 수, 파티션 목록, 라우팅 버전을 명시해야 한다. producer 재시작·다중 producer·애플리케이션 재발행에도 같은 이벤트의 배정이 재현되어야 한다. 매 발행 시 난수나 프로세스 내부 카운터를 고르는 방식은 이 조건을 만족하지 않는다. 기존 이벤트를 새 UUID로 만드는 원천 재수신 중복은 이 설계로 해결되지 않는다.

Kafka producer의 idempotence는 애플리케이션이 다시 `send`한 이벤트 전체를 중복 제거하는 기능이 아니다. 공식 API 설명은 애플리케이션 재전송을 중복 제거할 수 없고 producer 세션 내 범위임을 명시한다. 따라서 샤딩 후에도 내부 `eventId`에 대한 DB 멱등 처리와 payload 충돌 검증을 유지해야 한다. 마지막 문장은 이 프로젝트의 T02에 대한 설계 요구다. [KafkaProducer idempotence 설명](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)

## 3. 순서·소비 병렬성과 증설의 경계

**공식 계약:** 일반 consumer group에서는 한 파티션이 그룹 안의 한 소비자에 할당된다. 같은 소비자가 여러 파티션을 맡을 수 있다. 단일 토픽의 파티션이 3개라면 소비자 4개를 실행해도 네 번째 소비자에 그 토픽의 추가 파티션을 줄 수 없다. 앱이 소비와 처리 스레드를 분리할 수는 있지만, 이때 순서와 완료 offset 관리는 앱의 책임이 늘어난다. 이 설명은 현재 사용 중인 일반 `KafkaConsumer` 그룹에 관한 것이며 share group의 계약을 대신하지 않는다. [KafkaConsumer 그룹·멀티스레드 처리](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

Kafka가 보장하는 로그 읽기 순서는 파티션 안의 기록 순서다. 방송 하나를 여러 파티션으로 나누면 방송 전체에 대한 하나의 offset이나 로그 순서가 생기지 않는다. eventTime 순서도 이 보장에 포함되지 않는다. 따라서 방송의 상태 전환·시각 기준 결합·관측 품질을 단순 count와 같은 방식으로 취급하면 안 된다. 앞의 두 문장은 [Kafka 기본 순서 계약](https://kafka.apache.org/41/getting-started/introduction/#main-concepts-and-terminology), 뒤의 두 문장은 그 계약에 따른 설계 추론이다.

파티션을 늘리면 기본 키 해시의 분모가 달라져 이후 메시지의 파티션이 바뀔 수 있다. **기존 기록은 자동으로 새 파티션에 재분배되지 않는다.** Kafka는 토픽의 파티션 수 축소도 지원하지 않는다. [Kafka 토픽 변경](https://kafka.apache.org/41/operations/basic-kafka-operations/#modifying-topics)

**우리 설계에 대한 추론:** 샤드 수 변경도 라우팅 전환이다. `routingVersion`과 실제 `(topic identity, partition, offset)` 진행 위치를 함께 다뤄야 한다. 서로 다른 파티션의 offset 숫자를 비교해 방송 전체의 가장 최신 상태로 정하는 방법은 유효하지 않다. 새 토픽 전환, 기존 토픽의 버전 전환, 이전·이후 입력 병행 처리 중 어느 경로를 택할지 결정하고 replay manifest·중복 namespace·롤백 범위를 함께 검증해야 한다. 파티션을 늘렸다는 이유만으로 기존 hot backlog가 즉시 분산되지는 않는다.

## 4. 토픽 분리의 격리 범위

토픽별 보존 정책과 접근 권한을 나눌 수 있다. 그러나 같은 broker를 쓰는 토픽은 broker의 처리·네트워크 자원을 공유한다. Kafka의 기본 client quota는 user principal, client ID 또는 그 조합을 기준으로 설정하며, 토픽 이름을 나눴다고 별도 처리량 예산이 자동 배정되지는 않는다. quota는 broker마다 적용되므로 클러스터 전체 한도로 그대로 읽어서도 안 된다. [Kafka 멀티테넌시](https://kafka.apache.org/41/operations/multi-tenancy/), [Kafka quota 단위·적용 범위](https://kafka.apache.org/41/design/design/#quotas)

파티션 로그의 segment와 index는 파일 descriptor와 memory mapping을 사용한다. 한산한 토픽이 CPU 한 코어나 소비자 하나를 예약하는 것은 아니지만, 파티션 수를 늘리는 데 운영 비용이 전혀 없는 것도 아니다. 비용을 계산할 때 방송 수 대신 실제 partition·replica·segment·connection 수를 확인해야 한다. 앞 문장의 근거는 [Kafka OS 자원 설명](https://kafka.apache.org/41/operations/hardware-and-os/#os), 뒤의 두 문장은 이를 적용한 추론이다.

**설계 추론:** 인기 방송용 토픽 또는 소수의 트래픽 등급별 토픽은 보존·권한·운영 정책 분리에는 도움이 될 수 있다. 처리량 격리가 목적이면 producer/consumer의 quota 주체, 전용 실행 자원, broker 배치, 공유 DB까지 분리 범위를 명시해야 한다. 토픽 이름만으로 다른 방송의 지연을 막는다고 판정하지 않는다. quota로 제한된 상류 발행은 유한 버퍼와 연결 중단 정책(T11)에 어떤 영향을 주는지도 함께 검증한다.

## 5. 샤딩과 Kafka HA는 따로 검증한다

| 계약 | 확인한 의미 | 해석 시 주의점 |
|---|---|---|
| RF3 | 파티션 로그를 세 replica에 배치 | 서로 독립적인 세 소비 작업을 만드는 설정이 아님 |
| `acks=all`, `min.insync.replicas=2` | ISR가 2개 이상이어야 성공 가능하며 현재 ISR 전체의 확인을 기다림 | ISR3이면 3개가 확인한다. 항상 2개만 기다리는 설정이 아님 |
| ISR가 minISR 미만 | producer 오류 가능 | `NotEnoughReplicasAfterAppend`도 있으므로 오류를 미저장 확정으로 치환하지 않음 |
| KRaft controller 3개 | metadata quorum의 과반수 유지에 2개 필요 | controller 복제와 데이터 topic RF는 별개 |
| rack/AZ 배치 | 같은 파티션 replica를 가능한 서로 다른 rack에 배치 | 한 호스트의 컨테이너 여러 개로 호스트 장애 독립성을 입증하지 못함 |

복제 ACK 계약은 [Kafka topic minISR](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)와 [producer acks](https://kafka.apache.org/41/configuration/producer-configs/#acks), controller 수와 역할 분리는 [KRaft 운영](https://kafka.apache.org/41/operations/kraft/), rack 배치는 [replica rack 배치](https://kafka.apache.org/41/operations/basic-kafka-operations/#balancing-replicas-across-racks)를 확인했다. 표의 주의점 중 소비 작업 수·독립 장애 영역은 이 계약의 적용 결과다.

Kafka 4.1 신규 클러스터는 ELR가 기본 활성화되며, ISR가 minISR보다 작으면 high watermark가 진행하지 않는 규칙과 안전한 leader 후보 추적을 사용한다. ISR 밖의 안전한 ELR 후보를 무조건 unclean election으로 분류하면 안 된다. 실제 feature 상태와 `min.insync.replicas`의 적용 범위를 기록해야 한다. [Kafka 4.1 ELR](https://kafka.apache.org/41/operations/eligible-leader-replicas/)

**설계 추론:** 샤딩은 hot partition의 처리 병렬성을 높일 수 있고, RF/ISR/quorum/장애 영역은 장애 시 동작을 정한다. 둘의 효과는 별도다. RF1에서 측정한 샤딩 지연을 RF3·영역 간 통신·broker 장애 중 처리량으로 환산하지 않는다. 실제 운영 후보는 정상·단일 broker 장애·복귀 중에 각각 처리율, lag, ACK 지연, ISR, DB 지연을 대조해야 한다. 비용·VM 수·월 견적은 이번 자료 조사에서 확정하지 않는다.

## 6. PostgreSQL의 같은 집계 행이 다음 병목이 될 수 있다

PostgreSQL 16의 `INSERT ... ON CONFLICT DO UPDATE`는 관련 없는 오류가 없다면 insert 또는 update 결과를 원자적으로 보장한다. 동시에 같은 unique key를 수정하는 작업은 대기할 수 있다. 원자적 결과 보장과 무경쟁 병렬 처리는 다른 성질이다. [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT)

행 잠금은 같은 행의 다른 writer·locker를 막으며 보통 트랜잭션 종료까지 유지된다. 여러 행을 서로 다른 순서로 갱신하면 deadlock이 생길 수 있어 일정한 잠금 순서와 중단된 트랜잭션의 재시도가 필요하다. [PostgreSQL 16 행 잠금·deadlock](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)

Read Committed에서 먼저 갱신하던 트랜잭션이 끝나면 다음 updater는 갱신된 행 버전에 연산을 적용한다. 따라서 DB에서 `count = count + delta`를 실행하는 방식과 앱이 이전 count를 읽어 계산한 값을 덮어쓰는 방식은 동시성 의미가 다르다. Kafka를 나눈 뒤에도 같은 `(channelId, bucket)` 행을 모든 consumer가 매번 갱신하면 그 행에서 경쟁할 수 있다는 결론은 공식 갱신 규칙에 따른 추론이다. [PostgreSQL 16 Read Committed](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED)

### 부분 집계 후보의 이익과 비용 — 아직 검증하지 않은 설계 추론

| 후보 | 기대 이익 | 추가로 지켜야 하는 계약·측정 |
|---|---|---|
| 배치 안에서 동일 bucket의 delta를 합쳐 한 번 갱신 | 같은 행의 update 횟수 감소 | eventId 중복 제거 후에만 delta 구성. DB commit·progress·재전달의 원자성 유지 |
| `(channelId, bucket, shard)`별 부분 집계 행 | 같은 방송의 writer가 다른 행을 갱신 | 조회 시 SUM, 행·인덱스 증가, 늦은 이벤트, routingVersion·shard 변경 시 중복 합산 방지 |
| 별도 병합 단계 | 부분 집계의 읽기 비용을 최종 결과로 이동 | 병합 지연·실패·재시도·완료 기준과 두 번째 상태 저장 경계 추가 |

단순 정수 count는 중복이 제거된 각 부분의 합으로 병합할 수 있다. 최신 상태·카테고리 전환·관측 품질은 count의 교환 법칙만으로 정합성을 증명할 수 없다. 일부 샤드만 따라온 결과를 전체 방송의 정상 관측으로 표시하지 않도록 기대 shard 집합과 완료 기준을 정의해야 한다. API의 여러 SQL이 각각 다른 시점의 snapshot을 읽을 수 있다는 점도 조회 계약에 포함한다. snapshot의 공식 근거는 [PostgreSQL 16 격리 수준](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED), 병합·품질 조건은 이 프로젝트의 설계 추론이다.

## 7. 채택 결정 전에 남겨야 할 근거

아래는 공식 계약과 위 위험을 적용한 검증 요구이며, 이번 조사에서 실행한 시험 목록이 아니다.

1. **분배:** 다중 방송·인기 방송의 실제 shard→partition 배치, 다중 producer·재시작·중복 재발행에서 동일 eventId의 배정 재현, 의도한 버전 불일치 거부.
2. **정합성:** DB commit 직후 종료, 재전달, 일부 shard 지연·정지, 늦은 eventTime, 카테고리 전환에서 live와 고정 범위 replay 대조. count·ledger·progress·품질을 각각 확인.
3. **DB 경합:** 동일 입력·배치·자원에서 현행/샤딩/부분 집계를 비교. SQL 시간, lock wait, transaction·WAL 비용, pool 대기, API 지연, backlog 증가율을 기록.
4. **전환:** 기존·신규 라우팅의 혼재, topic identity와 manifest, 샤드 수·파티션 수 변경, 중단·롤백 후 중복과 공백을 검증.
5. **운영:** RF3/minISR2 환경의 정상·장애·복구에서 처리 SLO와 실제 보존 범위를 재측정. 최대 채널 수는 동시 연결·총 입력률·편중·DB와 함께 판정.

### 기존 샤딩 실험의 해석 정정

이번 검토에서 기존 정제 JSON을 다시 읽었다. `matrix-1-sharded-3.json`의 전체 파티션 건수는 `1790/1999/26511`이고, 24,000개 고유 입력을 차지한 hot 방송의 `appendOrderAudit.channelPartitions`는 `[2]`였다. 대조군 `matrix-1-hot-duplicates.json`도 hot 방송은 `[2]`, 전체 분포는 `1580/1896/26824`였다. 따라서 기존 `sharded-3` 실행은 hot 방송을 여러 파티션에 분산했다는 근거가 아니며, 두 실행의 p95 차이를 샤딩 효과로 설명할 수 없다. 이 판정은 공식 자료의 일반론이 아니라 저장소 근거를 재검토한 결과다. [기존 정제 결과](../poc/evidence/load-2026-10-26/report/summary.json)

해당 수치는 운영 최대 처리량, 모든 방송의 개선율, RF3 가용성 또는 제품 적용 완료의 근거로도 사용하지 않는다. 입력과 검증 범위는 [실험 검증](../pipeline/EXPERIMENTS_VERIFICATION.md), 최근 고정 자원 용량 판정은 [용량 검증](../CAPACITY_VERIFICATION.md)을 참조한다. 새 비교에서는 실제 hot 방송의 파티션 배치 확인을 성능 해석의 선행 조건으로 둔다.
