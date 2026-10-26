# Kafka replica 디스크 영구 손실과 빈 디스크 복구 조사

2026-10-26(KST) · #45 · F15 → T04/T10/T15

조사 시작 코드: `47d56590fea6603f17adc4ec1327d9cd052da3e6`. Kafka 문서는 4.1, 구현 근거는 Apache Kafka `4.1.2` 태그로 고정했다. 아래 수치·단계는 **실행 설계**이며 새 장애 시험을 수행한 결과가 아니다. 컨테이너 실행·코드 변경·commit·이슈 변경은 수행하지 않았다.

## 1. 현재 구현과 추가로 필요한 증거

| 대상 | 확인한 계약 | #45에서 보완할 부분 |
|---|---|---|
| `scripts/kafka-ha` | `scripts/infra up` 후 실제 HA 실험 실행 | 공유 PostgreSQL 사용과 별도 Kafka 소유 범위를 계속 구분 |
| `LocalKafkaCluster` | UUID project/owner, controller 3개·broker 3개, 고유 cluster ID, label이 있는 명시적 볼륨, 소유 자원만 정리 | 현재 stop/start는 원래 볼륨을 유지한다. broker 단위 볼륨 교체와 이전/새 directory ID 기록이 필요 |
| `infra/kafka-ha.compose.yaml` | `apache/kafka:4.1.2`, broker ID 1~3, controller ID 1001~1003, static quorum voters, 각 노드의 `/var/lib/kafka/data`, RF3/minISR2/unclean=false | controller 볼륨을 유지한 채 broker 데이터 볼륨만 손실시키는 별도 단계 |
| `KafkaHaExperiment` | 단일 broker/ISR1/controller quorum/RF1→3 시험, ACK 집합·broker 읽기·DB 대조 | `snapshot`에 ELR·lastKnownElr·epoch·directory ID가 없다. `publish`는 실패 원인과 ACK offset을 보관하지 않는다 |
| `KafkaSupport` | HA 시작에 ELR1 및 실제 topic 정책 확인, producer `acks=all`, idempotence, 유한 timeout | ISR1 시작 거부와 broker의 실제 Produce 거부를 구분해서 기록 |
| `ReplaySource` | topicId·partition 집합·earliest/end를 비교, `none` reset, 원본 topic/group 변경 없음 | 내용 hash·과거 leader epoch를 manifest에 저장하지 않는다. 범위 검사 통과를 내용 완전성 보장으로 해석하면 안 됨 |

코드 근거: [클러스터 소유권](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/test/java/io/chatpulse/pipeline/LocalKafkaCluster.java), [Compose](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/infra/kafka-ha.compose.yaml), [기존 장애 실험](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/test/java/io/chatpulse/pipeline/KafkaHaExperiment.java), [KafkaSupport](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/main/java/io/chatpulse/pipeline/KafkaSupport.java), [ReplaySource](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/main/java/io/chatpulse/replay/ReplaySource.java).

`docs/pipeline/KAFKA_HA_VERIFICATION.md`의 기존 성공은 같은 디스크로 복귀한 범위다. 새 볼륨 복구 성공, 마지막 유효 copy 손실 감지, 실제 디스크 I/O 오류·bit 손상은 각각 다른 증거다. 이번 제안은 소유 볼륨의 영구 제거를 모델링하며 SSD 고장·정전·다중 호스트/AZ 내구성까지 확대하지 않는다.

## 2. 판정에 필요한 Kafka 4.1 사실

### 복제와 ACK

- `acks=all`은 현재 ISR 전체의 복제를 기다리며, ISR 수가 minISR 미만이면 성공 ACK를 보장하지 않는다. `NotEnoughReplicas` 또는 append 이후의 `NotEnoughReplicasAfterAppend`가 발생할 수 있다. RF3/minISR2가 언제나 세 디스크에 복제됐다는 뜻은 아니다. [Topic 설정](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)
- 4.1의 strict minISR에서는 ISR이 minISR보다 작으면 HW가 앞으로 가지 않는다. ACK1로 ISR1의 leader에 쓴 tail도 소비자에게 새로 보이지 않을 수 있다. 따라서 이번 all-ACK 손실 시험을 만들려고 ISR1에서 성공한 all-ACK 입력을 가정해서는 안 된다. [ELR 설명](https://kafka.apache.org/41/operations/eligible-leader-replicas/), [4.1.2 Partition.maybeIncrementLeaderHW](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/cluster/Partition.scala)
- Kafka의 로그는 filesystem/page cache를 이용한다. 복제 ACK는 물리 매체별 fsync 완료나 같은 호스트의 전원 손실 내구성을 측정한 값이 아니다. 모든 최신 copy를 제거하면 복제만으로 원본을 만들어낼 수 없다. 후자는 시험의 정보 보존 조건에서 도출되는 판단이다. [공식 Persistence·Replication 설계](https://kafka.apache.org/41/design/design/)

### ELR, last known leader, unclean election

4.1 신규 cluster는 ELR 기본 활성이다. 후보는 ISR, ELR, 조건을 만족하는 last known leader 순서다. `min.insync.replicas`를 변경하면 ELR 상태가 지워질 수 있으므로 시험 중 minISR를 낮추거나 재설정하여 결과를 맞추지 않는다. [ELR 운영 문서](https://kafka.apache.org/41/operations/eligible-leader-replicas/)

4.1.2 `PartitionChangeBuilder`는 ISR/ELR이 비고 `lastKnownElr`가 단일 원소이며 해당 broker가 허용 후보이면 last known leader를 고르는 경로를 갖는다. 일반 `Election.UNCLEAN` 분기와 별개다. **unclean=false만 보고 모든 데이터 상실 후 영구적으로 leader가 없을 것이라고 단정하면 안 된다.** 여기서 source가 반환하는 `unclean=true` 내부 상태와 운영자가 unclean election을 켠 행위도 구분한다. [4.1.2 PartitionChangeBuilder](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java)

공식 통합 시험은 unclean shutdown을 보고한 ELR member를 제외하는 사례와, 모두 제외된 뒤 last known leader를 다시 선출하는 사례를 가진다. 해당 시험은 KIP-966 Part 1 범위라고 명시한다. KIP 전체에 적힌 향후 unclean recovery 절차를 4.1.2에서 이미 실행 가능한 기능으로 취급하지 않는다. [4.1.2 EligibleLeaderReplicasIntegrationTest](https://github.com/apache/kafka/blob/4.1.2/core/src/test/java/kafka/server/integration/EligibleLeaderReplicasIntegrationTest.java)

일반 unclean election은 불완전 replica를 선택하여 손실을 만들 수 있다. 이번 복구 단계에는 `unclean=true`, 수동 `--election-type unclean`, RF/minISR 하향, topic 삭제·재생성을 사용하지 않는다. [unclean 설정](https://kafka.apache.org/41/configuration/topic-configs/#unclean.leader.election.enable)

### ID와 epoch

| 값 | 같은 broker의 새 빈 볼륨에서 기대할 관계 | 의미와 한계 |
|---|---|---|
| `cluster.id` | 기존 cluster와 같음 | 다른 cluster의 복귀를 막는 식별자. 데이터 hash가 아님 |
| `node.id` | 교체 대상 broker ID와 같음 | logical broker ID 유지. 기존 프로세스와 동시 실행 금지 |
| `directory.id` | 이전 디스크와 다르고 다른 디스크와 중복 없음 | 새 저장소 incarnation. 예전 `meta.properties`를 빈 디스크에 복사하여 같은 디스크처럼 꾸미지 않음 |
| broker incarnation/epoch | 재등록 값을 관측 | 동일 node ID라도 재시작 등록은 별도 세대. partition leader epoch와 다른 값 |
| topicId | topic 삭제 없이 유지 | 복구 중 같은 topic인지 확인. 레코드 내용 보존 증명은 별도 |
| partition leader epoch | 선출 전후 값·offset 연결을 관측 | leader 변화와 로그 분기 처리에 사용. 증가 자체는 데이터 손실 판정이 아님 |

포맷은 기존 cluster ID를 사용한다. `Formatter`는 빈 디렉터리에 새 directory ID를 만들고, 기존 `meta.properties`는 cluster/node 일관성과 directory ID 중복을 검사한다. `--ignore-formatted`는 기존 디스크 내용을 복구하는 옵션이 아니다. [KRaft provisioning](https://kafka.apache.org/41/operations/kraft/), [4.1.2 Formatter](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/metadata/storage/Formatter.java), [MetaPropertiesEnsemble](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/metadata/properties/MetaPropertiesEnsemble.java)

현재 static voters 설정은 유지한다. 수동 포맷을 사용한다면 다음은 **대상 broker의 새 빈 볼륨만** 참조하는 설정에서 실행할 명령 형태다. `--standalone`/`--initial-controllers`로 별도 controller quorum을 만들지 않는다. `--no-initial-controllers`는 기존 cluster에 붙이는 공식 형태이며, static voters와 충돌하는 위 두 bootstrap 옵션과 구별된다. [StorageTool.runFormatCommand](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/tools/StorageTool.scala)

```text
kafka-storage.sh format --cluster-id <기존 소유 cluster ID> \
  --config <교체 대상 broker 설정> --no-initial-controllers
```

공식 JVM 이미지의 launch는 설정 생성·storage format을 포함하는 `KafkaDockerWrapper setup`을 거쳐 서버를 시작한다. 따라서 하네스가 이미지의 초기화를 이용할 수도 있지만, 컨테이너가 시작됐다는 것만 기록하지 말고 포맷 전 빈 상태와 실제 `meta.properties` 결과를 확인해야 한다. [4.1.2 Docker launch](https://github.com/apache/kafka/blob/4.1.2/docker/jvm/launch)

controller는 broker 등록의 cluster ID·중복 등록·directory ID를 검사한다. leader 허용 판단에도 replica에 배정된 directory가 해당 broker의 online directory인지 사용한다. 이것은 위치/등록 검사이며 레코드 전체 hash 대조가 아니다. [ClusterControlManager.registerBroker](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java), [ReplicationControlManager.LeaderAcceptor](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java)

follower는 leader epoch와 end offset으로 로그 분기를 찾아 잘라낸 뒤 복제를 계속할 수 있다. 새 디스크의 복구는 남아 있는 올바른 leader의 로그를 다시 받는 것이다. epoch 검사가 삭제된 바이트를 보관하는 별도 백업 역할을 하지 않는다. [AbstractFetcherThread의 epoch/truncation 처리](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/server/AbstractFetcherThread.scala)

## 3. 공통 계측과 소유권 경계

아래는 하네스 구현 제안이다. 서로 파괴적인 사례는 새 owner/cluster/topic/schema로 분리하고, 정상 복구와 ISR1 사례만 같은 cluster에서 연속 실행할 수 있다.

### 삭제를 허용하는 조건

1. 실행 시작 때 등록한 project/owner와 정확한 broker service ID를 사용한다. 사용자 임의 volume 이름이나 호스트 경로를 인자로 받아 삭제하지 않는다.
2. 해당 broker container의 owner label, Compose project/service, 실제 mount source/destination을 확인한다. 삭제 대상은 그 mount가 가리키는 `<project>_broker[1-3]-data` 중 선택한 하나다.
3. volume의 owner/Compose label도 별도로 검사하고, 그것을 사용하는 모든 container를 조회한다. 대상 broker 이외 참조가 있거나 controller/shared resource라면 중단한다.
4. 선택 broker를 정지하고 PID/container의 종료를 확인한 뒤 그 container만 제거한다. read-only 진단 helper도 종료한 뒤 해당 volume만 제거한다. live mount 안의 일부 `.log` 파일을 지우는 방식은 사용하지 않는다.
5. `volume inspect`의 실제 부재를 기록한 뒤 owner label을 갖는 새 빈 volume을 만든다. 같은 Docker volume 이름을 재사용한다면 이름만으로 새 디스크를 증명하지 말고 제거 성공·새 빈 상태·새 directory ID를 함께 기록한다.
6. 같은 node/cluster ID로 해당 broker만 다시 생성한다. 전체 `up`이 손실 단계에서 유지해야 할 다른 broker를 자동 복귀시키지 않도록 service를 한정한다.
7. controller 3개 볼륨·다른 프로젝트·공유 `chat-pulse` Kafka/DB·다른 worktree는 삭제 대상에서 제외한다. 종료 시 owner로 확인된 helper/container/network/volume/schema만 정리한다.

이는 `LocalKafkaCluster`의 기존 소유권 설계를 확장하는 제안이다. 단일 broker 볼륨 제거에는 그 broker의 모든 topic replica와 `__consumer_offsets` replica 손실도 포함된다. 특정 topic 파일만 지운 시험과 범위를 혼동하지 않는다.

### 장애 전 manifest

- 코드 SHA, clean/dirty, 이미지 ID/digest, 실제 Kafka/Java/client 버전, finalized features(`metadata.version`, `kraft.version`, ELR1), broker/topic 실제 minISR·unclean·retention 값을 기록한다.
- 단순한 합성 `BASE`/`TAIL` batch를 사용하고, producer callback의 성공 여부와 `(eventId, partition, offset, payload SHA-256)`을 연결한다. eventId는 합성 식별자다. ACK timeout은 `unconfirmed`이며 곧바로 미저장으로 단정하지 않는다.
- topicId·partition 집합, 각 partition의 leader/ISR/ELR/lastKnownElr, earliest/소비 가능한 end, broker별 directory ID·replica 크기/lag를 기록한다. `elr`가 API에서 미제공이면 빈 집합으로 바꾸지 말고 unavailable로 기록한다. [TopicPartitionInfo](https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/common/TopicPartitionInfo.java)
- 장애 전 고정 범위의 broker 레코드를 읽고 offset별 eventId/hash·전체 canonical digest를 만든다. 물리 record 수·고유 event 수·DB ledger 수·bucket 합계를 별도로 기록한다. 최종 정상 복구에서 ACK 집합 포함과 broker/DB 집합 일치를 검사한다.
- 소유 topic은 `delete`, 충분한 유한 retention, 유한 입력 크기/실행시간으로 고정한다. 보존 만료·compaction·DeleteRecords·동시 producer가 손실 원인을 바꾸지 않게 한다. 수치 상한은 실제 하네스에 기록하며 운영 용량으로 부르지 않는다.

### API 숫자와 실제 디스크 증거

Admin `listOffsets`의 `latest`를 broker별 물리 LEO로 부르지 않는다. 4.1.2 `Partition.fetchOffsetForTimestamp`는 client의 READ_UNCOMMITTED에 HW, READ_COMMITTED에 LSO, replica 쪽 조회에 LEO를 사용한다. ISR1에서 조회 실패도 가능하므로 `unavailable`과 원인 코드를 보존한다. [Partition 구현](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/cluster/Partition.scala)

`describeLogDirs`의 `ReplicaInfo.offsetLag`는 현재 log의 LEO와 partition HW의 차이에 관한 값이다. lag0은 세 디스크의 payload 동등성 증명이 아니다. JMX의 LogStartOffset/LogEndOffset과 directory offline 값은 broker별 진행을 보완한다. [ReplicaInfo](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/ReplicaInfo.html), [공식 모니터링](https://kafka.apache.org/41/operations/monitoring/)

중단한 broker의 소유 volume은 read-only helper로 확인한다. 실제 `.log`를 읽어 batch CRC·offset/epoch와 합성 eventId/hash를 대조하는 별도 probe가 가장 직접적이다. 공식 `kafka-dump-log.sh --deep-iteration --files <명시적 파일>`은 offset·batch CRC·epoch 검토에 참고할 수 있지만, 종료 코드만으로 모든 검사 통과를 계산하면 안 된다. `--print-data-log`/decoder 옵션은 payload를 출력하므로 공유 로그에 쓰지 않는다. [4.1.2 DumpLogSegments](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/tools/DumpLogSegments.scala)

기록은 허용된 숫자·코드·합성 eventId·hash만 남긴다. raw log volume, 원문 채팅, 닉네임, 실제 사용자 ID, 토큰은 공유 evidence에 넣지 않는다. 보관용 hash와 별개로 원본 segment 복사본을 복구 경로에 몰래 다시 공급하면 영구 손실 시험이 성립하지 않는다.

## 4. 사례 A — replica 1개 영구 손실 후 빈 디스크 복구

| 단계 | 실행 설계 | 통과에 필요한 관측 |
|---|---|---|
| A0 | RF3/minISR2, ELR1, quorum 정상에서 합성 입력 발행·DB drain | 모든 partition ISR3, 모든 ACK 입력 readback·DB 포함, 장애 전 manifest |
| A1 | partition0의 follower 하나를 선택해 정지 | 대상 node ID, 종료, 남은 ISR2와 정상 leader, controller quorum 유지 |
| A2 | 정지한 volume에서 A0 데이터 존재를 read-only 확인 후 volume 영구 제거 | 대상 volume 소유권·비어 있지 않던 record/hash·제거 성공 |
| A3 | broker가 없는 동안 추가 합성 입력 발행 | ISR2의 `acks=all` 성공, DB 반영; 대상 디스크가 없을 때도 진행했다는 시간 순서 |
| A4 | 같은 cluster/node ID, 새 빈 volume으로 대상 broker 재생성 | format 전 데이터 없음, directory ID 변경, topicId·partition 수 유지 |
| A5 | 재복제 완료 대기 후 새 입력 | ISR3 회복, 새 replica의 log 존재/lag 회복, 모든 단계 ACK 집합 포함 |
| A6 | producer/consumer 정지 후 재구성한 replica의 disk readback과 전체 broker/DB 대조 | A0+A3+A5의 offset별 hash 보존, missingAcknowledged=0, broker/DB 차이0, cleanup 완료 |

A6의 디스크 검사는 대상 broker를 다시 정지한 뒤 read-only로 수행할 수 있다. `bootstrap.servers`에 그 broker 하나를 넣은 일반 consumer도 metadata를 통해 다른 leader로 읽으므로, 그 연결만 성공한 것을 새 replica 자체의 readback이라고 기록하지 않는다. [KafkaConsumer의 broker/partition 추적 동작](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

복구 전후 whole segment 파일의 byte hash가 같아야 한다고 단정하지 않는다. 시험의 핵심 비교는 같은 topicId/partition/offset의 합성 eventId·key/value digest다. index·checkpoint·leader metadata 차이와 업무 레코드 차이를 구분한다.

## 5. 사례 B — ISR 부족 거부와 복귀

1. A의 복구 완료 이후 ISR3를 먼저 확인한다. 현재 partition0 leader를 남기고 다른 두 broker를 **원래 디스크를 유지한 채** 정지한다.
2. target partition ISR1, 실제 minISR2, controller quorum 정상을 확인한다. `KafkaSupport.ensureTopic(...HA)`가 거부하는지 관측하고 기존 정상 app의 시작 검사 실패와 실행 중 broker Produce 실패를 분리한다.
3. target partition에 식별 가능한 합성 1건을 기존 `acks=all` producer로 제출한다. 유한 delivery deadline 안에 성공 ACK0, 예외 원인 체인, actual delivery/timeout을 기록한다. 모든 예외를 같은 성공적인 거부로 계산하지 않는다. 인증/잘못된 주소/코드 오류는 시험 실패다.
4. 두 broker를 복귀시키고 ISR3·소비 재개를 확인한다. 미확인 1건이 최종 broker/DB에 존재하는지 별도로 확인하고, 이후 정상 all-ACK 입력을 보낸다.

선행 ISR1 상태를 확인한 뒤의 `NotEnoughReplicas`는 직접 거부 근거다. 제품 producer의 재시도로 최종 예외가 Timeout인 경우에는 metadata/metric 또는 별도 제한된 관측으로 실제 ISR 거부였는지 구분한다. `NotEnoughReplicasAfterAppend`/timeout을 무조건 “레코드가 절대로 저장되지 않음”으로 해석하지 않는다. [Produce의 minISR 실패 조건](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)

## 6. 사례 C — 최신 tail의 마지막 유효 copy 손실

### 왜 별도 fixture가 필요한가

세 replica가 같은 데이터를 가진 채 두 broker만 정지한 상태는 다른 두 디스크에도 유효 copy가 남아 있다. 반대로 세 볼륨을 모두 빈 것으로 바꾸면 stale copy가 남은 경계는 검증하지 못한다. 아래 fixture는 **baseline을 가진 오래된 디스크 1개는 보존하면서, ACK된 최신 tail의 두 copy를 순서대로 제거**한다. 여기서 “마지막 유효 copy”는 전체 topic이 아니라 특정 ACK tail을 완전히 가진 마지막 디스크를 뜻한다.

RF3/minISR2/all 계약상 tail ACK 시점에는 적어도 ISR2의 두 copy가 필요하다. 그 후 첫 copy를 영구 제거하면 1개, 두 번째를 제거하면 0개가 된다. 이 순서를 실제 증거로 만들며, ISR1에서 새 all-ACK를 성공시키는 우회는 하지 않는다.

### 실행 순서

대상 partition0의 leader를 `L`, 최신 follower를 `F`, 뒤처진 채 보존할 broker를 `S`로 부른다. 이름을 broker1/2/3으로 고정하지 말고 실제 metadata에서 결정한다. topic 자체는 제품과 같은 partition3/RF3를 유지하고 이 단계의 marker는 partition0에 명시 발행한다.

| 시점 | L 디스크 | F 디스크 | S 디스크 | 허용되는 주장 |
|---|---|---|---|---|
| C0 | BASE | BASE | BASE | 모든 replica baseline 보유 |
| C1: S 정지 후 tail 발행 | BASE+TAIL | BASE+TAIL | BASE | ISR2에서 TAIL all-ACK, S는 tail 전 상태 |
| C2: F 정지·삭제 | BASE+TAIL | 없음 | BASE | TAIL의 유효 copy는 L 하나 |
| C3: L 정지·삭제 | 없음 | 없음 | BASE | TAIL의 모든 실제 copy 상실, BASE 디스크는 존재 |
| C4: S 복귀, L/F 빈 디스크 복귀 | 빈 상태에서 시작 | 빈 상태에서 시작 | BASE 또는 복구 과정 변화 | 자동 선출·재복제 결과와 원본 회복 가능성을 별도 판정 |

1. **C0:** BASE를 발행·ACK·DB commit 후 S를 정지하고 해당 디스크의 BASE event/hash를 확인한다. S는 이후 C3까지 다시 시작하지 않는다.
2. **C1:** ISR이 정확히 `{L,F}`, S는 ISR/ELR 밖임을 확인한다. TAIL을 발행하여 all-ACK offset 목록·readback·DB 반영을 기록한다. S의 오프라인 log end가 TAIL 첫 offset보다 앞에 있음을 다시 확인한다.
3. producer를 중단하고 consumer가 고정 tail 끝까지 DB/offset을 저장한 후 consumer도 중단한다. DB와 원본 manifest를 보존한다. 다른 입력이 tail 증거를 덮지 못하게 한다.
4. **C2:** F를 정지하고 BASE+TAIL의 실제 disk readback을 확인한 뒤 F volume만 제거한다. L의 target log에서도 TAIL 존재를 관측한다. F는 이때 새 volume으로 복귀시키지 않는다. 복귀하여 복제받으면 “유일 copy” 조건이 사라진다.
5. **C3:** L을 정지하고 TAIL의 마지막 물리 존재를 read-only로 확인한 뒤 해당 volume을 제거한다. S의 기존 volume/BASE hash는 유지한다. controller quorum·metadata volume은 계속 유지한다.
6. **C4a:** S만 원래 디스크로 복귀시키고 유한 시간 동안 leader/ISR/ELR/lastKnownElr·directory·API 오류를 관측한다. S가 stale이며 유효 최신 후보가 없으면 무리하게 선출하지 않는다.
7. **C4b:** 동일 node/cluster ID의 L/F를 새 빈 volume으로 한 대씩 복귀시킨다. 각 단계 실제 후보/epoch/offset을 기록한다. leader 부재가 계속되는지, last known leader 복귀 경로 등으로 짧은/빈 로그가 가용해지는지 **관측 결과대로** 기록한다. 자동 동작의 정확한 순서를 이번 조사만으로 확정하지 않는다.
8. **새 producer 시작 전에** 장애 전 manifest 검증과 DB/source 진단을 실행한다. leader가 없으면 SOURCE_UNAVAILABLE, end가 줄었으면 OFFSET_TRUNCATED 또는 DATABASE_AHEAD_SOURCE 등 실제 경로를 기록한다. 정상 시작/원본 재생 완료로 표시하지 않는다.

정지 디스크에서 읽는 구간에는 read-only helper만 붙인다. 과거 `.log` 사본을 새 volume에 넣거나 BASE/TAIL payload를 시험 코드에서 다시 발행하지 않는다. 그렇게 얻은 데이터는 Kafka 복구 증거가 될 수 없다.

### 복원 불가 판정

`leader 없음`이나 단일 timeout만으로 영구 손실을 확정하지 않는다. 아래 증거를 연결해 **시험 cluster의 보존된 Kafka replica만으로 TAIL 복원 불가**라고 기록한다.

- TAIL이 실제 all-ACK된 offset/hash 목록이 있고 장애 전 readback이 맞는다.
- TAIL을 보유했던 L/F 물리 volume은 제거됐고, S는 TAIL 이전부터 오프라인이며 TAIL이 없다.
- 외부 백업·mirror·tiered storage·원천 재요청을 fixture에 연결하지 않았다.
- 원본 전체를 읽을 수 없거나 원본 manifest/record digest 대조에서 TAIL이 빠진다. API 조회 불가와 실제 빈/짧은 로그는 서로 다른 결과로 보존한다.
- DB에 TAIL의 ledger/bucket이 남아 있어도 그것을 Kafka 원본 복구라고 부르지 않는다. 일부 집계 결과의 존재와 모든 원본 이벤트 재구성 능력은 다르다.

사례의 기대 결과는 `lossBoundaryVerified=true`, `sourceRecoverySucceeded=false`처럼 나누는 것이 적절하다. 의도한 손실 감지를 PASS로 표시하더라도 사용자에게 “데이터 복구 성공”으로 노출하지 않는다. 명칭은 구현 시 정하되 이 두 상태는 합치지 않는다.

## 7. 대조 사례 D — 모든 replica의 빈 디스크 시작

별도 owner/cluster에서 정상 all-ACK BASE를 만든 뒤 broker 3개를 정지하고 **broker 볼륨만 모두 제거**한다. controller quorum과 topic metadata는 남긴다. 세 broker를 같은 ID·새 directory ID로 다시 생성하여 실제 선출/offset/읽기 상태를 기록한다.

이 경우는 `ALL_REPLICA_DATA_REMOVED`이고 사례 C의 `STALE_BASELINE_REMAINS_LAST_TAIL_COPY_LOST`와 구분한다. topicId가 그대로인데 원본 범위가 없을 수 있다는 경계를 보여줄 수 있다. 다만 재시작 후 정확히 leader 없음/offset0 중 어느 상태가 관측될지는 고정 assertion으로 단정하지 않는다. 새 입력을 넣기 전에 기존 manifest·DB 진행과 비교한다.

D만 실행하고 “마지막 유효 copy 손실 검증”이라고 결론 내리지 않는다. #45의 핵심은 A의 재복제 성공, B의 ISR 거부, C의 stale copy가 남은 손실 판정이다. D는 모든 데이터 제거라는 더 강한 별도 대조를 추가할 때 사용한다.

## 8. topicId·offset 진단의 범위

| 관측/현재 guard | 감지하는 경우 | 이 검사만으로 확정할 수 없는 것 |
|---|---|---|
| topicId 변경 | 같은 이름의 다른 topic, 현재 ReplaySource의 TOPIC_CHANGED | 같은 topicId 안의 로그 손실·내용 변경 |
| partition 집합 변경 | partition 증설 등 manifest 구조 변화 | 기존 partition 내부 보존 완전성 |
| current earliest > captured start | 보존한 prefix가 더는 없음 | 중간/끝의 내용 교체, 손실 원인 자체 |
| current end < captured end | 현재 읽기 가능한 tail 범위 축소 | 잘린 뒤 새 입력으로 end가 다시 커진 경우 |
| DB next > current end | DB가 가리키는 원본 진행이 현재 source보다 앞섬 | DB next가 범위 안에 있지만 이전 offset 내용이 다른 경우 |
| group offset과 DB next 일치 | 두 진행 숫자의 정렬 | source payload hash 동일성, `__consumer_offsets` 자체 손실 원인 |
| broker별 LEO/lag/ISR 정상 | 진행 및 현재 replica 동기화 상태 | 장애 전 원본과 동일한 내용인지 |
| 장애 전 ACK offset별 payload digest 대조 | 알려진 합성 입력의 실제 보존/변경 | 관측하지 않은 live 전체 데이터 완전성 |

현재 `RecoveryPlan`은 cluster/topic ID·partition 수와 보존 범위/DB/group 위치를 검사한다. `ReplaySource` manifest에는 leader epoch·내용 hash가 없다. 이 표의 한계는 현재 코드의 비교 항목에서 도출한 것이다. [RecoveryPlan](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/main/java/io/chatpulse/operations/RecoveryPlan.java), [ReplaySource](https://github.com/joejaeyoung/jigeumini/blob/47d56590fea6603f17adc4ec1327d9cd052da3e6/server/src/main/java/io/chatpulse/replay/ReplaySource.java)

Kafka client는 epoch 정보를 이용한 divergence 검사에서 `LogTruncationException`을 제공한다. `seek(TopicPartition, OffsetAndMetadata)`는 leader epoch를 함께 설정할 수 있지만 현재 replay는 `seek(partition,long)`을 쓴다. 따라서 `auto.offset.reset=none`만으로 과거 manifest의 모든 내용 변경이 감지된다고 주장하지 않는다. `ListOffsetsResultInfo.leaderEpoch()`는 관측에 보탤 수 있으나 현재 값 하나가 과거 원본 digest를 대신하지 않는다. [LogTruncationException](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/LogTruncationException.html), [seek API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#seek(org.apache.kafka.common.TopicPartition,org.apache.kafka.clients.consumer.OffsetAndMetadata)), [ListOffsets 결과](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/ListOffsetsResult.ListOffsetsResultInfo.html)

이번 실패 시험의 원본 범위가 사라졌을 때 fresh group을 만들어 earliest/latest로 맞추는 것은 복구 완료 절차가 아니다. group reset과 offset 숫자 정상화가 삭제 원본을 되돌려주지 않는다. `__consumer_offsets` replica 손실로 group 조회 자체가 안 되면 값을 0/lag0으로 채우지 않는다. raw source의 상태와 group coordinator 상태를 별도 관측하며, 필요하면 group commit 없는 수동 partition assign으로 source 읽기 가능성만 확인한다.

## 9. 정제 evidence와 완료 기준

각 단계는 wall clock 시각과 monotonic 경과시간, 실행 명령의 정제본, 기대 조건/실제 조건/예외 코드, 미관측 값을 남긴다. timeout은 무한 재시도로 숨기지 않고 원본 실패를 보존한다. 로컬 elapsed 값은 운영 RTO/SLA가 아니다.

필수 evidence:

- owner/project/실제 resource 이름·label 검증·mount 대상, 삭제 전후 volume 존재, 새 빈 상태, 이전/새 directory ID, 각 node/cluster ID.
- 실제 image/client/version/features와 topic 설정, controller quorum 상태, partition별 leader/epoch/replicas/ISR/ELR/lastKnownElr.
- 각 batch의 attempted/acknowledged/unconfirmed, ACK offset/hash, broker 물리/고유 record 수, DB ledger/bucket, source earliest/end와 broker별 물리 log 상태.
- A의 새 디스크에 이전·중단 중·복귀 후 ACK 데이터가 모두 존재한다는 readback; missingAcknowledged=0, broker/DB 차이0.
- B의 ISR1 선행 관측, all-ACK 거부 원인, 복귀 뒤 unconfirmed record 존재 여부, 이후 정상 ACK.
- C의 S가 BASE만 보유한 증거, L/F가 TAIL을 보유했던 증거, 두 volume 제거 순서, 마지막 copy 제거 시점, 원본 재생 불가와 DB 잔존의 분리.
- D를 실행했다면 C와 구별되는 fault code와 결과. 실행하지 않았다면 미검증으로 표시.
- 모든 오류에서도 소유 helper/container/network/volume/schema 정리 성공 여부. 공유 자원 정리를 성공으로 계산하지 않음.

완료는 **A의 재복제 성공 + B의 계약 거부/복귀 + C의 정확한 손실 조건과 복원 불가 판정**을 실제로 연결한 때다. 마지막 단계가 정상 로그 복구 실패라는 사실을 숨기지 않는다. 외부 백업/재수집 복구, controller metadata 디스크 손실, live I/O/bit 손상, 호스트/AZ 손실은 별도 시험이며 이번 조사나 로컬 volume 제거로 검증한 것으로 표시하지 않는다.
