# Kafka 복제·quorum 운영 계약

2026-10-26 · #29 / F15 → T04/T08/T10/T15

## 구성 결정과 근거

자체 운영 배치안은 3개 AZ에 Kafka용 EC2를 한 대씩 두고, 각 호스트에 broker1개와 전용 controller1개를 별도 컨테이너로 함께 실행한다. 총 broker3개/controller3개이며 프로세스별 CPU·메모리·디스크·재시작은 구분한다. 호스트 한 대 손실 시 broker와 controller가 동시에 사라지므로 두 역할의 결합 장애와 남은 ISR2/quorum2, 복구 부하를 실제 AZ에서 확인해야 한다. 로컬 리허설은 같은 호스트의6개 컨테이너이므로 실제 AZ·호스트·디스크 손상 내구성을 증명하지 않는다.

Kafka4.1은 새 클러스터에서 ELR이 기본 활성화된다. 이 프로젝트의 HA 모드는 finalized `eligible.leader.replicas.version=1`을 검사하고 그 계약으로만 시작한다. unclean election은 false다. ELR을 비활성으로 간주해 결과를 해석하거나, minISR를 변경해 가용성 수치를 맞추지 않는다.

- [KRaft 공식 운영 고려사항](https://kafka.apache.org/41/operations/kraft/): 중요 배치에서 역할 분리와 controller 과반수가 필요하다.3개 controller는1개 장애를 허용한다.
- [ELR 공식 설명](https://kafka.apache.org/41/operations/eligible-leader-replicas/):4.1 신규 기본값과 strict min ISR·리더 후보 의미를 확인했다.
- [공식 topic 설정](https://kafka.apache.org/41/configuration/topic-configs/): RF3/minISR2/acks=all 조합과 ISR 부족, consumer 가시성·보존 단위를 확인했다.

위 출처 조회일은2026-10-26이다. 로컬 측정 결과와 운영 배치 권고는 별개다.

## 앱의 시작 전 검사

`PIPELINE_KAFKA_DURABILITY=HA`로 채팅·DLQ·시청자 관측·알림 토픽에 같은 정책을 적용한다. 기본 LOCAL은 기존 RF1이다. 정책은 생성과 검사를 같이 소유하며 기존 토픽을 자동 수정하지 않는다.

| 계약 | LOCAL | HA |
|---|---:|---:|
| partition | 3 | 3 |
| 실제 replica 수 | 1 | 3 |
| min.insync.replicas | 1 | 2 |
| 시작 가능한 ISR | 1 이상 | 2 이상 |
| unclean.leader.election.enable | false | false |
| cleanup / timestamp | delete / CreateTime | delete / CreateTime |

retention.ms 기본86,400,000, partition별 retention.bytes 기본268,435,456, segment.bytes 기본33,554,432다. 각각 `PIPELINE_KAFKA_RETENTION_MS`, `PIPELINE_KAFKA_RETENTION_BYTES`, `PIPELINE_KAFKA_SEGMENT_BYTES`로 명시한다. 시간1분~30일, bytes는 segment 이상~1TiB, segment1MiB~1GiB의 유한 범위다. 생성 후에도 실제 값이 모두 일치해야 시작한다. 기본256MiB는 로컬 값이며 전체 방송 운영에 충분하다는 뜻이 아니다.

RF·설정 불일치는 실패하고 기존 값을 바꾸지 않는다. 토픽 조회의 미전파와 ISR/리더 준비만 공통10초 기한에서 기다린다. HA feature 조회10초·생성10초·준비10초·Admin 종료1초는 각각의 경계다. 이 시작 검사가 실행 내내 가용성을 보장하지 않으므로 운영 감시는 별도로 필요하다.

## RF1에서 RF3로 이행

앱의 HA 설정만 켜면 기존 RF1은 거부된다. 실제 replica 재배치가 먼저다. 원본 topicId와 partition 수, 현재 replica/ISR/minISR, committed offset과 end offset을 저장한다. 대상 broker3개와 rack 배치를 확인하고 명시적 재배치 JSON을 작성한다.

1. `kafka-reassign-partitions.sh --execute --reassignment-json-file <계획> --throttle <검토한 bytes/s>`로 replica를 늘린다. 기존 partition을 유지하며 partition 증설과 섞지 않는다.
2. `--verify`로 완료와 ISR3 회복을 확인한다. throttle 제거 여부도 검증한다.
3. `kafka-configs.sh`로 min.insync.replicas=2·unclean=false 및 보존 계약을 적용한다.
4. 기존·이후 합성 이벤트의 ACK·DB·offset·topicId 불변을 대조하고 앱을 HA 모드로 시작한다.
5. 잘못된 replica/rack 선택은 변경 전 명시 계획으로 되돌리되, 유일한 최신 replica를 제거하지 않는다. ISR1로 내리는 자동 우회는 없다.

[공식 replica 재배치와 RF 증가 절차](https://kafka.apache.org/41/operations/basic-kafka-operations/)를 따른다. 적용/검증은 운영자가 검토한 특정 topic·broker 목록에 한정한다.

## 용량·경보와 운영

- 보존 계획은 `이벤트/초 × 실제 직렬화 bytes × 보존초 × RF`에 segment/index·복제·재배치 여유를 더해 계산한다. hot partition의 비율로 개별 디스크/네트워크 상한도 확인한다. 압축률은 실측 전 가정값으로 적는다.
- partition3개에서 같은 group의4번째 consumer는 할당이 없을 수 있다. #11의 편향·공유DB 결과와 함께 판단하고 partition 증설 시 채널 key 배치·replay manifest가 바뀌는 계약을 따른다.
- offline partition>0, ISR<minISR, controller 과반 상실은 즉시 수집 중단/미확인 점검 대상이다. under-replicated partition 지속, 디스크70%/85%, lag의 시간 환산이 보존 여유를 잠식하는 경우 경고/중단 기준을 정한다. 이 비율은 초기 운영 제안이며 자동 증설 수치가 아니다.
- 정상 rolling은 한 번에 한 프로세스만 재시작하고 ISR3/quorum 정상과 소비 진행을 확인한 뒤 다음으로 넘어간다. 호스트·AZ 정비는 두 역할의 동시 손실로 취급하고 별도 결합 장애 절차와 승인 기준을 적용한다.
- controller quorum 상실은 metadata 변경을 막는다. 이미 알려진 leader로의 데이터 요청이 언제까지 이어지는지는 별도 관측하며 “즉시 모든 발행 중단”으로 가정하지 않는다.

## 리팩토링·검증 범위

KafkaTopicPolicy가 생성 설정과 시작 검사를 소유하고 공통 KafkaSupport가 시간 제한·Admin 수명을 관리한다. 각 기능은 동일 정책을 전달하며 독립적인 RF/retention 하드코딩을 만들지 않는다. 원본 토픽과 DLQ는 같은 partition 수를 유지한다.

로컬 검증은 broker1개 종료·복귀, broker2개 종료로 ISR1, controller1개와2개 종료를 서로 분리한다. 실행 소유 컨테이너·네트워크·topic/schema만 정리하고 공유 개발 Kafka나 다른 프로젝트는 중지하지 않는다. 명령·실제 결과·미검증 범위는 STATUS의 근거를 따른다. 외부 클러스터 생성·main 릴리스는 포함하지 않는다.
