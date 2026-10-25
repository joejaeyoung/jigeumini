# Kafka HA와 배포·복구 준비 조사

조사일: **2026-10-26 KST**. 코드 기준: `e1980f67dd7f58c135868a8b8b957906fa5f9783`.

이 문서는 공식 문서 조사와 코드 읽기의 결과다. 실제 Kafka HA·외부 배포·백업 복구를 실행한 증거가 아니다. 조사 중 Docker 실행, 인프라 변경, 외부 자원 생성, Git 변경을 하지 않았다. 구현·실험은 별도 이슈와 정제 근거로 완료를 판정한다. 기존 공개 채널 live 최대 3개 규칙은 유지한다.

## 1. 결정 제안

1. **배포 준비, Kafka 복제 장애 시험, PostgreSQL 복구, 수집 확장**을 별도 이슈로 만든다. 로컬에서 실제 컨테이너와 합성 입력으로 검증할 수 있는 부분부터 끝낸다.
2. 운영 Kafka의 기본 후보는 **3 controller + 3 broker, RF=3, minISR=2, acks=all**이다. 역할별 독립 프로세스와 최소 3개 장애 영역을 계획한다. 6개 프로세스가 반드시 6대 VM이라는 뜻은 아니지만, 역할별 자원 격리와 장애 영역 배치를 비용과 함께 결정해야 한다. 공식 KRaft 문서는 중요한 배포에서 역할 분리와 3개 이상 controller를 권고한다. [Kafka 4.1 KRaft](https://kafka.apache.org/41/operations/kraft/)
3. 처음부터 Kubernetes를 선택할 근거는 아직 없다. **고정 이미지의 웹·서버 실행, 준비 상태, DB migration, 종료, 이전 이미지 복귀**를 검증하는 공급자 독립 배포 묶음을 우선 만든다. 단일 호스트 Compose는 이 절차의 리허설 범위로 명명한다. 이는 현재 코드와 사용자 요구에 대한 설계 제안이다.
4. 처리량·채널 수·월 비용·RTO/RPO를 확정하지 않는다. #11의 합성 입력 실측, 실제 채널별 입력 분포, 장애 복구 측정, 선택한 공급자 견적을 결합해 운영 상한을 정한다. 팔로워 수를 Kafka 처리량의 직접 대리값으로 사용하지 않는다.

## 2. 현재 코드에서 확인한 배포 경계

아래는 구현된 사실이며, 설계 문서에만 있는 기능과 구분했다.

| 영역 | 현재 구현 | 다음 구현에서 확인할 점 |
|---|---|---|
| 인프라 | Kafka `4.1.2` 단일 combined node, RF1 내부 토픽 설정, PostgreSQL `16.14-alpine`, loopback 포트, named volume | 호스트·디스크 장애 HA 없음. 로컬 계정·PLAINTEXT 설정을 외부 환경에 그대로 사용하지 않음 |
| 토픽 생성 | `KafkaSupport.ensureTopic`이 3 partitions, RF1, delete, CreateTime, 24시간·partition당 256MiB, 32MiB segment를 새 토픽에 요청 | 기존 토픽은 아래 3절의 일부 속성만 검사하므로 실제 운영 정책 검사 필요 |
| Producer | acks=all, idempotence=true, 연결당 in-flight=1, delivery timeout 10초, client buffer 8MiB | 애플리케이션 worker가 한 건 ACK를 기다리는 직렬 방식. HA 복구가 10초를 넘으면 앱은 실패할 수 있음 |
| 수집 버퍼 | ACK 대기 포함 최대 1,000건, frame 단위 수락/거절, 실패 시 중단, 종료 drain 10초 | 재시작한다고 메모리의 미확인 입력이 복원되지 않음. async 후보 채택은 #11 결과와 실패 시험에 연결 |
| DB·프로세스 | collector/analyzer가 DB를 사용하며 `openMigrated`로 시작 시 migration. pool 최대 4개 | 별도 migration 작업·실행 계정 분리 필요. 이 코드 기준이며 #16 변경은 통합 시 재확인 |
| 조회·분석 | analyzer JVM에 API, 여러 consumer와 평가 scheduler가 함께 실행 | API 인스턴스만 늘리려면 조회 실행 역할을 분리하거나 중복 scheduler/공유 DB 비용을 먼저 검증 |
| HTTP | 스크립트가 analyzer를 `127.0.0.1:18080`에 바인딩 | 컨테이너 내부 서비스 바인딩은 별도 설정 필요. 외부 공개는 gateway를 통해 필요한 경로만 제공 |
| 웹 | React/Vite의 dist 빌드와 개발·preview 프록시 | 운영 정적 서버와 `/api` 프록시, SPA 직접 경로, 캐시 정책 필요 |
| 관측 | 애플리케이션 상태·복구 진단 API 있음. Actuator 의존성은 없음 | 아키텍처 문서의 Actuator/Micrometer는 현재 구현 완료가 아님 |
| 수집 중복 방지 | 같은 작업 경로의 lock을 이용한 PoC 중복 차단 | 다른 호스트의 동일 채널 수집을 막는 lease/fencing은 별도 구현 대상 |

코드 근거: [compose.yaml](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/compose.yaml), [KafkaSupport](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/KafkaSupport.java), [BufferedKafkaPublisher](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/BufferedKafkaPublisher.java), [PipelineConfiguration](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/PipelineConfiguration.java), [PostgresSupport](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/db/PostgresSupport.java), [실행 스크립트](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/scripts/pipeline), [서버 의존성](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/build.gradle), [README의 범위](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/README.md).

## 3. 토픽 설정은 생성 요청과 실제 상태를 나눠 관리한다

현재 저장소에는 `TopicSupport`라는 클래스 대신 `KafkaSupport.ensureTopic`이 있다. 기존 토픽을 만나면 `TopicExistsException`을 허용하고, 실제 **CreateTime, cleanup.policy=delete, partition 수=3**만 확인한다. RF, replica 배치, minISR, retention 시간·용량, segment, unclean election, ISR/leader 상태는 확인하지 않는다. 따라서 메서드가 성공해도 요청한 보존·복제 계약이 모두 적용됐다고 판단할 수 없다. [현재 구현](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/KafkaSupport.java)

**구현 제안:** 하나의 토픽 정책 객체에서 기대 partition/RF/minISR/retention/timestamp/cleanup을 정의하고, 관리 작업은 생성·변경을, 실행 프로세스는 읽기 전용 검사만 담당한다. 부팅 때 RF를 자동 변경하지 않는다. 잘못된 영구 설정은 안전한 오류 코드로 거부하고, 일시적 ISR 감소는 운영 상태로 표시한다. HA 중 degraded 시작 허용 조건과 완전 정상 상태를 분리해 불필요한 재시작 반복을 피한다. chat·observation·spike·DLQ 토픽과 내부 offset 토픽을 모두 검사 범위에 포함한다.

기존 토픽의 RF를 늘리려면 replica reassignment가 필요하다. `default.replication.factor`나 생성 코드만 바꿔서는 기존 RF1 토픽 전환을 입증할 수 없다. 내부 `__consumer_offsets`도 생성 시 RF 설정과 실제 배치를 따로 확인해야 한다. [Kafka 토픽 reassignment](https://kafka.apache.org/41/operations/basic-kafka-operations/#increasing-replication-factor), [Kafka 내부 토픽 설정](https://kafka.apache.org/41/configuration/broker-configs/#offsets.topic.replication.factor)

## 4. Kafka HA 계약

### 복제·quorum·장애 영역

- **Controller quorum과 데이터 복제는 별개다.** controller 3개는 1개 동시 실패에 대응하는 과반수를 구성한다. 이것만으로 데이터 토픽의 RF가 높아지는 것은 아니다. combined 모드는 broker와 controller를 독립적으로 재시작·확장하기 어렵다. [Kafka KRaft 배포 고려 사항](https://kafka.apache.org/41/operations/kraft/)
- 운영 후보의 broker는 서로 다른 장애 영역에 replica를 두고 `broker.rack`에 영역을 표현한다. Kafka의 rack 배치는 가능한 서로 다른 rack에 replica를 배치한다. 단일 호스트 컨테이너 3개는 호스트·디스크·전원·영역 장애를 분산하지 못한다. [Kafka rack 배치](https://kafka.apache.org/41/operations/basic-kafka-operations/#balancing-replicas-across-racks)
- **RF3/minISR2/acks=all은 정상 ISR3일 때 3개 ISR의 ACK를 기다린다.** 2개만 기다리라는 설정이 아니다. ISR2이면 두 복제본으로 계속 쓸 수 있고, ISR1이면 성공 ACK를 내지 않는 계약으로 검증한다. minISR 부족은 append 전후 오류로 나타날 수 있어 실패를 “broker에 절대 저장되지 않음”으로 환산하지 않는다. [Kafka topic minISR](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)
- `acks=1`은 leader만 확인하므로 follower 복제 전에 leader가 사라지는 실패 경계가 있다. idempotence는 client 재시도를 다루며 PostgreSQL 반영이나 원천에서 받은 모든 채팅의 보존을 의미하지 않는다. [Kafka producer ACK·idempotence](https://kafka.apache.org/41/configuration/producer-configs/#acks)
- `unclean.leader.election.enable=false`를 정책에 명시한다. ISR 밖 replica를 무조건 leader로 선택하면 데이터 손실이 가능하다. 단, **Kafka 4.1의 ELR는 안전성을 판정한 별도 후보**이므로 ELR 선출을 unclean 선출과 혼동하지 않는다. 신규 4.1 클러스터는 ELR가 기본 활성화된다. 시험 근거에 `metadata.version`, `kraft.version`, `eligible.leader.replicas.version`을 기록한다. [Unclean election](https://kafka.apache.org/41/configuration/topic-configs/#unclean.leader.election.enable), [Kafka 4.1 ELR](https://kafka.apache.org/41/operations/eligible-leader-replicas/)
- `__consumer_offsets`는 RF3, transaction topic을 사용하면 RF3/minISR2를 설정·확인한다. 현재 producer의 idempotence 활성화는 `transactional.id` 기반 Kafka transaction을 사용한다는 뜻이 아니다. [Kafka 내부 토픽 설정](https://kafka.apache.org/41/configuration/broker-configs/), [현재 producer](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/KafkaSupport.java)

### 로컬 시험이 입증할 수 있는 범위

아래는 **시험 설계**이며 아직 실행하지 않았다.

| 시나리오 | 구성·실패 주입 | 기대 검증 |
|---|---|---|
| 최소 smoke | 3 combined 중 leader 보유 node 1개 종료 | leader 변경·ISR3→2, 제한 시간 안의 새 ACK, ACK한 ID의 최종 조회·DB 반영 일치, 재기동 후 ISR3 |
| minISR 부족 | 3 controller + 3 broker에서 controller는 유지하고 broker 2개 중단 | controller quorum 정상과 ISR1을 동시에 관측. 새 성공 ACK 차단, 미확인 입력 계수, broker 복구 후 정합성 |
| controller 장애 | broker 3개는 유지하고 active controller 종료 | metadata leader 변경과 가용 시간 측정. 실제 데이터 손실·중복 대조 |
| quorum 상실 | broker는 유지하고 controller 2개 중단 | metadata 변경/선출 가능 범위와 클라이언트 오류를 관측. 기존 partition IO가 반드시 즉시 모두 중단된다고 가정하지 않음 |
| 순차 재시작 | 한 번에 node 1개만, 매번 ISR·quorum 정상 복귀 대기 | rolling 절차 완료·ACK/DB 보존 관계·회복 시간 |
| RF1→3 | 별도 시험 토픽에 seed 입력 후 replica reassignment | topic identity·offset·입력 불변, 실제 replicas/ISR3, 기존 manifest 재검증 |
| volume 보존 | 시험 프로세스 정지·같은 owned volume 재기동 | 토픽·group 진행·DB 집계 유지. volume 삭제 시험은 별도 자원에서만 |

**3 combined 중 2개를 끄면 broker 2개와 controller 2개를 함께 잃는다.** 이 결과만으로 minISR 차단 효과를 분리해 설명할 수 없다. 두 원인을 분리하려면 위처럼 역할별 프로세스를 두거나 데이터 복제 연결만 차단하는 별도 시험 설계가 필요하다. 본 조사에서는 원인 분리가 명확한 6개 프로세스 구성을 제안한다. 이는 KRaft·복제 계약에 따른 시험 설계 판단이다. [KRaft 역할](https://kafka.apache.org/41/operations/kraft/), [minISR](https://kafka.apache.org/41/configuration/topic-configs/#min.insync.replicas)

시험 하네스는 기존 chat-pulse 인프라와 다른 프로젝트명·포트·volume을 쓰고, 합성 입력만 발행하며, 실행 시간·입력 수·디스크·메모리 한도를 둔다. container ID·topic ID·feature·실제 ISR·ACK/미확인/DB 건수·runtime SHA를 정제해서 남긴다. 기존 volume을 재포맷하거나 기존 공개 입력을 다른 클러스터로 옮기는 동작은 이 시험에 포함하지 않는다.

## 5. RF1 PoC에서 전환하는 방법

두 경로 중 하나를 명시적으로 선택한다. 아래는 운영 전환 계획이며 실행 결과가 아니다.

### 같은 클러스터에서 데이터 유지

1. cluster/topic identity, partition별 log 시작·끝, consumer committed offset, DB progress, 토픽 설정을 기록한다.
2. broker를 추가한 뒤 replica reassignment 계획과 되돌릴 배치를 저장한다. 이동률 제한과 완료 검사를 사용해 부하를 통제한다.
3. 데이터·DLQ·알림·관측·내부 offset 토픽의 RF3와 ISR3를 확인한 뒤 minISR2 정책을 적용한다. 기존 RF1에서 minISR2만 먼저 설정하면 쓰기 차단 상태가 된다.
4. controller 단일점 해소는 별도 단계다. 현재 Compose는 `controller.quorum.voters`를 사용하는 static 구성이다. Kafka 4.1의 static→dynamic 절차와 feature 상태를 확인한 뒤 멤버 변경을 리허설한다. `voters` 문자열에 주소만 추가하고 기존 디스크를 다시 format하는 절차로 대체하지 않는다.

공식 근거: [replica 재배치·throttle·verify](https://kafka.apache.org/41/operations/basic-kafka-operations/#increasing-replication-factor), [KRaft static→dynamic와 controller 추가](https://kafka.apache.org/41/operations/kraft/#upgrade).

### 새 클러스터에서 새 수집 구간 시작

개인 PoC에서 과거 Kafka 로그를 계속 운영할 필요가 없다면 더 단순한 후보는 새 클러스터를 먼저 검증하고, 기존 수집을 종료·drain한 다음 새 수집 구간을 시작하는 것이다. 같은 채널을 양쪽 수집기에 동시에 연결하는 방식은 현재 내부 UUID 중복 제거로 방지되지 않는다. 전환 공백을 품질에 표시하고 기존 환경을 읽기 전용으로 남기는 방식을 제안한다.

새 클러스터의 동일 이름 토픽은 기존 topic identity·offset 의미가 같다고 가정할 수 없다. 현재 replay는 topic UUID와 partition 구성을 검사하므로 새 manifest를 capture해야 한다. 기존 manifest를 편집해서 통과시키거나 서로 다른 로그의 offset을 그대로 이식하지 않는다. 과거 데이터 이동이 필요하면 별도의 이관·중복·offset 매핑 시험을 추가한다. [현재 ReplaySource](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/replay/ReplaySource.java)

## 6. 데이터 증가와 Kafka 용량 판단

### 먼저 측정할 값

선정 채널 수와 함께 채널별 이벤트/초의 평상시·p95·최대 burst, 직렬화 후 record 크기, 실제 저장 bytes, 압축률, partition별 입력률·DB 처리율, producer ACK 지연·미확인 버퍼 나이, replay 처리율을 기록한다. 팔로워 수가 같아도 입력률은 다를 수 있으므로, 최종 수집 상한은 이 실측과 원천 연결 허용 범위를 함께 사용한다. 이는 이 서비스의 용량 계획 제안이다.

간단한 초기 계산식은 다음과 같다. **견적용 추정식이며 측정값을 대신하지 않는다.**

```text
논리 입력 bytes/초 ≈ 총 events/초 × 평균 직렬화 bytes/event
저장 요구량 ≈ 실제 압축 후 bytes/초 × 보존 초 × RF + index/segment/운영 여유
broker 간 복제 전송량의 초기 추정 ≈ 논리 입력률 × (RF - 1)
회복 시간의 초기 추정 ≈ backlog / (회복 중 처리율 - 새 입력률)
```

마지막 식은 처리율이 새 입력률보다 클 때만 유효하다. 압축·재전송·여러 consumer group의 fetch·네트워크 프로토콜 비용·편중을 따로 측정한다. RF3의 비용과 단일 노드 RF1 부하 결과를 같은 환경으로 비교하지 않는다.

현재 `retention.bytes=256MiB`는 **partition별 한도**이고 `retention.ms`와 함께 작동한다. 크기 한도가 먼저 적용되면 24시간 전부가 남지 않는다. segment 단위 삭제와 hot partition을 고려해 최소 실제 보존 시간을 관측한다. [Kafka retention 설정](https://kafka.apache.org/41/configuration/topic-configs/#retention.bytes)

partition 증가는 기존 입력을 다시 분산하지 않으며 key→partition 매핑을 바꿀 수 있다. 현재 코드의 정확히 3개 검사와 replay manifest의 partition 검사가 변경을 거부하므로, 자동 증설 전에 토픽 계약·집계 검증·전환 방식을 정해야 한다. [Kafka partition 변경](https://kafka.apache.org/41/operations/basic-kafka-operations/#modifying-topics), [현재 검사](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/KafkaSupport.java)

### 운영 지표와 판단

Kafka 공식 지표에는 `UnderReplicatedPartitions`, `UnderMinIsrPartitionCount`, `AtMinIsrPartitionCount`, leader election, request 시간, broker bytes, producer queue/batch/request, consumer lag, KRaft leader·high watermark 등이 있다. remote JMX는 기본 비활성이고 운영에서 사용할 때 접근 보호가 필요하다. [Kafka monitoring](https://kafka.apache.org/41/operations/monitoring/)

이를 서비스 지표에 결합한다. 아래 임계 조건은 운영 초안이며 시간 임계값은 실험 결과로 정한다.

| 관측 조합 | 판단·행동 제안 |
|---|---|
| ISR가 minISR보다 작음 | 쓰기 가용성·미확인 발행 경보, 새 채널 추가 중지, replica 복구 확인 |
| ISR가 minISR와 같음 | 다음 장애를 견딜 여유가 감소한 상태. rolling 작업 진행 보류 |
| Kafka lag 낮음 + collector 미확인 버퍼 나이 증가 | producer/연결 ACK 경로 확인. consumer 증설부터 하지 않음 |
| 한 partition만 lag 증가 | 채널 편중·SQL workload 확인. consumer 추가만으로 해결한다고 가정하지 않음 |
| 모든 partition lag 증가 + DB pool 대기·transaction 지연 | DB 병목 확인. consumer 증설로 DB 경합을 키우는지 검사 |
| log 시작 offset이 미처리/복구 시작 위치에 접근 | 입력 보존 위험. 복구에 필요한 보존 기간·용량 조정과 수집 예산 검토 |
| active channel·lease 대비 healthy 수집 비율 하락 | 원천 연결 제한/재접속/담당 인수인계와 broker 문제를 구분 |

PostgreSQL은 `pg_stat_activity`, `pg_stat_database`, `pg_stat_io`, `pg_stat_wal`, `pg_stat_replication`, `pg_stat_archiver` 등으로 DB IO·복제·보관 상태를 볼 수 있다. 앱의 pool 대기·처리 시간·조회 freshness와 함께 판단한다. [PostgreSQL 16 관측 통계](https://www.postgresql.org/docs/16/monitoring-stats.html)

## 7. 배포 묶음과 검증 절차

### 실행 단위와 이미지

**구현 제안:** 같은 서버 이미지에 collector/analyzer/migrate 역할을 명시하고, 웹은 dist 산출물을 정적 서버에서 제공한다. collector fleet이 구현되기 전에는 기존 유한 실행·최대 3채널 계약을 유지한다. analyzer를 여러 개 띄우는 변경은 scheduler·DB 부하·group 동작 검증을 선행한다.

다단계 빌드, 불필요한 파일 제외, 비 root 사용자, 이미지 digest 고정으로 리뷰한 산출물을 식별한다. SHA 태그만 기록하는 것보다 실제 digest·JAR SHA·Git SHA를 함께 남기는 편이 실행본 대조에 적합하다. digest 고정 후에도 업데이트 절차는 필요하다. [Docker build 권고](https://docs.docker.com/build/building/best-practices/)

Spring Gradle plugin의 `bootBuildImage`도 OCI 이미지를 만들 수 있고 비 root 실행을 지원한다. 저장소에서는 Dockerfile 다단계 빌드와 이 방식 중 하나를 선택해 실제 local image를 검증하면 된다. 조사한 공식 4.0 Gradle 문서 표시는 4.0.8이므로 프로젝트 4.0.9 실제 빌드 결과를 근거로 남긴다. [Spring Boot 4.0 OCI 빌드](https://docs.spring.io/spring-boot/4.0/gradle-plugin/packaging-oci-image.html)

`vite preview`는 로컬 빌드 확인용이며 운영 서버가 아니다. 운영 묶음은 정적 파일 제공, SPA fallback, `/api` 전달을 실제 웹 경로 테스트로 검증한다. [Vite 정적 배포](https://vite.dev/guide/static-deploy.html)

### 네트워크·설정

- gateway만 외부 HTTP(S)를 수신하도록 구성하고 Kafka·PostgreSQL·운영 진단은 내부 접근으로 제한하는 구조를 제안한다. Kafka는 각 client가 접근 가능한 `advertised.listeners`가 필요하므로 bootstrap 주소만 바꿔서는 충분하지 않다. [Kafka listener 설정](https://kafka.apache.org/41/configuration/broker-configs/#advertised.listeners)
- 외부 운영 후보에는 TLS, client 인증, topic/group 권한을 포함한다. 현재 Kafka PLAINTEXT는 loopback 개발 환경으로 한정한다. [Kafka 보안 기능](https://kafka.apache.org/41/security/security-overview/)
- secrets는 이미지·Git·정제 근거에 넣지 않는다. Compose는 서비스별 파일 mount로 secret을 전달할 수 있으므로 앱도 파일 기반 설정을 받을 수 있게 한다. Compose secret 자체를 클라우드 secret 관리·암호화 저장과 같은 보장으로 설명하지 않는다. [Docker Compose secrets](https://docs.docker.com/compose/how-tos/use-secrets/)

### 준비 상태와 종료

Spring Boot는 liveness/readiness를 분리하며, liveness에 공용 DB·Kafka 장애를 직접 연결하면 전체 프로세스를 반복 재시작할 수 있다. main HTTP port의 `/livez`, `/readyz`도 제공할 수 있다. readiness에 어떤 외부 의존성을 넣을지는 서비스 동작 기준으로 결정해야 한다. [Spring Boot 4.0 probes](https://docs.spring.io/spring-boot/4.0/reference/actuator/endpoints.html#actuator.endpoints.kubernetes-probes)

**제안:** 조회 서버는 schema 호환성과 실제 API serving을 확인한다. 데이터가 오래됨·consumer blocked·live 원천 미접속은 별도 품질/운영 상태로 드러낸다. 모든 일시 지연을 process 사망으로 처리하지 않는다. 수집기와 analyzer의 역할별 readiness를 정의하고, topic list·`pg_isready` 성공만으로 전체 파이프라인 정상이라고 표시하지 않는다.

Compose `depends_on`은 기본적으로 프로세스 시작만 기다린다. `service_healthy`와 `service_completed_successfully`를 사용하면 dependency health와 migration 작업 성공을 구분할 수 있다. 이 순서 제어는 실행 중 장애 복구를 대체하지 않는다. [Compose 시작 순서](https://docs.docker.com/compose/how-tos/startup-order/)

Boot 4.0의 web graceful shutdown은 기본 활성이고 종료 phase timeout을 설정할 수 있다. Compose는 기본 10초 후 강제 종료할 수 있으므로, 현재 producer drain 10초·consumer 종료·DB 정리까지 포함한 **실측 종료 예산보다 큰** stop grace를 설정한다. 숫자를 임의로 확정하지 말고 SIGTERM 후 새 수신 중지→ACK drain→마지막 DB/offset 정리→종료 순서를 검증한다. [Spring 종료](https://docs.spring.io/spring-boot/4.0/reference/web/graceful-shutdown.html), [Compose 종료 유예](https://docs.docker.com/reference/compose-file/services/#stop_grace_period)

### Migration와 rollback

아래는 프로젝트 적용 제안이다. 현재 구현은 시작 중 Flyway migrate를 수행한다. [현재 DB 초기화](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/db/PostgresSupport.java)

1. immutable image와 schema 호환 범위를 manifest에 기록한다.
2. 백업 완료·복구 가능성, migration dry run 또는 복제 DB 적용 결과를 확인한다.
3. 독립 migration 작업 하나가 새 schema를 준비하고, runtime은 schema를 검사한다. migration 계정과 runtime 권한을 나눈다.
4. 새 이미지에서 readiness·합성 smoke를 통과한 뒤 traffic을 전환한다.
5. 이전 이미지가 새 schema와 함께 동작하는 범위만 이미지 rollback으로 허용한다. destructive migration은 별도 forward fix/복구 절차가 필요하다.
6. 합성 입력을 계속 흘리면서 이전 이미지→새 이미지→이전 이미지의 API·DB·offset·중복을 대조한다. Docker image 교체 성공만으로 데이터 rollback 성공을 주장하지 않는다.

## 8. PostgreSQL 복구는 Kafka offset과 함께 시험한다

PostgreSQL은 SQL dump, 파일 백업, 연속 WAL 보관을 서로 다른 복구 방식으로 설명한다. `pg_dump`/`pg_restore`는 저장 시점의 DB를 다시 만드는 방법이다. PITR은 base backup과 이후 WAL이 필요하다. 단순 named volume 보존은 다른 호스트의 백업이나 특정 시점 복구 시험을 대신하지 않는다. [PostgreSQL 백업](https://www.postgresql.org/docs/16/backup.html), [SQL dump](https://www.postgresql.org/docs/16/backup-dump.html), [pg_restore](https://www.postgresql.org/docs/16/app-pgrestore.html), [PITR](https://www.postgresql.org/docs/16/continuous-archiving.html)

현재 consumer는 DB 반영 뒤 Kafka offset을 커밋하며, 재할당 때 Kafka committed offset으로 시작한다. **DB만 과거 백업으로 되돌리면 Kafka offset은 더 앞선 채 남아, 복원 DB에 없는 입력을 건너뛸 수 있다.** 이는 현재 코드의 저장 경계에서 도출한 복구 위험이다. 현행 replay는 별도 namespace용이므로 replay 성공을 운영 DB 복구 완료로 간주하지 않는다. [ProjectionConsumer](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/pipeline/ProjectionConsumer.java), [ReplaySource](https://github.com/joejaeyoung/jigeumini/blob/e1980f67dd7f58c135868a8b8b957906fa5f9783/server/src/main/java/io/chatpulse/replay/ReplaySource.java)

**별도 복구 이슈의 완료 조건 제안:**

- 합성 topic·DB·group으로 백업 시점 B와 더 진행한 시점 C를 만든다.
- B의 DB를 새 대상에 복원하고 Kafka committed=C 상태에서 자동 정상 시작을 거부하거나 차이를 명확히 진단한다.
- consumer 중지 상태에서 복원 DB progress와 보존된 Kafka offsets를 검사하고, 승인된 복구 계획의 시작 위치로만 다시 읽는다. 자동 latest/earliest reset으로 덮지 않는다.
- 보존 입력이 남아 있으면 ledger/bucket/알림·관측·카테고리·리포트 결과를 기대값과 비교한다. 알림 outbox 재전달도 멱등 경계를 확인한다.
- 보존 범위 밖이면 완전 복구를 실패로 판정하고 공백을 남긴다.
- dump checksum·schema 버전·복구 시작/끝·실제 RPO/RTO·권한·정제 결과를 기록한다. 실제 운영 데이터가 아닌 fixture만 공유한다.

PostgreSQL streaming replication은 기본 비동기이므로 failover 때 최근 commit이 사라질 수 있다. 동기 replication은 standby 확인을 기다려 지연·가용성 비용이 달라진다. 운영 DB를 관리형 standby로 고르더라도 이 설정과 offset 복구 계약을 확인해야 한다. [PostgreSQL 16 standby·동기 복제](https://www.postgresql.org/docs/16/warm-standby.html)

## 9. 이슈로 나눌 수 있는 구현 단위

번호는 실제 이슈 생성 때 연결한다. 모든 이슈에 기술 ID·failure case·검증 명령·정제 근거·리팩토링 점검·작업 로그를 요구한다. 새 기술 ID는 기존 T01~T12를 유지한 채 추가한다.

| 제안 이슈 | 완성되는 수직 경로 | 의존성·기술 연결 | 완료 근거 |
|---|---|---|---|
| 토픽 정책과 Kafka HA 장애 검증 | 앱 정책 검사→RF3 cluster→합성 발행→node 장애→DB/ACK 대조 | T02/T03/T04/T10, HA 관련 신규 T. #11의 계측 재사용 | 1 broker/2 broker/controller 장애 분리, 실제 ISR·feature·복구 시간, RF1 전환 검사 |
| 제한된 비동기 발행의 제품 적용 | 합성/fixture 수신→bounded async→실제 broker→동일 DB 결과 | #11 결과 후, T02/T06/T11/T12 | accepted/ACK/rejected/unconfirmed 보존, frame 전체 정책, 채널 공정성, drain·실패·heap 비교 |
| 이미지 배포·schema 적용·이전 버전 복귀 | 서버/웹 이미지→migration→readiness→API→SIGTERM→이전 이미지 | #16의 build identity·명시적 migrate 활용, 배포 관련 신규 T | 고정 digest, 비밀 미노출, container smoke·웹 직접 경로·DB 실패·rollback·종료 검증 |
| DB 백업과 Kafka 위치를 맞춘 복구 | 합성 입력→DB backup→추가 진행→새 DB restore→controlled replay→최종 집계 | T01/T02/T03/T04, 복구 관련 신규 T | DB 과거/Kafka 미래 불일치 감지, 보존 만료 실패, 복구 결과·RPO/RTO |
| 처리 예산·상태 관측·수집 확장 조건 | 원천/selector 예산→collector buffer→broker→consumer→DB 상태 연결 | 수집 범위·lease 이슈와 #11, T07/T08/T09/T11 | partition별 부하·ISR·buffer age·복구 예산을 한 결과로 확인, 추가 수집 중지/재개 결정 |

수집 채널 선택·lease/fencing은 원천 접근 범위 조사와 연결된 별도 이슈로 유지한다. 전체 방송 수집 가능 여부나 팔로워 임계값은 이 Kafka 조사만으로 확정하지 않는다. 웹·배포·HA local 시험은 합성 입력으로 진행 가능하다.

## 10. 외부 배포 직전에 필요한 최소 사용자 선택

위 로컬 구현·리허설을 끝내는 데 다음 선택은 필요하지 않다. 실제 비용이 생기는 외부 환경을 만들기 전, 결과와 예상 자원표를 보여준 뒤 묶어서 확인하면 된다.

- 사용할 공급자/계정·지역과 월 예산 상한.
- 공개 서비스 범위·도메인 및 실제 배포 승인.
- 단일 영역의 저비용 실험인지, 3개 장애 영역과 DB standby를 포함한 운영 HA인지.
- 허용 RPO/RTO와 원천 수집 범위/동의·접근 조건.

이 문서는 공급자 견적을 조사하지 않았고 월 비용·적정 VM 사양·전체 채널 수를 확정하지 않았다. 실제 외부 배포와 다중 장애 영역 실험을 하지 않은 상태에서 해당 항목을 완료로 기록하지 않는다.
