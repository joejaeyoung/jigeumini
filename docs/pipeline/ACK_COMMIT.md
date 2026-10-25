# ACK 수준과 offset commit 경계

2026-10-26(KST) · #31 / F11 → T01/T10

## 실험 계약

`./scripts/ack-commit <새 결과.json>`은 합성 입력과 소유한6노드 Kafka 클러스터·별도 DB schema에서만 실행한다. 고정 seed20261026의 eventId와 실행별 eventTime 기준을 결과에 남긴다. Kafka4.1.2, RF3/minISR2/ELR1/unclean=false, Java21, PostgreSQL16.14다. 원천을 호출하거나 제품 consumer group을 변경하지 않는다.

commit 대조는 동일1건을 새 group/projection에 사용한다. 별도 JVM에서 실제 Spring Kafka container를 구동하고 DB 전·DB commit 직후·offset commit callback 직후 `Runtime.halt`한다. 종료 hook commit이 개입하지 않는다. AUTO는 enable.auto.commit=true/interval100ms, BATCH는 auto=false/BATCH, MANUAL은 auto=false/MANUAL이며 DB 성공 뒤 acknowledge한다. poll 시작/반환·DB 완료·manual 요청·offset callback을 기록한다. MANUAL_IMMEDIATE와는 다르다.

추가 AUTO_POLL_AHEAD는 DB 작업을 완료하지 않고 다음 poll로 넘어가는 **의도적으로 잘못된 순서**다. AUTO 전체가 항상 누락된다고 주장하기 위한 사례가 아니다. 위험 순서의 메커니즘 시연이며 자연 발생 확률을 측정하지 않는다.

ACK 대조는 0/1/all 모두 idempotence=false·retries0·client in-flight1로 맞춰 ACK 정책만 비교한다. 제품의 idempotence=true·재시도 성능 측정이 아니다. 동일20건 정상 입력 뒤 두 broker를 내려 ISR1로 만들고 각5건을 발행한다. 마지막 ISR leader도 내리고 나머지 둘을 복귀시켜 실제 leader/ISR/ELR을 관측한 뒤 원래 디스크의 broker를 복구한다. API 성공·broker 확인·최종 broker UUID 집합·DB ledger/bucket을 구분한다. ACK0 offset -1을 확인 응답으로 세지 않는다.

시간·메모리·디스크 한도와 UUID 프로젝트/소유 label 정리는 #29 [HA 하네스](KAFKA_HA.md)를 재사용한다. 자식 JVM35초 한도·강제정리, producer/consumer/Admin 대기 한도가 있다. 장애 관측은 checkpoint에 쓰며 실패도 correct=false와 정리 결과를 보존한다.

## 실제 결과

`472908f43a4735ba85bb1d207771a80385a9cbeb`, UTC2026-10-25 16:49:04~16:50:48. [원본2회·정제 요약](../poc/evidence/ack-commit-2026-10-26/summary.json).

| 종료 경계 | AUTO/BATCH/MANUAL 각각의 DB/offset | 재시작 후 각 DB | 해석 |
|---|---|---:|---|
| DB 전 | 0/0 | 1 | 재전달 후 반영 |
| DB 직후 | 1/0 | 1 | 다시 소비하되 멱등 ledger로 중복 가산 없음 |
| offset 직후 | 1/1 | 1 | 재전달 불필요 |
| AUTO_POLL_AHEAD offset 직후 | 0/1 | 0 | 의도한 위험 순서에서1건 누락 |

정상9사례 누락0·집계 중복0, 위험 순서1사례 누락1을 기대대로 관측했다. AUTO의 offset callback은 세 번째 poll 시작 중·두 번째 poll 반환 후였다. BATCH/MANUAL은 두 번째 poll의 동기 listener 처리 이후 commit했다. MANUAL acknowledge는 DB 처리 뒤 요청됐으며, 이를 요청 순간 즉시 broker commit이라고 해석하지 않는다.

| ACK 수준 | 정상20 API 성공 / broker 확인 | ISR1의5건 API 성공 / broker 확인 | 원래 leader 복구 후 고유 broker/DB |
|---|---:|---:|---:|
| 0 | 20/0 | 5/0 | 25/25 |
| 1 | 20/20 | 5/5 | 25/25 |
| all | 20/20 | 0/0 | 20/20 |

마지막 ISR leader가 없을 때 세 topic 모두 leader=-1/ISR빈목록/ELR와lastKnownELR=[1]이었다. 다른 두 broker를 켰다고 자동으로 쓰기 가능해지지 않았다. 원래 디스크를 복귀시키자 모든 정상20건과 ACK0/1의 장애5건이 남았다. **이번 실행에서 손실0은 ACK1의 유일한 replica 디스크 영구 손실 내구성을 입증하지 않는다.** ACK0도 실제로 남았다는 관측만 있으며 API 확인 보장은 없다. all은 ISR1에서5건 모두 실패했다. 최종 broker/DB 집합 차이는0이다.

## 첫 실패와 리팩토링

첫 dba0474 실행은 commit10사례와 정상/ISR1 ACK 대조까지 통과했으나 마지막 checkpoint 이후 IllegalStateException으로 종료했다. 당시 결과는 IllegalStateException만 남아 정확한 메시지/내부 실패 지점은 확정하지 않는다. 마지막 checkpoint가 가리킨 관측 경계를 재검토해 쓰기 가능을 가정하지 않고 leader/ELR을 기록하도록 바꿨다. 두 실행 모두 소유 컨테이너·볼륨·schema 정리가 완료됐다.

리팩토링 점검에서 소유 자원 수명은 LocalKafkaCluster, commit 장애는 별도 JVM, 실험 진행/대조는 Session에 유지했다. 중단 쪽은 설정을 맞춘 테스트용 container이며 error handler/rebalance listener까지 생산 클래스와 같지는 않다. 재시작 대조는 실제 ProjectionConsumer를 사용했다. 대조 producer만 모든 ACK 수준에 동일 조건을 주입했다. 새로운 범용 장애 프레임워크는 만들지 않았다.

## 결정·공식 근거·미검증

제품은 auto=false, BATCH·동기 commit, DB 멱등 ledger, acks=all/idempotence=true를 유지한다. 단순 MANUAL 전환은 DB와 Kafka를 하나의 트랜잭션으로 만들지 않는다. HA는 별도로 RF3/minISR2·ELR 및 장애 도메인이 필요하다.

2026-10-26 확인한 공식 자료:

- [Spring Kafka commit 처리](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html): BATCH/MANUAL과 auto commit의 처리 주체·시점을 기준으로 실험했다.
- [KafkaConsumer 계약](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html): auto commit에서 다음 poll/close 전에 현재 poll 결과를 처리해야 하는 경계를 위험 사례와 연결했다.
- [Producer ACK 설정](https://kafka.apache.org/41/configuration/producer-configs/): ACK0/1/all 및 idempotence 제약을 구분했다.
- [ELR 계약](https://kafka.apache.org/41/operations/eligible-leader-replicas/): minISR와 선출 가능 후보를 확인하며 last ISR 상실의 결과를 일반적인 RF 수만으로 추정하지 않았다.

단일 호스트의 프로세스 중단/복귀·소량 합성 입력이다. 실제 AZ 상실·디스크 영구 손상·전원 장애·장기 부하·시계 점프·모든 client 버전의 동작은 미검증이다. DB 처리 지연/재할당 장기 부하는 [#11 검증](EXPERIMENTS_VERIFICATION.md)의 별도 범위를 참조한다. 최종 리뷰·CI는 STATUS에서 관리한다.

## 최신 통합 코드 재실행

49a3834의 clean tree에서 세 번째 실행을 수행해 전체13사례와 정리가 다시 통과했다. Java21.0.11/PostgreSQL16.14 및 실제 topic의 retention/minISR/unclean 설정을 원본에 추가했다. broker 물리/고유/DB는25/25/25,25/25/25,20/20/20이며 성공 입력별 부재0·실패 입력의 보존0을 집합으로 대조했다. 마지막 leader 부재 관측은15.048초이며 `docker stop --timeout 0`의 비정상 정지 조건이다. 정상 종료와의 차이나 관측 창 이후의 선출을 이 실험으로 확정하지 않는다.

검토 HEAD와 이전 실험 코드의 차이를 해소하기 위해 승인된 #27 발행과 #29 전체 분석기 정책 수정까지 통합한 뒤 재실행했다. 첫 두 실행은 당시 증거로 유지한다.
