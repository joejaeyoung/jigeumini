# Kafka 디스크 유실·빈 디스크 복구 검증 #45

2026-10-26 · F15/F11 → T04/T10/T15 · [PR #56](https://github.com/joejaeyoung/jigeumini/pull/56).

## 환경·범위

clean `f992ef12a9e6cbe47b85ff6ca04fd352fd902469`에서 두 시나리오를 각각 새6노드 cluster로 순차 실행했다. Kafka4.1.2·controller3/broker3·RF3/minISR2·ELR1·unclean=false, Java21.0.11이다. 실제 image ID와 고정 probe class hash는 [정제 근거](../poc/evidence/kafka-disk-loss-2026-10-26/summary.json)에 있다. PostgreSQL은 개발 인스턴스의 실행별 UUID schema를 사용했다.

삭제 전 project/owner/service·mount·volume label·단독 container 참조를 확인하고, 정지한 실험 broker의 volume만 제거했다. 새 volume의 entry0·metadata없음을 확인한 뒤 같은 node/cluster ID로 포맷·복귀했다. readonly probe는 network=none·128MiB이며 record offset/leaderEpoch·CRC·key/value SHA256과 metadata ID만 남겼다. 원문·닉네임·사용자 ID·토큰을 보관하지 않았다. SSD 고장·전원 손실·bit 손상·다중 host/AZ는 이 시험 범위가 아니다.

## A: 복제본 하나 손실 뒤 복구

02:53:08.475~02:54:24.959 UTC, 7단계 통과.

1. 초기10건을 실제 ACK→broker/DB로 대조하고 follower 디스크에도 같은 offset/hash10건을 확인했다.
2. 해당 디스크를 영구 제거했다. 남은ISR2에서10건을 추가해 DB20건을 확인했다.
3. 빈 디스크 복귀 후ISR3을 확인하고10건을 추가했다. 복구된 디스크를 정지해 offset/hash30건·CRC·같은node/cluster ID·새directory ID를 검증했다.
4. follower2개를 내려ISR1을 만들었다. 실제 HA 정책은 NotReady, 시작 검사는TOPIC_VISIBILITY_DEADLINE이었다. 별도 retries0 제어 발행은NotEnoughReplicasException, 제품 발행은약10초 뒤TimeoutException으로 ACK하지 못했다.
5. ISR3으로 복귀하고2건을 추가했다. attempted34(제어 거부1·제품 미확인1 포함), ACK32·broker32·DB32·bucket32, ACK누락0·집합차이0·미확인 중 실제존재0이었다.

복구 성공은 유효한 다른 복제본이 남아 있는 이 조건에 한정한다. acks=all을 항상3개 디스크에 저장된다는 뜻으로 해석하지 않는다.

## B: 마지막 유효 복사본 손실

02:54:35.986~02:57:01.956 UTC, 6단계의 의도한 손실 경계 확인·정리 통과. **sourceRecoverySucceeded=false**다.

초기10건이 있는 follower S를 내려 오래된 디스크를 보존했다. 남은 두 broker에서tail10건을 ACK하고 DB20건과 삭제 전 replay manifest를 고정했다. 두 최신 디스크를 차례로 정지·검사하여 각각20건 offset/hash가 있음을 확인한 뒤 둘 다 영구 삭제했다. S에는초기10건만 있으며 tail의 물리복사본은0개였다. 원본 segment를 다른 복구 경로로 공급하지 않았다.

S만 복귀, 첫 빈 디스크 복귀, 모든broker복귀를 각각 관측했다. 최종broker1/2/3의 실제Running·OOM없음·Admin등록을 확인했으며 topicId는 유지됐다. 최종 관측은leader없음·ISR없음·ELR없음·lastKnownElr=[1]이었다. fullISR를60초 기다려도 준비되지 않았고 source capture와 기존manifest validate는둘다SOURCE_UNAVAILABLE로 거부했다.

DB의20건은 그대로 남았지만 Kafka source 복구 성공으로 표시하지 않았다. 읽을 수 없는 source의 실제건수·누락건수는null이다. 물리tail삭제10건과 조회로 읽은 broker건수는 구분한다. 정책을 낮추거나 unclean election·topic 재생성을 수행하지 않았다. 이 제한된 관측으로 이후에도 영원히 leader가 없다고 주장하지 않으며, OFFSET_TRUNCATED를 관측한 실행으로 바꾸지 않는다.

## 결정·리팩토링·재현

[공식 자료 조사](../research/KAFKA_DISK_LOSS.md)의 ELR/lastKnownElr·strict minISR·ID 계약을 실제 결과와 연결했다. 유효 복사본이 남으면 새 디스크로 재구성할 수 있지만, 마지막 최신 복사본을 없애면 metadata나 오래된 디스크만으로 ACK된 입력을 복원할 수 없다. 운영에서는 다중 장애영역과 별도 원천/backup 보존 여부를 함께 결정해야 한다.

LocalKafkaCluster의 기존 소유권 경계를 재사용하고 readonly disk probe·시나리오 판정을 분리했다. 코드 식별자를 위한 공통 framework는 추가하지 않았다. 진입 Claude43b3474 REQUEST_CHANGES를 반영한1ec8f61은APPROVE FOR EXPERIMENT였고, f992ef1은Gradle 인자 guard와로그만 추가했다. 실제 미실행과 실제결과를 구분했다. 최종 HEAD Claude·push/PR CI는별도 병합 게이트다.

```sh
./scripts/check
./scripts/kafka-disk-loss recover work/disk-recover.json
./scripts/kafka-disk-loss last-copy work/disk-last-copy.json
```

두 실행 모두 이번 실행의 container/volume/schema만 정리했다. 기존 개발 데이터·다른 서비스는 유지했다. 최종 source 불가 진단은 일반 metadata/API 실패와 구분하도록 고정 오류와 실제 broker 복귀 조건을 요구한다.
