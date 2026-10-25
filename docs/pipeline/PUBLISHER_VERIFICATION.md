# 유한 비동기 발행 검증

#27 / F14 → T02/T06/T10/T11/T12 · 실행2026-10-26(KST)

## 실제 입력·DB 대조

[정제 근거](../poc/evidence/publisher-2026-10-26/summary.json)와 같은 폴더의7개 원본을 보존한다. 합성20채널·seed20261025·단일Kafka4.1.2/RF1·PostgreSQL16.14·Java21.0.11, 공유 개발 호스트다. 정상5회는1c2c3ba, product-hot과 broker 장애는0791dc1에서 실행했다. 정상 성능치를 후속 예외 종료 보완 commit의 실측이라고 표시하지 않는다.

| 입력 | 수락/ACK | 거절/미확인 | DB unique | p95/p99 ms | 최대 pending/inflight | 최대 lag |
|---|---:|---:|---:|---:|---:|---:|
| baseline.json | 1440/1440 | 10/0 | 1440 | 5857.560/6099.478 | 미계측/미계측 | 5 |
| product-baseline.json | 15000/15000 | 0/0 | 15000 | 30.043/62.182 | 20/20 | 10 |
| product-burst.json | 120000/120000 | 0/0 | 120000 | 57.872/80.801 | 120/64 | 58 |
| product-control.json | 1500/1500 | 0/0 | 1500 | 14.372/25.863 | 4/4 | 0 |
| product-hot.json | 30300/30300 | 0/0 | 30000 | 276.490/317.437 | 81/64 | 290 |
| serial-control.json | 1500/1500 | 0/0 | 1500 | 34.783/42.257 | 미계측/미계측 | 3 |

전부 accepted=ACK+unconfirmed, DB 기대 bucket/replay 차이0, 소유 topic/group/schema 정리를 통과했다. 직렬500/s는목표미달이고 제품500/s는15,000건,2,000/s·60초는120,000건을 완료했다. hot30300건은300중복을 포함하며 DB30000건이다. 100/s 통제 입력은 양쪽1500건 전부 처리했다. records/request는 직렬1, 제품통제2.003·500/s10.007·burst40.013이며 Kafka client 이동창 rate와 전체 wall-clock 처리율을 구분한다.
## 실제 broker PAUSE/STOP
공유 broker는 유지하고 소유권 라벨이 붙은512MiB broker·데이터 볼륨을 따로 만들었다.6경우 모두수락/거절/미확인 계수·상한30·복구 broker offset 범위 대조가 통과했다. 종료 뒤 해당 컨테이너·명시적 데이터 볼륨이 없음을 확인했다.
- 제품 PAUSE: ACK31, 거절1, 미확인0, peak pending/inflight30/30. 용량 초과 전에 받은 큐는 복구 후 drain했다.
- 제품 STOP: ACK1, 거절1, 미확인30(queued29+failedUnconfirmed1), peak pending30. 첫20건의 admission0.963ms, 다음10건0.539ms, 전체사례14.831초. 미확인 oldest의 Instant JSON 직렬화도 실제 결과로 확인했다.
- 실험용 비동기 후보는 STOP 첫20건 admission이40.145초였고 callback 실패 후11건을 거절했다. 이 후보의 callback 안 send 구조는 제품으로 채택하지 않았다. 제품은 전용 dispatcher에서 send를 호출한다.
## 단위·통합·리팩토링
로컬단위120·하네스16·JAR, 1c2c3ba 실제Kafka/DB81이 통과했다. 단위15사례에서 채널별 한도/다른 채널 진행, FIFO/순환 배분, frame 원자 거절, 물리중복, callback 선행실패·동기실패·Error, blocked send 중 admission, 종료/늦은ACK/동시callback을 확인했다. 실제통합의 product-smoke는 두독립projection과중복/replay를 대조한다.
한 모듈에서 한도와 상태 전이를 소유한다. Kafka호출과join은 잠금 밖이며 진단map도1000채널로 유한하다. 물리sequence와eventId를 분리하고 frame 검사순서는결정적이다. 최종CI/Claude승인/병합 상태는[STATUS](../STATUS.md)에서 관리한다.
## 한계
공정성은 결정적 단위시험이며 실제 다중WebSocket SLA가 아니다. 유한메모리로 원천backpressure나JVM손실복원을 보장하지 않는다. 원천채팅 누락/중복·전체플랫폼용량·RF3는 별도다. 오래된#11 broker실험은 컨테이너만 정리 확인했고 익명볼륨 근거가 없다는 정정도 유지한다.
