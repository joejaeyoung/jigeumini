# 수집기 시계 오차·독립 네트워크 단절·오래된 owner 재개 조사

2026-10-26(KST) · #44 · F13 → T02/T11/T14

조사 기준 코드: `bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434`. Java 21·PostgreSQL 16 공식 문서와 저장소 구현을 확인했다. 아래는 **구현 전 기준점의 시험 설계**이며 현재 코드 상태나 실행 결과가 아니다. 이후 Clock 전달·proxy·발행 계측 구현과 실제 결과는 [검증 문서](../fleet/FAILURE_BOUNDARIES_VERIFICATION.md)를 따른다.

## 1. 먼저 구분해야 하는 계약

1. 현재 임대의 DB 만료는 PostgreSQL의 `clock_timestamp()`, 로컬 발행 허용 시간은 같은 JVM의 `System.nanoTime()`으로 판정한다. 수집기 벽시계 오차만으로 DB 만료를 이동시키는 구조는 아니다.
2. 원천 메시지의 시각 검증과 보고서에는 벽시계가 쓰인다. 현재 `ChatProtocol`은 `msgTime > receivedAt + 5초`를 `INVALID_TIME`으로 버린다. **receipt Clock을 -5분 이동하면 현재 시각의 정상 합성 메시지도 거절된다.** 임대 시험 성공을 전체 수집의 시계 오차 내성으로 확대하면 안 된다.
3. DB owner/epoch 조건과 로컬 permit은 오래된 owner의 새로운 수락·전송을 제한한다. 마지막 permit 확인을 이미 통과한 호출, Kafka client에 넘긴 입력, 늦은 ACK callback까지 취소하는 broker fencing은 아니다.

구현 근거: [CollectorLeases](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorLeases.java), [LeaseAuthority](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/LeaseAuthority.java), [ChatProtocol](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatProtocol.java), [BufferedKafkaPublisher](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/pipeline/BufferedKafkaPublisher.java).

## 2. 현재 코드와 기존 증거의 범위

| 대상 | 확인한 구현 | 시험에 필요한 보완 |
|---|---|---|
| `CollectorLeases` | 이 저장소에는 `LeaseRepository`가 없다. 이 클래스가 plan 행 잠금, DB 시각, owner/epoch 조건부 갱신·반환을 소유 | 추측한 저장소 추상화를 만들기보다 현재 DB 경계를 기준으로 장애 주입 |
| `LeaseAuthority` | package-private `LongSupplier` 생성자, 기본 `System::nanoTime`; 갱신 응답 지연을 차감하고 만료·epoch 변경·revoke 뒤 복귀 거부 | 기존 단위 seam을 유지. 요청 시작과 authority가 동일 시간원을 사용하게 연결 |
| `CollectorWorker` | 내부에서 `new LeaseAuthority()` 고정 생성. `tick` 요청 시작은 `System.nanoTime`; 시작·완료는 `Instant.now` | 현재는 자식 JVM에서 Clock 객체를 만들어도 동작에 연결되지 않음 |
| `ChannelProbe`·`ChatConnection`·`ProbeStats` | 연결·재시도·heartbeat 기한은 경과 시간, receipt·관측·시작 보고는 직접 `Instant.now` | 실제 receipt까지 Clock을 전달해야 시계 오차 시험이 됨 |
| `FleetWorkerProcess` | 실제 자식 JVM, loopback 합성 WS, TTL 6초, worker 90초, publisher 30/20/4, producer/report 지연 fixture | Clock 오프셋·변경 명령, phase 장벽, per-event 전송/ACK 계측 추가 제안 |
| `FleetIntegrationTest` | 실제 DB/Kafka, 두 JVM, SIGSTOP/CONT, DB TCP socket 단절, 사후 broker→DB 집합 대조 | WS 단절, 시계 오차, 오래된 프로세스가 살아 있는 동시 DB takeover, ACK 단계 구분 부족 |

코드 근거: [CollectorWorker](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorWorker.java), [ChannelProbe](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChannelProbe.java), [ChatConnection](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatConnection.java), [ProbeStats](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ProbeStats.java), [자식 JVM](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/test/java/io/chatpulse/fleet/FleetWorkerProcess.java), [기존 통합 시험](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/test/java/io/chatpulse/fleet/FleetIntegrationTest.java).

기존 증거를 보완할 때 특히 다음을 남긴다.

- SIGSTOP 시험은 old session의 `receivedAt >= 부모의 resumedAt`을 비교한다. 서로 다른 오프셋을 가진 JVM에서는 인과관계 판정으로 사용할 수 없다. -5분이면 재개 후 입력이 누락되고 +5분이면 재개 전 입력까지 포함될 수 있다.
- DB 단절 시험은 첫 worker가 종료한 뒤 두 번째 worker를 시작한다. 두 프로세스가 동시에 살아 있는 상태의 인계를 증명하지 않는다.
- 기존 DB proxy는 listener와 현재 socket을 닫는 즉시 실패 주입이며, 복구·blackhole 기능은 없다. listener만 닫아서는 pool의 기존 연결이 끊어지지 않는다는 점은 현재 구현이 처리한다.
- 기존 합성 WS는 접속 집합만 보유한다. worker별 연결, 장애 뒤에도 전달된 frame/heartbeat, phase별 메시지 식별은 추가 계측이 필요하다.
- 기존 대조는 `accepted = acknowledged + unconfirmed`, broker 레코드 수 범위, broker event ID 집합과 DB ledger 일치다. ACK된 개별 ID 목록은 없다. DB projection은 수집 종료 후 시작하므로 장애 중 집계가 계속 진행했다고 주장할 수 없다.
- `stats.messages`는 decode 후 sink 수락 전에 증가한다. 원천 frame 수·유효 decode 수·publisher 수락 수·ACK 수를 같은 숫자로 취급하지 않는다.

기존 문서: [fleet 처리 계약](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/docs/fleet/README.md), [fleet 검증 범위](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/docs/fleet/VERIFICATION.md).

## 3. 공식 시간 API와 현재 임대 판정

### Java의 벽시계와 경과 시간

`Instant.now()`는 system UTC clock을 고정 사용한다. `Instant.now(clock)`처럼 호출 경로가 주입된 clock을 읽어야 대체 시간이 적용된다. [Java 21 Instant](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Instant.html#now(java.time.Clock))

`Clock.offset(base, duration)`은 base의 instant에 양수·음수 duration을 더한다. 반환 객체는 불변이므로 실행 중 점프는 thread-safe clock의 offset을 원자적으로 바꾸거나 참조를 교체해 구현한다. 시간대는 instant를 날짜·시간으로 해석하는 정보이므로 `-Duser.timezone`만 바꾸는 것으로 ±5분 오차를 주입할 수 없다. [Java 21 Clock](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Clock.html)

`System.nanoTime()`은 벽시계와 무관한 경과 시간용이며 기준점은 JVM마다 다를 수 있다. 같은 JVM의 두 값 차이만 비교한다. 부모와 자식의 raw nanoTime을 빼지 않는다. API는 나노초 단위 해상도나 서로 다른 호스트의 동일한 진행 속도를 보장하지 않는다. [Java 21 System.nanoTime](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#nanoTime())

이로부터 이번 Clock 주입의 범위는 **주입을 소비하는 애플리케이션의 벽시계 경로**라고 판단한다. OS 시간, DB 서버 시간, Kafka client 내부 시간, native socket timer, NTP slew, suspend 중 시간 진행, VM별 경과 시간의 속도 오차는 변경하지 않는다. 이 항목들을 실제로 시험했다고 기록하지 않는다.

### PostgreSQL 시각과 잠금

`now()`·`CURRENT_TIMESTAMP`·`transaction_timestamp()`는 transaction 시작 시각이다. `statement_timestamp()`는 최신 command 수신 시각이고 `clock_timestamp()`는 호출 당시 시각이므로 같은 SQL 안에서도 달라질 수 있다. [PostgreSQL 16 현재 시각](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)

현재 코드는 `collector_plan ... FOR UPDATE` 잠금을 얻은 **뒤** `clock_timestamp()`를 한 번 읽어 그 tick의 판정에 재사용한다. transaction 시작 시각으로 바꾸면 잠금 대기 전의 시간을 쓸 수 있으므로 동일한 변경이 아니다. `FOR UPDATE`는 같은 행의 다른 writer/locker를 transaction 종료까지 기다리게 하지만 WS·Kafka 요청까지 묶지 않는다. [PostgreSQL 16 행 잠금](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS), [현재 SQL 순서](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorLeases.java)

현재 산식은 다음과 같다. `n0`는 DB 요청 **전** 같은 worker의 nanoTime, `d`는 DB에서 읽은 시각, `E`는 DB expiry다.

```text
localDeadline = n0 + max(0, E - d - 2초)
로컬 permit 유효 = 현재 nanoTime < localDeadline
DB 갱신 유효 = owner/epoch 일치 ∧ valid_until > DB now ∧ selected/policy 일치
```

수집기 벽시계 오프셋은 이 식에 들어가지 않는다. DB 응답 지연은 이미 `n0`부터 흐른 시간으로 차감된다. 응답이 너무 늦거나 기존 permit이 만료됐다면 같은 epoch의 늦은 Grant도 권한을 되살리지 않는다. DB가 commit했지만 응답을 받지 못한 경우는 DB lease가 잠시 남고 worker가 멈추는 상태가 가능하므로, 실패 응답만 보고 DB rollback이라고 단정하지 않는다. 이는 코드에서 도출한 시험 가설이다. [LeaseAuthority.update](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/LeaseAuthority.java)

DB wall clock의 큰 전진은 로컬 deadline이 남았는데 DB lease가 만료된 상황을, 후퇴는 takeover 지연을 만들 수 있다는 것이 위 식에서 도출되는 위험이다. 수집기 Clock 주입은 이를 검증하지 않는다. DB 시각 함수 문서도 단조 증가를 보장하지 않는다. DB clock 조작은 별도 격리 시험 과제로 남긴다. [PostgreSQL 시각 의미](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)

## 4. 최소 시험 seam 제안

아래는 구현 후보이며 현재 코드에 존재하는 기능으로 취급하지 않는다.

1. `CollectorWorker`에서 wall `Clock`을 받아 시작·완료, `ProbeStats`, `ChannelProbe`를 거쳐 `ChatConnection`의 receipt·관측 시각까지 전달한다. 기본 경로는 `Clock.systemUTC()`로 유지한다. 부모 fixture가 만든 오프셋을 보고서에만 쓰는 방식은 제외한다.
2. 필요하다면 worker의 DB request start와 `LeaseAuthority`에 같은 `LongSupplier`를 연결한다. 하나만 fake 시간원으로 바꾸지 않는다. 실제 두 JVM 시험에서는 기본 `System::nanoTime`을 유지하고 wall clock만 바꾼다. 단조 시간의 속도 오차·점프는 별도 단위 모델 시험이다.
3. 자식 JVM fixture에 초기 offset ±300초와 `phaseId → offsetSeconds` 명령 장벽을 둔다. 전환 완료 응답은 phase 번호, 실제 주입 clock의 instant, 자식 내부 elapsed만 기록한다. 런타임 임의 시간 변경을 제품 CLI에 노출할 필요는 없다.
4. DB proxy와 WS proxy를 worker별 loopback endpoint로 분리한다. DB는 PostgreSQL로, WS는 합성 서버로만 전달한다. `CUT`은 기존·신규 연결을 모두 차단하고 `HEAL`은 신규 연결을 다시 허용한다. `BLACKHOLE`은 연결 종료 없이 전달을 멈추며 버퍼는 유한해야 한다. 패킷·SQL·WS payload를 로그로 남기지 않는다.
5. `PublishingAuthority.Permit` 또는 dispatcher의 최종 check 직전 장벽과 producer callback wrapper를 시험에만 연결한다. 장벽 대기 중 authority/publisher lock을 쥐지 않는다. 실제 Kafka 사용은 유지한다.

### 시계와 무관한 phase 식별

부모가 `phaseId`, 합성 source sequence, connection 번호를 발급한다. 자식은 event ID·collection session ID와 이 식별자의 관계만 보고한다. 현재 이벤트 schema를 바꾸지 않으려면 부모가 지정한 **서로 다른 합성 `msgTime`**을 marker로 쓰고 `(합성 channel, msgTime)`으로 phase를 대응시킬 수 있다. 실제 원천 message ID나 텍스트를 수집할 필요가 없다. 동일 source marker를 두 세션이 읽어 생성한 서로 다른 event ID도 별도로 세므로 중복 수신을 숨기지 않는다.

임대 격리 사례의 marker 시각은 예컨대 부모 시작 시각보다 10분 전으로 정해 양쪽 ±5분 receipt의 프로토콜 범위 안에 둔다. 이는 **의도적으로 과거 시각을 넣은 합성 live frame**이며 최근 이력 요청이 아니다. 이 방법만으로 현재 원천 시각 처리도 정상이라고 주장하지 않는다. 별도 사례에서는 부모 현재 시각을 그대로 보내 -5분 worker의 `INVALID_TIME` 증가와 미발행을 확인한다. +5분 worker에서는 decode 성공 여부, received/source 차이, late 계수를 확인한다. `receivedAt`이 Kafka record timestamp에도 사용된다는 영향은 별도 관측값으로 남긴다. [ChatProtocol](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatProtocol.java), [ChatEvent](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatEvent.java), [발행 timestamp](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/pipeline/BufferedKafkaPublisher.java)

`FleetPlan.from`의 비합성 selection freshness도 호출자 벽시계에 의존한다. 이미 적용된 SYNTHETIC plan으로 worker를 시험하는 결과는 이 activation 경로를 포함하지 않는다. [FleetPlan](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/FleetPlan.java)

## 5. 실제 두 프로세스 시험 순서

### A. 반대 방향 벽시계와 실행 중 점프

1. 소유 schema/topic, 합성 채널 두 개, capacity 1인 자식 A/B를 준비한다. A는 +300초, B는 -300초. DB·Kafka·부모 시각은 바꾸지 않는다.
2. lease 소유권 분리와 각 process의 실제 receipt offset을 관측한다. 임대 격리 marker를 보내 두 worker의 ACK와 DB 반영을 확인한다.
3. 두 번 이상의 DB 갱신을 확인한 뒤 A를 -300초, B를 +300초로 바꾼다. 각각 -600/+600초의 점프다. 장벽 응답 뒤 DB expiry의 추가 갱신, 같은 owner/epoch, 새 marker의 ACK를 확인한다.
4. 부모의 현재 source timestamp 사례를 따로 실행해 프로토콜의 앞선 timestamp 거부를 확인한다. 정상 입력 없는 worker의 단순 생존만으로 수집 정상이라고 판정하지 않는다.
5. grace 종료 및 인계는 부모의 phase 순서와 DB 관측으로 판정한다. 시작/완료 wall timestamp가 역전될 수 있으므로 그 차이로 timeout이나 성공을 판정하지 않는다.

**기대 계약:** 주입 clock을 사용하는 벽시계는 실제로 달라지고 lease 갱신·epoch는 벽시계 점프 때문에 변경되지 않는다. 현재 source timestamp가 -5분 receipt에서 거절되는 현상은 현재 처리 계약의 관측 결과이며, 이 조사에서 임의로 허용 창을 넓히자는 결론은 내리지 않는다.

### B. DB만 단절하고 원천 WS 경로는 유지

1. A의 JDBC만 소유 DB proxy를 통과시킨다. A의 WS는 별도 정상 proxy, Kafka는 정상 경로다. B도 미리 실행해 같은 채널을 대기하게 한다. 부모는 장애 없는 직접 DB 관측 연결을 사용한다.
2. A가 연결되고 DB renew를 확인한 후 DB proxy의 기존·신규 socket을 끊는다. 즉시 전후 합성 WS 전달 카운터와 정상 control 연결을 관측하여 DB 장애가 source 경로 자체를 끊지 않았음을 확인한다.
3. A의 신규 수락/전송 거부, socket의 협력 종료, 실패 코드, 마지막 DB valid_until을 기록한다. source socket은 worker가 스스로 닫으므로 장애 뒤 영구히 열려 있어야 한다고 assert하지 않는다.
4. B가 **실제 DB expiry 뒤** epoch를 증가시켜 획득하는 것을 기다린다. 테스트 SQL로 `valid_until`을 과거로 덮어쓰지 않는다. A 종료 전·후와 B 획득 시점을 별도로 기록한다.
5. DB proxy를 복구해도 종료/revoke된 같은 A가 자동 재획득하지 않는지 확인한다. owner/epoch가 옛 값인 직접 renewal은 실패하고, 옛 release 뒤에도 B의 owner/epoch/valid_until이 유지되는지 실제 SQL 결과로 확인한다.

**추가 blackhole 사례:** A의 DB TCP만 종료 없이 멈추고 WS는 계속 전달한다. `tick`은 worker lock을 잡은 채 JDBC를 기다리므로 main loop와 `close`도 지연될 수 있다. 반면 ChannelProbe와 publisher permit은 별도 경로에서 local deadline을 확인한다. 따라서 **새 전송 차단 시점과 프로세스/report 종료 시점을 분리**해서 측정한다. 현재 pool 연결 대기 3초, PG socketTimeout 10초, SQL timeout과 transaction timeout을 포함한 실제 시간을 기록하며 TTL 6초만으로 종료 상한을 주장하지 않는다. [CollectorWorker](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorWorker.java), [PostgresSupport](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/db/PostgresSupport.java)

DB proxy가 마지막 renewal의 commit 응답만 숨긴 경우에는 부모 DB 관측으로 실제 expiry 갱신 여부를 기록한다. post-commit Grant 전달을 코드 장벽으로 늦추는 시험은 응답 지연 모델이며 실제 네트워크에서 commit 응답을 잃은 시험과 구분한다. delayed Grant가 도착해도 만료된 local authority가 되살아나지 않아야 한다.

### C. WS만 단절하고 DB는 유지

1. A의 synthetic WS proxy만 끊고 DB proxy와 Kafka는 정상으로 둔다. B는 같은 채널 대기 상태로 둔다.
2. A가 source 실패/재시도를 관측하는 동안 owner/epoch가 같고 DB expiry가 계속 갱신되는지 확인한다. DB의 `connected=false`는 source 상태 heartbeat 반영 결과이며 물리 TCP 상태의 즉시 측정으로 취급하지 않는다.
3. 재시도 예산 안에서 WS proxy를 복구한다. 새 collection session으로 다시 연결되고 같은 owner/epoch에서 marker 발행이 재개돼야 한다. DB lease가 계속 유효하면 B가 채널을 가져가면 안 된다.
4. source 중단 구간의 합성 marker 유실과 회복 뒤 marker를 구분한다. 원천 재전송/최근 이력 기능이 없으므로 gap이 0이라고 요구하지 않는다.

현재 기본 heartbeat는 ping 20초·pong timeout 10초, source 연결 재시도는 최대 6회와 backoff다. WS blackhole은 단순 socket-close보다 감지가 늦다. 이를 검증한다면 DB renew를 두 번 이상 확인하고 timeout→재연결을 기다리는 유한 사례로 분리한다. source 재시도가 끝나면 worker 전체가 종료되고 실패 release에 30초 retry delay가 붙는다. **WS 단절 즉시 다른 worker가 인계한다는 계약은 없다.** 현재 자식 duration 90초·plan 120초와 재시도/handshake 대기 예산을 확인하여 duration 종료를 재시도 소진으로 오인하지 않는다. [ChannelProbe](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChannelProbe.java), [ChatConnection](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatConnection.java), [release retry delay](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorLeases.java)

이 시험은 WS 경로만 다룬다. 실제 치지직의 HTTP room/token 조회, DNS·TLS·인증·공식 API 권한까지 시험한 것으로 표시하지 않는다.

### D. 오래된 owner를 멈췄다가 재개

1. 오프셋이 있는 A의 세션과 ACK marker를 기록하고 SIGSTOP한다. B를 미리 대기시킨 뒤 DB TTL에 의한 owner 변경과 epoch 증가를 관측한다.
2. B takeover 뒤에만 발급하는 별도 source marker를 A의 열려 있는 socket에도 보낸다. 이는 A가 멈춘 동안 OS/socket에 남는 입력을 재개 후 받는 사례다.
3. A를 SIGCONT하고 위 marker가 old session의 새 Kafka 입력으로 나타나지 않는지 검사한다. 부모 `resumedAt`과 child `receivedAt` 비교는 사용하지 않는다.
4. A의 로컬 권한 만료/실패 종료, 옛 owner/epoch의 DB renewal 실패, 옛 release가 B의 lease를 바꾸지 않는 것을 기록한다.
5. A가 멈춘 동안 물리 TCP 연결이 남아 B와 겹칠 수 있다. `physicalSourceConnections`, `databaseValidLeases`, `admittedEvents`를 따로 기록한다. 이 현상을 곧바로 유효 임대 중복으로 판정하지 않는다.

이 결과도 A가 마지막 check를 이미 통과한 입력까지 모두 사라진다는 뜻은 아니다. 아래 단계별 실험으로 구분해야 한다.

## 6. 임대 상실과 이미 진행 중인 ACK를 구분하는 실험

현재 publisher는 admission에서 permit을 캡처하고 queue에서 꺼낸 뒤 `permit.check()`를 다시 호출한다. 그 다음 `kafka.send`와 callback 사이에는 authority 검사가 없다. 성공 callback은 `finished` 이전에 처리되면 ACK 계수를 증가시킨다. [발행 구현](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/pipeline/BufferedKafkaPublisher.java)

| 단계 | 재현 장벽 | 반드시 볼 결과 | 확대하면 안 되는 주장 |
|---|---|---|---|
| Q: 수락했지만 최종 permit 확인 전 | 지정 합성 event를 queue에 둔 뒤 check 직전에 장벽; lease 만료/인계 뒤 해제 | 옛 permit 거부, 실제 Producer.send 호출 0, broker marker 0, 수락했으나 미확인 계수 | 모든 ACK가 상실 순간 취소됨 |
| R: 최종 permit 통과 후 실제 send 전 | producer wrapper 진입에서 delegate.send 직전 대기; 다른 스레드의 만료/revoke·인계 뒤 delegate 호출 | 현재 코드에서는 옛 epoch 입력의 ACK가 가능함; event ID·호출 순서·broker offset 기록 | 이 사례를 broker에서 이미 inflight였다고 부름 |
| I: 실제 Kafka 성공 후 애플리케이션 callback 전달 지연 | 실제 producer callback 결과를 보관하고 publisher 쪽 callback만 지연; broker readback 확인→lease 상실→callback 해제 | broker 입력이 상실 전에 존재했고 ACK 관측은 뒤에 발생했음을 입증 | broker가 상실 뒤 append했다거나 네트워크 ACK 패킷 손실을 재현했다고 부름 |

Q 장벽은 producer wrapper만으로 만들 수 없다. wrapper 진입 시점에는 최종 permit check가 끝났기 때문이다. `Permit.check` 또는 dispatcher 직전의 별도 seam이 필요하다. R은 이미 문서화된 check→send 경쟁 창의 재현이다. I의 callback wrapper는 Kafka I/O thread를 장벽에서 막지 않고 결과만 보관한 뒤 별도 executor에서 callback을 전달하는 방식이 적합하다.

ACK callback 해제는 publisher의 최대 10초 drain 관측 경계 안에서 수행한다. `finished` 뒤 callback은 최종 ACK 계수에 반영되지 않으므로 broker 존재와 `unconfirmed`를 함께 기록한다. B를 미리 준비해 startup 때문에 이 경계를 놓치지 않도록 한다. 장벽 만료는 시험 실패로 남기며 성공 조건을 맞추려고 lease를 강제 갱신하지 않는다.

실제 client 내부에 요청이 남아 있는 채 네트워크 단절/복구를 겪는 사례까지 요구한다면 Kafka protocol/advertised listener 경로를 포함한 별도 계측이 필요하다. 위 I만 실행하고 이를 완전히 검증했다고 쓰지 않는다. 현재 maxInFlight 기본 64, 이 fixture는 4이므로 실제 `sending`·queue 최고치와 해당 event들의 단계 분포를 기록한다. **임대 상실 뒤 관측한 ACK 수가 0이어야 한다는 전역 assert는 현재 계약과 맞지 않는다.**

## 7. 정제 증거 형식과 판정

최소 결과에는 다음 값을 포함한다. 모든 식별자는 합성 입력 또는 실행이 발급한 UUID/숫자만 사용한다.

- 실행일, code SHA, Java/DB/Kafka 버전, fixture명, 두 child PID/owner, 합성 channel 수, TTL·safety margin·queue/inflight 한도.
- 각 child의 초기/변경 offset, Clock 전환 phase 응답, phase별 실제 receipt offset 확인값. 각 프로세스 내부 elapsed와 부모 phase 순서를 기록하고 서로 다른 JVM nanoTime은 직접 비교하지 않는다.
- DB 관측의 owner/epoch/valid_until/databaseNow, renew 성공·거절, release 전후 새 owner 유지, DB 단절 종류와 cut/heal 순서.
- proxy별 기존/신규 connection 수와 전달 byte 수, 합성 WS frame·pong 수, 물리 source connection 수. payload·SQL·인증 값은 기록하지 않는다.
- source marker → session/event ID, 수락/거절/실제 send/실제 producer 결과/publisher ACK 관측 단계, broker partition/offset, DB event ID 집합 대조.
- phase별 `INVALID_TIME`, decode 수, admitted/acknowledged/unconfirmed, source gap, 재연결 session 수, queue/inflight 최고치, 종료 코드·report 완료까지의 부모 elapsed.

기존 개수 등식과 함께 **ACK event ID 집합 ⊆ broker event ID 집합 = DB ledger event ID 집합**을 확인한다. 미확인 입력은 broker에 존재할 수도 있으므로 broker 집합을 ACK 집합과 같다고 강제하지 않는다. 동일 source marker를 여러 세션이 읽어 서로 다른 event ID를 만든 경우도 별도 계수로 남긴다. 현재 sourceMessageId는 null이고 eventId는 수신 시 새 UUID이므로 DB ledger 일치가 원천 중복 제거를 입증하지 않는다. [ChatEvent](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/poc/ChatEvent.java)

`CollectorLeases.status`는 여러 `clock_timestamp()` 호출을 포함하므로 만료 경계에서 모든 숫자가 정확히 같은 시각을 뜻한다고 가정하지 않는다. 합격 판정에는 부모의 별도 관측 쿼리에서 한 DB 시각을 캡처하여 owner/epoch/expiry와 함께 저장하는 방식을 권한다. `connected`도 최근 heartbeat 상태라는 의미를 유지한다. [status 구현](https://github.com/joejaeyoung/jigeumini/blob/bc2ff5bc44a6809b52838507eb5bc2b6b4ba6434/server/src/main/java/io/chatpulse/fleet/CollectorLeases.java), [같은 SQL 안에서 달라지는 clock_timestamp](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)

## 8. 실행 경계와 완료 범위

fixture가 생성한 UUID schema/topic/group, loopback proxy, synthetic WS, 직접 시작한 child PID만 조작·정리한다. SIGSTOP 프로세스는 finally에서 SIGCONT한 뒤 정상 종료를 우선한다. 다른 worktree, 공유 DB 프로세스, 호스트 clock, OS 방화벽, 공개 원천 연결을 변경하지 않는다. 실제 원천 ID·채팅 원문·닉네임·사용자 ID·토큰은 읽거나 저장하지 않는다. live PoC 최대 공개 채널 3개 정책은 유지하며 이번 시험은 합성 원천만 사용한다.

추천 구현 순서는 **Clock 연결과 phase marker → DB/WS proxy 분리 → 두 JVM 시계 점프·각 단절 → SIGSTOP 재개 → Q/R/I 대조**다. 회귀 검증에는 Java 변경 확인과 실제 broker/DB integration을 포함하되 이 조사 파일은 실행 근거로 사용하지 않는다. docs에는 실제 실행 명령·결과와 함께 OS/DB 시계 변화, clock rate drift, broker fencing, source 무손실, 실제 치지직 연결은 검증하지 않았다고 남긴다.
