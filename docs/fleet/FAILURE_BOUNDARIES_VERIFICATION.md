# 수집기 시계·분리 장애·발행 경계 검증 #44

2026-10-26 · F13 → T02/T11/T14 · [PR #58](https://github.com/joejaeyoung/jigeumini/pull/58).

## 범위와 계측

실제 Java 수집기2개, 합성 WebSocket 서버, Kafka4.1.2와 PostgreSQL16.14를 사용했다. 워커의 wall Clock을 주입하되 DB clock_timestamp와 System.nanoTime 기반 임대 권한은 기존 계약을 유지한다. OS/NTP 자체를 바꾼 실험은 아니다. 원천과 DB 경로에 별도 TCP proxy를 두고 전달/폐기 byte, DB 임대와 epoch, 고정 marker의 Kafka/DB/DLQ 귀속을 확인한다.

GitHub Actions의 `94da3ad`에서 push40260027568·PR40260031297의 Fleet14개가 각각143.817/143.001초에 통과했다. 전체 push는성공, PR은 기존 Outbox 시험1개 실패였다. #52/#54 반영 후 최종 HEAD의 리뷰·CI는 별도다. [정제 근거](../poc/evidence/collector-boundaries-2026-10-26/summary.json)에 두 실행과 XML/JSON hash를 기록한다. 아래 두 프로세스 표의 수치(인계6.082초·DB32byte·원천100byte·backend52회/366행 포함)는 [94da3ad push 실행](../poc/evidence/collector-boundaries-2026-10-26/push.json)의 값이다. integrated-local.json의 실행 값과 합산하지 않는다.

통합 코드 `34e824e`의 로컬 `./scripts/check`는 단위152·하네스24·JAR, 실제 Kafka/DB Fleet20개는 143.251초에 실패0으로 통과했다. #54 시작 복구/유한 실패와 정상 인계5회, #44 경계7개를 함께 실행했다. [통합 근거](../poc/evidence/collector-boundaries-2026-10-26/integrated-local.json)는 기존 CI14개와 별도 실행으로 보존한다. Standards/Spec 검토 범위는 `420efa3..34e824e`이며 각각 지적0·차단0이다.

## 실제 두 프로세스 결과

| 조건 | 관측 | 집합 대조 |
|---|---|---|
| 시계+300/-300초, 실행 중-600/+600초 점프 | 각 워커의 적용 응답 뒤 DB 임대 owner/epoch1 유지·2초 이상 갱신. 실제 수신시각 차이는±약300,000ms. 현재 시각 marker 하나는 INVALID_TIME, 과거 marker는 양쪽 수락 | accepted/ACK/broker5, 정상DB2·격리3, 미확인0 |
| A의 DB blackhole, 원천 연결 유지, B 대기 | DB32byte 폐기·원천100byte 전달. 6.082초 뒤 B epoch2 인계 시 A 프로세스 생존. A의 인계 후 marker0 | accepted/ACK/broker/DB3, 미확인0 |
| WebSocket만 CUT, DB 유지 | 동일owner/epoch1 갱신, 별도 source session으로 재연결. 단절 중 합성 marker1건은 복원0 | accepted/ACK/broker/DB2, 미확인0 |
| 시계가 앞선 A를 SIGSTOP한 뒤 B 인계, A 재개 | marker 시점 물리socket2개, broker marker1개, 이전session의 인계 후 marker0 | accepted/ACK/broker2, 정상DB1·격리1, 미확인0 |

미래 receipt로 들어간 일부 이벤트는 기존 live retention 정책에 따라 `OUTSIDE_LIVE_RETENTION`으로 격리된다. 정상 ledger만 보고 누락으로 계산하지 않고 broker source offset 전체를 정상 ledger와 격리 기록의 합집합에 대조한다. 시계 시험이 집계 정확한 시각이나 신뢰 가능한 wall timestamp를 보장하는 것은 아니다.

DB blackhole의 인계 후 이전 session marker0은 무권한 발행이 없었다는 관측이다. 해당 순간에 이미 소켓이 닫혔는지와 permit 거부를 이 사례만으로 구별하지 않는다. 최종 permit 검사 자체는 별도 producer barrier 사례에서 확인한다.

DB blackhole 중 backend52회·366행을 표본으로 읽었고 transactionOpen1행·blocking0행이었다. 연결을 임의의 SQL 한가운데에서 차단해 plan row lock을 확정적으로 유지한 시험은 아니다. 실제 DB 서버 다운이나 네트워크 패킷 재정렬과도 구분한다. 두 번의 CI 성공을 가능한 모든 스케줄의 증명으로 확대하지 않는다.

## 마지막 permit 검사와 broker ACK의 경계

다음3개는 실제 DB 임대 만료·epoch2 취득과 실제 Kafka producer를 사용하지만, 두 JVM 수집기 시험과 별도로 같은 JVM의 barrier로 정확한 경합 지점을 고정한다.

| 정지 위치 | 인계 전 broker | 해제 후 accepted/ACK/unconfirmed | 최종 broker/DB |
|---|---|---|---|
| 큐 수락 후 마지막 permit 검사 전 | 0 | 1/0/1, PUBLISH_AUTHORITY_LOST | 0/0 |
| 마지막 permit 통과 후 send 직전 | 0 | 1/1/0 | 1/1 |
| broker ACK 후 앱 callback 전달 전 | 1 | 1/1/0 | 1/1 |

모든 경우 실제 producer sent/ACK UUID와 broker/DB 집합 차이는0이다. 마지막 permit을 통과한 발행은 이후 lease 인계로 broker에서 취소되지 않는다. 협력 워커의 로컬 차단이며 Kafka transactional fencing이나 DB epoch 조건부 append가 아니다. 오래된 물리socket이 존재한다는 사실만으로 중복 발행을 추정하지 않으며, 이미 진행 중인 ACK도 새로운 무권한 수락과 구분한다.

## 구조 점검과 재현

Clock 전달은 수집기·프로토콜 처리·통계에 실제 사용하는 seam만 추가했다. 네트워크 proxy와 실제 producer 관측 fixture는 test에 둔다. payload나 원문을 출력하지 않고 고정 코드·합성 ID·숫자로 대조한다. 시작 진단은 #54와 통합해 중복 fixture 책임을 정리한다. 제품 임대 TTL·발행 동시성·큐 상한과 broker 정책은 유지한다.

```sh
./scripts/check
./gradlew --console=plain :server:integrationTest --tests 'io.chatpulse.fleet.FleetIntegrationTest'
./scripts/integration
```

직전 인계에서의 의미 있는 동시 경합을 확인한 결과이며 공개 원천, 전체 방송, 외부 호스트/AZ, broker 측 fencing은 별도 범위다. 실제 source 공백은 복구되지 않는다.

## 이 실행에 포함하지 않은 경계

Clock offset/점프만 주입했으며 시계 속도 오차(drift), DB 서버 시계 변경, DB commit 응답만 유실, WS 무응답(heartbeat timeout), 실제 Kafka 경로의 네트워크 분리는 이 #44 실행에 포함하지 않았다. ChatConnection의 heartbeat 기한은 `System.nanoTime()`으로 판정하고 `lastAlive`는 관측 시각에 사용한다. 통합 코드에서 잔존 `Instant.now()`는 없음을 확인했으나 오프셋 Clock의 장시간 heartbeat 회귀를 추가로 실행한 것은 아니다.

`34e824e..e90dc32`의 변경은 docs와 근거 정제 스크립트뿐이다. 제품/시험 Java 코드는 실제 로컬 Fleet20 실행과 같다. 조사 문서는 구현 전 기준점의 설계이며 현재 구현과 검증 결과는 이 문서를 따른다.


## 유휴 transaction 잠금 후속 검증 #60

e90dc32의 PR CI에서 실제 DB blackhole 인계가 실패했다. 유휴 plan 잠금을 재현하고 서버 유휴 기한과 미배정 충돌의 제한 재시도를 보완했다. [별도 검증](DB_LOCK_VERIFICATION.md)에 실패·대조·ea7da9b의 실제Fleet23개 통과를 기록한다. 위94da3ad/34e824e의 실행별 수치와 구분하며 새 시험은 실제plan잠금 응답 뒤 client 장벽을 추가했다.
