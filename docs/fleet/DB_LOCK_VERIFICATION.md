# 유휴 임대 잠금·미배정 복구 검증 #60

2026-10-26 · F13 → T02/T08/T11/T14 · [조사](../research/FLEET_DB_LOCK.md) · [처리 계약](README.md).

## 실패를 구분한 근거

| 실행 | 코드·주입 | 관측 |
|---|---|---|
| PR58 CI40263104263 | e90dc32, 실제 두 JVM·DB TCP blackhole | epoch2 기대/실제1. 이전 backend의 idle transaction80개, 그 PID를 기다리는 transactionid 잠금17개 표본. 대기 worker 수집0·실패 종료 |
| 수정 전 재현 | f37a4c0 제품 + d1ee36d와 동일한 미커밋 fixture | 실제 plan 행 잠금 응답 직후 client 장벽·DB blackhole. epoch2 기대/실제1. 실행 중 수분의 관측 공백이 있어495.508초를 복구 시간으로 사용하지 않음 |
| 두 번째 실행 | d1ee36d | 장애 주입 전에 A가 종료. owner가 null인 진단에서 실패했으므로 잠금 가설 검증에서 제외. 이전 observed 파일은 이 실행의 근거가 아님 |
| 유휴 제한만 추가 | d1ee36d + 임대 transaction의 유휴2초 | 이전 잠금 PID는 사라졌으나 대기 worker가 이미 실패 종료. epoch2 기대/실제1,39.867초. 유휴 잠금 해제만으로 인계가 완성되지 않음 |
| 제한 재시도 추가 집중 실행 | d1ee36d + ea7da9b와 동일한 미커밋 제품/시험 코드 | JDBC/worker 단위7·실제 Kafka/DB4 통과. 인계6.228초·이전 PID 종료·대기 재시도1·epoch2·이전 session marker0·ACK/broker/DB3 일치 |

[최초 CI 진단](../poc/evidence/fleet-db-lock-2026-10-26/ci-failure.json)과 [대조 실행](../poc/evidence/fleet-db-lock-2026-10-26/diagnosis.json)은 실패와 전제 실패를 포함한다. 최초 CI의 최종 SQLSTATE, 두 번째 실행의 사전 종료 원인, 첫 실행의 호스트 관측 공백 원인은 확정하지 않는다. 실제 유휴 잠금과 대기 종료 경로를 재현하고 수정했으며 모든 과거 실패가 단일 원인이었다고 주장하지 않는다.

## 검사 방법과 안전 경계

- loopback 합성 WebSocket, 실제 Kafka4.1.2/PostgreSQL16.14, 별도 JVM 두 개를 사용한다. 공개 원천을 호출하지 않는다.
- 기존 worker가 연결·marker ACK를 완료하고 대기 worker가 DB에 등록된 다음, 실제 `FOR UPDATE` 응답 뒤의 fixture 장벽을 건다. 그 이후 DB proxy를 BLACKHOLE로 바꾸며 원천 TCP는 통과시킨다. DB가 유휴 transaction 상태라는 실제 표본과 잠금 backend PID 종료를 확인한다.
- 추가 실제 시험은 미배정 worker의 일시 잠금 후 수집, 영구 잠금의5회 제한·원천0, 보유 임대 충돌의 재시도0·종료·새 epoch를 각각 검사한다. 임대가 있는 worker의 실패를 자동 재획득으로 바꾸지 않는다.
- JDBC 합성 실패는55P03의 rollback 완료 뒤 분류,08006/08007/57014/40001 제외, rollback 실패 제외를 검사한다. 불확정 transaction 결과는 worker 호출 경계에서 합성 주입하며 실제 commit 응답 유실 시험을 수행했다는 뜻이 아니다.
- 잠금 장벽이 통신을 계속 차단한 동안에도 로컬 permit은 만료한다. 이후 marker0만으로 소켓 종료와 발행 permit 차단의 기여를 분리할 수 없다. 별도의 #44 SIGSTOP·permit/send/ACK 경계 검증을 함께 읽는다. Kafka broker fencing이나 공백 복구 보장이 아니다.

## 리팩토링 점검

DB SQLSTATE 분류는 CollectorLeases의 transaction 완료 경계에 모으고, 재시도 횟수·보유 여부는 수집 수명을 소유한 CollectorWorker에서 판정한다. 원천 프로토콜·Kafka 발행 버퍼·DB epoch/기한 계산은 보존한다. 시험 장벽은 test DataSource에만 있으며 제품 분기나 장애 주입 API를 만들지 않는다.

전체 회귀와 최종 리뷰·CI·병합 상태는 [STATUS](../STATUS.md)에 기록한다. 실행 근거는 검증된 revision별로 추가한다.


## 고정 코드의 전체 회귀

`ea7da9b326a1cbf7160cb661df289509b2283175`의 `./scripts/check`에서 단위159·하네스24·실행 JAR가 통과했다. 실제 Fleet23개(기존20개와 새3개)를 모두 실행했고 실패/오류0이다. 기존 blackhole 시험의 주입 시점을 실제 plan 잠금 응답 직후로 고정했다. 잠금 없이 tick 사이에 단절되는 별도 blackhole 시험은 이 revision에서 실행하지 않았으며 이전94da3ad의 비고정 주입 근거와 구분한다. JAR·각 XML·현재 실행에서 만들어진 관측 파일의 SHA256을 [기본 검사](../poc/evidence/fleet-db-lock-2026-10-26/check.json)와 [전체 Fleet 근거](../poc/evidence/fleet-db-lock-2026-10-26/full-fleet.json)에 보관한다. 실행 중 docs만 추가됐고 제품·시험 코드는 고정됐다.

[집중 실행 근거](../poc/evidence/fleet-db-lock-2026-10-26/focused.json)의6.228초는 해당 실행의 관측값이다. 전체 회귀의 시각·개별 계수는 별도 근거를 사용하며 운영 RTO 상한으로 일반화하지 않는다. 웹 코드 변경은 없고 전체 push/PR CI에서는 실제 통합 suite와 웹 검증을 다시 확인한다.

`f37a4c0...ea7da9b`의 읽기 전용 Standards는 하드/heuristic/차단0, Spec은 지적/차단0이었다. 두 리뷰는 코드·명세·원본 hash를 검사했으며 직접 테스트를 실행하거나 최종 CI 성공을 판정한 기록은 아니다.

DB backend 표본은 최대100개로 제한한다. 표본 수를 지속 시간으로 환산하지 않으며 인계 시간은 별도 nanoTime 경계로 측정한다. 최초 표본 수 차이만으로 실행 간 부하나 복구 속도를 비교하지 않는다.
