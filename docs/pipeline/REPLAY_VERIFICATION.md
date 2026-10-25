# 고정 offset 재처리 검증

2026-10-25 · F08 → T01/T02/T03/T04 · 구현 commit `577f1f6`.
진행 상태는 [STATUS](../STATUS.md), 처리 계약은 [REPLAY](REPLAY.md), 실제 manifest·계수·비교 결과·실행 JAR SHA-256은 [정제 JSON](../poc/evidence/replay-2026-10-25.json)에 있다.

환경은 Java 21, Kafka 4.1.2 단일 broker, PostgreSQL 16.14다. `./scripts/check`의 Java 단위 55개·하네스 16개와 JAR 빌드, `./scripts/integration`의 실제 Kafka/DB 29개(재처리 10개)가 통과했다. 모든 재처리 입력은 합성 데이터다.

| 시험 | 기대값·실제 결과 |
|---|---|
| 고정 입력 2회 | partition별 `[0,3)`, `[0,2)`, `[0,0)`. 각 read=5, filtered=1, unique=3, duplicate=1, error=0. 3개 정확한 10초 bucket과 양쪽 합계 3 일치, 차이 0 |
| 늦은 순서·고정 end | eventTime 역순을 올바른 bucket에 반영. capture 후 빈 partition에 추가한 입력은 제외 |
| live 불변 | 운영 ledger/bucket·group offset·snapshot·급상승·알림 상태가 전후 동일, replay group offset 미커밋 |
| 실행 JAR CLI | capture/create/worker/status/compare/cancel 실제 프로세스 성공. 비교 불일치 JSON 및 종료 코드 2, 다른 manifest·미완료 run 비교 거부 |
| 빈 입력·필터 | 빈 범위 및 모두 필터 제외된 범위도 고정 end에서 완료. 전체 시간 한도를 0으로 주입해 TIME_LIMIT 실패 확인 |
| 취소 | QUEUED 즉시 취소, 250건 입력의 RUNNING은 첫 배치 후 취소. 부분 진행 보존, 완료·후속 쓰기 차단 |
| worker 강제 종료 | 배치 DB commit 직후 별도 JVM halt(91). RUNNING·부분 결과 확인 후 heartbeat를 31초 이전으로 조정해 watchdog 실패 판정. 이전 owner 완료 차단, 새 run으로 1건 재처리 |
| 입력 만료·변경 | deleteRecords로 시작 offset 만료, topic 재생성의 ID 변경, partition 증설을 각각 명시적 실패로 구분 |
| 멱등·poison | 배치 내부/간 동일 eventId 중복은 한 번만 반영. 200개 고유+배치 밖 중복은 read=201·unique=200·duplicate=1. payload 충돌과 malformed는 실패 배치 rollback, 원문 출력 없음 |
| SQL 오류 | bucket 쓰기 실패를 주입해 ledger·bucket·progress·계수가 모두 rollback |
| 격리·잠금 | live의 49시간 이전 입력 제한 유지, 명시적 replay 경로만 허용. 살아 있는 worker의 session lock은 watchdog·동시 worker 진입 차단 |

두 run 일치만으로 정확성을 판단하지 않고 알려진 채널·10초 bucket 기대값도 대조했다. 불일치/오류 시험의 부분 계수는 마지막 성공 commit까지의 값이며 실패 배치 읽기 시도는 포함하지 않는다.

새 실방송, 대용량·HA·네트워크 분할·다중 호스트는 검증하지 않았다. worker 종료 시험은 heartbeat 나이를 조정하므로 실제 30초 복구 시간 측정이 아니다. 시간 제한 시험도 30분을 실제 기다린 시험이 아니다. Kafka retention은 현재 남은 입력만 재현하며 24시간·7일 전체 데이터, 미래에 도착할 입력, 과거 품질·알림 재현이나 전체 Exactly-once를 보장하지 않는다. replay 결과 자동 삭제·운영 승격은 후속 범위다.
