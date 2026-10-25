# Kafka·PostgreSQL 수직 구현 검증

2026-10-25 · Java 21, Spring Boot 4.0.9, Kafka 4.1.2, PostgreSQL 16.14.

## 실제 공개 방송

갱맘 GBM (`dbd6e0689ff9debf8bb00a415786654c`), 익명 READ, 로그인 쿠키 없이 검증했다.

| 실행 | 시각 (KST) | 수신 | Kafka ACK | DB ledger 증가 | bucket 증가 | 결과 |
|---|---|---:|---:|---:|---:|---|
| 120초·60초에 강제 재접속 | 14:56:57~14:58:58 | 63 | 63 | 63 | 63 | PASS |
| 최종 JAR·분석기 none 재시작·60초 수신 | 15:00:33~15:01:33 | 38 | 38 | 38 | 38 | PASS |

최종 실행도 30초에 강제 재접속했고 이후 일반 채팅 32건이 추가 수신됐다. 발행 거절·미확인은 0건이다.
각 실행 전후 같은 namespace의 총합을 대조했다. 다른 생산자가 없는 로컬 환경의 검증이며 원천 누락률을 뜻하지 않는다.

- [120초 수신](evidence/live-120s.json) · [120초 대조](evidence/reconciliation-120s.json)
- [최종 60초 수신](evidence/final-live-60s.json) · [최종 60초 대조](evidence/reconciliation-60s.json)
- [최종 consumer offset](evidence/consumer-offsets.txt) · [10초 bucket 조회](evidence/buckets.json)
- [실행 JAR hash·라이브러리·이미지](evidence/execution.json)

첫 120초 실행 이후 빈 partition의 최초 offset 기록을 보완했다. 최종 JAR는 분석기를 기본 `none` 설정으로 재시작한 후 다시 실수신했다. 위 두 실행의 JAR hash는 구분한다.

## 자동 검사

로컬 테스트 **26개**, 실제 Kafka·PostgreSQL 통합 테스트 **8개** 모두 통과. [suite 요약](evidence/tests.json)

- 동일 eventId의 배치 내부 중복·재전달은 집계를 재증가시키지 않는다. 발생 시각 10초 경계도 검사했다.
- bucket 반영 뒤 SQL 오류를 주입하면 ledger·bucket·progress가 함께 rollback된다.
- 같은 eventId의 payload 충돌은 새 이벤트를 포함한 전체 트랜잭션을 rollback한다.
- 별도 JVM을 DB commit 직후 `halt(91)`로 종료했다. offset 미완료를 확인하고 같은 group을 재시작하여 재전달 후 집계 1건을 유지했다.
- DB 오류 동안 offset을 넘기지 않고, 오류 해제 후 자동 재시도로 반영한다.
- malformed 입력은 반복 실패 상태로 표시하고 offset을 진행하지 않는다.
- 입력이 없는 3개 partition을 최초 earliest로 초기화한 뒤 none으로 재시작해 새 입력을 처리했다.
- 발행 중 이벤트도 큐 한도에 포함되며, 큐 초과·발행 실패를 성공으로 보고하지 않는다.

실행 명령: `./scripts/check`, `./gradlew --console=plain integrationTest`. GitHub CI 워크플로에도 통합 검증을 추가했으나 원격 CI는 실행하지 않았다.

## 실행 하네스와 화면

실행 중 `build/libs` JAR를 재빌드하면 lazy class loading이 실패하는 문제가 첫 시도에서 발생했다. 그 실행은 성공 표본에서 제외했다. 이미 DB에 반영된 34건은 삭제하지 않고 후속 실행의 baseline으로 구분했다.
`scripts/pipeline`과 `scripts/poc`가 매번 `work/runtime/`에 별도 JAR 복사본을 만들도록 수정했다. 복사본으로 실행한 분석기가 동작 중일 때 재빌드한 뒤에도 API 정상 응답과 잘못된 파라미터 HTTP 400을 확인했다.

조회 화면을 실제 브라우저에서 열어 DB 총합·채널별 최근 5분 건수·RUNNING 상태 표시를 확인했다. UI는 5초마다 갱신한다.

## 범위

로컬 단일 Kafka broker(RF=1) 기능 검증이다. HA, 대용량 성능, 원천 중복 제거·접속 공백 복원, coverage, DLQ, retention 자동 정리, 운영 replay와 정식 순위 snapshot은 미구현이다. [실행·처리 계약](README.md)에 HLD 대비 범위를 명시했다.

## 고정 offset 재처리 추가 검증

2026-10-25 F08 → T01/T02/T03/T04의 CLI·실제 Kafka/PostgreSQL 검증은 [재처리 검증 기록](REPLAY_VERIFICATION.md)에 분리했다. 위 M1 수치와 당시 미구현 목록은 최초 실행 시점의 기록이다.
