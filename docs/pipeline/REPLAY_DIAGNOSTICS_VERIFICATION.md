# 재처리 CLI 후속 진단 검증

2026-10-25 · #16 / F08 → T01/T02/T03/T04. [처리 계약](REPLAY.md).

합성 입력·로컬 Kafka4.1.2 단일 broker/RF=1·PostgreSQL16.14·Java21 범위에서 검증한다. 새 공개 방송 수집은 수행하지 않는다. 각 테스트는 UUID topic/schema/group을 만들고 정리한다.

- 실행 JAR: capture/create/worker/status/compare/cancel에 retention을 연결하고 같은run 비교를 INCOMPATIBLE_RUNS로 거부한다.
- 조회 격리: stale RUNNING의 UPDATE 거부 trigger 상태에서도 status 성공, Flyway 이력 테이블을 제거해도 기존 결과 조회가 성공하고 테이블을 다시 만들지 않는다.
- 빌드: 실제 생성/실행 fingerprint를 기록하고 합성 UPDATE로 worker build를 다르게 만든 완료 run의 비교를 거부한다.
- 보존: 두 offset의 서로 역순인 Kafka receivedAt 표본을 그대로 표시하고 prefix 삭제 후 가용 폭1/누락 폭1·RETENTION_EXPIRED를 확인한다.
- 고정 오류: INVALID_DATE/TOPIC_NOT_FOUND/SOURCE_UNAVAILABLE을 각각 CLI 잘못된 날짜·실제 broker의 없는 topic·닫힌 로컬 포트로 구분한다.
- 제어 record: 실제 transactional producer의 commit marker가 꼬리인 범위는 NO_PROGRESS, 뒤에 데이터가 더 온 범위는 OFFSET_GAP이다. commit 응답 직후 end가 아직1인 관측을 확인해 broker end=2를 기다린 뒤 고정 manifest를 만든다.
- watchdog: 다른 active worker가 전역 lock을 유지하는 동안 죽은 run의 reap0, 해제 뒤 reap1이며 정상 run은 COMPLETED를 유지한다.
- 수동 하네스 확인: 잘못된 날짜로 종료 코드2·INVALID_DATE를 받은 뒤 새 runtime JAR0·manifest 생성0을 확인한다. SIGKILL/호스트 종료 뒤 자동 정리를 주장하지 않는다.

최종 전체 회귀·실행 commit/JAR hash·Claude/CI 결과는 진행 중이며 STATUS에서 관리한다. 삭제된 입력·미래 지연 입력·원천 중복·live snapshot/알림 재현·HA는 이 보완의 검증 범위가 아니다.


초기8ac4f77은 Claude APPROVE다. 빌드 리소스 누락의 초기화 예외를 고정 BUILD_UNAVAILABLE로 바꾸고, 실제 JAR 복사본에서 리소스를 제거한 CLI가 원본 stack trace 없이 이 코드만 반환하고 run을 생성하지 않는 검사를 추가했다. runtime 복사 전 EXIT trap 설치·HUP 처리도 보완했다. V10은 UUID 임시 테스트 스키마에만 적용/정리했으며 공유 개발 DB에 선행 적용하지 않았다. 선행 V7/V8/V9 병합 후 통합 검증을 다시 고정한다.

## 통합 기준의 최종 로컬 검증

#5/#6/#7 코드를 통합한 `ff5c30f`에서 `./scripts/check` Java84·하네스16·JAR, `./scripts/integration` 실제 Kafka/DB74가 통과했다. [정제 근거](../poc/evidence/replay-diagnostics-2026-10-26.json)에 JAR/XML hash와 합성 입력·미검증 범위를 기록했다. 8ac4f77·61784de는 Claude APPROVE이며, 이후 migrate의 build ID 검사를 DB 변경 앞으로 옮기고 변조 JAR 시험의 경로를 finally에서 원복했다. 선행 #7의 heartbeat fixture 변경은 단위 검증으로 이어서 확인한다. 웹 변경은 없으며 #7의29/31 근거를 유지한다. 최종HEAD 리뷰·CI·병합은 STATUS를 따른다.
