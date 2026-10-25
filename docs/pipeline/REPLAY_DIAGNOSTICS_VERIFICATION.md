# 재처리 CLI 후속 진단 검증

2026-10-25 · #16 / F08 → T01/T02/T03/T04. [처리 계약](REPLAY.md).

합성 입력·로컬 Kafka4.1.2 단일 broker/RF=1·PostgreSQL16.14·Java21 범위에서 검증한다. 새 공개 방송 수집은 수행하지 않는다. 각 테스트는 UUID topic/schema/group을 만들고 정리한다.

- 실행 JAR: capture/create/worker/status/compare/cancel에 retention을 연결하고 같은run 비교를 INCOMPATIBLE_RUNS로 거부한다.
- 조회 격리: stale RUNNING의 UPDATE 거부 trigger 상태에서도 status 성공, Flyway 이력 테이블을 제거해도 기존 결과 조회가 성공하고 테이블을 다시 만들지 않는다.
- 빌드: 실제 생성/실행 fingerprint를 기록하고 worker build가 다른 완료 run의 비교를 거부한다.
- 보존: 두 offset의 서로 역순인 Kafka receivedAt 표본을 그대로 표시하고 prefix 삭제 후 가용 폭1/누락 폭1·RETENTION_EXPIRED를 확인한다.
- 고정 오류: INVALID_DATE/TOPIC_NOT_FOUND/SOURCE_UNAVAILABLE을 각각 CLI 잘못된 날짜·실제 broker의 없는 topic·닫힌 로컬 포트로 구분한다.
- 제어 record: 실제 transactional producer의 commit marker가 꼬리인 범위는 NO_PROGRESS, 뒤에 데이터가 더 온 범위는 OFFSET_GAP이다. commit 응답 직후 end가 아직1인 관측을 확인해 broker end=2를 기다린 뒤 고정 manifest를 만든다.
- watchdog: 다른 active worker가 전역 lock을 유지하는 동안 죽은 run의 reap0, 해제 뒤 reap1이며 정상 run은 COMPLETED를 유지한다.
- 하네스: 잘못된 날짜로 종료 코드2·INVALID_DATE를 받은 뒤 새 runtime JAR0·manifest 생성0을 확인한다. SIGKILL/호스트 종료 뒤 자동 정리를 주장하지 않는다.

최종 전체 회귀·실행 commit/JAR hash·Claude/CI 결과는 진행 중이며 STATUS에서 관리한다. 삭제된 입력·미래 지연 입력·원천 중복·live snapshot/알림 재현·HA는 이 보완의 검증 범위가 아니다.
