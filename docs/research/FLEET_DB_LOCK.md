# DB 단절의 유휴 임대 잠금 조사 #60

2026-10-26 · F13 → T02/T08/T11/T14 · [이슈 #60](https://github.com/joejaeyoung/jigeumini/issues/60).

## 관측과 대조

PR58 e90dc32의 PR CI40263104263에서 blackhole 뒤 epoch2 인계가 일어나지 않았다. 이전 worker의 backend는80개 표본 모두 `idle in transaction / ClientRead`였고 대기 worker는 해당 PID의 transactionid 잠금에 걸린17개 표본 뒤 실패 종료했다. 당시 SQLSTATE는 기록되지 않아 잠금 timeout이라는 최종 예외까지 단정하지 않는다. 이전 worker의 실제 SQL 잠금 뒤 응답 처리를 멈추는 장벽으로 같은 상태를 재현하고 대기 worker의 등록을 먼저 확인한다.

가설은 ① 유휴 transaction의 plan 잠금 유지와 대기 worker의 실패 종료, ② 대기 worker의 시작 지연, ③ CI 실행 속도 저하 순서다. 시작 완료·DB 등록과 실제 잠금 대기를 별도로 계측해②를 구분한다. 과거 #54의 metadata 시작 실패와 원인이 같다고 해석하지 않는다.

## 공식 자료

확인일은2026-10-26이다. PostgreSQL16의 [클라이언트 설정](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT)은 열린 transaction이 다음 client 요청을 기다리며 유휴 상태인 경우 세션을 종료하는 timeout을 제공한다. 기본0은 비활성이다. statement_timeout은 실행 중인 SQL, lock_timeout은 개별 잠금 대기를 제한하므로 둘만으로 유휴 transaction의 잠금 수명을 제한하지 못한다.

[PostgreSQL SQLSTATE 목록](https://www.postgresql.org/docs/16/errcodes-appendix.html)에서55P03은 lock_not_available,57014는 query_canceled이며 연결 오류는08계열이다. 오류 메시지 문자열 대신 코드를 판정한다.

Spring7.0.10의 [SQLStateSQLExceptionTranslator](https://raw.githubusercontent.com/spring-projects/spring-framework/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java)는55P03을 특정 잠금 예외로 매핑하지 않는다. 특정 Spring 잠금 예외 클래스만 잡으면 이 경로를 놓칠 수 있다. [TransactionTemplate](https://raw.githubusercontent.com/spring-projects/spring-framework/v7.0.10/spring-tx/src/main/java/org/springframework/transaction/support/TransactionTemplate.java)은 callback 실패 후 rollback을 수행하고, [DataSourceTransactionManager](https://raw.githubusercontent.com/spring-projects/spring-framework/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/DataSourceTransactionManager.java)는 rollback/commit 실패를 TransactionSystemException으로 구분한다.

## 수정 경계

수집 임대 transaction에만 `SET LOCAL idle_in_transaction_session_timeout='2s'`를 적용한다. 기존 잠금1초·SQL3초·클라이언트 transaction4초 제한과 병행하며 전역 DB 설정을 바꾸지 않는다. 이 경로는 DB 호출 사이에 원천 HTTP나 Kafka 요청을 수행하지 않는다. 서버 유휴 timeout은 wall clock RTO 보장이나 장애 중 전체 입력 복원을 뜻하지 않는다.

rollback을 완료한 임대 tick의55P03만 도메인 잠금 충돌로 전달한다. 아직 임대를 갖지 않은 worker는 기존1초 scheduler 간격으로 최대5회 연속 재시도하고6번째 충돌에서 종료한다. 정상 tick 뒤 연속 횟수를 초기화한다. 기존 실행 기한도 적용하며 누적 재시도 수만 결과에 남긴다. 보유 임대가 있거나 종료 중이면 재시도하지 않는다. 연결 실패·SQL 취소·불확정 commit·rollback 실패도 기존 실패 종료 경계를 유지한다.

실제 실행 revision·실패/성공·남은 범위는 검증 기록에 추가하고 현재 진행은 [STATUS](../STATUS.md)에서 관리한다.
