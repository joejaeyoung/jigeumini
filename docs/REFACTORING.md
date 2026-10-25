# 코드 구조 검토와 리팩토링 결정

2026-10-25 · 기준 `707230a` · 이슈 #17 · 최종 코드 commit `f6a4d63`.
진행 상태는 [STATUS](STATUS.md)에서 관리한다. 기능 추가 대신 기존 동작을 유지하면서 책임과 중복을 정리했다.

## 검토 범위와 선택

서버 `poc`, `pipeline`, `metadata`, `quality`, `replay`와 웹 `app`, `features`, `shared`, 실행 스크립트·CI의 파일 구성, 주요 호출 관계, 상태/트랜잭션 소유, 중복 설정을 검토했다. 전수 보안·성능 감사나 모든 분기의 새 검증을 의미하지 않는다.

| 영역 | 발견한 점 | 결정 |
|---|---|---|
| 품질·순위 | RankingStore가 세션·버퍼·소비 관측 SQL과 순위/급상승 저장을 함께 소유 | 읽기·품질 판정을 CollectionQuality에 모으고 RankingStore는 기존 트랜잭션과 결과 저장을 유지 |
| DB 초기화 | PipelineConfiguration과 ReplayCli에 같은 Hikari 한도·timeout·Flyway 초기화 중복 | PostgresSupport.openMigrated로 통합. 성공 시 호출자가 pool 소유, 초기화 실패 시 즉시 정리 |
| 웹 API | client.ts에 HTTP·집계·방송 정보 계약이 혼재하고 quality.ts에 공통 필드 중복 | transport/fields/buckets/metadata로 분리. 각 호출부가 실제 사용하는 모듈을 직접 import |
| 수집·발행 | 프로토콜/HTTP 어댑터·연결 감독·EventSink가 이미 분리됨. 발행 큐는 동시성·종료 규칙 보유 | 이 범위에서 구조 변경하지 않음. 버퍼/발행 병렬성은 #11의 부하·장애 근거와 함께 판단 |
| replay | 상태 쓰기·비교 SQL이 한 Store에 있고 조회 migration/임시 JAR/진단 정책 개선 후보 존재 | #16은 관찰 가능한 정책 변경을 포함하므로 이번 책임 분리와 혼합하지 않음. Store 추가 분리는 후속 기능의 실제 변경 지점으로 판단 |
| 메타데이터 | 캐시와 원천 HTTP 어댑터가 분리되고 테스트에서 시계·실행기를 대체함 | 구현체 하나뿐인 새 인터페이스나 범용 캐시 계층을 추가하지 않음 |
| 웹 화면·하네스 | 짧은 화면 컴포넌트와 polling 공통화가 존재. launch JAR 복사 코드가 중복됨 | 화면 구조는 유지. 임시 파일 수명·signal 전달의 변경은 #16에서 다룸. refactor 브랜치는 CI 대상에 추가 |

## 보존한 처리 계약

CollectionQuality는 package-private 내부 모듈이다. RankingStore와 같은 JdbcTemplate/DataSource를 사용하고 스스로 트랜잭션을 시작하지 않는다. 따라서 기존 REPEATABLE READ·advisory lock 안에서 관측을 조회하고 순위 snapshot·급상승 상태를 확정한다. SQL, clock 호출 위치, 채널→partition 계산, 창 뒤 첫 안전한 drain과 caught-up 대조를 그대로 옮겼다. 사용하지 않던 Health의 intervals/loss 포장만 제거하고 QualityPolicy.Result를 반환한다.

`io.chatpulse.db.PostgresSupport`는 서버와 CLI의 pool=4, 연결 대기 3초, 검증 2초, PostgreSQL connect/socket 3초/10초를 유지한다. 각 호출은 별도 pool을 소유하며 JVM 전역 pool을 만들지 않는다. migration의 RuntimeException/Error 모두 pool을 정리하고, 정리 실패는 원래 오류의 suppressed 예외로 보존한다. replay의 조회 명령도 migration을 실행하는 기존 정책은 #16에서 별도로 개선한다.

웹 transport는 기존 8초 timeout, 상위 취소 신호, HTTP/네트워크/계약 오류 분류를 유지한다. fields는 channel ID·timestamp·안전한 정수 검증의 정본이다. buckets와 metadata는 각 응답 스키마와 요청-응답 일치 검사를 소유한다. 기존 client.ts 재수출 계층은 남기지 않았으며 테스트도 실제 모듈을 호출한다. HTTP 경로·JSON·사용자 화면은 바꾸지 않았다.

## 회귀 검증

[실행 근거](poc/evidence/refactor-2026-10-25.json)에 코드 commit, JAR SHA-256, 환경, suite별 개수를 기록했다. 입력은 합성 HTTP/WS/API와 격리된 실제 로컬 Kafka topic·DB schema다.

| 명령 | 실제 결과·대조 범위 |
|---|---|
| `./scripts/check` | Java 단위 55개·하네스 16개·JAR 빌드, 문서/기술 ID 연결 통과 |
| `./scripts/integration` | 실제 Kafka/PostgreSQL 29개 통과. 품질 제외·창 뒤 drain·소비 따라잡음·사후 정정·독립 소비·중복/rollback·고정 replay/운영 불변·실행 JAR CLI |
| `./scripts/web check` | 타입 검사·기존 단위 17개·제품 빌드 통과. API 계약·metadata 만료·요청 실패/취소 검증 |
| `./scripts/web test:e2e` | Chromium 22개 통과. 관심·상세·기간 전환·집계 미확인·품질/리본·반응형 표시 |

내부 메서드 호출 횟수나 파일 배치를 복제하는 테스트는 추가하지 않았다. 기존 외부 interface를 통한 기대 동작으로 회귀를 확인했다. T01/T02/T04/T08/T11의 기존 범위를 보존한 근거이며 새 기술 목표 해결이나 처리량 향상 근거는 아니다. 실방송·새 플랫폼 프로토콜·HA·고부하·실기기는 이번 검증에 포함하지 않는다.
