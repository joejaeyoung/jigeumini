# Chat Pulse — 설계 검토 기록

2026-10-25 · v0.2 · 문서 검토, 구현 시험 전

전체 구조는 개인 Kafka 프로젝트의 목적에 맞는다. 수집기와 두 소비 그룹, PostgreSQL 집계, 별도 재처리라는 구성은 유지한다. v0.1에서 구현자가 서로 다르게 해석할 수 있었던 실패 경계와 계산 규칙을 구체화했다. 아직 실제 연결·성능·장애 복구를 검증한 것은 아니다.

## 발견 사항과 반영한 결정

| 중요도 | v0.1의 빈틈 | 발생 가능한 문제 | v0.2 결정 |
|---|---|---|---|
| 높음 | ack mode와 실패 배치 처리 범위 미정 | 실패 record를 건너뛰거나 DB 반영 전에 offset 진행 | BATCH ack, poll 배치 전체 성공 후 반환, DB ledger 원자 반영, 실패 시 전체 재시도 |
| 높음 | 오류 handler의 재시도 종료 동작 미정 | 기본 recoverer가 로그만 남기고 입력을 건너뜀 | 인프라 오류는 pause/poll 기반 재시도, 예상 밖 오류는 container 중지; 고정 버전 통합 시험 |
| 높음 | freshness 설명에 계산 가능한 기준 부재 | 조용한 방송을 장애로 표시하거나 Kafka 이전 적체 누락 | end-offset 표본 대비 DB 진행·consumer heartbeat와 미발행 버퍼 나이를 함께 판정 |
| 높음 | DLQ 전송 뒤 결과 품질 처리 부재 | 빠진 이벤트가 있어도 정상 순위로 표시 | 알려진 영향 구간을 불완전 처리; 범위 불명은 projection 품질 경고 |
| 높음 | 재처리 비교 대상·실행 주체 불명확 | 계속 변하는 live 결과와 비교, 202 이후 작업 미실행 | 고정 manifest·완료 기준 run 비교, 별도 worker와 명시적 작업 상태 |
| 중간 | 장애 발견 전 이미 발행한 snapshot 처리 미정 | 사후 알려진 손실을 사용자에게 숨김 | 건수·순위 고정, 품질만 하향 정정하고 비교 대상에서 제외 |
| 중간 | 0건 채널·연속 급증·cooldown 경계 미정 | 채팅 없는 정상 채널 누락, 평가 누락 후 거짓 연속 알림 | 채널 목록 LEFT JOIN, 정확히 10초 연속, 누락·cooldown 시 초기화 |
| 중간 | 멱등 ledger 삭제와 오래된 재투입 충돌 | 삭제한 eventId가 다시 집계됨 | 운영 재투입 MVP 제외, live 입력 나이 제한, ledger 보존 기준 명시 |
| 중간 | 단일 collector라는 선언만 존재 | 실수로 두 번 실행해 원천 이벤트 이중 수집 | 단일 호스트 공유 파일 잠금; 다중 호스트는 미지원 |
| 중간 | 일시 부하 목표와 지속 용량이 혼동될 여지 | ledger·디스크가 예상보다 빠르게 증가 | 30분 소스 표본과 DB/Kafka 디스크 실측을 용량 결정의 통과 조건에 추가 |

## 범위를 조정한 부분

**DLQ의 운영 재투입은 MVP에서 제외했다.** DLQ 격리·진단은 구현하고, 로직 수정 후 검증은 원본이 남아 있는 범위를 새 replay run에서 실행한다. 운영 결과를 복구하는 redrive는 향후 별도 요구사항이다. 이를 추가하면 분석별 retry 경로, 원래 eventId 보존, 보존기간 이후 중복 방지, 과거 품질 복구 정책까지 함께 설계해야 한다. PRD FR-09와 아키텍처 ADR-09에 같은 결정을 반영했다.

## 아직 문서만으로 확정할 수 없는 항목

| 항목 | 확인 방법 | 통과 전 가능한 작업 |
|---|---|---|
| 현재 치지직 익명 수신 가능 여부 | Spring PoC로 공개 2~3채널 30분, ping/재접속/채팅방 변경 확인 | 이벤트 계약·합성 입력 개발 |
| timestamp·고유 ID·메시지 종류 | 실제 frame을 메모리에서 확인하고 식별정보를 제거한 fixture 작성 | 소스 어댑터 인터페이스 작성 |
| 지원 라이브러리 조합 | Java 21과 Spring Boot BOM 호환표 확인, 이미지 태그 고정 | 계산 도메인 구현 |
| 지속 수집 가능한 규모 | 수신율·이벤트 크기·ledger 저장량 실측 후 보존/디스크 예산 확정 | 제한된 부하 시험 준비 |
| 급증 임계값의 제품 유용성 | 실제 표본에서 C/B와 탐지 빈도 확인 | 현재 v1 고정 예제로 계산 검증 |

## 구현 착수 권고

먼저 M0 수집 PoC를 수행한다. 성공하면 M1에서 한 채널의 Kafka→DB→조회 흐름과 DB commit 직후 종료 시험까지 완성한다. 이후 순위·급증·운영 화면을 확장한다. 초기 단계에서 다중 collector, Kafka Streams, 외부 알림을 추가할 필요는 없다.

수용 예제는 [HLD 11.1](HLD.md#111-구현-시-고정할-수용-예제)에 기록했다. 문서 내 링크와 예제 수식·시간 경계는 확인했으며, 서비스 코드·실방송·통합 테스트는 이번 검토의 실행 범위가 아니다.

참조: [Spring Kafka listener container](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html), [오류 처리](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html), [PostgreSQL INSERT](https://www.postgresql.org/docs/current/sql-insert.html). 세부 설정은 구현 시 채택 버전에서 다시 확인한다.
