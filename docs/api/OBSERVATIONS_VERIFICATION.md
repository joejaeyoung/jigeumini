# 시청자 관측 검증

2026-10-25 · #5 / F04 → T02/T04/T06. [처리 계약](OBSERVATIONS.md)·[코드/JAR/suite 정제 근거](../poc/evidence/observations-2026-10-25.json).

## 실행 범위

서버 코드 `8eaf39b`의 `./scripts/check` Java76·하네스16·JAR가 통과했다. 기본값과 다른 환경 변수 주기90초/TTL240초의 연결 검증을 보완한 `bd81332`에서 `./scripts/integration` 실제 Kafka/PostgreSQL53개가 통과했다. 관측 통합6·알림5·DLQ10·토픽2·기존 파이프라인9·품질11·replay10이다. 웹은 `8eaf39b`에서 `./scripts/web check` 타입·단위25·빌드와 `./scripts/web test:e2e` Chromium26개가 통과했고 이후 웹 코드는 변경하지 않았다.

Kafka4.1.2 단일 broker/RF=1, PostgreSQL16.14, Java21. 실제 broker/DB 검사는 합성 ID·수치·UTC 시각과 UUID topic/group/schema로 격리하고 해당 자원을 정리한다. 메타데이터 필드는 별도 [공개3채널 단발 HTTP 표본](../poc/evidence/observation-source-2026-10-25.json)에서 확인했다. 실제 장기 관측·오프라인 전환·429 재현을 했다고 주장하지 않는다. 사용자 확인을 마친 live PoC 서비스는 종료한 채로 유지했다.

## 확인한 경계

| 경계 | 기대와 실제 결과 |
|---|---|
| 원천 수치 | 실제0명 유지, 문자열/음수/소수/안전 정수 초과/누락은 null. 제한·오프라인·429/500에서 과거 수치를 복사하지 않음 |
| 수집 활성화 | 채널 start 전 요청0, 진행 중 중복 예약0, stopped 뒤 대기 요청의 HTTP/발행 없음. 프로토콜 fixture의 복수 관측자 모두 종료 통지 |
| 429 | 가상 시계의60/120/240/480/900/900초 대기, 정상 응답 뒤60초 복귀. 실제900초 대기 시험은 아님 |
| 역순·중복·null | Kafka에 나중 시각20명→이전 시각null→같은20명. DB2행·시각순 조회·commit3, strict 재시작 후 중복에도2행·commit4 |
| 내용 충돌 | 같은 UUID 다른 수치는 배치 전체 실패. 기존3행·progress4 보존 |
| DB 장애 | progress INSERT SQL trigger 거부 시 이력과 progress 모두 rollback, Kafka offset0. trigger 제거 후 자동 재시도·commit3 |
| DB/offset 사이 종료 | 별도 JVM halt(97). DB1·Kafka offset0, 동일 group strict 재시작 뒤DB1·offset1 |
| 보존 |7일 경계 포함·직전 표본 제외, 이전 run까지 정리.1초 후 경계 정리, 재전달해도 부활0. live processed_event 변경0 |
| 분 경계 | 나노초 직전 표본이 DB에서 다음 분으로 반올림되는 기존 실패1/1을 재현. 마이크로초 내림 후 경계 전/후가 각 원래 창1행으로 유지 |
| 토픽 이력 | cleanup.policy=delete 명시. 기존 compact·잘못된 partition/timestamp 설정은 변경 없이 거부 |
| Spring 연결 | 로컬 합성 HTTP→실제 CollectorConfiguration/복수 관측자→Kafka→DB73명. start 전 HTTP0. 기본값과 다른 환경 변수 TTL240을 실제 payload에서 확인 |
| JSON 계약 | 비어 있지 않은 같은 JSON fixture를 MockMvc와 웹 Zod에서 검증.0과 null 유지 |
| 실행 JAR | 격리 analyzer의 관측 consumer3 partition 할당, 완료60분 point와 실제 입력 관측0명·ISO 시각. 기존 복구·알림 API 회귀 유지 |
| 웹 | 같은 시간축의 실제7초 위치,0명, null/TTL/방송 변경의 선 끊기,60분/30분 변경, 빈 상태·실패·320px 넘침 없음·44px 기본 선택 메뉴의 관측 선택 |

테스트 XML에서 합성 private canary가 노출된 파일은0개다. UI 스크린샷은 합성 fixture만 사용해 320/1440px 화면을 확인했다.

## 해석

Spring 연결 검사는 실제 HTTP fixture·발행·DB를 사용하지만 `PocRunner.run`과 실제 WebSocket을 함께 실행한 live 검사가 아니다. 파일 잠금 이후 start 순서는 기존 프로토콜 테스트와 코드 구조에서 확인했다. 메타데이터 캐시 빈은 analyzer 역할에만 등록되며 collector의 PublicObservationSource와 같은 역할에서 중복 주입되지 않는다. 사용자 정의 source를 추가하면 일반 구성의 ConditionalOnMissingBean 등록 순서를 별도로 확인해야 한다.

Kafka offset 순서와 observedAt 순서는 같다고 가정하지 않는다. 입력 기록의 멱등성과 시간축 정렬을 검증했으며 다중 producer·partition 증설·고부하·복제 HA는 #11 범위다. 7일 저장 정책은 Kafka24시간 전체 가용성이나 삭제된 원본 복원을 보장하지 않는다. 원천 소스와 UI 캐시는 서로 다른 경계이고 현재값을 과거에 소급하지 않는다. 여러 호스트의 수집 중복 방지, 관측 토픽용 replay CLI는 미구현이다.

리뷰·CI·병합 상태는 [STATUS](../STATUS.md)에서 관리한다.
