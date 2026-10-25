# 배포·복구 리허설 검증

2026-10-26 KST · #30 / F16 → T04/T08/T16. 현재 상태는 [STATUS](STATUS.md), 운영 선택·용량·한계는 [운영안](DEPLOYMENT_OPERATIONS.md), 정본 명령은 [배포 패키지](../deploy/README.md)를 따른다.

## 실행 식별

- 이전 이미지 commit: `4f85f1ded5bd51f85be9a4fb842ae85144cde458`.
- 최신 이미지/하네스 commit: `1bde2b779a265932a2e37aaaa179171a7777aca2`, clean tree.
- 명령: `./scripts/deploy-images work/images-finalized.json`, `./scripts/deploy-rehearsal work/images-baseline.json work/images-finalized.json work/deploy-sixth`, `./scripts/check`.
- 단일 호스트 Docker, Kafka4.1.2/RF1·PostgreSQL16.14, 합성 seed20261026. eventTime은 관측 기준120초 전부터 두 배치10건씩 사용한다. 실제 원천 호출·외부 배포·registry push는 없다.
- Git/image ID/JAR SHA/buildId·UTC시각·phase별 계수는 [여섯 번째 원본](poc/evidence/deployment-2026-10-26/sixth.json), 파일 hash/이전 실행·제한은 [정제 요약](poc/evidence/deployment-2026-10-26/summary.json)에 있다. 기준 이미지와 후보는 모두 V12 migration 집합이다.

## 결과

| 경계 | 실제 결과 |
|---|---|
| 명시적 초기화 | source identity4개 조회, group6개 반환 확인. runtime CREATE TABLE은 SQLSTATE42501로 거절 |
| 이미지/네트워크/비밀 | 서버USER10001·웹USER101, 앱 root 읽기전용, web loopback만 공개, web과DB망 분리. 비밀 환경값 없음·파일 주입 |
| 수집 CLI | runtime 비밀로 readonly status 성공·assigned0, 비활성 worker 시작 거절. 공개 원천 호출0 |
| DB 중단 | readiness503·liveness200·API503 고정 JSON, DB 복구 뒤readiness200 |
| 이미지 교체/복귀 | 기준→후보→기준의 HTTP revision/buildId와 집계 snapshot 대조 |
| schema 불일치 | checksum 손상 후보는 exit2·SCHEMA_NOT_READY·OOM없음, 자동repair없음, 원래checksum/기준이미지 복귀 |
| 웹 | 정상8경로 heading/새로고침·리포트 상세20건, 정상 API응답26개 전부200, 503 오류 표시·재시도 회복, static/SPA/revision nosniff, 375px overflow없음·pageerror0 |
| 새DB 복원 | 백업10건/pending outbox1개, 최신 원래DB20건/알림2개. 복원DB+기존group 시작은 RECOVERY_ALIGNMENT_REQUIRED·exit2·OOM없음 |
| DB/Kafka 위치 복구 | 새group6개를 DB next에 준비. 최종20건·피드2개·미발행0·선택한 ledger/집계 hash차이0. 기존group offsets 전후일치·chatpulse와restored DB이름 차이 확인 |
| 종료/정리 | 최종SIGTERM1.255초·exit143·OOM없음, 소유컨테이너/볼륨 제거. 생성 secret/dump 삭제. 진행중 HTTP요청완료 시험은 아님 |

17단계 모두 통과했다. 마지막 `./scripts/check`는 Java142·하네스16·JAR가 통과했다. 전체 실제 Kafka/DB 회귀와 최종 CI 결과는 STATUS/정제요약에 이어 기록한다.

최신6차의 복원 후 검증 완료까지 **18.900초**였다. 서로 다른 코드/검사 강도의6회 범위는17.105~24.188초이며 분포나 운영RTO 추정으로 사용하지 않는다. 기존 API 정지 후 새DB 생성 직전부터 dump복원·거부진단·새group준비·readiness·집계 및 기존offset 대조까지 포함한다. 장애감지/운영자 판단/클라우드 provisioning은 제외한다. 보존 Kafka가 있는 합성20건의 누락10→0이며 실제 원천 RPO0 또는 운영 RTO 보장이 아니다.

snapshot은 ledger eventId/원천offset, 채팅·category·report bucket, observation eventId, notification/spike ID, outbox발행상태를 정규화해 hash 대조한다. 모든 payload 열·updatedAt·snapshot/evaluation 과거 이력을 완전 비교한 것은 아니다. synthetic spike/outbox를 SQL로 주입했으므로 급상승 탐지 전체 경로 검증으로 확대하지 않는다.

## 이전 실행과 리뷰 보완

첫16단계(88b30e6)와 두 번째17단계(36f3503)는 원본을 유지한다. 당시 브라우저는 SPA shell/overflow만 검사했고 등록되지 않은 `/reports`도 포함했다. exit!=0만 확인한 startup 실패·상수였던 일부 진단 필드를 원인/측정 근거로 해석하지 않는다. Claude의 변경 요청 후 stable코드·DDL42501·기존offset실조회·실제API/화면을 추가한230852b의 세 번째17단계가 통과했다. 네 번째는 #28/#31 통합과완료된분 구간 고정까지 반영했다.

별도 RecoveryPlanIntegrationTest4개는 Kafka앞섬·새groupDB위치·기존offset불변·보존만료·동명topic재생성·Flyway checksum/미래migration거부를 검증한다. 최초setup4실패는 Admin timeout설정 충돌이었고 정확한 소유schema4/topic16만 정리했다. 이 실패와 후속은 WORK_LOG/정제요약에 남겼다.

## 리팩토링과 미검증 범위

FailureDiagnostics로 startup/CLI 허용코드와 예외 타입명을 공유하고 실제 메시지·비밀을 출력하지 않는다. 복원 계획은 RecoveryPlan, schema/connection은 PostgresSupport, 비밀/정책은 RuntimeEnvironment가 담당한다. 관측 HTTP예산은 실제 전송 직전 hook에서 매 요청에 적용하며 offline fallback2회도 단위 HTTP fixture로 대조한다. 기존 ObservationCollector와fleet 임대수명을 재사용했다.

T16은 부분검증이다. 동일V12 이미지 복귀와strict미래migration거부만 시험했고 실제새migration을포함한릴리스의복원/재생까지연결한시험은아직없다. 해당작업을시작할때이전schema백업/보존입력/실행이미지로추가리허설한다. 외부TLS·다중AZ·DBstandby/PITR·중앙경보·비밀회전·실제동의/전체방송수집은이로컬실행범위밖이다.

다섯번째는운영진단코드확대·FLEET_LIVE_DISABLED실제확인·로컬관측요청생략계약을반영한이미지다. 첫네실행을보존한다. UI503은브라우저요청모의이며실제프록시503은DB중단HTTP단계에서별도로검증한다. 로컬전체91개통과실행은종료직전후속빌드와겹쳤으므로고정commit최종근거에서제외하고최종CI를사용한다.

여섯 번째는 수집 전 Kafka 준비, 예상한 SourceFailure만 관측 생략, 종료 후 생략 횟수 보존까지 통합한 이미지다. 단위142개와17단계 리허설을 통과했다. 서로 다른 코드의 복구6회 표본은17.105~24.188초이며 운영 SLA로 해석하지 않는다. 관측 활성 FleetCli의 실제 원천 호출은 실행하지 않았다.
