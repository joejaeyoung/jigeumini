# 진행 상태

2026-10-26 · 기능 이슈 병합·후속 구조 개선 회귀 기준

## 기술 검증 보강 #41~#49

2026-10-26 사용자 요청으로 미검증 경계를 독립 이슈로 등록했다. 기반은 PR #40 병합 `dcdc244`다.
구현 이슈 완료와 기술 검증 범위를 구분하며, 아래 외부 조건이 준비되지 않은 이슈는 실제 실행을 완료로 표시하지 않는다.

| 이슈 | 기술 목표 | 상태·선행 조건 |
|---|---|---|
| [#41 trend SQL rollback·재전달](https://github.com/joejaeyoung/jigeumini/issues/41) | T02/T03/T05/T08 | 완료: [PR #50](https://github.com/joejaeyoung/jigeumini/pull/50) 최종 `bde3a47` Claude APPROVE·push40253684968/PR40253689010 성공 후 `c2aeffc`로 병합. [검증](pipeline/TREND_RECOVERY_VERIFICATION.md)·[정제 근거](poc/evidence/trend-recovery-2026-10-26.json) |
| [#42 새 migration 릴리스 복구](https://github.com/joejaeyoung/jigeumini/issues/42) | T04/T16 | [PR #51](https://github.com/joejaeyoung/jigeumini/pull/51) `44615d0` Claude APPROVE·push40254939425 성공, PR40254943726 Outbox 시험 실패로 병합 대기(#52). V13 복구16단계와 V12 회귀17단계 통과; 근거는 PR51 문서 |
| [#43 전체 집계 동시 부하·지속 실행](https://github.com/joejaeyoung/jigeumini/issues/43) | T02/T07/T08/T11/T12 | 진행: 전체 집계·API uniform/hot/steps와 30분 soak 실제 통과. 실행 결과·최종 리뷰 정리 중(#43 브랜치) |
| [#44 수집기 시계·네트워크 분리](https://github.com/joejaeyoung/jigeumini/issues/44) | T02/T11/T14 | [PR #58](https://github.com/joejaeyoung/jigeumini/pull/58) 실제 시계·DB/원천 분리·인계 시험 통과. 최종 통합 전 선행 #52/#54 반영 필요 |
| [#45 Kafka 디스크 영구 손실](https://github.com/joejaeyoung/jigeumini/issues/45) | T04/T10/T15 | [PR #56](https://github.com/joejaeyoung/jigeumini/pull/56) 격리 디스크 유실 fixture 구현·진입 리뷰 보완 중; 실제 손실 실험은 미실행 |
| [#46 PostgreSQL 시점 복구](https://github.com/joejaeyoung/jigeumini/issues/46) | T02/T04/T16 | [PR #57](https://github.com/joejaeyoung/jigeumini/pull/57) physical PITR fixture·진입 리뷰 완료; 실제 base backup/WAL 복원 실험은 미실행 |
| [#47 실제 공식 인증·동의 수집](https://github.com/joejaeyoung/jigeumini/issues/47) | T13/T14 | 외부 조건 필요: 공식 앱·동의 채널·안전한 자격 증명 주입 경로 |
| [#48 실제 다중 호스트/AZ](https://github.com/joejaeyoung/jigeumini/issues/48) | T08/T10/T15 | #43/#45 및 외부 계정·리전·AZ·예산 필요 |
| [#49 외부 배포·운영 복구](https://github.com/joejaeyoung/jigeumini/issues/49) | T08/T13/T15/T16 | #42/#43/#46/#47/#48 및 배포 대상·도메인·운영 목표 필요 |
| [#52 Outbox 거부 시험 준비·CI 안정화](https://github.com/joejaeyoung/jigeumini/issues/52) | T03/T08/T10 | 로컬 보완 검증 완료: `cb098c0` 단위147·하네스20·실제 broker 독립20회 통과. Standards/Spec 차단0. [조사](research/OUTBOX_READINESS.md)·[근거](poc/evidence/outbox-readiness-2026-10-26.json). 원래 CI 원인은 미확정이며 최종65ff2d7 Claude APPROVE·PR40258127624 성공, push40258124001 기존 Fleet 실패로 병합 대기(#54) |
| [#54 수집기 시작 진단·CI 인계](https://github.com/joejaeyoung/jigeumini/issues/54) | T08/T14 | 로컬 재현·보완 완료: 초기 bootstrap 차단으로 metadata timeout·보고서 누락 재현, 임대 전 timeout 최대5회 후 복구/영구 차단을 실제 검증. ea3eb34 단위151·하네스20·Fleet13 통과. [조사](research/FLEET_STARTUP.md)·[근거](poc/evidence/fleet-startup-2026-10-26.json). 과거 CI 원인 미확정; PR55 최종 게이트 대기 |

공개 원천과 외부 자원은 준비 조건을 별도로 확인한다. 로컬 검증은 합성 입력과 해당 실행 소유 자원만 사용한다.
이번 검증을 위해 개발 Kafka/PostgreSQL을 다시 기동했다. 다른 worktree와 서비스는 변경하지 않는다.

## 전체 코드 구조 개선 #39

`refactor/codebase-depth` · 기준 `c48a190` · [이슈 #39](https://github.com/joejaeyoung/jigeumini/issues/39).
서버·웹·실행 스크립트를 검토하고 소비 공통 계약, 수집기 실행 자원, 웹 시간축/표현, 관리 CLI의 JAR 수명을 정리한다.
F01/F03/F04/F05/F06/F07/F08/F13/F16 → T01/T02/T03/T04/T05/T08/T11/T14/T16의 기존 계약 보존 작업이다.
구현·로컬 회귀 완료. [구조 결정](REFACTORING.md#2026-10-26-전체-코드-검토-39)과 [정제 근거](poc/evidence/refactor-2026-10-26.json)에 기준/후속 commit·JAR hash·실행 범위를 연결했다.
최초 `e5222e0`의 실제 Kafka/DB94개, 리뷰 반영 `07021e4`의 단위147·하네스20·수집기 집중3개, 지정 Node24.20.0의 웹29·Chromium31이 통과했다.
Standards/Spec 정적 검토는 각각 지적0건이고 최초 코드의 Claude 승인을 확인했다. 최종 통합 근거는 [PR #40](https://github.com/joejaeyoung/jigeumini/pull/40)의 정확한 최종 HEAD Claude 승인·push/PR CI·병합 기록이다. 기존 기술 목표의 검증 범위는 승격하지 않는다.

| 단계 | 현재 상태 | 다음 통과 조건 |
|---|---|---|
| M0 익명 수집 PoC | 기술 검증 통과: 2채널 30분·재접속, 최종 JAR 스모크·자동 테스트 | 공개 서비스 이용 조건·장기 운영 범위는 별도 확인 |
| M1 한 채널 수직 구현 | 완료: 실수신→Kafka→DB→조회, 34개 테스트·재시작 검증 통과 | 후속 집계·복구 검증은 아래 완료 근거 참조 |
| M2 순위·급증 기능 | #4·#12·#13 Claude 승인·CI 통과 후 develop 병합 완료 | 실방송 장기 관측·운영 확대·고부하 평가 비용 |
| M3 장애·재처리 | #9/#10/#8/#16 구현·검증·develop 병합 완료 | 실제 원천 공백·보존 만료 입력은 복원하지 않음 |
| M4 성능·운영 검증 | #11/#26/#27/#28/#29/#30/#31 구현·리허설·develop 병합 완료 | 실제 원천 인증·동의, 외부 배포·운영 SLA, 새 migration 릴리스 복구는 별도 범위 |

세부 근거: [PoC 검증 기록](poc/VERIFICATION.md). 제품 범위: [PRD](design/PRD.md).

M1 실행·결정: [Kafka·PostgreSQL 파이프라인](pipeline/README.md).

## 이슈 완료 요약

요청한 기존 이슈 7건과 운영 확장 이슈 6건을 각각 구현·검증하고 develop에 병합했다. 각 최종 HEAD에 Claude APPROVE와 push/PR CI SUCCESS를 확인했으며, 2026-10-26 기능 작업 종료 시 열린 이슈0건을 확인했다. 이후 사용자 요청의 구조 개선은 위 #39에서 별도로 추적한다. 아래 이력의 테스트 수는 당시 실행 범위로 보존한다.

| 이슈 | 작업·기능 | 기술 목표 | 병합 PR |
|---|---|---|---|
| #10 | DLQ·소비 복구 진단 F09 | T01/T03/T05/T08 | [#19](https://github.com/joejaeyoung/jigeumini/pull/19) |
| #8 | Outbox 관심 방송 알림 F07 | T02/T03/T08/T10 | [#20](https://github.com/joejaeyoung/jigeumini/pull/20) |
| #5 | 시청자 관측·채팅 비교 F04 | T02/T04/T06 | [#21](https://github.com/joejaeyoung/jigeumini/pull/21) |
| #6 | 카테고리 순위 F05 | T02/T06/T07/T08 | [#22](https://github.com/joejaeyoung/jigeumini/pull/22) |
| #7 | 방송 리포트 F06 | T02/T04/T08 | [#23](https://github.com/joejaeyoung/jigeumini/pull/23) |
| #16 | Replay CLI 보완 F08 | T01/T02/T03/T04 | [#24](https://github.com/joejaeyoung/jigeumini/pull/24) |
| #11 | 부하·편향·복구 실험 F10 | T06/T07/T08/T09/T11/T12 | [#25](https://github.com/joejaeyoung/jigeumini/pull/25) |
| #26 | 방송 발견·팔로워 기준 선정 F12 | T13/T08/T11 | [#32](https://github.com/joejaeyoung/jigeumini/pull/32) |
| #27 | 유한 비동기 발행 F14 | T02/T06/T10/T11/T12 | [#33](https://github.com/joejaeyoung/jigeumini/pull/33) |
| #29 | Kafka HA·장애 복구 F15 | T04/T08/T10/T15 | [#34](https://github.com/joejaeyoung/jigeumini/pull/34) |
| #28 | 수집기 배정·임대·인계 F13 | T13/T14/T02/T11 | [#35](https://github.com/joejaeyoung/jigeumini/pull/35) |
| #31 | ACK·커밋 방식 비교 F11 | T01/T10 | [#36](https://github.com/joejaeyoung/jigeumini/pull/36) |
| #30 | 배포·백업 복구 리허설 F16 | T04/T08/T16 | [#37](https://github.com/joejaeyoung/jigeumini/pull/37) |

최종 기능 PR #37의 `3b6d662` CI는 **서버 단위142·실제 Kafka/PostgreSQL92·하네스16·웹 단위29·Chromium31**을 통과했다. 마지막 실제 이미지/하네스는 clean `1bde2b7`이며 배포17단계가 통과했다. [최종 게이트 근거](poc/evidence/final-gates-2026-10-26.json)에 HEAD·리뷰·CI·XML hash·병합 커밋을 연결했다.

공개 채널3개는 익명 PoC의 자체 상한이다. 전체 목록 발견과 채팅 접근 권한을 분리했고, 팔로워1,000명은 민감도 비교가 가능한 검토안이다. 실제 공식 인증·동의/전체 수집(T13), 새 migration을 포함한 릴리스 복구·외부 운영(T16)은 부분 검증으로 유지한다. [원천 조사](research/CHZZK_SCALE.md)·[선정 계약](discovery/README.md)·[Kafka/인프라 조사](research/KAFKA_DEPLOYMENT.md)·[배포 운영안](DEPLOYMENT_OPERATIONS.md)에 재개 조건을 기록했다.

기능 작업 종료 시 로컬 PoC/API/웹과 테스트 Kafka/PostgreSQL을 정지했다. #39의 실제 회귀에서 테스트 Kafka/PostgreSQL만 재기동하고 검증 후 다시 정지했다. 테스트 데이터 볼륨은 보존했고 리허설 소유 자원·생성 비밀·dump는 정리했다. 다른 worktree와 `phone-*` 서비스는 변경하지 않았다. main 릴리스와 외부 배포는 실행하지 않았다.

### 관심 방송 알림 #8

`feat/spike-notifications`, F07 → T02/T03/T08/T10. [알림 계약](pipeline/NOTIFICATIONS.md)의 spike/outbox 원자 저장·ACK 뒤 전달 완료, 독립 feed consumer와 멱등 저장, 브라우저 관심/읽음·지연/정정 표시를 구현했다. [검증 기록](pipeline/NOTIFICATIONS_VERIFICATION.md)·[정제 근거](poc/evidence/notifications-2026-10-25.json): Java 단위67·하네스16·실제 Kafka/DB47·웹 단위21·Chromium25와 빌드 통과. outbox commit/ACK 직후 JVM 종료, broker 거부, feed SQL rollback, 60초 소비 중단 중 ranking 진행·재시작 후 회복을 확인했다.

단일 broker/RF=1·합성 입력 범위다. broker 전체 단절·복제 HA·고부하는 #11에서 구분하며 feed/live ledger 자동 정리는 남는다. [PR #20](https://github.com/joejaeyoung/jigeumini/pull/20)은 최종 HEAD `ad30d4c` Claude APPROVE, push CI40198062793·PR CI40198067014 SUCCESS 후 `9bd4e8a`로 develop에 병합했다. #8과 보드는 완료이며 병합 후 develop CI40198557216도 통과했다.

Claude 초기 리뷰는 `8f23629` APPROVE다. 비차단 지적 중 웹의 600초+1마이크로초 지연 경계 오류를 재현해 수정했고, 저장 payload 오류의 고정 503·명시적 topic bean 주입·순환 소수 ratio 검증을 보완했다. `c031171`의 로컬 회귀63/16/45/21/25와 Claude 승인을 확인했다. 토픽 초기화 보완 `d8f3955`의 최종 서버67/16/47 회귀와 Claude 승인 후 최종 문서 HEAD `ad30d4c`도 Claude APPROVE·push/PR CI를 통과했다. 웹은 이후 변경 없이21/25 근거를 유지한다. 영구 outbox 선두 차단·버전 이행·오래된 CreateTime 보존과 공유 연결 비용은 계약에 명시했다.

사용자 요청의 [실행 확인](poc/evidence/runtime-smoke-2026-10-25.json): 실제 서버에 연결한6개 경로에서 JS/API 오류0, 데스크톱과320px 알림 화면의 가로 넘침0. 공개3채널은 완료1분 순위에서 모두 HEALTHY였다. 약5분30초의 수신6,965=ACK6,965=DB ledger/bucket 증가6,965, 거절/미확인0. 사용자가 확인 후 종료를 요청해 계획600초 전에 중단했으며 CANCELLED를 완료 시험PASS로 바꾸지 않는다. 실제 급상승/알림은 관측하지 않았다. 수집기·18080·18082는 종료하고 통합 검증용 Kafka/DB만 유지한다. 원문·제목·채널명은 검증 출력에 저장하지 않았다.

### 토픽 초기화 간헐 실패 보완

PR #20의 `3977d34`는 Claude 승인·push CI를 통과했으나 PR CI run `40196842128`에서 `DeadLetterIntegrationTest.setup → ensureTopic.describeConfigs`의 UnknownTopicOrPartitionException으로 실패했다. PR #18과 같은 위치다. 실제 로컬 생성/조회200회에서는 재현0회이며 broker 내부 원인은 미확정이다. 생성·토픽 조회 성공 뒤 설정 조회가 한 번 실패하는 Admin 응답을 주입해 기존 시작 실패를 재현했고, 해당 오류만 공통10초 기한 안에서 재조회하도록 보완했다. 권한/설정 오류 즉시 거부, 중단 신호 복원, 종료 한도의 단위 회귀4개와 실제 새 토픽20개의 최초/기존 초기화·잘못된 설정 거부가 통과했다. `d8f3955`의 전체 단위67·하네스16·실제 Kafka/DB47 통과와 Claude APPROVE를 확인했다. [처리 계약](pipeline/TOPIC_READINESS.md)·[정제 근거](poc/evidence/topic-readiness-2026-10-25.json)에 원인 미확정과 API 응답 회귀의 범위를 구분한다. 최종 `ad30d4c`의 push/PR CI가 모두 통과했고 PR #20으로 병합했다.

### 시청자 관측·채팅 비교 #5

`feat/viewer-observations`, F04 → T02/T04/T06. [계약](api/OBSERVATIONS.md)에 새60초 관측·nullable 수치·원천 방송 ID·독립 Kafka 이력/멱등 저장·7일 정리·동일 시간축 그래프를 구현했다. [실제 공개 필드 확인](poc/evidence/observation-source-2026-10-25.json)은3채널 각1회 HTTP 표본이며 장기 live 검증과 구분한다. [검증 기록](api/OBSERVATIONS_VERIFICATION.md)·[정제 근거](poc/evidence/observations-2026-10-25.json): 최종 서버/웹 구현 `8eaf39b`의 단위76·하네스16·웹25·Chromium26, 기본값과 다른 환경 변수 TTL 검사를 보완한 `bd81332`의 실제 Kafka/DB53이 통과했다. 역순·중복·null·DB rollback·재시작·DB 직후 halt(97)·이전 run을 포함한7일 정리·분 경계 정밀도·실제 HTTP→Spring collector→Kafka/DB 연결·비어 있지 않은 서버 JSON/웹 공유 fixture·모바일44px 선택을 확인했다.

초기 `1cd60af`와 보완 `8eaf39b`는 Claude APPROVE다. PostgreSQL의 나노초→마이크로초 반올림 실패를 재현해 내림으로 수정했고, 카테고리 값 정제와 영구 계약 오류의 보류·직렬화 이행 한계를 명시했다. [PR #21](https://github.com/joejaeyoung/jigeumini/pull/21)은 최종 `5ec96db` Claude APPROVE·push CI40201134329·PR CI40201136793 SUCCESS 후 `98fe3a3`으로 develop에 병합했다. #5·보드는 완료이며 병합 후 develop CI40201674996도 통과했다.

관측은 기존 수집기 파일 잠금 이후 채널 start에서 활성화하고 stopped 이후 새 조회를 멈춘다. 다른 호스트 간 lease는 없다. 비압축 이력을 보장하기 위해 공통 토픽 초기화에서 cleanup.policy=delete를 명시·검사한다. 기존 compact 토픽은 설정을 바꾸지 않고 거부한다. 원천 시각/시청자 수를 추정하거나 현재값을 과거로 채우지 않는다. 기존 replay CLI는 관측 토픽을 지원하지 않는다. 사용자가 확인한 live PoC는 내려둔 상태를 유지한다.

### 카테고리 반응 #6

`feat/category-reactions`, F05 → T02/T06/T07/T08. [계약](api/CATEGORIES.md)의 독립 채팅/관측 소비와 시각 결합·원자 보정·품질 조회·웹을 구현했다. [검증 기록](api/CATEGORIES_VERIFICATION.md)·[정제 근거](poc/evidence/categories-2026-10-25.json): 리뷰 보완 `de67c97`에서 Java79·하네스16·실제 Kafka/DB61, 변경 없는 웹9cedeff에서 단위27·Chromium28과 빌드가 통과했다. c7b80b4·de67c97 Claude APPROVE를 확인했다. [PR #22](https://github.com/joejaeyoung/jigeumini/pull/22)은 최종3dfae68 Claude APPROVE·push CI40203009525·PR CI40203013704 SUCCESS 후5a419e3으로 develop에 병합했다. #6·보드는 완료다. 채팅 이전 최신 관측의 허용 나이는 min(120초, 표본TTL), 미상/만료는 unknown으로 분리한다. 늦은 관측은 해당 시각부터120초 범위의 보존된 배정만 원자적으로 정정한다. 별도 작업 폴더에서 개발했고 승인된 #5의 `98fe3a3`을 통합했다. 실제 JAR의6개 partition·비어 있지 않은 JSON과 모바일 그래프·기여 방송을 확인했다. 부하 편향·다중 소비자 수치는 #11 실험 전까지 완료로 주장하지 않는다.

### 방송 요약 #7

`feat/broadcast-reports`, F06 → T02/T04/T08. [계약](api/REPORTS.md)·[검증 기록](api/REPORTS_VERIFICATION.md)·[정제 근거](poc/evidence/reports-2026-10-25.json). 독립 관측 세션/별칭/귀속/집계, DB 원자 보정·BATCH commit, 품질 완료 분·피크, API/웹과 종료7일 보존을 구현했다. 초기55726f0의 Java84·하네스16·실제Kafka/DB68·웹29·Chromium31 이후 리뷰로 부분 정리 잔여 관측 재계산과48시간 재개 충돌을 수정했다. 976f88e의85/16/70에 이어 최종e1980f6 로컬Java85/하네스16/JAR, 최종CI 실제Kafka/DB71(리포트10)/웹29/Chromium31이 통과했다.

[PR #23](https://github.com/joejaeyoung/jigeumini/pull/23)은 최종e1980f6 Claude APPROVE·push40206632119/PR40206635890 성공 후 `a69760a`로 develop에 병합했다. #7·보드는 완료다. 공통 소비 수명주기 추출과 세션/품질/피크 경계의 [리팩토링 점검](WORK_LOG.md)을 남겼다. 단일broker/RF1·합성 입력 범위이며 장기 미종료 세션 비용·전체 원천 수집·HA는 별도다.

CI40204570648의 pong 미응답 fixture는 WS_CLOSED 뒤 timeout 관측이 없어 실패했다. 1.2초 승인 지연으로 기존1초 예산 결함을 재현하고 실제 채팅/ping/pong0/timeout을 기다리도록 보완했으나 원래 연결 종료의 내부 원인은 미확정이다. CI40205131666·40205136741의 나노초 기대값은 PostgreSQL 마이크로초 계약에 맞췄다. 만료 원천ID 재등장도 부분 정리 후 합산하지 않고 rollback→정리→재시도로 검증했다.

## 재처리 CLI 후속 보완 #16

`fix/replay-diagnostics`, F08 → T01/T02/T03/T04. [진단 검증](pipeline/REPLAY_DIAGNOSTICS_VERIFICATION.md)·[정제 근거](poc/evidence/replay-diagnostics-2026-10-26.json): 조회의 migration/watchdog 분리·readonly, 명시적migrate, 동일run/다른workerbuild 비교 거부, 보존offset과제한된Kafka시각 표본, 오류구분, runtimeJAR 정리를 구현했다. #5/#6/#7 통합ff5c30f에서 Java84·하네스16·실제Kafka/DB74·JAR 통과,8ac4f77/61784de Claude APPROVE를 확인했다. 선행#7의heartbeat fixture 보완을 통합한63ff05d에서도 Java84·하네스16이 통과했으며 a8ef43a(63ff05d 이후 문서 변경만)의 Claude 최종 APPROVE를 받았다. CI40205388582·40205422320은 선행리포트의 시각 정밀도 시험에서 실패했다. #7의976f88e 수정과791ff1a 근거를 통합한 c222ee6 push40206443045/PR40206447239가 성공했다. 승인·병합된 최종#7(e1980f6/a69760a)을537b3cc에서 통합했고 [작업 로그](WORK_LOG.md)에 리팩토링 점검을 기록했다. 최종7863093 Claude APPROVE·push40207808425/PR40207811784 성공 후 PR#24를cf4049e로develop에병합했다. 최종CI Java85/실제KafkaDB77이며 #16·보드는완료다. 실제controlrecord꼬리/중간·전역workerlock·readonly상태조회·변조JAR코드를 검증했으며 서로다른build비교는 DB값을 바꾼 합성주입이다. SIGKILL/HUP종료·변조JAR workerclaim은 미검증이며 live/외부배포는 수행하지 않았다.


## 오류 격리·복구 진단 #10

`feat/dlq-recovery` · F09 → T01/T03/T05/T08. `b58d084`의 origin/develop에서 시작했다. [격리·진단 계약](pipeline/DLQ.md)대로 ranking/trend의 영구 계약 오류만 격리하고, DLQ ACK 후 정상 ledger/bucket·격리 기록·진행 위치를 같은 DB 트랜잭션에 저장한다. DB commit 후 Kafka BATCH offset commit을 유지한다. 같은 배치의 eventId 충돌 후보는 모두 제외하고 기존 ledger는 보존한다.

[검증 기록](pipeline/DLQ_VERIFICATION.md)과 [정제 근거](poc/evidence/dlq-2026-10-25.json): 서버 단위 58개·하네스 16개·실제 Kafka/PostgreSQL 40개·웹 단위 17개·Chromium 23개, JAR/웹 빌드 통과. DLQ broker 거부/복구·ACK 직후 halt(93)·DB commit 직후 halt(94)·DB 장애·재전달·양쪽 projection·보존 만료·topic 재생성·실행 JAR HTTP를 확인했다. Claude 최초 리뷰의 정상 offset 재분류 문제를 재현해 수정했고, 재전달 사이의 보존 시각 초과·새 충돌 및 ACK 뒤 SQL rollback을 추가 검증했다. 격리 기록은 신규/기존 채널의 품질 제외와 과거 순위·급상승 정정에 반영하며 운영 API/화면은 0 lag·미관측·만료를 구분한다. [PR #19](https://github.com/joejaeyoung/jigeumini/pull/19)는 Claude 승인·최종 HEAD push/PR CI 통과 후 `c86f670`으로 develop에 병합됐다. 병합 후 develop CI도 통과했고 #10·보드는 완료다.

합성 입력·단일 broker/RF=1 범위다. broker 전체 중단·네트워크 단절·복제 HA·새 live·고부하는 미검증이다. 오류 수정 검증은 별도 replay만 허용하며 자동 재투입하지 않는다. #11의 부하/확장/장시간 회복과 #16 replay 보완은 남는다.

시험 설계에서 RF=1/min.insync.replicas=2로는 Kafka 4.1.2의 유효 최소 ISR 제한 때문에 발행이 거부되지 않음을 확인했다. 이를 장애 근거로 계산하지 않고 DLQ max.message.bytes=64의 실제 broker 거부로 교체했다. PR #18 push CI의 `describeConfigs` 단계 `UnknownTopicOrPartitionException`은 같은 HEAD 재실행에서 통과했으나 원인은 미확정이며 이번 작업에서 근본 원인을 수정하지 않았다.

## 코드 구조 리팩토링

2026-10-25 · `refactor/module-boundaries` · [이슈 #17](https://github.com/joejaeyoung/jigeumini/issues/17).
[구조 검토와 결정](REFACTORING.md)에 전체 영역의 후보와 선택 이유를 기록했다. CollectionQuality 추출, PostgresSupport 초기화 통합, 웹 API 계약·HTTP 모듈 분리를 구현했다. Java 단위 55개·하네스 16개·실제 Kafka/PostgreSQL 29개·웹 단위 17개·Chromium 22개와 JAR/웹 빌드가 통과했다. [정제 근거](poc/evidence/refactor-2026-10-25.json)의 코드 commit·JAR hash로 실행 범위를 고정했다. [PR #18](https://github.com/joejaeyoung/jigeumini/pull/18)은 Claude 승인·최종 CI 통과 후 `b58d084`로 develop에 병합됐으며 #17은 완료다.

F03/F08 → T01/T02/T04/T08/T11의 기존 계약 보존 작업이며 기술 목표 상태를 새로 승격하지 않는다. 외부 API·SQL·transaction/offset 경계·품질/시간 기준·DB schema는 유지했다. 서버/CLI의 Hikari 한도·초기화와 웹 요청 timeout/취소·Zod 검증도 동일하다. 새 live·고부하·HA 검증은 수행하지 않았다. #16의 replay 정책 변경과 #10·#11은 별도 범위다.

## 고정 offset 재처리 #9

`feat/kafka-replay` · F08 → T01/T02/T03/T04. [재처리 계약](pipeline/REPLAY.md)의 로컬 CLI로 manifest 생성·작업 생성/조회/취소·worker·watchdog·결과 비교를 구현했다. 원본 topic을 만들거나 운영 group offset·집계·snapshot·알림을 수정하지 않는다. 실패 run은 자동 재개하지 않고 새 run으로 재실행한다. [검증 기록](pipeline/REPLAY_VERIFICATION.md): 단위 55개·하네스 16개·실제 Kafka/PostgreSQL 통합 29개(재처리 10개)가 통과했다. [PR #15](https://github.com/joejaeyoung/jigeumini/pull/15)는 Claude 승인·CI 통과 후 develop에 병합됐고 #9·보드는 완료다. 비차단 후속 제안은 #16에서 추적한다.

## 기술 목표 추적표

2026-10-25 · 브랜치 `feat/technical-goals` · [PR #14](https://github.com/joejaeyoung/jigeumini/pull/14).
[기술 목표·기능 매핑](TECH_GOALS.md)의 T ID별 상태 정본이다.
기존 검증은 `44b6321` M1 기록을 기준으로 연결하고 #3의 독립 소비·품질 관측 검증 범위를 추가했다. `검증됨`은 아래 범위만 의미하며 기능 전체 완료와 구분한다.

<!-- tech-status:start -->
| 기술 ID | 상태 | 검증 근거 | 확인한 범위 | 남은 문제·다음 행동 |
|---|---|---|---|---|
| T01 | 검증됨 | [ACK·commit 대조](pipeline/ACK_COMMIT.md), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | 실제 AUTO/BATCH/MANUAL 각3개 종료 경계·재시작 대조, 정상9건 누락0/중복0, 잘못된 다음 poll 순서에서누락1 재현 | 합성 단일 입력·지정 client/container 버전 범위, 제품BATCH 유지 |
| T02 | 검증됨 | [trend SQL rollback·동일 offset 재전달](pipeline/TREND_RECOVERY_VERIFICATION.md), [리포트 귀속·중복·rollback](api/REPORTS_VERIFICATION.md), [관측 중복·역순·rollback](api/OBSERVATIONS_VERIFICATION.md), [알림 멱등·rollback](pipeline/NOTIFICATIONS_VERIFICATION.md), [DLQ 멱등·충돌](pipeline/DLQ_VERIFICATION.md), [M1 중복·rollback](pipeline/VERIFICATION.md#자동-검사), [M2 독립 소비](api/QUALITY_RANKINGS.md#검증-범위), [재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | M1 재전달·충돌·SQL rollback, ranking/trend의 동일 eventId 중복 입력을 각각 1건 반영; replay 배치 내부/간 중복·충돌·SQL rollback; 알림 spikeId 중복·충돌·SQL rollback | trend commit 시점 ledger/bucket/progress 쓰기 후 rollback·실행 중 회복·DB commit 직후 halt와 동일 offset 재수신도 검증. 원천 중복·전체 Exactly-once는 범위 밖 |
| T03 | 검증됨 | [trend 복구](pipeline/TREND_RECOVERY_VERIFICATION.md), [Outbox commit·ACK 직후 종료](pipeline/NOTIFICATIONS_VERIFICATION.md), [DLQ ACK·DB commit 직후 종료](pipeline/DLQ_VERIFICATION.md), [JVM 종료·재시작](pipeline/VERIFICATION.md#자동-검사), [replay worker 종료](pipeline/REPLAY_VERIFICATION.md), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | DB commit 직후 halt(91), 동일 group 재전달 뒤 집계 1건; replay commit 후 JVM 종료·watchdog 실패·새 run 재실행; DLQ ACK 후 halt(93)·DB commit 후 halt(94), 재전달 격리 1건·집계 2건; F07 outbox commit halt(95)·ACK halt(96) 후 물리2/feed1 | 모든 장애·고부하 복구로 확대 해석하지 않음 |
| T04 | 검증됨 | [재처리 후속 진단](pipeline/REPLAY_DIAGNOSTICS_VERIFICATION.md), [리포트 active 보호·정리](api/REPORTS_VERIFICATION.md), [관측7일 보존](api/OBSERVATIONS_VERIFICATION.md), [고정 입력 재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | 동일 manifest 2회·알려진 bucket 대조, 고정 end·만료/재생성/증설 거부, 취소·worker 종료·live 불변 | 보존된 offset만 대상; 삭제 입력·미래 지연 입력·운영 snapshot 재현·HA는 범위 밖 |
| T05 | 검증됨 | [trend 복구](pipeline/TREND_RECOVERY_VERIFICATION.md), [DLQ·DB 장애·재전달](pipeline/DLQ_VERIFICATION.md), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | 형식/schema/충돌 격리·정상 입력 진행, ACK 전 차단, ACK 직후 종료·동일 failureId 중복, DB 복구와 원자 rollback | 단일 broker의 발행 거부 범위; #11 실제 단절·대량 오류/장시간 회복, 자동 정리 정책은 별도 |
| T06 | 검증됨 | [고정/증설·복수 producer·늦은 시각](pipeline/EXPERIMENTS_VERIFICATION.md), [카테고리 결합](api/CATEGORIES_VERIFICATION.md) | 로컬3partition에서 채널 배치·append/eventTime 차이, 동일UUID 재전달·두producer bucket 정합성, 실제3→6증설의 키 이동·기존manifest 거부 | 실방송 전역 순서/외부 원천 중복 제거 보장이 아님; 운영 증설은 별도 이행 |
| T07 | 검증됨 | [균등/80% 편향·shard 대조](pipeline/EXPERIMENTS_VERIFICATION.md) | 동일입력30000/물리30300, channelId/shard3의 partition·lag·p95/p99·DB/replay 대조; 현재 제품키 channelId 유지 결정 | 단일호스트·단회 측정. shard는 품질/offset 계약 이행 전 제품 채택 안 함 |
| T08 | 검증됨 | [trend 복구](pipeline/TREND_RECOVERY_VERIFICATION.md), [consumer1~4·독립 회복](pipeline/EXPERIMENTS_VERIFICATION.md), [복구 진단](pipeline/DLQ_VERIFICATION.md), [#39 회귀](poc/evidence/refactor-2026-10-26.json)  #54 초기 bootstrap timeout의 제한 재시도·복구/유한 실패는 [조사](research/FLEET_STARTUP.md) 범위로 추가 검증.| partition 입력/commit률·lag·DB 지연·버퍼 나이, 4번째 유휴, A60초 중단 중 B진행·최종50000 일치·회복 표본상한 | 전체 제품 projection의 동시 부하·자동 확장 운영은 미검증. 표본 사이 순간 최대치 미확인 |
| T09 | 검증됨 | [독립 heartbeat/poll/membership 장애](pipeline/EXPERIMENTS_VERIFICATION.md) | 별도JVM STOP/CONT와 재할당/재가입, max.poll6초·처리8초 정지, consumer1→2→4→3의 commit/중복/DB/replay 정합성 | heartbeat A 지연은 부모 표본만 포함. 복구 시각은 최초0lag 표본까지 상한 |
| T10 | 검증됨 | [ACK·commit 대조](pipeline/ACK_COMMIT.md), [HA 검증](pipeline/KAFKA_HA_VERIFICATION.md) | RF3/minISR2/ELR1에서 ACK0/1/all, ISR1·마지막leader상실/복구 후broker/DB집합차이0; ACK0확인보장없음·all의ISR1거절 | 원래디스크복귀범위; 유일replica디스크영구손실·AZ내구성미검증 |
| T11 | 검증됨 | [실제 broker PAUSE/STOP·버퍼/메모리](pipeline/EXPERIMENTS_VERIFICATION.md), [품질 계측](api/QUALITY_RANKINGS.md#검증-범위), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | 상한1000 부하·상한30 장애, 물리 accepted=ACK+unconfirmed·전량 거절, JVM heap/RSS·미확인 나이·종료 계측 경계 | [제품 한도·callback/종료·합성 공정성 검증](pipeline/PUBLISHER_VERIFICATION.md). JVM 강제 종료의 메모리 입력 복원 불가 |
| T12 | 검증됨 | [직렬/비동기 동일입력 대조](pipeline/EXPERIMENTS_VERIFICATION.md) | 100/s1500개 통제 대조와500/s목표 비교, request/batch·p95/p99·메모리·계수·순서·replay. 비동기 후보 채택 | [제품 적용·채널 공정성과 callback/종료 검증](pipeline/PUBLISHER_VERIFICATION.md). 앱창과 Kafka request inflight는 별개 |
| T13 | 부분 검증 | [선정 검증](discovery/VERIFICATION.md) | 공식 명세 조사·합성 HTTP/정규화 선정16개·실행JAR 하한 민감도5개 | 실제 앱 인증·전체 목록·동의/공식 채팅 수신은 미검증. 수집기 임대 연결은 T14 근거 참조 |
| T14 | 검증됨 | [임대 검증](fleet/VERIFICATION.md), [최종 CI](poc/evidence/fleet-2026-10-26/summary.json), [#39 회귀](poc/evidence/refactor-2026-10-26.json)  #54 초기 bootstrap timeout의 제한 재시도·복구/유한 실패는 [조사](research/FLEET_STARTUP.md) 범위로 추가 검증.| 단위134·통합88(임대7), 정상/만료 인계·DB 단절·SIGTERM 저장·producer 준비 전 무수집·재개 후 기존 session 입력0 | 협력 worker의 DB 임대·로컬 permit 범위. broker fencing/무공백/시계 drift는 미검증. 과거 Linux 실패와 재현 원인의 동일성은 미확정 |
| T15 | 검증됨 | [실제6노드HA](pipeline/KAFKA_HA_VERIFICATION.md) | RF3/minISR2/ELR1의broker·quorum분리장애·RF1→3·ACK/broker/DB집합대조 | 로컬단일호스트범위. 실제AZ/디스크손상/장기부하는미검증 |
| T16 | 부분 검증 | [배포 검증](DEPLOYMENT_VERIFICATION.md), [정제 근거](poc/evidence/deployment-2026-10-26/summary.json), [#39 회귀](poc/evidence/refactor-2026-10-26.json) | 1bde2b7 clean 이미지17단계: 실패원인/DDL42501·기존offset불변·새DB6group복구·실제API/리포트20건·웹8경로+상세·선택집계차이0·관측6회17.105~24.188초(코드/검사강도 차이·운영RTO 아님) | 로컬RF1·합성·동일V12 이미지 범위; 외부배포/운영SLA/PITR/파괴적migration 미검증. 실제 새 migration 전후 rollback은 미검증. 최종 이미지17단계·PR37 회귀 완료. 새 migration 릴리스 전에 추가 복구 리허설 필요 |
<!-- tech-status:end -->

하네스는 기술 ID·기능 연결·상태 행·검증 근거 누락과 기준 commit 대비 ID 삭제를 검사한다.
AGENTS·이슈/PR 템플릿에 갱신 절차를 연결했고, `./scripts/check` 및 CI의 `tech-goals`에서 실행한다.
이 문서화 작업은 새로운 Kafka 실험의 통과 기록이 아니다.

최초 하네스 검증: `./scripts/check` 통과(추적 검사기 테스트 11개, 문서 링크, 서버 검사·JAR는 Gradle
UP-TO-DATE). `--base develop` 검사, 잘못된 기준 ref의 실패, CI YAML·shell 구문도 확인했다.
추적 테스트는 ID 누락·미등록 참조·중복·기능 연결 누락·근거 없는 검증 상태·표식 손상·기준 대비 동시 삭제를 포함한다.
이번 변경에서 Kafka/DB 통합·live 수신·원격 CI는 새로 실행하지 않았다.

PR #14 리뷰 후 보완: Claude CLI가 `7f2cf86`에서 지적한 force-push 이전 SHA 확보,
`—` 빈 값 검사, 공백 정렬된 기준선의 기능 ID 삭제 검사를 수정했다. 로컬 테스트 16개에
실제 임시 bare 저장소의 force-push·shallow clone·이전 SHA fetch와 확보 실패 시 이벤트별 처리를 포함했다.
push 기준 확보 실패는 경고로 명시하고 PR base 확보 실패는 차단한다. 첫 HEAD의 원격 서버/웹/기술 목표 CI는 통과했다.
수정 HEAD의 재리뷰·최종 CI·병합 결과는 PR #14에서 확인한다. 새 Kafka 기술 실험은 수행하지 않았다.

## 수집 품질·순위·급상승 구현

선행 [PR #4](https://github.com/joejaeyoung/jigeumini/pull/4) → [PR #12](https://github.com/joejaeyoung/jigeumini/pull/12) → [PR #13](https://github.com/joejaeyoung/jigeumini/pull/13)을 Claude CLI 승인·최신 CI 통과 후 develop에 병합했다. #1·#2·#3과 보드는 완료다. #14 하네스도 #13을 통해 통합됐다.

최종 리뷰 반영: 집계 미확인과 빈 응답 구분, 서버 기준 상세 조회, 배수 내림 표시, 창 뒤 첫 안전한 버퍼 표본과 Kafka 따라잡음 대조. collector 5초·observer 1초, 2분 버퍼 표본, 경계+8초 평가 및 벽시계 재예약, STALE 30초가 현재 계약이다. 단위 52개·하네스 16개·실제 Kafka/PostgreSQL 19개·웹 단위 17개·Chromium 22개와 빌드 통과. 15 tick의 주입한 관측 위상·집계 기대값과 120 tick의 0.3/3/5초 비용·±2초 시계 보정을 구분해 시험했다. 실제 고부하·큰 시계 이동·다중 호스트·새 실방송은 미검증이며 상세 계약은 [품질 문서](api/QUALITY_RANKINGS.md)를 따른다.

아래 구현·검증 항목은 최초 M2 구현 당시 기록이다. 당시 테스트 개수와 만료 기준은 위 최종 리뷰 반영으로 갱신됐다.

2026-10-25 · [이슈 #3](https://github.com/joejaeyoung/jigeumini/issues/3) · 브랜치 `feat/quality-rankings`. [메타데이터 PR #12](https://github.com/joejaeyoung/jigeumini/pull/12)의 브랜치에서 분기해 변경을 분리했다. #2·#3만 구현하며 #5~#11은 이번 범위에 포함하지 않는다. Claude 승인·CI 통과 후 develop 병합을 완료했다.

- 기술 매핑: F03 → T02·T08·T11. 동일 eventId 중복을 ranking/trend 각각 1건으로 반영하고, ranking 중지 중 trend 처리·빈 partition freshness·미확인 버퍼 품질 제외를 검증했다. 이 최초 구현 당시에는 trend 충돌/rollback·consumer 확장·메모리·공정성 검증을 남겼다. 이후 #41의 [trend 장애 검증](pipeline/TREND_RECOVERY_VERIFICATION.md)과 #11/#27의 근거를 위 기술 목표 표에 반영했다.
- 수집 관측: 프로토콜 승인·원천 frame/pong 생존·종료·손실을 메모리에 기록하고 5초마다 DB에 flush한다. 연결 합집합, collector/source heartbeat 만료, 발행 대기·in-flight 적체, invalid 메시지와 미확인 발행을 품질에 반영한다. 일반 PoC는 DB 없이 유지한다.
- 독립 분석: ranking과 별도 trend group/ledger/bucket. 빈 poll heartbeat와 3개 end-offset 표본으로 조용한 partition도 freshness를 판정한다. 실패 입력을 건너뛰는 동작은 추가하지 않았다.
- 순위: 완료된 1분/5분, 관측률 95%와 손실·지연 조건, 정상 0건·동률·정확히 60초 전 순위 비교. REPEATABLE READ·advisory lock·unique snapshot으로 중복 평가를 막는다. 미확인/제외 채널은 rank/count=null과 참고 건수를 제공한다.
- 급상승: 독립 trend 집계, 소수 기준선, C≥100·증가≥50·비율≥2, 10초 간격 2회 연속, 5분 cooldown. 중복 tick·누락 tick·품질 실패·재시작에도 DB 상태를 기준으로 판정한다. 사후 공백은 품질 경고를 붙이고 기존 snapshot 건수/순위를 재계산하지 않는다.
- API·웹: 순위 snapshot, cursor 급상승 피드, 완료된 분별 추이, 공개 품질 요약. 기존 원시 bucket 조회는 운영 진단용으로 보존한다. 20초 만료·품질 제외·참고 건수·공백을 구분하고 유효한 피드에만 청록/코랄 리본을 표시한다. [계약](api/QUALITY_RANKINGS.md)에 기준과 제한을 기록했다.
- 검증: `./scripts/check` 단위 테스트 50개·실행 JAR 빌드 통과. `./scripts/integration` 실제 Kafka/PostgreSQL 테스트 16개 통과. `./scripts/web check` 타입·단위 테스트 16개·제품 빌드 통과. `./scripts/web test:e2e` 합성 API Chromium 테스트 19개 통과.
- 추가 근거: 57초/56.999초 경계·세션 중첩, 250/5와 251/5, 연속/누락 tick·cooldown, 정상 0건·동률·불변 snapshot/지연 추이, 제외·사후 정정·이전 순위·cursor·즉시 disable, collector DB 기록을 검사했다. 실제 broker에서 ranking 소비를 멈춘 뒤 trend만 새 이벤트를 반영하는 것을 확인했다.
- 실제 로컬 실행: 분석기를 V2 migration과 새 trend 그룹으로 실행했다. ranking/trend 각각 3 partitions에 poll/따라잡음 관측이 기록됐고 완료 snapshot이 생성됐다. 18082 탐색의 기존 방송 카드와 준비 중 상태, 급상승 warmup, JS 오류 0을 확인했다. 기존 DB 기록에는 M2 품질 정보가 없어 정상 순위나 배수를 만들지 않았다.
- 화면 확인: 1440/320px 합성 급상승 카드에서 상단 메뉴·작은 V자 리본·2.2배 청록/3.2배 코랄·기록 시각과 기준선을 확인했다. 합성 캡처만 gitignored `work/surges-*.png`에 둔다.
- 제한: 새로운 실방송 채팅 수집이나 6분 실제 급상승 유도는 하지 않았다. 프로토콜은 loopback WS/HTTP, 판정은 합성 입력과 실제 로컬 broker/DB로 검증했다. 보존 자동 삭제·관리자 UI·다중 호스트 fencing·DLQ·재처리·장시간 부하·외부 배포는 범위 밖이다. 현 단계의 snapshot 정정 범위는 최근 7일이다.

## 방송 메타데이터 구현

아래 구현·검증 항목은 메타데이터 최초 구현 시점의 기록이다. 최종 통합 검증은 품질·순위 절의 리뷰 보완 기록을 따른다.

2026-10-25 · [이슈 #2](https://github.com/joejaeyoung/jigeumini/issues/2) · 브랜치 `feat/broadcast-metadata`. 최초 구현은 `feat/monorepo-web`에서 분기했으며 선행 [PR #4](https://github.com/joejaeyoung/jigeumini/pull/4)는 develop에 병합됐다. Claude 리뷰 승인·CI 통과 후 PR #12를 develop에 병합했고 이슈 #2·보드는 완료 처리했다.

- 서버: 익명 공개 live-detail 조회, 명시적인 종료 확인, 타입이 있는 `/api/channels` 목록·개별 조회. 수집 이력 최근 20개와 요청 ID 목록을 구분한다. 메타데이터는 Kafka·DB에 저장하지 않는다.
- 캐시: 60초 TTL·실패 재시도 15초·이전 공개 텍스트 최대 5분, 256개 제한, 동시 갱신 4개·대기 32개, 동일 채널 요청 합치기. 성인·접근 제한·404·잘못된 응답에서 이전 공개 정보를 지운다. 만료 후 LIVE·이미지를 유지하지 않는다.
- 웹: 실제 방송 제목·채널명·카테고리·썸네일·방송 상태, 제목/채널명/카테고리 검색·필터, 관심·상세 연결. 이미지 실패 시 대체 화면, 채팅 집계와 별도 오류 처리, 최근 기록 없는 채널은 `—`. 기존 홈 서비스 일러스트와 급상승 준비 화면을 유지한다.
- 계약·원천: [방송 정보 API](api/BROADCAST_METADATA.md)에 공식 API와 익명 어댑터의 차이, 이용 조건 확인 범위, 오류·캐시·이미지 정책을 기록했다.
- 로컬 검증: `./scripts/check` 서버 단위 테스트 41개·JAR 빌드 통과. `./scripts/integration` 실제 Kafka/PostgreSQL 테스트 9개 통과. `./scripts/web check` 타입·단위 테스트 13개·제품 빌드 통과. `./scripts/web test:e2e` 합성 API 브라우저 테스트 16개 통과. 320/768/1440px 메타데이터 카드의 가로 넘침 없음과 합성 화면을 확인했다.
- 실제 공개 조회: 기존 DB의 수집 채널 1개를 입력으로 앱의 `Jigeumini/0.1`·JSON 요청 헤더를 사용했다. OPEN·adult=false, 공개 제목/카테고리, `{type}` 썸네일 형식과 `livecloud-thumb.akamaized.net` 호스트를 확인했다. 썸네일 HTTP 200. 일부 기본 curl 요청은 시간 초과했으나 앱 요청은 정상 응답했다.
- 실제 웹: 분석기를 새 JAR로 재시작하고 18082 탐색 화면에서 카드 1개·실제 로드된 이미지 1개·LIVE 표시 1개·카테고리 필터를 확인했다. JS 오류 0개. 원천 제목·공개 채널명·원천 body·실제 방송 캡처를 파일이나 검증 출력에 남기지 않았다. 채팅 신규 수집은 수행하지 않았다.
- 제한: 실제 제한/성인/종료/장애 전환은 합성 fixture로 검증했고 실방송에서 강제 재현하지 않았다. 외부 배포·공식 앱 승인·전체 방송 검색·영상 재생·Safari/Firefox·실기기는 검증하지 않았다. 합성 화면 캡처만 gitignored `work/metadata-*.png`에 둔다.

## 실제 웹·모노레포 1차 구현

아래 구현·검증 항목은 M1 웹 최초 구현 당시의 기록이다. 당시 미구현 메타데이터·품질 기능은 위 절에서 추가됐으며 수치도 해당 실행 시점을 뜻한다.

2026-10-25 · [이슈 #1](https://github.com/joejaeyoung/jigeumini/issues/1) · Claude 리뷰 승인·CI 통과 후 PR #4를 develop에 병합했고 이슈 #1·[보드](https://github.com/users/joejaeyoung/projects/1)는 완료 처리했다.

- 저장소: 원격 주소를 변경된 이름 `joejaeyoung/jigeumini`로 맞췄다. 기존 기준 커밋 `44b6321`에서 `main`·`develop`을 만들고 기본 브랜치를 `main`으로 설정했다. `feat/monorepo-web`에서 구현하며 기존 `feat/chat-pipeline`은 보존한다.
- 모노레포: 서버 소스·테스트·리소스와 빌드를 `server/` Gradle 서브프로젝트로 이동했다. `web/`은 npm workspace의 React·TypeScript·Vite 제품 웹이다. 루트 `scripts/` 실행 계약을 유지하고 JAR·테스트 결과 경로와 CI를 갱신했다.
- 웹 구현: 확정 디자인의 상단 메뉴·밝은 민트 색상·카드 구조, 홈·탐색·검색·관심 방송·상세 경로. 기존 `/api/status`, `/api/buckets`를 프록시로 연결했다. 1분/5분 합계, 채널 ID 검색, 브라우저 저장, 30분/60분의 기록된 10초 구간 표시를 제공한다.
- 리뷰 보완(2026-10-25): 관심 방송의 첫 조회·조회 실패 시 카드를 숨겨 확인된 빈 결과와 구분한다. 지연 응답→503→빈 응답 재시도 e2e를 추가해 총 10개 통과했고 웹 타입·단위 9개·빌드도 통과했다. `scripts/web`의 무인자 기본 dev 실행을 수정했다. 상세 화면의 분석기 상태 경고는 후속 품질 화면에서 다룬다.
- 상태 처리: 런타임 응답 검사, 요청 취소·8초 제한, 응답 후 5초 polling, 로딩·오류·재시도·이전 데이터 표시. 기록 없는 구간/관심 채널에 0건을 만들지 않는다.
- 미구현 구분: M1 API에 방송 제목·썸네일·카테고리·검증된 급상승이 없어 대체 화면과 준비 중 안내를 표시한다. 2배대 청록·3배 이상 코랄 리본 컴포넌트는 보존했지만 실제 배수를 임의 계산하지 않는다. 홈 이미지는 AI 서비스 일러스트다.
- 로컬 검증: `./scripts/check` 서버 단위 테스트 26개와 JAR 빌드 통과. `./scripts/integration` 실제 Kafka·PostgreSQL 통합 테스트 8개 통과. `./scripts/web check` 타입 검사·단위 테스트 9개·제품 빌드 통과. `./scripts/web test:e2e` 합성 API 기반 브라우저 테스트 9개 통과. 320/375/414/768/1440px에서 가로 넘침·모달 Escape와 포커스 복귀를 확인했다.
- 실제 조회 확인: `./scripts/web dev`의 18082에서 기존 로컬 조회 서버 18080의 응답을 확인했다. 조회 시점에 최근 5분 bucket은 비어 있었으며 웹도 빈 상태를 표시했다. 새 실방송 수집은 수행하지 않았다. 데이터가 있는 화면은 합성 API 시나리오로 검증했다. 18083의 제품 빌드에서도 직접 경로·API 프록시·빈 상태·JS 오류 없음을 확인했다.
- 후속 이슈: [방송 메타데이터 #2](https://github.com/joejaeyoung/jigeumini/issues/2), [M2 순위·급상승 #3](https://github.com/joejaeyoung/jigeumini/issues/3)를 보드 Todo로 등록했다.
- 제한: 외부 배포·PR 병합·Safari/Firefox·실기기 검증은 수행하지 않았다. 임시 캡처는 gitignored `work/ui-design/`에 둔다. Git Flow 운영은 [개발 구조](DEVELOPMENT.md)를 따른다.

## 확정 디자인 기준

- 서비스명은 지금이니로 확정했다. 제품 웹·디자인 시안·서버 운영 화면의 로고, 제목, 안내와 현재 설계 문서에 반영했다.

- 원본 시안은 `web/design-preview/`에 보존한다. `./scripts/ui-preview`의 18081은 합성 데이터 디자인 검토용이며 제품 웹 18082와 구분한다.
- 상단 메뉴를 사용하는 웹사이트 형태, 밝은 민트 색상과 방송 이미지 중심 카드 구성을 확정했다.
- 반응 태그는 높이 28px·숫자 14px의 V자 끝 리본이며 2배대 청록·3배 이상 코랄로 구분한다.
- 시안의 가상 방송·건수·카테고리·썸네일을 실제 서버 데이터로 오인하게 제품에 옮기지 않는다. 실제 이미지와 반응 배수는 후속 API 구현 후 연결한다.

- #12 리뷰 보완(2026-10-25): 메타데이터만 먼저 도착해도 미확인 집계를 기록 없음으로 표시하지 않는다. 집계 실패와 기간 전환 지연을 포함한 웹 e2e 19개 및 웹 check 통과.

## 운영 확장 후속 이슈

2026-10-26 사용자 요청에 따라 [#26](https://github.com/joejaeyoung/jigeumini/issues/26) 발견·선정 → [#27](https://github.com/joejaeyoung/jigeumini/issues/27) 비동기 발행 → [#28](https://github.com/joejaeyoung/jigeumini/issues/28) 수집기 인계, [#29](https://github.com/joejaeyoung/jigeumini/issues/29) Kafka HA → [#31](https://github.com/joejaeyoung/jigeumini/issues/31) ACK/커밋 실험과 [#30](https://github.com/joejaeyoung/jigeumini/issues/30) 배포/복구 리허설을 등록했다. 공식 자료는 [치지직](research/CHZZK_SCALE.md)·[Kafka/배포](research/KAFKA_DEPLOYMENT.md)에 사실/제안/미확인을 구분했다.  #26의 공식 목록 어댑터·결정적 선정 미리보기는 develop에 병합했다. 외부 배포·main 릴리스는 기존 요청의 별도 실행 경계를 유지한다.

### 방송 선정 #26

F12 → T13/T08/T11. fae9531에서 Java101·하네스16·JAR와 실행JAR cutoff5종을 검증했다. [선정 계약](discovery/README.md)·[검증](discovery/VERIFICATION.md)·[정제 근거](poc/evidence/selection-2026-10-26.json). Client 인증 목록 어댑터는 기본 비활성이며 page/request/time/body 상한·부분 결과·안전한 필드만 남긴다. 선정은 순수 정책으로 분리하고 공식/합성의 실제 두 adapter를 검토했다. 원문/비밀 헤더를 저장하지 않으며 assigned/connected/healthy는미관측null이다. 최종f9db520 Claude APPROVE·push40209813744/PR40209816956 SUCCESS 후 PR32를1089522로 develop에 병합했고 #26/보드를 완료했다. CI는 단위101·실제KafkaDB77·하네스16·웹29/31이다. 공개live와 외부배포는 실행하지 않았다.

#26 a31eebe Claude APPROVE·push40209024613/PR40209027685 성공 이후 비차단 리뷰6개를 반영했다. b200134에서 단위101·하네스16·JAR·정책5종 CLI 재실행이 통과했으며 최종 HEAD 리뷰/CI와 develop 병합도 완료했다.

### 부하·편향·복구 실험 #11

F10 → T06/T07/T08/T09/T11/T12. [계약](pipeline/EXPERIMENTS.md)과 [검증 해석](pipeline/EXPERIMENTS_VERIFICATION.md), 원본40회·정제표/HTML을 보존했다. 과거 broker실패2회·계측확인9회를 숨기지 않고 sampler 보완 후12프로필을 재검증했다. steady600.001초300,000건·독립A/B·replay차이0, 비동기500/s15,000·burst2,000/s120,000완료; 직렬500/s는1370수락 뒤10거절로 목표미달이다. aab0b76 push40209922599/PR40209926881은 단위91·하네스16·실제KafkaDB80·웹29/31 통과했다. 소유볼륨 보완cc0a1c1에서 broker네경우를 재검증하고 과거3개의볼륨근거없음을표시했다. 최종1617bdb Claude APPROVE·push40211847614/PR40211851666 SUCCESS 뒤 PR25를f94fc38로develop에병합했다. CI단위107·실제KafkaDB80·하네스16·웹29/31이며 #11/보드는완료다. live는종료상태다.

### 유한 비동기 발행 #27 검증

F14 → T02/T06/T10/T11/T12. [계약](pipeline/PUBLISHER.md)·[검증](pipeline/PUBLISHER_VERIFICATION.md)·[정제 근거](poc/evidence/publisher-2026-10-26/summary.json). 로컬단위120·하네스16·JAR,실제KafkaDB81,실제입력6회+소유broker장애6경우를 통과했다. 제품500/s15,000·2,000/s120,000·편중/중복30300→DB30000, replay차이0. 채널별공정성은 합성단위범위이며 소스복원/HA는 포함하지 않는다. #11은 f94fc38로 병합했다. 최종8ad0817 Claude APPROVE·push40212712923/PR40212715517 SUCCESS 뒤 PR33을597a162로 develop에 병합했다. CI단위120·실제81·하네스16·웹29/31이며 #27/보드는 완료다.

### 수집기 배정·임대 #28

F13 → T13/T14/T02/T11. [계약](fleet/README.md)의 DB 임대·단조 기한 권한·수집기 수명·CLI를 구현했다. [검증](fleet/VERIFICATION.md)과 [정제 근거](poc/evidence/fleet-2026-10-26/summary.json)에 실제 두 JVM의 정상/만료 인계·DB 단절·계수와 UUID 집합 대조를 보관했다.

초기 SIGTERM 진단 파일 누락은 저장 중5초 지연 회귀로 보완했다. 후속 Linux 인계 실패는 자식 진단 부재로 원인 동일성을 확정하지 못했다. 별도로 producer 생성3초 지연에서 큐20 초과를 재현하고 Kafka 준비 후 임대/수신을 시작하도록 수정했다. 큐 상한과 실행 중 장애 계약은 유지하며 실패 JSON을 항상 보관한다.

최종 `1aa8059`는 Claude APPROVE·push40218155876/PR40218160268 SUCCESS, 단위134·실제88(임대7)·하네스16·웹29/31을 확인했다. PR35를 `79d0f8d`로 develop에 병합했고 이슈/보드는 완료다. 이미 보낸 입력의 broker fencing·원천 무공백·전체 공식 수집을 보장하지 않는다.

### Kafka HA #29 실제 검증

F15 → T04/T08/T10/T15. [계약](pipeline/KAFKA_HA.md)·[검증](pipeline/KAFKA_HA_VERIFICATION.md)·[정제근거](poc/evidence/kafka-ha-2026-10-26/summary.json). 01e943e에서실제6노드의broker1/2대손실·controller1/2대손실·복구·RF1→3이행·최종ACK/broker/DB대조10단계통과. main172시도/171ACK=broker=DB,이행20일치,정리완료. 처음3실패와보완도보존했다. a68011e에서 trend DLQ의 LOCAL 기본값 호출을 수정하고 전체 Spring 분석기·네 HA topic·10건 ranking/trend를 추가한11단계가 통과했다. 최종1e814c7 Claude APPROVE·push40214416664/PR40214422119 SUCCESS 후 PR34를6abd51b로 develop에 병합했다. CI 단위124·실제81·하네스16·웹29/31 통과, #29·보드 완료다. 실제AZ/디스크손상/외부배포는 미검증이다.

### ACK·commit 경계 #31

F11 → T01/T10. 472908f의 실제6노드/별도JVM commit10사례·ACK3수준 대조가 통과했다. [검증·결정](pipeline/ACK_COMMIT.md)과 [원본3회](poc/evidence/ack-commit-2026-10-26/summary.json)에 첫 실패와 보완, 위험한 AUTO poll 순서의 누락1·정상9사례누락0·복구broker/DB차이0을 기록했다. 제품BATCH/acks=all 설정은 유지한다. 최신49a3834 clean tree 재실행도13사례·정리 통과했고 물리/고유/DB 및 실제 설정을 기록했다. 최종cb5fa38 Claude APPROVE·push40215140028/PR40215143544 SUCCESS 후 PR36을50cd6ad로 develop에 병합했다. CI단위124·실제81·하네스16·웹29/31이며 #31·보드 완료다.

### 배포·복구 #30

F16 → T04/T08/T16. 비특권 이미지·비밀 파일·내부망·readiness·명시적 migration, 복원 DB의 source identity/보존/진행 위치와 Kafka offset 검사를 구현했다. [배포 검증](DEPLOYMENT_VERIFICATION.md)과 [정제 근거](poc/evidence/deployment-2026-10-26/summary.json)에 초기 실패와 여섯 번의 이미지 리허설을 보관했다.

최신 clean `1bde2b7`의17단계는 이미지 교체/복귀, DB 장애, 고정 거절 코드, runtime DDL42501, 새 DB·새 group6개 복구 후 합성20건/알림2개·선택 집계 차이0·기존 offset 불변, 실제 웹8경로와 리포트 상세를 확인했다. API 응답26개 모두200, 마지막 종료1.255초·복구18.900초다. 코드/검사 강도가 다른 복구6회 범위17.105~24.188초는 운영 RTO/SLA가 아니다. 소유 자원·생성 비밀·dump 정리를 확인했다.

최종 `3b6d662`는 Claude APPROVE·push40218491808/PR40218495927 SUCCESS와 단위142·실제92·하네스16·웹29/31을 확인했다. PR37을 `d8b864d`로 develop에 병합했고 이슈/보드는 완료다. 이미지 실행 commit과 이후 문서/테스트 정리 변경을 구분한다. T16은 동일V12 복귀와 strict schema 거부·로컬 합성 복구 범위의 부분 검증이며, 새 migration 릴리스 복구·외부 배포/다중AZ/PITR·관측 활성 CLI의 실제 원천 연결은 미검증이다.
