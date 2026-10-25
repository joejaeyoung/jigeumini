# 진행 상태

2026-10-25 · 브랜치 `fix/replay-diagnostics`

| 단계 | 현재 상태 | 다음 통과 조건 |
|---|---|---|
| M0 익명 수집 PoC | 기술 검증 통과: 2채널 30분·재접속, 최종 JAR 스모크·자동 테스트 | 공개 서비스 이용 조건·장기 운영 범위는 별도 확인 |
| M1 한 채널 수직 구현 | 완료: 실수신→Kafka→DB→조회, 34개 테스트·재시작 검증 통과 | M2 집계 품질·순위 정책 구현 |
| M2 순위·급증 기능 | #4·#12·#13 Claude 승인·CI 통과 후 develop 병합 완료 | 실방송 장기 관측·운영 확대·고부하 평가 비용 |
| M3 장애·재처리 | #9·PR #15 병합 완료, #10·PR #19 develop 병합 완료 | #8 outbox 병합 완료, #16 replay 후속 보완 |
| M4 성능 검증 | 미착수 | 합성 부하·디스크·지연·복구 시간 보고 |

세부 근거: [PoC 검증 기록](poc/VERIFICATION.md). 제품 범위: [PRD](design/PRD.md).

M1 실행·결정: [Kafka·PostgreSQL 파이프라인](pipeline/README.md).

## 남은 이슈 구현

#8 관심 방송 알림 → #5 시청자 관측 → #6 카테고리 순위 → #7 방송 요약 → #16 replay 보완 → #11 부하·복구 실험 순서로 진행한다. 각 이슈는 별도 기능 브랜치와 develop PR에서 Claude 승인·최종 CI를 확인한다.

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

`feat/broadcast-reports`, F06 → T02/T04/T08. #5 병합98fe3a3에서 시작했고 승인된 #6/PR #22 병합5a419e3을 통합했다. [계약](api/REPORTS.md)·[검증 기록](api/REPORTS_VERIFICATION.md): 독립 관측 세션/귀속/별칭/집계, DB 원자 보정과 BATCH commit, 품질 완료 분/피크, API/웹 목록→요약, 종료7일 정리와 active/작업 보호를 구현했다. 순수정책4개·report실제Kafka/DB7개·category회귀8개를 확인했고 최신 전체 회귀·Claude 리뷰·CI를 진행한다. 최근 Java84·하네스16·웹29/빌드가 통과했으며 동일 SQL snapshot 보완 뒤 최종 검사로 다시 확인한다. 채팅 침묵/HTTP실패/TTL만료로 종료하지 않으며 원문·닉네임·사용자 ID·토큰을 저장하지 않는다. #16은 별도 작업 폴더의61784de Claude APPROVE·기존base67/16/53 검증까지 마쳤고 이 변경 통합 뒤 최종 검증한다.

## 재처리 CLI 후속 보완 #16

`fix/replay-diagnostics`, F08 → T01/T02/T03/T04. #5/#6 최종 CI·리뷰를 기다리는 동안 독립 범위를 진행한다. 조회의 migration/watchdog 제거·명시적migrate, 동일 run 및 다른 worker 빌드 비교 거부, 보존 offset 폭과 경계 Kafka timestamp 표본, 잘못된 날짜/없는 토픽/접속 실패 구분, runtime JAR 종료 정리를 구현했다. 실제 control record 꼬리/중간과 전역 worker lock의 watchdog 지연을 합성 Kafka/DB에서 검증 중이다. 최종 근거·검증·Claude 승인·CI는 아직 완료하지 않았다.

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
| T01 | 부분 검증 | [DLQ ACK·commit 경계](pipeline/DLQ_VERIFICATION.md), [M1 자동 검사](pipeline/VERIFICATION.md#자동-검사), [재처리 격리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | BATCH의 DB 성공 이후 커밋·DB 실패 시 진행 차단; DLQ ACK 전 원본 offset 차단; replay는 Kafka commit 없이 별도 DB progress 사용 | F11에서 Auto/BATCH/MANUAL 비교, 직접 ACK와 컨테이너 커밋 구분 |
| T02 | 부분 검증 | [리포트 귀속·중복·rollback](api/REPORTS_VERIFICATION.md), [관측 중복·역순·rollback](api/OBSERVATIONS_VERIFICATION.md), [알림 멱등·rollback](pipeline/NOTIFICATIONS_VERIFICATION.md), [DLQ 멱등·충돌](pipeline/DLQ_VERIFICATION.md), [M1 중복·rollback](pipeline/VERIFICATION.md#자동-검사), [M2 독립 소비](api/QUALITY_RANKINGS.md#검증-범위), [재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | M1 재전달·충돌·SQL rollback, ranking/trend의 동일 eventId 중복 입력을 각각 1건 반영; replay 배치 내부/간 중복·충돌·SQL rollback; 알림 spikeId 중복·충돌·SQL rollback | 신규 trend의 SQL rollback 전용 장애 실행은 남음; 원천 중복·전체 Exactly-once는 범위 밖 |
| T03 | 검증됨 | [Outbox commit·ACK 직후 종료](pipeline/NOTIFICATIONS_VERIFICATION.md), [DLQ ACK·DB commit 직후 종료](pipeline/DLQ_VERIFICATION.md), [JVM 종료·재시작](pipeline/VERIFICATION.md#자동-검사), [replay worker 종료](pipeline/REPLAY_VERIFICATION.md) | DB commit 직후 halt(91), 동일 group 재전달 뒤 집계 1건; replay commit 후 JVM 종료·watchdog 실패·새 run 재실행; DLQ ACK 후 halt(93)·DB commit 후 halt(94), 재전달 격리 1건·집계 2건; F07 outbox commit halt(95)·ACK halt(96) 후 물리2/feed1 | 모든 장애·고부하 복구로 확대 해석하지 않음 |
| T04 | 검증됨 | [리포트 active 보호·정리](api/REPORTS_VERIFICATION.md), [관측7일 보존](api/OBSERVATIONS_VERIFICATION.md), [고정 입력 재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | 동일 manifest 2회·알려진 bucket 대조, 고정 end·만료/재생성/증설 거부, 취소·worker 종료·live 불변 | 보존된 offset만 대상; 삭제 입력·미래 지연 입력·운영 snapshot 재현·HA는 범위 밖 |
| T05 | 검증됨 | [DLQ·DB 장애·재전달](pipeline/DLQ_VERIFICATION.md) | 형식/schema/충돌 격리·정상 입력 진행, ACK 전 차단, ACK 직후 종료·동일 failureId 중복, DB 복구와 원자 rollback | 단일 broker의 발행 거부 범위; #11 실제 단절·대량 오류/장시간 회복, 자동 정리 정책은 별도 |
| T06 | 부분 검증 | [카테고리 시각 결합·보정](api/CATEGORIES_VERIFICATION.md), [관측의 도착 순서와 시각](api/OBSERVATIONS_VERIFICATION.md) | 역순 observedAt·중복의 시각순 조회; 채팅 이전 관측과120초 유효 범위 결합·늦은 짧은TTL 보정 | #11 동일 키 분포·다중 producer·증설 영향 실험 |
| T07 | 부분 검증 | [카테고리 동시 합산](api/CATEGORIES_VERIFICATION.md) | 두 채널의 동일 카테고리100건·중복100건에서 합계100 유지. 채널별 bucket 저장 후 조회 합산 | #11 균등/80% 편향과 키 분산 후보 비용·처리량 비교 |
| T08 | 부분 검증 | [독립 report 중단·재시작](api/REPORTS_VERIFICATION.md), [알림60초 중단·복구](pipeline/NOTIFICATIONS_VERIFICATION.md), [복구 진단](pipeline/DLQ_VERIFICATION.md), [M2 독립 소비·freshness](api/QUALITY_RANKINGS.md#검증-범위), [구조 변경 회귀](REFACTORING.md#회귀-검증) | 빈 poll·partition별 end offset과 따라잡음 관측, ranking 중지 중 trend 계속 처리; 두 group의 partition lag·첫 미처리 offset 시각·격리/만료 진단; 알림60초 중단 중 ranking 진행·feed 회복 | #11 consumer 1/2/3/4·입력률·처리율·DB 병목·회복 시간 비교 |
| T09 | 미착수 | — | timeout 설정 구현, 제한별 장애 실험 미실행 | #11 poll 초과·heartbeat 단절·consumer 추가/종료를 각각 재현 |
| T10 | 부분 검증 | [알림 ACK·거부·DB 대조](pipeline/NOTIFICATIONS_VERIFICATION.md), [실수신 ACK·DB 대조](pipeline/VERIFICATION.md#실제-공개-방송) | RF=1, acks=all에서 ACK와 DB 반영 대조; 알림 broker 거부·ACK 뒤 재발행2/feed1 | F11 ACK 수준·재시도 비교; ISR·복제 내구성은 미검증 |
| T11 | 부분 검증 | [버퍼 상한](pipeline/VERIFICATION.md#자동-검사), [M2 품질 관측](api/QUALITY_RANKINGS.md#검증-범위), [구조 변경 회귀](REFACTORING.md#회귀-검증) | in-flight 포함 상한·실패 계수, 채널별 미확인 나이·손실의 품질 제외와 DB 기록 | #11 지연 ACK·단절·실제 메모리·동시 채널 공정성·종료 경계 검증 |
| T12 | 미착수 | — | 직렬 ACK worker 기준선만 존재 | #11 bounded async 후보와 처리량·메모리·실패 추적 비교 |
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

- 기술 매핑: F03 → T02·T08·T11. 동일 eventId 중복을 ranking/trend 각각 1건으로 반영하고, ranking 중지 중 trend 처리·빈 partition freshness·미확인 버퍼 품질 제외를 검증했다. 신규 trend의 충돌/rollback 전용 장애 시험, consumer 수별 확장·실제 메모리·채널 공정성은 남아 있어 해당 목표 전체 완료로 판정하지 않는다.
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
