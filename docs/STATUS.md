# 진행 상태

2026-10-25 · 브랜치 `feat/dlq-recovery`

| 단계 | 현재 상태 | 다음 통과 조건 |
|---|---|---|
| M0 익명 수집 PoC | 기술 검증 통과: 2채널 30분·재접속, 최종 JAR 스모크·자동 테스트 | 공개 서비스 이용 조건·장기 운영 범위는 별도 확인 |
| M1 한 채널 수직 구현 | 완료: 실수신→Kafka→DB→조회, 34개 테스트·재시작 검증 통과 | M2 집계 품질·순위 정책 구현 |
| M2 순위·급증 기능 | #4·#12·#13 Claude 승인·CI 통과 후 develop 병합 완료 | 실방송 장기 관측·운영 확대·고부하 평가 비용 |
| M3 장애·재처리 | #9·PR #15 병합 완료, #10 DLQ·소비 복구 진단 구현 중 | DLQ ACK/DB/offset 장애 검증·Claude 리뷰·CI, #16 replay 후속 보완 |
| M4 성능 검증 | 미착수 | 합성 부하·디스크·지연·복구 시간 보고 |

세부 근거: [PoC 검증 기록](poc/VERIFICATION.md). 제품 범위: [PRD](design/PRD.md).

M1 실행·결정: [Kafka·PostgreSQL 파이프라인](pipeline/README.md).

## 오류 격리·복구 진단 #10

`feat/dlq-recovery` · F09 → T01/T03/T05/T08. `b58d084`의 origin/develop에서 시작했다. 영구 계약 오류만 안전한 DLQ로 격리하고 정상 이벤트·격리 기록·진행 위치를 같은 DB 트랜잭션에 저장한다. DLQ ACK 실패와 DB 오류는 배치를 재시도한다. ranking/trend 양쪽에 적용하고 수집 불완전 품질·로컬 운영 진단을 연결한다. 실제 broker/DB 장애 시험 전이며 완료 판정은 검증 후 갱신한다.

PR #18 push CI의 `describeConfigs` 단계 `UnknownTopicOrPartitionException`은 같은 HEAD 재실행에서 통과했으나 원인은 미확정이다. 이번 구현의 결과와 구분해 추적한다.

## 코드 구조 리팩토링

2026-10-25 · `refactor/module-boundaries` · [이슈 #17](https://github.com/joejaeyoung/jigeumini/issues/17).
[구조 검토와 결정](REFACTORING.md)에 전체 영역의 후보와 선택 이유를 기록했다. CollectionQuality 추출, PostgresSupport 초기화 통합, 웹 API 계약·HTTP 모듈 분리를 구현했다. Java 단위 55개·하네스 16개·실제 Kafka/PostgreSQL 29개·웹 단위 17개·Chromium 22개와 JAR/웹 빌드가 통과했다. [정제 근거](poc/evidence/refactor-2026-10-25.json)의 코드 commit·JAR hash로 실행 범위를 고정했다. 구현·로컬 검증은 완료했으며 Claude 리뷰·CI·병합 이력은 [PR #18](https://github.com/joejaeyoung/jigeumini/pull/18)에 기록한다.

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
| T01 | 부분 검증 | [M1 자동 검사](pipeline/VERIFICATION.md#자동-검사), [재처리 격리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | BATCH의 DB 성공 이후 커밋·DB 실패 시 진행 차단; replay는 Kafka commit 없이 별도 DB progress 사용 | F11에서 Auto/BATCH/MANUAL 비교, 직접 ACK와 컨테이너 커밋 구분 |
| T02 | 부분 검증 | [M1 중복·rollback](pipeline/VERIFICATION.md#자동-검사), [M2 독립 소비](api/QUALITY_RANKINGS.md#검증-범위), [재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | M1 재전달·충돌·SQL rollback, ranking/trend의 동일 eventId 중복 입력을 각각 1건 반영; replay 배치 내부/간 중복·충돌·SQL rollback | 신규 trend의 충돌·rollback 장애별 전용 실행은 남음; 원천 중복·전체 Exactly-once는 범위 밖 |
| T03 | 검증됨 | [JVM 종료·재시작](pipeline/VERIFICATION.md#자동-검사), [replay worker 종료](pipeline/REPLAY_VERIFICATION.md) | DB commit 직후 halt(91), 동일 group 재전달 뒤 집계 1건; replay commit 후 JVM 종료·watchdog 실패·새 run 재실행 | F07 outbox의 별도 장애 경계 검증; 모든 장애·고부하 복구로 확대 해석하지 않음 |
| T04 | 검증됨 | [고정 입력 재처리](pipeline/REPLAY_VERIFICATION.md), [구조 변경 회귀](REFACTORING.md#회귀-검증) | 동일 manifest 2회·알려진 bucket 대조, 고정 end·만료/재생성/증설 거부, 취소·worker 종료·live 불변 | 보존된 offset만 대상; 삭제 입력·미래 지연 입력·운영 snapshot 재현·HA는 범위 밖 |
| T05 | 부분 검증 | [DB 오류·malformed 검사](pipeline/VERIFICATION.md#자동-검사) | DB 복구 재시도, 잘못된 입력의 offset 차단 | #10 DLQ·실패 분류·ACK 경계와 정상 입력 진행 검증 |
| T06 | 미착수 | — | channelId·3 partition 설정 구현, 키별 순서 실험 미실행 | #11 동일 키 분포·다중 producer·증설 영향 실험 |
| T07 | 미착수 | — | hot key 분포·shard 비교 근거 없음 | #11 균등/80% 편향과 키 분산 후보 비용 비교 |
| T08 | 부분 검증 | [M2 독립 소비·freshness](api/QUALITY_RANKINGS.md#검증-범위), [구조 변경 회귀](REFACTORING.md#회귀-검증) | 빈 poll·partition별 end offset과 따라잡음 관측, ranking 중지 중 trend 계속 처리 | #11 consumer 1/2/3/4·입력률·처리율·DB 병목·회복 시간 비교 |
| T09 | 미착수 | — | timeout 설정 구현, 제한별 장애 실험 미실행 | #11 poll 초과·heartbeat 단절·consumer 추가/종료를 각각 재현 |
| T10 | 부분 검증 | [실수신 ACK·DB 대조](pipeline/VERIFICATION.md#실제-공개-방송) | RF=1, acks=all에서 ACK와 DB 반영 대조 | F11 ACK 수준·재시도 비교; ISR·복제 내구성은 미검증 |
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
