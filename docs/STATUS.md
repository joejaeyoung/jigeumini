# 진행 상태

2026-10-25 · 브랜치 `feat/quality-rankings`

| 단계 | 현재 상태 | 다음 통과 조건 |
|---|---|---|
| M0 익명 수집 PoC | 기술 검증 통과: 2채널 30분·재접속, 최종 JAR 스모크·자동 테스트 | 공개 서비스 이용 조건·장기 운영 범위는 별도 확인 |
| M1 한 채널 수직 구현 | 완료: 실수신→Kafka→DB→조회, 34개 테스트·재시작 검증 통과 | M2 집계 품질·순위 정책 구현 |
| M2 순위·급증 기능 | #2 메타데이터·#3 수집 품질 기반 순위/급상승과 웹 연결 구현 | PR 검토·병합, 실방송 장기 관측·운영 확대 |
| M3 장애·재처리 | 미착수 | 독립 소비·DLQ·고정 offset replay |
| M4 성능 검증 | 미착수 | 합성 부하·디스크·지연·복구 시간 보고 |

세부 근거: [PoC 검증 기록](poc/VERIFICATION.md). 제품 범위: [PRD](design/PRD.md).

M1 실행·결정: [Kafka·PostgreSQL 파이프라인](pipeline/README.md).

## 수집 품질·순위·급상승 구현

2026-10-25 · [이슈 #3](https://github.com/joejaeyoung/jigeumini/issues/3) · 브랜치 `feat/quality-rankings`. [메타데이터 PR #12](https://github.com/joejaeyoung/jigeumini/pull/12)의 브랜치에서 분기해 변경을 분리했다. #2·#3만 구현하며 #5~#11은 이번 범위에 포함하지 않는다. 보드는 In Progress, 검토·병합은 남겨둔다.

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

2026-10-25 · [이슈 #2](https://github.com/joejaeyoung/jigeumini/issues/2) · 브랜치 `feat/broadcast-metadata`. 선행 모노레포 [PR #4](https://github.com/joejaeyoung/jigeumini/pull/4)가 미병합이므로 `feat/monorepo-web`에서 분기했다. 보드는 In Progress이며 검토·병합은 남겨둔다.

- 서버: 익명 공개 live-detail 조회, 명시적인 종료 확인, 타입이 있는 `/api/channels` 목록·개별 조회. 수집 이력 최근 20개와 요청 ID 목록을 구분한다. 메타데이터는 Kafka·DB에 저장하지 않는다.
- 캐시: 60초 TTL·실패 재시도 15초·이전 공개 텍스트 최대 5분, 256개 제한, 동시 갱신 4개·대기 32개, 동일 채널 요청 합치기. 성인·접근 제한·404·잘못된 응답에서 이전 공개 정보를 지운다. 만료 후 LIVE·이미지를 유지하지 않는다.
- 웹: 실제 방송 제목·채널명·카테고리·썸네일·방송 상태, 제목/채널명/카테고리 검색·필터, 관심·상세 연결. 이미지 실패 시 대체 화면, 채팅 집계와 별도 오류 처리, 최근 기록 없는 채널은 `—`. 기존 홈 서비스 일러스트와 급상승 준비 화면을 유지한다.
- 계약·원천: [방송 정보 API](api/BROADCAST_METADATA.md)에 공식 API와 익명 어댑터의 차이, 이용 조건 확인 범위, 오류·캐시·이미지 정책을 기록했다.
- 로컬 검증: `./scripts/check` 서버 단위 테스트 41개·JAR 빌드 통과. `./scripts/integration` 실제 Kafka/PostgreSQL 테스트 9개 통과. `./scripts/web check` 타입·단위 테스트 13개·제품 빌드 통과. `./scripts/web test:e2e` 합성 API 브라우저 테스트 16개 통과. 320/768/1440px 메타데이터 카드의 가로 넘침 없음과 합성 화면을 확인했다.
- 실제 공개 조회: 기존 DB의 수집 채널 1개를 입력으로 앱의 `Jigeumini/0.1`·JSON 요청 헤더를 사용했다. OPEN·adult=false, 공개 제목/카테고리, `{type}` 썸네일 형식과 `livecloud-thumb.akamaized.net` 호스트를 확인했다. 썸네일 HTTP 200. 일부 기본 curl 요청은 시간 초과했으나 앱 요청은 정상 응답했다.
- 실제 웹: 분석기를 새 JAR로 재시작하고 18082 탐색 화면에서 카드 1개·실제 로드된 이미지 1개·LIVE 표시 1개·카테고리 필터를 확인했다. JS 오류 0개. 원천 제목·공개 채널명·원천 body·실제 방송 캡처를 파일이나 검증 출력에 남기지 않았다. 채팅 신규 수집은 수행하지 않았다.
- 제한: 실제 제한/성인/종료/장애 전환은 합성 fixture로 검증했고 실방송에서 강제 재현하지 않았다. 외부 배포·공식 앱 승인·전체 방송 검색·영상 재생·Safari/Firefox·실기기는 검증하지 않았다. 합성 화면 캡처만 gitignored `work/metadata-*.png`에 둔다.

## 실제 웹·모노레포 1차 구현

2026-10-25 · [이슈 #1](https://github.com/joejaeyoung/jigeumini/issues/1) · [GitHub Projects](https://github.com/users/joejaeyoung/projects/1)에서 In Progress로 관리한다. PR 검토·병합은 남겨둔다.

- 저장소: 원격 주소를 변경된 이름 `joejaeyoung/jigeumini`로 맞췄다. 기존 기준 커밋 `44b6321`에서 `main`·`develop`을 만들고 기본 브랜치를 `main`으로 설정했다. `feat/monorepo-web`에서 구현하며 기존 `feat/chat-pipeline`은 보존한다.
- 모노레포: 서버 소스·테스트·리소스와 빌드를 `server/` Gradle 서브프로젝트로 이동했다. `web/`은 npm workspace의 React·TypeScript·Vite 제품 웹이다. 루트 `scripts/` 실행 계약을 유지하고 JAR·테스트 결과 경로와 CI를 갱신했다.
- 웹 구현: 확정 디자인의 상단 메뉴·밝은 민트 색상·카드 구조, 홈·탐색·검색·관심 방송·상세 경로. 기존 `/api/status`, `/api/buckets`를 프록시로 연결했다. 1분/5분 합계, 채널 ID 검색, 브라우저 저장, 30분/60분의 기록된 10초 구간 표시를 제공한다.
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
