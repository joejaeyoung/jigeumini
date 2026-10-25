# 방송 발견·수집 대상 선정

2026-10-26 · #26 / F12 → T13/T08/T11. [공식 자료 조사](../research/CHZZK_SCALE.md), [현재 상태](../STATUS.md).

## 원천 범위와 초기 정책

공식 Live API는 전체 라이브 목록을 페이지로 조회하고 Channel API는 팔로워 합계를 제공한다. 채팅 구독에는 별도의 사용자 인증·동의가 필요하다. 목록에 있다는 이유로 채팅에 접속하지 않는다. 현재 익명 READ 수집과 공식 Socket.IO 세션은 다른 프로토콜이다. [Live](https://chzzk.gitbook.io/chzzk/chzzk-api/live), [Channel](https://chzzk.gitbook.io/chzzk/chzzk-api/channel), [Session](https://chzzk.gitbook.io/chzzk/chzzk-api/session).

초기 **선정 검토안**은 팔로워1,000명 이상·접근 가능성을 운영자가 확인한 일반 라이브·최대20개다. 1,000은 제품 가설이며 공식 허용 기준이나 실측으로 확정한 용량이 아니다. `minFollowers=null/0/1000/5000/10000`으로 같은 합성 입력을 비교할 수 있다. 실제 후보 분포와 수신률을 확인한 뒤 운영 설정을 정한다. 팔로워 합계가 채팅 events/s와 같은 의미는 아니다.

미리보기는 `activationAllowed=false`이고 수집기를 시작하지 않는다. 운영자 `allowedChannels` 선언은 실제 OAuth/토큰 검증이 아니다(`accessEvidence=OPERATOR_DECLARED_NOT_RUNTIME_VERIFIED`). 합성/공식 목록, `SYNTHETIC`/`UNOFFICIAL_POC`/`OFFICIAL_AUTHORIZED` 수신 계획을 구분한다. 기존 공개 live 검증의 최대3채널·30분 제한은 유지한다.

## 선정 규칙

한 채널에 상호 배타적인 주 사유를 부여한다. 순서는 다음과 같다.

1. 한 scan에 같은 채널의 다른 방송 ID가 있으면 `AMBIGUOUS_BROADCAST`.
2. 비활성 `NOT_LIVE`, 성인 또는 성인 여부 미확인 `ADULT_OR_UNKNOWN`.
3. 명시 제외 `EXCLUDED`, 접근 확인 목록 밖 `ACCESS_NOT_DECLARED`.
4. 관측 시각이 미래이거나 TTL 이상이면 `STALE_METADATA`. TTL 만료를 방송 종료나 0건으로 바꾸지 않는다.
5. 팔로워 cutoff가 설정됐는데 값이 없으면 `FOLLOWERS_UNKNOWN`, 하한 미만은 `FOLLOWERS_BELOW_MINIMUM`. 실제0은 유효하다. null cutoff는 팔로워 조건을 사용하지 않지만0 cutoff는 값을 확인해야 한다.
6. 적격 후보를 현재 시청자 수 내림차순 → 팔로워 합계 내림차순 → channelId 오름차순으로 정렬한다. 미확인 수치는 마지막이다. 상한 밖은 `CAPACITY`.

동일 channelId/liveId가 다시 나타나면 최초 정규화 관측을 유지하고 `duplicateRows`를 센다. 목록 이동 중 고정 snapshot을 보장한다는 공식 계약은 확인하지 못했다. scanComplete=true도 해당 순회를 마쳤다는 뜻이다. 전체 플랫폼 무누락·전체 대표성·실제 메시지 수신률을 뜻하지 않는다.

## 실행

Java21·Python3과 기존 Gradle wrapper를 사용한다. Kafka/DB/웹 서버를 기동하지 않는다.

```bash
./scripts/selection fixture docs/discovery/synthetic-policy.json docs/discovery/synthetic-catalog.json work/selection.json
```

fixture는 `source=SYNTHETIC`의 정규화 목록만 받는다. 합성 평가 시계는 파일의finishedAt이며 같은 입력·설정은 같은 결과를 만든다. 예제는 발견8/적격2/선정2, 채널3→2 순서다. 예제의 maxChannels=3은 작은 합성 예시이며 실제 운영20채널 검증이 아니다.

실원천 목록 조회는 아래 환경 변수와 명시적 명령이 있어야 실행된다. 자격 증명은 실행 환경에서만 주입하고 설정 파일·터미널 명령·로그에 값을 넣지 않는다. 공개 API를 호출한 검증은 아직 없다.

```bash
# 설정 파일에서 sourceMode와 운영자가 확인한 allowedChannels를 지정한다.
# 실행 환경에 CHZZK_CLIENT_ID, CHZZK_CLIENT_SECRET을 별도로 주입한다.
SELECTION_OFFICIAL_ENABLED=true ./scripts/selection official work/selection-policy.json work/official-selection.json
```

결과 경로는 새 파일이어야 하고 덮어쓰지 않는다. stdout은 scope·개수·scanComplete·activationAllowed만, 상세 정제 결과는 지정 JSON에 기록한다. 완료 scan은 종료0, 부분 scan은 결과를 남기고 종료2다. 설정/파일/자격 증명 오류는 원문 없는 `SELECTION_COMMAND_FAILED`로 종료2다. 실패는 기존 실행 계획을 대체하거나 연결을 끊는 명령이 아니다.

## 유한 발견·필드·호출 계약

- 정해진 공식 host만 Client 인증 GET으로 사용하며 redirect를 따르지 않는다. 채팅·OAuth·개인 팔로워 목록·송신 API를 호출하지 않는다.
- 목록 최대20행/페이지, 팔로워 합계 최대20 ID/요청. 페이지 순회 후 발견한 채널의 합계를 조회한다. 합계 누락은null이다.
- `maxPages`1~1000, `requestBudget`1~2000, `deadline`1~120초, 원천 요청 최대5초/남은 기한, 응답 body1MiB, fixture4MiB, 설정1MiB다. 실제 host는 최소1초 간격을 적용한다. 이 간격은 자체 예산이며 공식 quota로 주장하지 않는다.
- 공개 문서에서 숫자 호출 quota를 확인하지 못했다. 429는 재시도하지 않고 `RATE_LIMITED`로 부분 결과를 반환한다. 권한 오류·전송 실패·JSON/필드 오류·반복cursor·페이지/요청/시간 상한을 고정 result로 구분한다.
- 저장 허용 필드는 channelId/liveId, 팔로워·시청자 합계, adult/live/구분모호 여부, 관측/scan 시각, 수치·고정 코드뿐이다. 원문 body·제목·채널명·이미지·태그·cursor·비밀 헤더는 버린다. 원천 오류의 자유 문자열을 로그에 쓰지 않는다.

## 범위 표시

`counts.discovered/eligible/selected`와 제외 사유별 수, scanComplete·result·scanAgeSeconds를 함께 확인한다. `assigned/connected/healthyWindow/healthyOfSelected`는 아직 관측하지 않았으므로null이다. 실제 수집기와 연결되기 전0 또는100%로 표시하지 않는다.

`selectedOfEligible`은 후보 대비 용량 충족률, `selectedOfDiscovered`는 이번 목록 대비 선정 비율이다. 0분모는null이다. 부분 scan의 비율을 전체 플랫폼 비율로 쓰지 않는다. 전체 치지직을 대표하지 않는 인기 방송 중심 표본이라는 편향이 있다. 실제 API/웹의 수집 건강 상태 연결은 수집기 배정 #28에서 이 계약을 사용한다.

## 리팩토링 점검과 한계

공식 HTTP와 합성 fixture라는 실제 두 원천을 `BroadcastCatalog` 경계에 두고, `SelectionPolicy`는 I/O 없는 선정 규칙만 소유한다. 페이지/요청 예산과 원문 제거는 공식 adapter 안에 숨겼다. CLI는 명령·설정·새 결과 파일 수명만 소유하며 Spring 서버나 DB를 만들지 않는다. 범용 HTTP 프레임워크나 다른 메타데이터 캐시를 이 경로에 결합하지 않았다.

실제 앱 인증·채널 동의·quota·전체 페이지 순회, 동적 목록 누락률, 실방송20개·공식 채팅 수신·토큰 갱신은 미검증이다. 새 토큰 저장소를 만들지 않았다. 최종 검증과 commit·명령·근거는 [검증 기록](VERIFICATION.md)을 따른다.
