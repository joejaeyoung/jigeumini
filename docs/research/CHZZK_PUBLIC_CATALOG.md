# 앱 등록 없이 공개 방송 목록을 발견하는 대안

조사일: **2026-10-26 KST**. 기술 연결: **F12/F13 → T13/T14/T08/T11**. 치지직 공개 웹의 정적 JavaScript와 커밋을 고정한 클라이언트 소스를 조사하고, 앱 키·쿠키 없는 공개 HTTP 조회를 제한된 예산으로 실행했다. 소스가 보내는 요청과 실제 서버의 응답·전수성을 구분한다. 진행 상태 정본은 [STATUS](../STATUS.md)다.

## 1. 결론

**가장 직접적인 대안은 치지직 공개 웹이 사용하는 `/service/v1/lives` 목록을 페이지 순회하는 것이다.** 개발자센터의 `/open/v1/lives`와 다른 경로다. 현재 저장소의 `scripts/discover`도 이미 전자를 사용하지만, `size=10`인 첫 페이지만 읽고 후속 cursor는 처리하지 않는다. 따라서 앱 등록을 대안 탐색의 선행 조건으로 둘 필요가 없다. [현재 스크립트](../../scripts/discover), [치지직 공개 웹 번들](https://ssl.pstatic.net/static/nng/glive/resource/p/static/js/index-DWn2Eab5.js)

이번 실측에서는 **50개씩 20페이지, 1,000행에서 서로 다른 방송 995개**를 확인했다. 목록 요청 20회와 팔로워 필드 확인을 위한 채널 상세 1회가 모두 HTTP 200이었다. 20페이지 예산에서 중단했고 다음 cursor가 남아 있으므로 전체 방송 수를 측정한 결과가 아니다. [정제 근거](../poc/evidence/public-catalog-research-2026-10-26.json), [실측 범위](#8-실제-공개-http-대조)

제안하는 조합은 **공개 목록 순회로 새로운 방송을 발견하고, 발견한 채널의 registry를 유지하며, 선정된 채널의 현재 방송 상태를 별도로 확인**하는 것이다. 카테고리별 목록은 특정 장르의 보충·대조에 쓸 수 있다. 이 조합은 목록 장애 때 이미 알고 있는 채널을 다시 확인할 수 있지만, 한 번도 발견하지 못한 채널이나 수집 주기 사이에 시작·종료한 방송까지 보장하지는 않는다. 이는 아래 인터페이스에 기초한 설계 제안이며, 전수 수집 실측 결과가 아니다.

목록에 나온다는 것과 익명 채팅 연결이 성공한다는 것은 별개다. 공개 방송을 발견한 뒤에도 성인·지역·기타 접근 제한, 채팅 가능 상태, 연결 실패를 따로 판정한다. 발견 수를 전체 치지직 채팅의 수집률로 표시하지 않는다. [기존 수집 범위 조사](CHZZK_SCALE.md), [고정 소스의 라이브 상태 필드](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/live.ts#L96-L123)

## 2. 대안 비교

아래 비용과 우선순위는 프로젝트 설계 판단이다. 서비스의 공식 호출 허용량을 뜻하지 않는다.

| 대안 | 발견할 수 있는 범위와 근거 | 비용·한계 | 판단 |
|---|---|---|---|
| 공개 웹의 라이브 JSON 목록 | `/service/v1/lives`; 공개 웹과 비공식 클라이언트에 페이지 순회 구현이 있다. | 페이지 수만큼 HTTP 요청. 순위 변화·방송 시작/종료·접근 제한으로 전수성이 달라질 수 있다. | **주 경로 후보** |
| 카테고리 목록 → 카테고리별 라이브 | `/service/v1/categories/live` → `/service/v2/categories/{type}/{id}/lives`. | 카테고리 페이지와 각 카테고리의 방송 페이지가 필요하다. 장르 변경·카테고리 누락·중복을 별도로 다뤄야 한다. | 장르별 보충·교차 확인 |
| 알려진 채널 registry + 상태 polling | 채널 ID별 `/polling/v2/channels/{channelId}/live-status`; 현재 코드와 기존 라이브러리가 사용한다. | 조회 대상 수에 비례한다. 새로운 채널을 스스로 발견하지 못한다. | 목록 장애 시 보조 경로 |
| 공개 웹을 브라우저로 열어 DOM 추출 | 웹 코드에는 목록 추가 조회와 `liveId` 중복 제거가 있다. | 브라우저 실행·렌더링·스크롤·화면 구조 변경을 처리해야 한다. 같은 원천 목록의 전체성 한계는 남는다. | JSON 변경 원인 진단·수동 대조용 |
| 제3자 통계·방송 디렉터리 | 서비스마다 공개 순위·방송 탐색 화면을 제공한다. | 수집 지연·선정 편향·재배포 조건·API 제공 여부가 다르다. 최신성·전체성을 대신 보장하지 않는다. | 외부 비교 자료, 주 원천으로 채택하지 않음 |

공개 목록과 카테고리 구현 근거는 아래 3절, registry는 5절, 제3자 서비스 자체 안내는 6절에 연결한다. DOM 경로의 운영 비용은 구현 방식에서 도출한 추론이며 브라우저 수집 성능을 측정하지 않았다.

## 3. 현재 first-party 웹 코드에서 확인한 사실

공개 홈페이지가 로드한 자산은 다음과 같다. 공개 HTML과 정적 자산만 읽었으며 로그인·앱 키·쿠키를 주입하지 않았다. 자산의 읽기 성공만으로 개별 API의 무인증 호출 성공을 판정하지 않는다.

- URL: [index-DWn2Eab5.js](https://ssl.pstatic.net/static/nng/glive/resource/p/static/js/index-DWn2Eab5.js)
- 자산 크기: **5,527,696 bytes**
- SHA-256: `d491596c36863630d058897c4499ad31626528c6df3b08b39d6baab0dd6d35f8`
- 아래 위치는 UTF-8 디코딩 후 **0부터 센 문자 위치**다. 압축 번들은 한 줄이 길어, 심볼과 문자 위치를 함께 적었다. 공개 정적 코드는 임시 `work/`에만 보관하며 공유 문서에 복제하지 않았다. 방송 API 응답은 메모리에서만 읽었다.

| 코드 위치 | 소스에서 확인한 요청·처리 |
|---|---|
| 3행, `ms` 230331 / `gs` 230569 | `LIVE_SERVICE`의 각각 v1/v2 HTTP 클라이언트다. |
| 13행, `$p` 379681 | 라이브 목록 요청에 `size`, `sortType`, `concurrentUserCount`, `liveId`를 직렬화한다. `size=0`은 query에서 생략한다. |
| 96행, `$p` 호출 3488447 | 앞 응답의 `page.next`에서 시청자 수·라이브 ID를 읽어 다음 요청에 전달하고, 합친 목록은 `liveId`로 중복 제거한다. `RECOMMEND` 모드는 크기 0을 전달하는 분기가 있다. |
| 96행, `K2` 3405296 / 호출 5305247 | 카테고리 목록 query는 `size`, `categoryId`, `categoryType`, `concurrentUserCount`, `openLiveCount`다. 호출부는 다음 cursor를 사용하며 합친 카테고리는 ID로 중복 제거한다. |
| 96행, `J2` 3405546 / 호출 5311175 | 카테고리별 라이브 query에 `concurrentUserCount`, `liveId`, `size`, `sortType`를 전달한다. 후속 페이지를 합칠 때 라이브 ID 중복을 제거한다. |
| 3행, `xf` 350069 | 단일 채널 상세를 조회한다. 같은 번들에 채널의 `followerCount`를 사용하는 화면 코드가 존재한다. |

근거: [동일 정적 자산](https://ssl.pstatic.net/static/nng/glive/resource/p/static/js/index-DWn2Eab5.js). 위 내용은 클라이언트 구현 관찰이다. `sortType`의 서버 측 허용 enum 전체, 기본 정렬·최대 `size`·cursor의 snapshot 일관성·호출 quota를 정의한 공개 계약으로 취급하지 않는다. 검색으로 찾은 `orderType`를 라이브 목록의 인자로 혼용하지 않는다.

### 커밋을 고정한 구현과의 대조

| 고정 소스 | 확인한 내용 | 적용 범위 |
|---|---|---|
| `wzgrx/PureLive` `a15cfc7c73401bd2b10a22538647554515d13734` | 목록 URL·query·cursor parsing·중복 제거를 구현한다. [chzzk_api.dart 535–637](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_api.dart#L535-L637) | 현재 웹 코드와 같은 경로·cursor 필드의 구현 사례다. 네이버의 지원 계약은 아니다. |
| 같은 커밋 | 카테고리 `page.next`의 scalar 필드를 query로 돌려주고, 실제 요청 루프는 최대 4페이지다. [parser 436–483](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_api.dart#L436-L483), [요청 100–122](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_site.dart#L100-L122) | 4페이지는 그 클라이언트의 상한이다. 전체 카테고리가 항상 4페이지 안에 든다는 근거로 쓰지 않는다. |
| 같은 커밋 | lives `size=30`, categories `size=50`, 페이지 번호 재순회 상한 20을 설정한다. [상수 262–275](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_api.dart#L262-L275) | 클라이언트가 선택한 값이며 서버 최대 허용량을 증명하지 않는다. |
| 같은 커밋 | 요청 헤더에 UA/Accept/Origin/Referer를 둔다. [헤더 214–229](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_api.dart#L214-L229), [HTTP 호출 73–84](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_site.dart#L73-L84) | 이 메서드에 공식 Client 인증을 주입하는 코드는 없다. 우리 환경의 응답 성공 여부는 실측으로 구분한다. |
| `liuchuancong/pure_live_TV` `6a1186630b41df36d0ae4ed7bb996d6c1c745897` | cursor 포함 경계를 가정해 다음 요청을 `size+1`로 하고 첫 중복을 버린다. [202–224](https://github.com/liuchuancong/pure_live_TV/blob/6a1186630b41df36d0ae4ed7bb996d6c1c745897/lib/platforms/chzzk/chzzk_api.dart#L202-L224) | 위 최신 PureLive 주석은 경계를 제외한다고 설명한다. 서로 다른 구현의 가정이므로 어느 쪽도 서비스 계약으로 단정하지 않는다. |

이 비교의 결론은 **응답이 준 cursor를 사용하고, 실제 ID 중복·반복 cursor를 검출하라**는 것이다. 마지막 행의 라이브 ID만 임의로 만들거나, 페이지 경계를 항상 1개 버리면 된다고 가정하지 않는다. 반복 cursor를 발견하면 전체 순회 성공으로 처리하지 않는다.

## 4. 전체 커버리지와 정렬 변화

공개 웹 자체가 `liveId` 중복 제거를 수행한다는 것은 확인했다. 다만 이 코드만으로 중복의 실제 발생 빈도나 원인을 알 수는 없다. 시청자 수를 cursor의 일부로 사용하는 가변 목록에서 순위 이동·새 방송·종료 방송 때문에 여러 페이지가 같은 순간의 스냅샷이 아닐 수 있다는 것은 **설계상 추론**이다. [first-party 호출부](https://ssl.pstatic.net/static/nng/glive/resource/p/static/js/index-DWn2Eab5.js), [기존 클라이언트의 페이지별 중복 제거](https://github.com/wzgrx/PureLive/blob/a15cfc7c73401bd2b10a22538647554515d13734/packages/live_core/lib/src/sites/chzzk/chzzk_site.dart#L147-L179)

따라서 다음을 구현 계약의 후보로 둔다.

1. scan 시작·종료 시각, 실제 요청 수, 읽은 페이지 수, 고유 방송 수, 중복 수를 남긴다.
2. `page.next` 종료를 확인한 경우와 요청 예산·기한 때문에 중단한 경우를 나눈다. 빈 페이지와 유효한 다음 cursor가 함께 있으면 계약 불일치로 관측한다.
3. 반복 cursor·중복 폭증·잘못된 JSON·429/5xx·필드 누락을 구분하고, 실패를 모든 채널의 방송 종료로 바꾸지 않는다.
4. 공개 ID인 `channelId + liveId`로 중복을 제거하고, 임대 계획의 채널 키와 방송 회차를 구분한다.
5. 화면은 “이번 탐색에서 발견한 방송 수”와 “실제 선정·정상 수집 방송 수”를 표시한다. 끝까지 읽었어도 “전체 방송 100%”로 표현하지 않는다.
6. 카테고리 대조에서 처음 본 방송이 있으면 전역 목록의 잠재적 누락 신호로 본다. 서로 다른 시각의 두 목록을 전수성의 정답·오답으로 바로 판정하지 않는다.

위 동작은 제안이며 이 문서 작성으로 구현한 것은 아니다. 기존 [방송 발견 계약](../discovery/README.md)의 부분 결과·선정 이유·관측 시각·수집 비활성 미리보기 모델을 재사용할 수 있다.

## 5. 팔로워 조건과 registry polling

`kimcore/chzzk`의 고정 커밋 `9208ad6901242c2854c44d6b13e219d7d023239d`는 `/service/v1/channels/{channelId}`를 호출하며, 채널 타입에 `followerCount`·`openLive`를 정의한다. 쿠키는 인증 옵션을 가진 경우에만 붙인다. [채널 조회와 HTTP 요청](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/client.ts#L51-L93), [채널 필드](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/channel.ts#L3-L18)

이 타입·메서드 정의는 팔로워 기준의 구현 가능성을 뒷받침하지만, 현재 라이브 목록에 팔로워 합계가 항상 포함된다거나 단일 요청으로 전체 채널의 합계를 받을 수 있음을 증명하지 않는다. `followerCount`가 목록에 없으면 **합계 cache가 없거나 만료된 채널마다 추가 조회**가 필요하다. 비공식 경로에서 사용할 수 있는 배치 합계 조회 계약은 이번 조사로 확인하지 못했다. 팔로워 개인 목록은 호출할 필요가 없다.

자체 비용 모델은 다음과 같다. 서버가 허용한 요청 예산이나 실제 플랫폼 규모가 아니다.

```text
P = 실제 읽은 라이브 페이지 수
U = 팔로워 합계 캐시가 없거나 만료된 후보 채널 수
R = 상태를 따로 확인하는 registry 채널 수

공개 발견 + 팔로워 갱신 요청 ≈ P + U + 재시도
registry 한 회 조회 요청 ≈ R + 재시도
```

팔로워 기준을 쓰려면 최종 후보에만 합계를 조회하고 TTL 캐시를 사용하는 것이 요청 수를 줄인다. 처음부터 임의로 팔로워 1,000명 이상을 운영 기준으로 확정할 근거는 없다. `minFollowers`를 비활성화한 목록 발견부터 관측하고, 대표성·연결 수·실제 채팅 events/s를 보고 선정값을 결정할 수 있다. 팔로워 수는 Kafka 입력률의 대용 지표가 아니다. [선정 정책과 비용 모델](CHZZK_SCALE.md#5-스트리머-선정-정책-제안)

알려진 채널의 현재 방송은 `/polling/v2/channels/{channelId}/live-status`로 확인하는 코드가 두 라이브러리에 있다. 이 경로는 채팅 연결에 필요한 `chatChannelId`를 찾는 데도 사용된다. [kimcore status](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/live.ts#L152-L171), [sdkim96/chzzk-go 고정 커밋](https://github.com/sdkim96/chzzk-go/blob/3ac96faac23536fe99f5f519b0a68b280f8667e8/unofficial/live.go#L16-L46)

registry는 오래된 공개 채널 ID와 마지막 관측만으로 구성할 수 있다. 새 채널 발견은 목록 경로가 담당해야 한다. 상태 응답 실패·만료는 `UNKNOWN`이며 `OFFLINE`이나 채팅 0건과 구분한다. HTTP polling 증가와 WebSocket 동시 연결 증가는 서로 다른 비용이므로 각각 제한한다.

## 6. 브라우저·검색·제3자 디렉터리

### 브라우저와 검색

브라우저로 공개 목록을 스크롤하며 카드의 링크를 읽는 방법도 가능하지만, 공개 웹 코드는 결국 같은 JSON 목록을 페이지별로 더 읽는다. 따라서 JSON 경로의 목록 범위를 브라우저만으로 넓힐 근거는 없다. 렌더링·스크롤 누락·DOM 변경을 추가로 다루게 된다. 운영 수집보다는 API 변경 시 요청·응답 형태를 대조하는 진단 수단으로 남기는 편이 단순하다. [현재 웹의 목록 호출부](https://ssl.pstatic.net/static/nng/glive/resource/p/static/js/index-DWn2Eab5.js)

키워드 검색은 기존 `kimcore/chzzk`가 `keyword`·`offset`·`size`를 보내는 메서드다. 알고 있는 채널을 찾는 보조 기능으로 볼 수 있지만, 빈 문자열이나 임의 키워드 조합으로 전체 방송을 열거할 수 있다고 확인하지 않았다. [search.ts 198–216](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/search.ts#L198-L216)

### 제3자 목록

- NexBot은 자체 안내에서 치지직 공개 라이브 목록을 약 10분마다 수집하며, 그 사이에 짧게 방송한 경우나 방금 시작·종료한 방송이 반영되지 않을 수 있다고 설명한다. 외부 목록은 실시간 상태의 별도 기준 시각을 가진다. 해당 안내에서 전수 목록을 재사용할 API 계약은 확인하지 못했다. [NexBot 통계 안내](https://nexbot.shop/stats/guide)
- 도치서치는 자체 운영정책에서 치지직 Open API를 주요 출처로 밝히고, 완전성·실시간성을 보장하지 않으며 DB의 전부·상당 부분을 복제하는 체계적 자동 수집을 금지한다. 이 서비스를 대신 긁는 방안을 주 경로로 제안하지 않는다. [도치서치 운영정책 제2·4항](https://dochisear.ch/policy)

이는 각 서비스가 자기 서비스에 대해 밝힌 내용이다. 치지직의 접근 정책이나 다른 통계 서비스의 조건으로 일반화하지 않는다. 공개 페이지가 보인다는 사실만으로 방송 목록의 재제공 계약이 생기는 것은 아니다.

## 7. 저장·접근 경계와 미확인 사항

- 조사 결과에 원천 응답 전체, 방송 제목·채널명·썸네일, 채팅 원문·닉네임·사용자 ID·토큰을 보존하지 않는다. 공개 방송 식별자와 개인 작성자 ID를 구분하고, 공유 실측 근거는 가능한 한 수·필드명·타이밍·hash로 정제한다.
- 성인/접근 제한 표시는 후보 발견과 채팅 연결 가능 여부를 구분하는 입력이다. 목록에 없거나 값이 누락되면 제한이 없다고 단정하지 않는다. 고정 소스도 `adult`, `krOnlyViewing`, `blindType`, `userAdultStatus`, `chatActive`를 별도 필드로 둔다. [라이브 상태 타입](https://github.com/kimcore/chzzk/blob/9208ad6901242c2854c44d6b13e219d7d023239d/src/api/live.ts#L96-L123)
- 비공식 웹 경로의 공식 quota, IP별 제한, burst 제한, 전체 목록 보장, 페이지 최대 크기, 정렬별 cursor 계약은 확인하지 못했다. 클라이언트 설정·소량 성공·429가 없었다는 사실을 공식 한도로 바꾸지 않는다.
- 공개 정적 소스가 API를 호출하는 사실과 운영 확대의 안정성은 별개다. 이 조사는 대량 연결·장시간 수집·제한 방송 접근·인증 우회·외부 배포를 수행하지 않았다.
- Kafka HA와 bounded publisher는 Kafka 이후의 처리 및 로컬 상한을 다룬다. 목록에서 발견하지 못한 방송, 연결 전·끊긴 동안의 원천 채팅을 복구하지 못한다. [기술 목표](../TECH_GOALS.md), [수집기 계약](../fleet/README.md)

## 8. 실제 공개 HTTP 대조

기준 revision은 `128f3e6065da681c9c38f0b54ae6f9112429b498`이다. 임시 조사 명령은 `python3 work/public-catalog-research/probe.py`이며, 명령·probe SHA-256·실제 응답 시각·페이지별 수치·원천 응답 hash를 [정제 근거](../poc/evidence/public-catalog-research-2026-10-26.json)에 기록했다. 원천 응답은 보존하지 않으므로 hash만으로 과거 건수를 재계산할 수는 없다. 같은 절차를 다시 실행하면 그때의 다른 목록을 관측한다.

### 실행 조건

- 2026-10-26 **15:49:46~15:50:06 KST**에 페이지 순회와 채널 상세를 조회했다. 앞선 첫 페이지 확인은 15:47:01 KST다.
- `curl -q`로 사용자 curlrc를 비활성화하고 기본 TLS 인증서 검증을 사용했다. 쿠키·Client-Id·Client-Secret·OAuth를 전달하지 않았다. User-Agent는 `Jigeumini-Research/0.1`이다.
- 목록은 `size=50`, 최대 20페이지, 요청 시작 사이 최소 1초 간격으로 호출했다. 각 페이지 시작 전에 75초 예산을 확인하고 개별 요청은 15초·응답 1 MiB로 제한했다. `sortType`은 지정하지 않았다.
- 응답 `page.next`의 `concurrentUserCount`와 `liveId`를 다음 요청의 query 인자로 전달했다. HTTP 실패·잘못된 응답·반복 cursor는 중단 조건이며, 자동 재시도는 하지 않았다.
- `channelId`와 `liveId`는 실행 메모리에서만 중복 계산에 사용했다. 채널 ID·이름·제목·응답 원문을 결과 파일에 저장하지 않았다. 채팅 WebSocket 연결은 **0개**다.

### 결과

| 관측 | 결과 | 해석 |
|---|---|---|
| 사전 첫 페이지 | `size=20`, HTTP/API code 200, 20개, 다음 cursor의 두 필드는 정수 | 인증 없는 첫 목록 조회 확인 |
| 실제 페이지 순회 | 20요청 모두 HTTP/API code 200, 각각 50행 | 후속 cursor를 이용한 공개 목록 조회 확인 |
| 합친 목록 | 1,000행, 고유 채널 995개·고유 라이브 995개 | 서로 다른 방송 995개를 실제 발견 |
| 중복 | 19페이지에서 이전 페이지와 4행, 20페이지에서 1행 중복. 페이지 내부 중복 0 | ID 중복 제거가 필요함을 실측. 서버 내부 원인은 미확정 |
| 종료 | 19.136초, `page_budget`, 마지막 다음 cursor 존재 | `scanComplete=false`. 995를 전체 방송 수로 쓰지 않음 |
| 성인 표시 | 1,000행 중 `adult=true` 45행, `false` 955행 | 행 기준 합계이며 중복 제거 후 채널 수가 아님. 접근 가능성은 별도 |
| 목록의 팔로워 수 | 1,000행 모두 `followerCount` 없음 | 이번 응답의 채널 객체에서도 없음 |
| 채널 상세 1회 | `/service/v1/channels/{channelId}` HTTP/API code 200, `followerCount`가 0 이상 정수 | 단일 공개 채널의 팔로워 합계 조회 확인. 전체 분포·배치 조회는 미측정 |

첫 Python urllib 시도는 로컬 인증서 검증 오류(`SSLCertVerificationError`, verify code 20)로 HTTP 응답 전에 실패했다. 이를 서버의 인증 요구나 요청 거절로 계산하지 않았다. TLS 검증을 끄지 않고 curl의 기본 검증으로 위 결과를 얻었다.

이번 실측은 공개 목록 어댑터를 만들 근거다. 전체 cursor 종료, 모든 방송의 채팅 접근, 카테고리별 대조, 장시간 요청 안정성, 공식 quota, 원천 채팅 무누락은 확인하지 않았다. 제품 수집기·Kafka·DB·웹은 재기동하지 않았다.

## 9. 프로젝트에 적용할 순서

다음은 구현 제안이다. 이 조사에서는 제품 코드를 바꾸지 않았다.

1. 기존 `BroadcastCatalog` 경계에 공개 목록 어댑터를 추가하고, 커서 순회·중복 제거·부분 완료·요청 예산을 처리한다. 공식 어댑터의 인증 모델을 그대로 적용하지 않는다.
2. 알려진 공개 채널 목록과 마지막 관측 시각을 보관한다. 전체 목록을 한 번 읽지 못했다고 기존 채널을 모두 종료 처리하지 않는다.
3. 팔로워 하한이 필요한 선정 후보에만 상세 조회·TTL 캐시를 적용한다. 미확인 값을 0으로 바꾸지 않고, 실제 분포를 보기 전에는 특정 하한을 확정하지 않는다.
4. 기존 익명 READ 수집기와 채널 임대 경계에 연결한다. 방송 발견 수, 선정 수, 실제 연결 수, 정상 관측 수를 따로 기록한다. 현재 PoC의 3채널 검증 상한은 자동으로 늘어나지 않는다.
5. 카테고리 목록은 별도 호출 예산 안에서 보충·대조한다. DOM 수집은 공개 웹 요청 변경을 진단할 때 사용한다.

이 경로의 조사·제한된 실측에 공식 앱 등록은 필요하지 않았다. #47은 공식 인증·동의 경로를 선택할 때의 검증 이슈로 유지하고, 익명 경로의 확대 검증과는 분리한다.
