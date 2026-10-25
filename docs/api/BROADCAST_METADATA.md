# 공개 방송 메타데이터 계약

이 API는 방송 제목·공개 채널명·카테고리·썸네일·방송 상태를 조회한다. 채팅 집계와 독립적으로 동작하며 방송 중이라는 이유로 채팅 수집이 정상이라고 판단하지 않는다. 채팅 원문·시청자 닉네임·사용자 ID·인증 토큰을 읽거나 저장하지 않는다. 공개 채널명은 화면 표시 목적의 메모리 캐시에만 두고 DB·파일·로그에는 기록하지 않는다.

## 원천과 이용 범위

- 공식 [Live API](https://chzzk.gitbook.io/chzzk/chzzk-api/live)는 등록한 애플리케이션의 Client 인증으로 `/open/v1/lives` 목록을 제공한다. 공식 응답에는 `liveTitle`, `liveThumbnailImageUrl`, `adult`, 카테고리와 공개 채널 정보가 있다.
- 현재 익명 PoC 수집 환경에 맞춰 `https://api.chzzk.naver.com/service/v2/channels/{id}/live-detail` 공개 조회 어댑터를 분리했다. 경로와 필드 구조의 참고 근거는 [chzzk 라이브 어댑터 소스](https://github.com/kimcore/chzzk/blob/main/src/api/live.ts)다. 공식 Open API 계약으로 취급하지 않는다.
- live-detail의 `content=null`만으로 종료를 추측하지 않는다. `/service/v1/channels/{id}`의 채널 ID와 `openLive=false`까지 확인한 경우에만 종료로 표시한다. `OPEN`은 방송 중, `CLOSE`/`CLOSED`는 종료다. 명시되지 않은 상태는 미확인이다.
- User-Agent는 `Jigeumini/0.1`, Accept는 JSON이다. 쿠키·인증 토큰을 쓰지 않고 리디렉션을 따라가지 않는다. 성인·blind·401/403은 제한 상태로 처리하며 인증 우회를 시도하지 않는다.
- 공개 응답 성공은 재배포·상업 이용 허가의 증거가 아니다. [기존 이용 조건 조사](../chzzk-feasibility.md)와 [개발자센터 약관](https://developers.chzzk.naver.com/termsDetail)을 바탕으로 공개 운영 전 앱 승인·콘텐츠 표시 범위를 확인해야 한다. 공식 인증 어댑터는 `MetadataSource` 경계에서 교체할 수 있다.
- 원천 주소는 서버 코드에 고정한다. 요청자가 임의 URL이나 인증 정보를 전달할 수 없다. 외부 조회 중지는 분석기 `--metadata.enabled=false` 또는 `METADATA_ENABLED=false`로 설정한다. 실행 명령은 [루트 README](../../README.md)가 정본이다.

## HTTP 계약

모든 메타데이터 응답은 `Cache-Control: no-store`다. ID는 32자리 소문자 hex이고 유효하지 않은 ID·빈 ids·20개 초과 목록은 원천 요청 전 HTTP 400으로 거부한다.

| 경로 | 응답과 범위 |
|---|---|
| `GET /api/channels` | 현재 projection/run에 수집 이력이 있는 최근 20개 채널. 최신 bucket 순, 동률은 ID 순. `{asOf, scope: COLLECTED_CHANNELS_ONLY, channels}` |
| `GET /api/channels?ids=<id>,<id>` | 요청한 채널 최대 20개, 중복 제거. `{asOf, scope: REQUESTED_CHANNELS, channels}`. 관심 채널 조회용이며 수집 여부를 보증하지 않음 |
| `GET /api/channels/{id}` | 단일 메타데이터. 수집 이력이 없어도 공개 정보 조회 가능 |

채널 디렉터리는 기존 `chat_bucket_10s`를 읽기만 한다. 최근 조회 기간에 기록이 없어도 과거 수집 채널을 보여주며, 이 경우 웹의 건수는 `—`다. 전체 치지직 검색이나 전체 방송 목록이 아니다. 검색·카테고리 필터는 현재 표시 대상에 적용한다. 관심 채널은 20개 단위로 조회한다.

단일 메타데이터 필드:

| 필드 | 타입·의미 |
|---|---|
| `channelId` | 검증한 채널 ID |
| `channelName`, `title` | 문자열 또는 null, 최대 120/300자 |
| `categoryId`, `categoryName` | 문자열 또는 null, 최대 120자 |
| `thumbnailUrl` | 허용된 HTTPS 이미지 URL 또는 null |
| `liveStatus` | `LIVE`, `OFFLINE`, `UNKNOWN`, `RESTRICTED`, `NOT_FOUND` |
| `freshness` | `FRESH`, `STALE`, `UNAVAILABLE` |
| `reason` | `NONE`, `PENDING`, `RESTRICTED`, `NOT_FOUND`, `RATE_LIMITED`, `UPSTREAM_ERROR`, `INVALID_RESPONSE`, `DISABLED` |
| `checkedAt`, `expiresAt` | ISO UTC 시각 또는 null. STALE은 마지막 성공 조회 시각·만료 시각을 유지 |

신규 조회는 `UNKNOWN / UNAVAILABLE / PENDING`을 즉시 반환하고 백그라운드에서 갱신한다. 비공개·제한·삭제·스키마 오류는 제목·채널명·카테고리·이미지를 반환하지 않는다. `OFFLINE`은 공개 채널명만 유지하고 이전 방송 제목·이미지를 제거한다. adult 필드가 없거나 불명확해도 제한 없음을 추측하지 않는다. 응답 body·예외 원문은 오류 응답에 넣지 않는다.

## 캐시와 장애 경계

- 인메모리 캐시 최대 256개. 공개 상태 TTL 60초, 실패 재시도 간격 15초, 동일 채널 갱신은 하나로 합친다.
- 원천 동시 요청 4개, 대기 큐 32개. 포화 시 HTTP 조회를 막지 않고 기존 상태나 PENDING을 반환한다. 후속 조회에서 다시 시도한다.
- 연결 3초·응답 읽기 5초 제한, JSON body 최대 1MiB. 수집기의 인증·연결 흐름과 별도다.
- 만료 시 LIVE 표시와 썸네일을 즉시 제거한다. 429·전송/5xx 실패에 한해 이전 공개 텍스트를 STALE로 사용할 수 있으며 마지막 성공 후 5분까지만 응답에 사용한다. 실패가 반복되어도 기한을 늘리지 않는다. 캐시 엔트리는 다음 접근에서 만료 정리한다.
- 성인·접근 제한·404·잘못된 응답은 이전 공개 정보를 무효화한다. 원천이 아직 통지되지 않은 변경은 TTL 동안 반영이 늦을 수 있다.
- 브라우저도 `expiresAt` 이후 LIVE·이미지를 숨기고 마지막 성공 5분 후 공개 텍스트를 지운다. API 장애가 LIVE 상태를 무기한 고정하지 않는다. 메타데이터 장애가 채팅 건수 조회를 막지 않는다.

## 이미지 표시

`liveImageUrl`을 우선 사용하고 없으면 `defaultThumbnailImageUrl`을 사용한다. `{type}`은 `480`으로 치환한다. HTTPS, 사용자 정보·별도 포트 없음, `livecloud-thumb.akamaized.net` 또는 `.pstatic.net`/`.naver.net` 하위 호스트만 허용한다. `livecloud-thumb.akamaized.net`은 실제 공개 응답에서 확인한 호스트이며 다른 Akamai 호스트까지 허용하지 않는다.

서버는 이미지를 내려받아 저장하거나 프록시하지 않는다. 브라우저는 `referrerPolicy=no-referrer`와 lazy loading으로 이미지를 표시한다. 이미지 로드 실패·없음·방송 종료·제한·만료 시 대체 화면을 유지한다. 영상 재생이나 화면 스트리밍 기능은 아니다. 홈 대표 이미지는 기존 서비스 일러스트를 유지한다.

## 재현 가능한 검증

- 서버 HTTP fixture: 공개 필드 추출, 템플릿 이미지, 인증 미사용, 성인·blind·누락 필드, 종료·null 상태, 401/403/404/429/5xx/redirect, 잘못된 채널·JSON·대용량 응답, URL 허용 범위.
- 캐시: 동시 요청 합치기, TTL·재시도·5분 만료, 제한 전환, 용량 상한·큐 거절, 비활성화. 시계를 주입해 실제 대기 없이 검사한다.
- API/DB: 입력 제한·타입 계약·no-store, projection/run 격리와 과거 채널 조회. broker/DB는 기존 통합 검증으로 함께 검사한다.
- 웹: 계약 위반·요청 ID 대조, 20개 단위 조회, 만료·검색·0건 오인 방지, 합성 제목·이미지·필터·저장·상세, 이미지 실패·제한 전환·API 장애, 모바일/데스크톱 크기.

실제 실행일·명령·결과·실방송 확인 범위는 [STATUS](../STATUS.md)에 기록한다. 자동 테스트는 합성 데이터와 로컬 서버만 사용한다.
