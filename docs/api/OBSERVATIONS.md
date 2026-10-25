# 시청자 관측과 채팅 비교

2026-10-25 · #5 / F04 → T02/T04/T06. 현재값을 보여주는 방송 정보 캐시와 별도로 관측 이력을 남긴다.

## 원천과 시각

공개 `GET /service/v2/channels/{channelId}/live-detail`의 `concurrentUserCount`는 현재 시청자 수(명), `liveId`는 원천 방송 식별자다. [공개 비성인3채널의 정제 표본](../poc/evidence/observation-source-2026-10-25.json)에서 각 한 번 실제 정수 필드를 확인했다. 이 표본이 전체 채널·장기 가용성을 보장하지 않는다. 원천 `openDate`는 시간대가 없어서 해석·저장하지 않는다. `observedAt`은 새 응답을 받은 수집기 UTC 시각이며 실제 방송 시작이나 카테고리 전환 시각이 아니다.

원천 계약 검토는 [기존 메타데이터 계약](BROADCAST_METADATA.md), [공식 방송 API](https://chzzk.gitbook.io/chzzk/chzzk-api/live), [공개 구현의 필드](https://github.com/kimcore/chzzk/blob/main/src/api/live.ts)를 참고했다. 공식 인증 API와 익명 공개 조회의 이용 범위를 같다고 주장하지 않는다. 소프트콘을 데이터 공급원으로 쓰지 않는다.

원문·닉네임·사용자 ID·토큰·방송 제목·공개 채널명·이미지는 이력 이벤트에 넣지 않는다. 채널 ID, 숫자 방송 ID, 카테고리 ID/이름만 식별·분류에 사용한다. 원천 응답 본문과 예외 메시지를 로그에 남기지 않는다. 합성 HTTP fixture는 제한/429/null/잘못된 타입을 검증한다.

## 수집

- 기존 collector에 선택한 채널만 새 HTTP 요청한다. 일반 `scripts/poc`와 analyzer의 화면 캐시는 이력을 생성하지 않는다.
- 기본60초, 설정 `observations.interval-seconds`는60~900초. 표본 TTL `observations.ttl-seconds`는 기본120초이며 주기 이상·최대1800초다. 해당 표본보다 세밀한 측정을 주장하지 않는다.
- HTTP connect3초/read5초, 응답1MiB, 요청 worker2개·대기20개. 채널별 한 요청만 진행한다. 429는60/120/240/480/900초 대기하며 성공·다른 응답 후 기본 주기로 돌아간다. 다음 시도는 응답 관측 시각 기준이다.
- `CollectionObserver.start/stopped`가 채널 활성 범위를 정한다. PoC 파일 잠금을 얻기 전에는 조회하지 않고 해당 채널이 중단된 뒤 새 관측을 예약하지 않는다. 진행 중 요청의 정리는 컨텍스트 종료에서 최대15초 기다린다. WebSocket 콜백은 활성 집합만 바꾸고 HTTP/DB를 호출하지 않는다.
- `observations.enabled=false`로 collector의 관측을 끌 수 있다. Spring 환경 변수 `OBSERVATIONS_ENABLED=false`, `OBSERVATIONS_INTERVAL_SECONDS`, `OBSERVATIONS_TTL_SECONDS`로 스크립트 실행에도 설정한다.
- 중복 방지는 한 프로세스의 채널 요청과 같은 checkout의 수집기 파일 잠금 범위다. 서로 다른 checkout/호스트의 분산 lease는 없다. 그 범위는 운영 확대 전에 보완한다. 현재 live 하네스는 최대3채널 제한을 유지한다.

## 이벤트와 저장

`broadcast.observed.v1`은 key=channelId, CreateTime=observedAt, 3 partitions/RF=1의 **비압축 이력** 토픽이다. `observations.topic`으로 별도 이름을 지정한다. 채팅/알림 토픽과 같은 이름은 거부한다. 공통 초기화는 cleanup.policy=delete를 명시·검사하고 compact 기존 토픽을 변경 없이 거부한다. 생성·기존 설정 확인은 [공통 기동 계약](../pipeline/TOPIC_READINESS.md)을 따른다.

이벤트는 schemaVersion=1, UUID eventId, runId, channelId, observedAt, ttlSeconds, nullable viewerCount, liveStatus, quality, nullable sourceBroadcastId, broadcastIdentity, nullable categoryId/categoryName이다. 최대4KiB, 정수 문자열·소수 건수·음수·JS 안전 정수 초과를 거부한다. 실제0명은 OBSERVED/0, 없는 수치는 MISSING_VIEWER_COUNT/null이다. 실패·제한·오프라인에서는 이전 수치를 복사하지 않는다. 방송 ID가 없으면 UNAVAILABLE이며 내부 ID를 원천 ID처럼 꾸미지 않는다.

별도 `<pipeline.group>-observations` consumer가 V7 `broadcast_observation`과 `observation_progress`를 같은 DB 트랜잭션에 저장한다. (runId,eventId) 중복은 한 번, 내용 충돌은 배치 실패다. DB commit 뒤 Spring BATCH 동기 offset commit이다. 기본 reset=none이며 새 그룹의 최초 실행만 `--observations.offset-reset=earliest`를 쓴다. 유효한 기존 offset을 되감지 않는다. 실패는 고정 코드와 RETRYING으로 노출하고1초 재시도한다. 내부 관측 계약 오류를 자동으로 건너뛰거나 원문 DLQ에 넣지 않는다.

역순으로 도착한 값은 실제 observedAt 위치에 저장한다. 서버 시각보다5초 넘는 미래는 거부한다. 조회의 현재값으로 과거를 채우지 않는다. ACK 실패는 OBSERVATION_PUBLISH_UNCONFIRMED와 이력 공백으로 남고 재시도한 새 HTTP 관측을 이전 시각에 붙이지 않는다. 메모리 입력의 강제 종료 복원과 원천 중복 제거는 보장하지 않는다.

## 보존과 용량

Kafka24시간/partition당256MiB/segment32MiB는 시간·용량 중 먼저 제한되는 보존 정책이다. 정확한24시간 가용성이나 용량 상한을 보장하지 않는다. DB 이력은 observedAt 기준7일이며 매60초 최대10,000행 정리한다. 7일 경계는 포함하고 그 이전만 삭제한다. 삭제된 과거 입력이 재전달되면 이력을 다시 만들지 않고 진행 위치만 기록한다. live 채팅 ledger·다른 projection은 삭제하지 않는다.

| 입력 가정 | 하루 이벤트 | 7일 이벤트 | 최대4KiB payload만 계산한7일 크기 |
|---|---:|---:|---:|
| 3채널·60초 | 4,320 | 30,240 | 118.125MiB |
| 목표20채널·60초 | 28,800 | 201,600 | 787.5MiB |

이는 각 요청당 한 표본의 산술 상한 추정이다. JSONB·행·인덱스·WAL·Kafka overhead와 여러 collector·재발행은 별도이며 실측 DB 크기라고 표시하지 않는다. 기본20채널의 하루 payload 상한은112.5MiB다. 기존 replay CLI는 채팅 이벤트 전용이며 관측 토픽 재처리를 지원한다고 주장하지 않는다. Kafka에서 삭제된 표본과 수집 공백은 복원할 수 없다.

## 조회와 웹

`GET /api/v1/channels/{id}/timeline?seconds=3600`은 서버가 정한 완료된 분의 `[from,to)` 창을 반환한다. 1800/3600초만 허용한다. chat은 기존 품질 판정을 따른30/60개 완료1분 point, observations는 실제 관측 시각 순서의 최대1,000개다. 많으면 최근1,000개와 truncated=true를 반환한다. 두 자료는 같은 시간 범위지만 각각 조회하므로 DB 전체의 단일 원자 snapshot이라는 보장은 없다. `asOf`와 no-store, 잘못된 인자400·조회 실패503을 구분한다.

상세 기본 창은1시간이다. 시청자(명)와 분당 채팅(건)을 별도의 세로축, 동일한 가로 시간축으로 표시한다. 시청자 점을 키보드/클릭으로 선택할 수 있다. 실패/null 표본, 두 표본 TTL 중 짧은 값 초과, 원천 방송 ID 변경에서는 선을 끊는다. 첫 관측 이전/마지막 관측 이후로 연장하지 않는다. 선은 표본 사이의 변화 표시이며 그 사이 실제 수치를 측정한 것이 아니다. 채팅 품질 미확인은 음영과 `—`, 관측 정상0건은0으로 표시한다. 갱신 실패에는 이전 조회 결과라고 안내한다.

`GET /api/operations/observations`는 consumer 실행·할당·성공 배치·실패 시도·고정 오류 코드를 제공한다. RUNNING은 전체 입력의 completeness나 낮은 lag의 증거가 아니다.

검증 결과·구현 버전과 남은 범위는 [STATUS](../STATUS.md)에서 관리한다.
