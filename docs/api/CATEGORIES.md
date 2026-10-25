# 카테고리별 채팅 반응

2026-10-25 · #6 / F05 → T02/T06/T07/T08. 수집 대상 내 완료된5분 채팅을 당시 관측한 카테고리로 묶는다. 최대20개 선정 채널 범위이며 전체 치지직 점유율이 아니다.

## 시각 결합과 보정

별도 `<pipeline.group>-categories` group이 채팅과 `broadcast.observed.v1`을 함께 소비한다. 메타데이터 화면 캐시를 사용하지 않는다. 채팅 eventTime 이전/동일 시각의 최신 관측을 고르고 `eventTime < observedAt + min(120,ttlSeconds)`일 때만 카테고리를 사용한다. 같은 observedAt은 eventId 내림차순으로 결정한다. 이후 관측, 만료, 오프라인·제한·실패·카테고리 누락은 `unknown`이다. 관측 시각은 HTTP 응답 시각이며 실제 전환 시각은 모른다. 카테고리 이름은 조회 종료 시각 이전의 마지막 이름을 표시하며 과거 이름 변경까지 재현하지 않는다.

채팅 시각은 PostgreSQL 저장 전에 마이크로초로 내림한다. source category ID의 SHA-256을 조회 키로 쓰고 미상 키는 `unknown`이다. 카테고리/채널/10초 bucket을 저장하므로 여러 채널이 같은 카테고리에 몰려도 동일 bucket 행 하나를 공유하지 않는다. 조회에서 합산한다. 이 구조의 hot key 처리량·비용은 #11에서 측정하며 개선 수치를 주장하지 않는다.

같은 배치의 새 관측을 먼저 저장하고 새 채팅을 배정한다. 늦은 관측은 `[observedAt, observedAt+120초)` 안의 보존된 채팅만 현재의 전체 관측 이력으로 재평가한다. 새 TTL이 짧아져 그 이후가 미상으로 바뀌는 경우도 포함한다. 각 변경의 이전 bucket을 빼고 새 bucket을 더하며 음수는 DB 제약으로 거부한다. 채널별 advisory transaction lock과 정렬된 bucket 갱신으로 동시 배정·보정을 직렬화한다. 실제 채팅이 새로 추가되거나 새 관측이 저장된 배치마다 run의 `revision`을1 올린다. 관측만 추가되어 건수가 같아도 증가하며 같은 이벤트 재전달에는 증가하지 않는다. 행별 배정 revision도 보관한다.

## 처리와 복구

V8의 category 전용 source identity·관측 이력·event ledger·bucket·revision·progress를 사용한다. 입력 검증 뒤 모든 배정·보정·DB progress는 같은 트랜잭션, 그 성공 뒤 BATCH 동기 Kafka commit이다. 같은 eventId의 내용 충돌과 SQL 실패는 배치 전체 rollback이며 기존 ranking/trend 이력을 수정하지 않는다. 관측 runId가 다르면 배정 없이 진행 위치만 기록한다. 채팅은 기존 이벤트 계약·live 보존 범위를 적용한다. receivedAt이48시간을 넘은 입력은 현재 계약상 차단된다. Kafka 보존을 늘릴 때 자동 skip으로 바꾸지 말고 이 보존/격리 계약을 함께 변경해야 한다.

`categories.offset-reset`을 우선하고 생략하면 `pipeline.offset-reset`을 상속한다. 기본none, 최초 새 그룹에만earliest를 명시한다. 채팅/관측 모두3 partitions이며 합계6개다. topic ID를 DB에 고정하고 시작·배치·관측 때 확인한다. 재생성·partition 수 변경은 차단하며 기존 offset을 되감거나 새 토픽에 자동 결합하지 않는다.

영구 계약 오류·내용 충돌은 카테고리 소비를 계속 차단할 수 있다. RETRYING/고정 코드로 표시하고1초 간격으로 재시도한다. 이 버전에는 카테고리용 DLQ나 자동 skip이 없다. 생산자/소비 계약을 수정한 뒤 같은 group에서 재시도하며, 해소할 수 없는 충돌은 별도 격리 절차가 필요하다. ranking/trend의 #10 DLQ 성공이 category group의 성공을 뜻하지 않는다. 시작 때 topic 확인에 실패하거나 source ID가 바뀌면 analyzer 구성 전체가 기동하지 못한다. 원천 연결/토픽 정합성을 복구한 뒤 같은 설정으로 재시작한다. 토픽을 재생성했다면 이전 namespace에 자동 연결하지 말고 새 run/group으로 분리해 재구축 범위를 판단한다. 실행 중에는 group 소비가 분리되지만 기동 장애까지 독립적인 것은 아니다.

종료 요청과 이미 실행 중인 sampler SQL이 겹치면 다른 reader에서 READY가 마지막 표본의 freshness 기한까지 남을 수 있다. freshness 기한이 지나면 보류하며 즉시 상태 전파를 보장하지 않는다.

DB pool·broker·호스트 자원은 공유하므로 논리적 독립성이 자원 장애 격리를 보장하지 않는다.

## 품질과 조회

조회 종료는 `floor((현재UTC-8초)/60초)`의 완료 분이다. 현재 순위는 앞5분, 상세 추이는 앞60분이다. category SQL은 REPEATABLE_READ에서 이력·bucket·revision을 읽는다. 기존 ranking 품질은 별도 조회이므로 둘 사이 단일 원자 snapshot은 보장하지 않는다.

5초마다 두 입력 토픽의 end offset과 마지막 poll을 관측한다.6개 partition 모두 관측/poll15초 이내·따라잡음10초 이내이며 조회 종료 뒤 따라잡았고, 최신 관측 end 이상을 DB progress가 처리하고 차단 상태가 없어야 READY다. 그 외 LAGGING이며 순위 건수/비중을 보류한다. 샘플 간격 사이의 장애·새 입력은 다음 관측에 반영되므로 순간 완전성을 주장하지 않는다.

5분의 모든1분 품질이 HEALTHY인 채널만 순위 합계·비중에 포함한다.1시간 추이는 각 분의 HEALTHY 채널만 포함한다. 실제 관측으로 카테고리 소속을 확인한 조용한 채널은0건, 품질 충족 채널이 하나도 없으면null이다. 관측 사이 만료 구간은unknown 소속이다. 같은 채널이 창 안에서 카테고리를 바꾸면 각 카테고리의 관측 채널 수에 포함될 수 있다. 관측 채널 수는 카테고리 소속/입력이 확인된 채널 수이며 전체 방송 수가 아니다.

- `GET /api/v1/categories`: `[from,to)`·scope·READY/LAGGING·revision·unknownReferenceCount·truncated와 카테고리별 nullable count·referenceCount·관측/품질 충족 채널 수·비중·기여 채널. 비중 분모에는unknown을 포함한 품질 충족 건수를 사용한다. 분모0이면 비중null. 건수 내림차순, 동률은categoryKey 순서다.
- `GET /api/v1/categories/{key}/timeline`: 같은 범위 기준의 연속60개1분 point, 품질 충족/관측 채널 수·nullable count·referenceCount·revision·scope·truncated.
- `GET /api/operations/categories`: 실행·할당·배치·실패 시도·원천 확인 상태·lastFailure 고정 코드. 원천 변경/접속 실패·내용 충돌·입력 계약·DB 오류를 구분한다. RUNNING만으로 낮은 lag를 주장하지 않는다.

최대20개 최근 활동 채널만 포함하고 초과하면truncated를 표시한다. no-store·잘못된key400·DB/계약 조회 실패503을 구분한다. 웹은 비중 범위와 미상 참고 건수, 기여 방송·최근1시간 막대/표, 정상0과 미확인 음영, 조회 실패 시 이전 결과를 표시한다. 기여 방송의 공개 이름은 기존 메모리 캐시에서만 읽고 category DB에는 저장하지 않는다.

## 보존과 한계

정리 한도는 테이블당 분당10,000행(평균 초당 약167행)이다. 지속 입력/정리 backlog가 이 한도를 넘으면 물리 저장기간이7일보다 길어질 수 있으며,7일은 정확한 용량/삭제 기한 보장이 아니다. #11에서 입력률과 정리 처리율을 함께 측정한다.

현재 run의 채팅 ledger·bucket은7일, 관측은 시각 결합 경계를 위해7일+120초 보존한다. 매60초 각 테이블 최대10,000행을 정리한다. eventTime이7일 이전인 재전달은 재집계하지 않는다. 다른 run의 category 데이터는 자동 삭제하지 않으므로 run 변경 시 해당 namespace의 운영 정리가 필요하다. 원래 live ledger·replay 작업은 삭제하지 않는다. Kafka의 시간/용량 보존이 먼저 끝난 원본을 재구축할 수 없으며 category용 replay CLI는 없다.

원문·닉네임·사용자 ID·토큰·방송 제목은 저장하지 않는다. 공개 category 이름·채널 ID, UTC 시각·숫자·payload hash만 저장한다. 검증 상태는 [STATUS](../STATUS.md), 검증 범위는 [검증 기록](CATEGORIES_VERIFICATION.md)을 따른다.
