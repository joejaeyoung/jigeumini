# 방송별 관측 리포트

#7 / F06 → T02·T04·T08. 채팅·방송 관측을 독립 report group으로 소비해 관측 범위의 요약을 만든다. 상태 정본은 [STATUS](../STATUS.md), 자동 검증은 [검증 기록](REPORTS_VERIFICATION.md)이다.

## 방송 구분과 종료 근거

- 검증된 원천 방송 ID가 있으면 `(channelId, sourceBroadcastId)`로 안정적인 내부 리포트 UUID를 만든다. 자정·재접속·같은 ID의 재관측으로 나누지 않는다.
- 원천 ID가 없으면 첫 LIVE 관측의 내부 anchor를 사용한다. 이전 시각 표본이 늦게 도착해도 기존 anchor를 유지한다. 늦은 원천 ID로 귀속이 바뀌면 이전 URL은 별칭을 통해 새 리포트로 연결한다.
- 명시적인 OFFLINE은 `OFFLINE_OBSERVED`, 다른 원천 ID의 LIVE는 `SOURCE_CHANGED`로 관측 세션을 닫는다. 채팅 침묵·HTTP 오류·UNKNOWN·TTL 만료는 종료 근거가 아니다. 마지막 LIVE 유효 시간이 지나면 상태만 UNKNOWN으로 바뀌며 `UNCONFIRMED`가 남는다.
- 원천 ID 없는 표본이 연속되면 같은 내부 관측 세션으로 묶는다. 그 사이 실제 방송이 바뀌었는지는 확인할 수 없으므로 `identityUnconfirmed=true`다. 내부 세션을 실제 방송 ID처럼 표시하지 않는다.
- `observedFrom/observedTo`는 해당 세션의 최초/마지막 표본 시각이다. `endedAt`은 종료 근거를 관측한 시각이며 실제 시작·종료 시각을 추정하지 않는다. 모든 응답은 `scope=OBSERVED_ONLY`, `partialObservation=true`다.

## 멱등 저장·늦은 입력

V9의 `report_*` namespace는 live ledger·순위·알림을 수정하지 않는다. source topic ID, 관측/채팅 ledger와 payload hash, 세션/별칭, 10초 bucket, partition 진행 위치를 보관한다. 원문·닉네임·시청자 사용자 ID·토큰·공개 제목/채널명/이미지를 저장하지 않는다.

채팅은 eventTime 이전 최신 관측과 결합한다. 해당 관측이 LIVE이고 나이가 `min(120초, ttlSeconds)` 미만인 경우에만 방송에 배정한다. 미확인/만료/종료 구간은 미배정 계수로 남긴다. 동시 시각은 eventId 오름차순으로 세션을 계산하며 마지막 eventId 표본이 채팅 배정을 결정한다. 원천의 실제 전환 시각은 알 수 없다.

한 배치의 새 입력·세션 계산·관측 귀속·채팅 재배정·bucket 증감·버전·진행 위치는 같은 DB 트랜잭션이다. 채널 잠금을 정렬해 획득하고 bucket 증감도 정렬한다. 같은 ID의 동일 payload는 재증가하지 않으며 충돌/SQL 실패는 전체 rollback이다. 관측이 늦게 도착하면 `[observedAt, observedAt+120초)`와 변경된 관측 ID에 붙은 보존 이벤트를 다시 배정한다. 긴 ID 목록은 PostgreSQL uuid 배열 한 개로 전달한다.

`receivedAt-eventTime>=120초`를 late로 센다. 최근7일 eventTime의 과거 bucket과 리포트를 고치지만 실시간 spike/outbox를 생성하지 않는다. 공통 live-age 계약은 유지하므로 receivedAt이48시간보다 오래된 미처리 입력은 차단된다. eventTime7일 밖 입력은 결과 부활 방지를 위해 배정하지 않고 진행 위치만 반영한다. 이미 삭제된 입력/관측·Kafka 만료분을 재생성할 수 있다는 뜻이 아니다.

## 독립 소비와 품질

group은 `${pipeline.group}-reports`, 시작 위치는 `reports.offset-reset` → `pipeline.offset-reset`(기본none)다. earliest는 처음 커밋이 없는 partition에만 적용한다. DB 성공 뒤 Spring BATCH·동기 offset commit이며 auto commit은 꺼져 있다. category와 report가 공통 `DualTopicConsumer`를 사용하되 store/group/오류 코드/진행 위치는 별도다.

채팅/관측 각각3개, 총6개 partition의 poll/end/DB 진행을 확인한다. 최근15초 이내 poll·관측, 최근10초 내 따라잡음, 완료 경계 이후 caught-up, DB offset≥알려진 end, 차단 없음이 필요하다. 토픽 ID가 바뀌거나 partition 수가 다르면 처리하지 않는다. report용 DLQ·자동 skip·고정 범위 replay는 없다. 운영 상태는 `/api/operations/reports`의 고정 코드로 확인하며 `lastFailure`는 성공 뒤 NONE으로 돌아간다.

논리적 독립 소비이며 같은 JVM·DB pool·broker를 공유한다. analyzer 시작도 두 토픽 검증에 의존한다. source 검사/표본은 Kafka·DB 사이 원자적 snapshot이 아니며 관측 주기 사이 입력은 다음 보정에 반영된다. 종료 중 sampler의 SQL 경쟁은 최대 freshness 만료까지 READY를 남길 수 있다. 고부하 자원 격리를 입증하지 않는다.

## 집계·피크 규칙

| 필드 | 의미 |
|---|---|
| referenceCount | 방송에 배정해 관측한 전체 채팅. 불완전/진행 중 분도 포함한 참고 누적값 |
| qualifiedCount | 품질을 충족한 완료 분의 합계. 해당 분이 없으면null, 정상 조용한 분은0 |
| maximumPerMinute | 품질을 충족한 완료1분 중 최대. 없으면null |
| viewerMaximum | 실제 숫자로 관측한 LIVE 시청자 수의 최고값.0은 유효하고 누락은null |
| completedMinutes / healthyMinutes | 평가할 완료 분 수와 품질을 충족한 분 수 |
| lateCount | 배정된 채팅 중120초 이상 지연 입력 수 |
| unassignedChannelCount | 이 채널 최근7일의 미배정 채팅. 특정 방송에 임의 가산하지 않음 |

분 경계는 UTC 정각이다. metricsFrom은 최초 관측 분 시작, metricsTo는 `floor((now-8초)/1분)`과 종료 관측 시각(미종료는 마지막 표본+TTL) 중 이른 분 경계다. 첫/마지막의 부분 분은 방송 구분 coverage가1이 아니면 제외한다. 수집 관측률≥95%, 알려진 loss/격리 없음, 당시 HEALTHY ranking snapshot·사후 정정 없음, 방송 구분 coverage=100%, report freshness를 모두 요구한다. 당시 snapshot이 없는 과거는 정상0으로 만들지 않는다.

리포트 데이터와 품질 조회는 같은 REPEATABLE READ DB snapshot에서 수행한다. `QualityHistory`는 세션·loss/격리·snapshot을 각 한 번 읽고 구간 합집합을 분별로 나눈다. 분마다 SQL을 반복하지 않는다. 정상 품질 판단은 기존 activity 조회와 대조한다.

피크는 양수인 정상 완료 분의 국소 최대값이다. 같은 값이 이어지면 첫 분, 양옆 연속 정상 분을 포함한 최대3분 구간을 후보로 삼는다. 품질 공백을 건너지 않으며 겹친 후보만 합친다(끝과 시작이 맞닿기만 하면 별도). 합친 구간의 최고값, 동률이면 먼저인 피크를 선택하고 최고값 내림차순·시각 오름차순으로 상위3개를 반환한다. 영상 클립은 만들지 않는다.

## API·버전·웹

- `GET /api/v1/channels/{channelId}/reports`: 최근7일에 관측/종료 근거가 있는 기록과 미종료 기록, 최근순100개. 더 있으면truncated. source ID 자체 대신 identity 구분을 반환한다.
- `GET /api/v1/reports/{UUID}`: 요약·피크·분별 품질. 별칭은 canonical reportId로 응답한다. 없거나 만료된 종료 기록은404, 잘못된 ID는400, 조회 실패는원문 없는503. 응답은no-store다.
- 목록의 `data-N`은 저장 revision이다. 상세 `report-v1-<SHA256>`는 입력 버전과 실제 계산한 품질/상태/요약 내용의 digest이며 조회 시각은 제외한다. 지연 입력·품질 정정·TTL 만료로 표시 내용이 바뀌면 버전도 바뀐다.
- `updatedAt`은 마지막 **저장 집계 반영 시각**, `asOf`는 조회/품질 판정 시각이다. 품질만 바뀌면 reportVersion은 바뀌고 updatedAt은 그대로일 수 있다.
- `/channels/:id` → `/channels/:id/reports` → `/reports/:id`로 연결한다. 내부 세션·종료 미확정·부분 관측·품질 미확인·이전 조회 결과를 구분한다. 요약은 전체 보존 관측 범위이며 그래프/표는 마지막360분으로 제한하고 잘림을 명시한다. 시청자 수를 선형 보간하거나 누락을0으로 채우지 않는다.

## 보존과 한계

매60초 현재 run의 종료 근거가7일보다 오래된 세션을 자식부터 정리한다. event/observation/bucket은각10,000행까지 삭제한 후 자식이 없는 세션을 삭제한다. 미배정 입력의7일 보존과 관측 결합의120초 여유도 적용한다. 별칭으로 대체된 빈 세션·0건 bucket·연결 대상이 사라진 별칭을 정리한다. 별칭 대상이 남아 있으면 이전 URL을 유지한다.

입력 트랜잭션은 cleanup 공유 잠금, 정리는 배타 try-lock을 사용해 진행 중 작업이 있으면 건너뛴다. active/UNKNOWN 미종료 세션과 그 입력은 침묵만으로 삭제하지 않는다. live ledger를 삭제하지 않는다. 보존 삭제량보다 입력이 많으면 물리 삭제가7일보다 늦어질 수 있다. 다른 run 정리, 오래된 미종료 세션의 운영 종료 정책은 별도다.

세션 재계산은 해당 채널의 보존 관측을 읽는다. 미종료 이력이 길면 입력 처리·상세 전체 요약의 CPU/메모리·조회 비용도 늘어난다. SQL5초·transaction20초 한도와 공유 pool을 사용하며 장시간/다중 사용자 비용은 #11 등 실측 전까지 성능 보장으로 주장하지 않는다. 세션/정제 이벤트의 JSON schema와 algorithm 변경에는 명시적 데이터 이행이 필요하다.
