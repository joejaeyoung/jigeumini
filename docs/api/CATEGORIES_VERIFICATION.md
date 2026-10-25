# 카테고리 반응 검증

2026-10-25 · #6 / F05 → T02/T06/T07/T08. [계약](CATEGORIES.md).

## 현재 확인 범위

합성 입력·로컬 Kafka4.1.2 단일 broker/RF=1·PostgreSQL16.14·Java21을 사용한다. UUID topic/group/schema를 각 테스트에서 만들고 정리하며 실제 공개 채널에는 새 요청을 보내지 않는다. 리뷰 보완을 반영한 `de67c97`에서 `./scripts/check` Java79·하네스16·JAR, `./scripts/integration` 실제Kafka/DB61, `./scripts/web check` 타입·단위27·빌드, `./scripts/web test:e2e` Chromium28이 모두 통과했다. 웹은 변경 없는9cedeff의27/28 근거를 유지한다. [정제 근거](../poc/evidence/categories-2026-10-25.json)에 코드/JAR/XML hash를 고정했다.

| 검사 | 확인 범위 |
|---|---|
| 시각 결합 | 채팅 이전 최신 관측·120초 경계·미상·역순 관측·중복·보정 revision |
| 짧은 TTL 보정 | 새60초 TTL이 기존120초 관측을 대체한 뒤 만료 구간을unknown으로 재배정 |
| 원자성 | progress SQL trigger 실패 시 새 채팅/관측/배정 정정/bucket/revision 전체 rollback, 복구 뒤 정확한 합계 |
| 충돌·동시 입력 | 같은ID 다른 payload 거부·두 채널 같은 카테고리의100개 고유 이벤트와 중복100개를100건 반영 |
| 독립 소비 | category 중단 중 ranking3건으로 진행·category1건 유지, strict 재시작 뒤3건·중복도3건 |
| 원천·freshness | topic 재생성 거부,6개 partition·양쪽 토픽 poll/end/DB 진행 필요, 알려진 lag·차단·만료 시 보류 |
| 보존 |7일 경과 ledger/bucket 정리와 오래된 입력의 부활 방지 |
| 조회·웹 | 품질 충족 합계·미상 포함 분모·0건/공백·60개1분·기여 방송·정정 버전·320px·실패 후 이전 결과 |

처음 집중 검사에서 테스트 DataSource 객체 분리로 rollback이 동작하지 않은 fixture 결함과, SQL CASE의 caught_up_at 타입을 timestamptz로 지정하지 않은 구현 결함을 재현했다. 같은 DataSource를 사용하고 명시적 cast를 추가한 뒤 집중6개가 통과했다. 알려진 lag를 즉시 제외하는 읽기 품질 보완·최종 웹 스타일·선행 #5 통합 이후 전체61개가 통과했다. 실행 JAR에서는 category6개 partition·비어 있지 않은 집계 참고1/품질 미확인null·60개1분 JSON 응답을 확인했다. 합성 화면을320/1440px에서 직접 확인했고 그래프 높이/표 폭을 조정했다. 결과 XML의 private canary 노출0이다.

## 미검증 범위

실방송 장기 관측·실제 카테고리 전환 시각·다중 인스턴스/partition 증설·고부하·hot key 성능·복제 HA는 검증하지 않았다. 독립 group의 짧은 중단/재시작은 논리적 진행 검증이며 고부하 자원 격리나60초 회복 실험을 뜻하지 않는다. #11에서 별도로 측정한다.


## 리뷰 후 보완

초기 `c7b80b4`와 보완 `de67c97`는 Claude APPROVE다. 조회/정리용 `(run_id,event_time)` 인덱스, store/consumer의 동일 readyTopic 주입, 입력·내용 충돌·DB·원천의 lastFailure 고정 코드를 추가했다. 실제 잘못된 Kafka 입력에서 CATEGORY_INVALID_EVENT·offset 정지·기존3건 보존을 확인했다. 다른 run·48시간 live 계약·기동 결합·종료 관측 경쟁·분당10,000행 정리 한계를 계약에 명시했다.

2026-10-25T14:28:36Z의 로컬 합성20채널·1시간 조회는 관측1,200행/채팅0행/품질미확인에서 2,053.36ms였다. SQL 호출 수가 많은 구현의 단일 측정이며 고부하·7일 ledger 크기·다중 사용자·p95 보장이 아니다. #11에서 공유 DB/pool과 polling 부하를 함께 측정한다.


기본 collector의 전체 live 실행을 추가로 수행하지 않았다. 기본 source 빈은collector 역할의 PublicObservationSource 하나이며 MetadataService 구성은analyzer 전용이라는 코드를 확인했다. #5의 실제 HTTP fixture/Spring collector 연결과 프로토콜 시험 범위를 넘겨 주장하지 않는다. Gradle integrationTest는 bootJar에 의존하므로 실제JAR 시험 전에 해당 소스의 JAR를 만든다. 종료한 별도 JVM은 대기 후에만 해당 group/schema/topic을 정리한다.
