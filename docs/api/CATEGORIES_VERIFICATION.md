# 카테고리 반응 검증

2026-10-25 · #6 / F05 → T02/T06/T07/T08. [계약](CATEGORIES.md).

## 현재 확인 범위

합성 입력·로컬 Kafka4.1.2 단일 broker/RF=1·PostgreSQL16.14·Java21을 사용한다. UUID topic/group/schema를 각 테스트에서 만들고 정리하며 실제 공개 채널에는 새 요청을 보내지 않는다. 최종 전체 회귀·코드/JAR hash는 선행 #5 병합 후 고정한다.

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

처음 집중 검사에서 테스트 DataSource 객체 분리로 rollback이 동작하지 않은 fixture 결함과, SQL CASE의 caught_up_at 타입을 timestamptz로 지정하지 않은 구현 결함을 재현했다. 같은 DataSource를 사용하고 명시적 cast를 추가한 뒤 집중6개가 통과했다. 읽기 품질 보완·최종 웹 스타일·선행 #5 통합 이후 전체 검사를 다시 실행한다.

## 미검증 범위

실방송 장기 관측·실제 카테고리 전환 시각·다중 인스턴스/partition 증설·고부하·hot key 성능·복제 HA는 검증하지 않았다. 독립 group의 짧은 중단/재시작은 논리적 진행 검증이며 고부하 자원 격리나60초 회복 실험을 뜻하지 않는다. #11에서 별도로 측정한다.
