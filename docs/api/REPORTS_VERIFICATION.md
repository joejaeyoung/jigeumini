# 방송 리포트 검증

2026-10-25 · #7 / F06 → T02/T04/T08. [계약](REPORTS.md).

## 로컬 자동 검증

합성 이벤트·로컬 Kafka4.1.2 단일 broker/RF1·PostgreSQL16.14·Java21을 사용한다. 테스트마다 UUID topic/group/schema를 만들고 정리하며 공개 플랫폼에는 새 요청을 보내지 않는다. 실제 공개 PoC는 사용자의 확인 후 종료한 상태를 유지한다.

- 순수 정책4개: UNKNOWN/침묵/재접속에서 종료 금지, 자정·source ID 변경/OFFLINE, 늦은 관측의 내부 anchor/별칭, 피크 동률·plateau·겹침·공백.
- 실제 Kafka/DB7개: 시각 결합·늦은 귀속/120초 입력·중복·자정·재열기, 기존 링크 별칭과 빈 세션 정리, 진행 위치 SQL 실패의 전체rollback/충돌, active/진행 작업 보호·live 불변·만료 입력 부활 방지, 독립 report 중단/재시작 중 ranking 진행, 실제 실행JAR API, 품질 요약과 사후 정정.
- 요약 기대값: 합성3분의 참고14건, 정상0/미확인11/정상3, 품질 합계3·최대3·시청자 최고0·피크1개. 120초 지연1건 뒤 참고15/품질4/late1, 중복은 그대로. 품질 정정 뒤 정상0만 남아 합계0·최대0·피크없음, 소비 차단은null. spike/outbox는0이다.
- `QualityHistory`의 구간 품질을 기존 activity와 대조한다. 실제 DB 조회 직렬화 JSON을 `server/src/test/resources/broadcast-report.json`으로 공유해 웹의0/null/시각/품질 계약을 검사한다.
- 웹은 목록→상세, 부분/미확정, 내부 관측 세션, 조회 지연, 빈 목록, 피크, 정상0과음영공백, 갱신 실패의 이전 결과를 확인한다.

최초 저장 집중검사5개와 category 회귀8개가 통과했고, API/요약까지 report7개가 통과했다. 이후 동일 SQL snapshot의 품질 조회와 최종 웹을 포함한 전체 회귀·정제 근거·Claude 리뷰·CI 결과는 완료 후 STATUS에서 갱신한다. 실행 중인 시험을 통과 기록으로 계산하지 않는다.

## 미검증 범위

새 live·실제 원천 방송 ID 변경·장시간 미종료 세션·고부하·복제 HA·다중 호스트·report 전용 강제 JVM 종료/고정 범위 replay는 미검증이다. 실제JAR 시험은메타데이터 외부조회 disabled이며 합성 Kafka 입력이다. 세션/별칭 SQL rollback은 검사했지만 모든 DB/broker 장애 지점의 무손실을 주장하지 않는다. 공유 pool의 논리적 소비 독립성은 자원 격리가 아니다.
