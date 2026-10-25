# 방송 리포트 검증

2026-10-25 · #7 / F06 → T02/T04/T08. [계약](REPORTS.md).

## 로컬 자동 검증

합성 이벤트·로컬 Kafka4.1.2 단일 broker/RF1·PostgreSQL16.14·Java21을 사용한다. 테스트마다 UUID topic/group/schema를 만들고 정리하며 공개 플랫폼에는 새 요청을 보내지 않는다. 실제 공개 PoC는 사용자의 확인 후 종료한 상태를 유지한다.

- 순수 정책4개: UNKNOWN/침묵/재접속에서 종료 금지, 자정·source ID 변경/OFFLINE, 늦은 관측의 내부 anchor/별칭, 피크 동률·plateau·겹침·공백.
- 실제 Kafka/DB7개: 시각 결합·늦은 귀속/120초 입력·중복·자정·재열기, 기존 링크 별칭과 빈 세션 정리, 진행 위치 SQL 실패의 전체rollback/충돌, active/진행 작업 보호·live 불변·만료 입력 부활 방지, 독립 report 중단/재시작 중 ranking 진행, 실제 실행JAR API, 품질 요약과 사후 정정.
- 요약 기대값: 합성3분의 참고14건, 정상0/미확인11/정상3, 품질 합계3·최대3·시청자 최고0·피크1개. 120초 지연1건 뒤 참고15/품질4/late1, 중복은 그대로. 품질 정정 뒤 정상0만 남아 합계0·최대0·피크없음, 소비 차단은null. spike/outbox는0이다.
- `QualityHistory`의 구간 품질을 기존 activity와 대조한다. 실제 DB 조회 직렬화 JSON을 `server/src/test/resources/broadcast-report.json`으로 공유해 웹의0/null/시각/품질 계약을 검사한다.
- 웹은 목록→상세, 부분/미확정, 내부 관측 세션, 조회 지연, 빈 목록, 피크, 정상0과음영공백, 갱신 실패의 이전 결과를 확인한다.

최종 구현 `55726f0`에서 `./scripts/check` Java84·하네스16·JAR, `./scripts/integration` 실제 Kafka/DB68(리포트7·공통 소비자 추출의category회귀8 포함), `./scripts/web check` 타입·단위29·빌드, `./scripts/web test:e2e` Chromium31이 통과했다. [정제 근거](../poc/evidence/reports-2026-10-25.json)에 코드/JAR/XML/공유JSON hash를 기록했다. 합성320/1440px 화면을 확인하고 작은 화면의 표를 내부 가로 스크롤로 보완했다. SQL private canary 노출0이다. Claude 승인·CI·병합은 STATUS에서 추적한다.

## 미검증 범위

새 live·실제 원천 방송 ID 변경·장시간 미종료 세션·고부하·복제 HA·다중 호스트·report 전용 강제 JVM 종료/고정 범위 replay는 미검증이다. 실제JAR 시험은메타데이터 외부조회 disabled이며 합성 Kafka 입력이다. 세션/별칭 SQL rollback은 검사했지만 모든 DB/broker 장애 지점의 무손실을 주장하지 않는다. 공유 pool의 논리적 소비 독립성은 자원 격리가 아니다.

## PR CI의 기존 heartbeat fixture 보완

최초 PR CI40204570648은 ChannelProbeTest의 pong 미응답 검사에서 실패했다. artifact에10.046초 실행·WS_CLOSED가 남았고 heartbeat timeout은 없었다. 그 연결 종료의 원인은 미확정이다. 별도1.2초 승인 지연으로 기존1초 예산의 DURATION_COMPLETE 조기 종료를 재현했다. [프로토콜 검증](../poc/PROTOCOL.md#heartbeat-회귀-시험의-관측-대기)에 따라 실제 채팅/ping/미응답/timeout 관측까지 유한 대기하도록 시험만 바꿨고 ChannelProbeTest7개가 통과했다. 최종단위/CI는 수정HEAD에서 다시 확인한다.

## 리뷰·CI 보완

Claude 초기 리뷰55726f0은 부분 보존 정리와 재계산의 충돌, earliest 안내와48시간 차단의 충돌로 REQUEST_CHANGES였다. 재계산에서 정리 대상 세션의 잔여 관측을 제외하고 리포트에7일 eventTime 계약을 적용했다. 새 회귀는10,005이벤트·10,006관측 중 각각1만행 삭제 후 새 내부 방송 입력,3일 지난 입력 재개,8일 입력 제외/offset 진행, 미래receivedAt 거부를 검사한다. TTL240에서도 리포트 구분은120초로 통일하고 모든 리포트 읽기SQL에5초 한도를 적용했다.

09b5a03의 push/PR CI40205131666·40205136741은 시각 기대값의 나노초와 관측의 마이크로초 내림 차이로 동일 시험에서 실패했다. 원격 XML은070074210ns 대070074000ns를 기록했다. 해당 기대 시각을 계약 정밀도로 고정했다. heartbeat 시험은 두 실행 모두 통과했다.

보완 구현976f88e에서 `./scripts/check` Java85·하네스16·JAR와 `./scripts/integration` 실제Kafka/DB70(리포트9)이 통과했다. 위1만행 경계 회귀·3일 입력 재개·공유fixture DB동일성 검사를 포함한다. 웹 코드/fixture는55726f0 이후 바꾸지 않아29/31 근거를 유지한다. 정제 JSON의reviewCorrections에 코드/JAR/XML hash를 추가했다.

976f88e는 Claude APPROVE를 받았다. 비차단 항목도 보완해 만료 관측을 배정/coverage에서 제외하고, 같은 원천ID 재등장 시 부분 삭제된 집계를 되살리지 않고 정리 뒤 재시도하게 했다. anchor 삭제를 명시해 부분 정리 회귀를 결정적으로 만들었다. 새 실제DB시험은 부분 bucket 삭제→입력 rollback→cleanup→동일입력재시도1건을 확인한다. 이 추가 보완의 최종검증은 별도로 기록한다.
