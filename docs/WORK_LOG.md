# 구현·검증·리팩토링 작업 로그

작업에서 내린 결정과 실행 사실을 시간순으로 남긴다. 현재 상태와 다음 행동은 [STATUS](STATUS.md)가 정본이다. 수치는 해당 commit·실행 범위에만 적용한다.

## 2026-10-26 · 방송 리포트 #7 · F06 → T02/T04/T08

- 독립 report projection, 관측 세션·늦은 귀속·품질 요약·피크·웹 목록과 상세를 구현했다. 초기 리뷰의 부분 정리 재계산과 48시간 입력 차단 문제를 수정했다.
- 리팩토링 점검: 실제 두 호출자인 category/report의 Kafka 소비 수명주기를 `DualTopicConsumer`에 모았다. 저장/조회/세션 전이/품질 이력/피크 판정을 각 책임으로 나눴다. 범용 projection 프레임워크를 추가할 근거는 없어 현재 경계를 유지했다.
- 회귀: category 기존 8개와 report 최종 10개를 포함한 실제 broker/DB 검증, 동일 서버 JSON을 사용하는 웹 계약, 320/1440px 합성 화면을 확인했다. 만료 원천 ID 재등장·부분 삭제 후 재시도 회귀를 추가했다.
- 최종 `e1980f6`: 로컬 Java85·하네스16·JAR, 원격 실제 Kafka/DB71·웹29·Chromium31. Claude APPROVE와 push40206632119/PR40206635890 성공 후 PR #23을 `a69760a`로 develop에 병합했다.
- [정제 근거](poc/evidence/reports-2026-10-25.json). 단일 broker/RF1·합성 입력이다. 미종료 장기 세션 비용·원천 전체 수집·복제 HA는 입증하지 않았다. heartbeat fixture의 원래 WS_CLOSED 내부 원인도 미확정으로 남겼다.

## 2026-10-26 · replay 진단 #16 · F08 → T01/T02/T03/T04

- readonly 조회와 명시적 migration을 분리하고 생성/worker 빌드 식별, 고정 offset 보존 진단, 안전한 오류 코드, 실행 JAR 수명 처리를 보완했다.
- 리팩토링 점검: `PostgresSupport`가 DB pool 설정·소유·실패 정리를 공유하고, `ReplayBuild`가 빌드 식별 리소스와 검증을 소유한다. 조회가 migration/watchdog를 호출하지 않도록 명령 경계를 정리했다. 비교 SQL과 실행 상태 쓰기는 같은 저장 계약을 사용하므로 새 Store interface를 추가하지 않았다.
- 실제 CLI/JVM·Kafka control record·읽기 전용 trigger·보존 삭제·전역 worker lock을 검증했다. `ff5c30f` Java84/하네스16/실제Kafka·DB74, 후속 c222ee6 push40206443045/PR40206447239 성공을 기록했다.
- 승인된 #7의 최종 develop을 `537b3cc`에서 통합했다. [진단 검증](pipeline/REPLAY_DIAGNOSTICS_VERIFICATION.md)과 [정제 근거](poc/evidence/replay-diagnostics-2026-10-26.json)는 실행 시점별 수치를 구분한다. SIGKILL/HUP, 서로 다른 실제 두 JAR의 비교, 원천 공백 복원은 검증하지 않았다.

## 2026-10-26 · 방송 발견·선정 #26 · F12 → T13/T08/T11

- 공식 Live/Channel/Session/인증 문서를 다시 확인하고 전체 발견과 채팅 접근권·공개quota 미확인을 분리했다. [치지직 조사](research/CHZZK_SCALE.md)와 [배포/HA 조사](research/KAFKA_DEPLOYMENT.md)를 저장했다. #26~#31의 독립 범위·의존성과 검증 기준을 등록했다.
- 리팩토링 점검: 공식 HTTP와 합성 fixture의 실제 두 adapter를 BroadcastCatalog로 연결하고, SelectionPolicy의 선정·제외 사유·분모는 I/O 없이 검증한다. CLI는 설정·새 파일·종료만 소유한다. 수신 프로토콜이나 기존 메타데이터 캐시를 새 경로에 결합하지 않았다.
- fae9531 Java101·하네스16·JAR. 합성HTTP/선정/CLI16개와 실제JAR cutoff미설정/0/1000/5000/10000을 확인했다. [정제 근거](poc/evidence/selection-2026-10-26.json). 실제 앱 승인·목록/동의/수신 성공은 검증하지 않았다.

### #26 Claude 리뷰 후 검증 경계 보완

- a31eebe 리뷰는 APPROVE였다. 비차단 지적 중 HTTP 응답 뒤 별도 스레드 단언이 테스트에 전달되지 않는 점을 수정해 요청 query·헤더를 테스트 본문에서 검사한다.
- monotonic 시계의 첫 요청 기준을 현재 nanoTime으로 초기화하고 출력 부모 경로를 원천 호출 전에 검사한다. TTL 테스트 이름과 실제 판정 범위를 맞췄다.
- 하한5종의 정책 파일·실행 명령을 저장하고 공식 반복 query/null cursor의 실응답 미확인을 조사 한계에 추가했다. 원천 호출은 계속 비활성 기본값이다.

### #29 Kafka HA 계약 구현·리팩토링 점검

- 공식4.1 KRaft·ELR·topic·재배치 문서를 확인했다. ELR 신규 기본값을 기록하고 HA 시작에서 finalized feature1을 요구한다.
- 토픽 생성과 실제 RF/minISR/retention/unclean 검사를 KafkaTopicPolicy에 모았다. 공통 Admin 수명·재시도는 KafkaSupport, 각 기능은 정책 전달만 담당한다. 기본 LOCAL은 유지한다.
- 새 기능용 설정을 별도 record로 묶어 수집 발행 설정과 분리했다. 실제 장애 프로세스·RF1→3 전환은 별도 유한 하네스로 검증한다.

## 2026-10-26 · #29 HA 장애 리허설 구현

- 실제3전용 controller·3broker를 UUID Compose project로 격리한다. 노드별 소유권·명시적 데이터 볼륨·이미지의 다른 VOLUME 경로 tmpfs·검증 후 소유 자원 정리를 추가했다. 기존 서비스는 정지하지 않는다.
- broker1대 손실/ISR1/복구, controller1대/다수 손실/복구, RF1→3 이행, ACK→broker 읽기→DB 집계를 별도 단계로 기록한다. 단계별 JSON checkpoint와 유한 호출/발행 기한을 둔다. 단일 호스트 테스트를 AZ 장애 증명으로 보지 않는다.

- #29 첫6노드 실행은 broker1대중단/재합류까지통과했으나 ISR1단계의ExecutionException으로중단·소유자원정리완료했다. 당시세부조회단계가없어정확한실패원인을소급확정하지않는다. 내부consumer_offsets의minISR2불충족으로진행위치조회도불가능할가능성을분리하고조회불가를0lag로표시하지않도록계측을보완한다. 재실행에서구체단계/원인타입을확인한다.

- #29 두번째실행은ISR1에서GROUP 조회는성공하고END_OFFSETS_READ가TimeoutException인것을실제기록했다. 이전원인이동일하다고소급확정하지않는다. ISR1발행ACK0/진단미관측·ISR복구는통과했다. controller1대손실뒤새토픽준비에서실패해원인체인과단계를추가하고타임아웃만유한40초범위에서재시도/기록한다. 설정불일치는재시도하지않으며제품초기화기한은바꾸지않는다.

- #29 세번째실행은controller1대손실시준비타임아웃(DisconnectException체인)1회뒤성공,과반손실시새토픽ACK실패와기존토픽발행관측,quorum복구까지통과했다. RF1새토픽의별도Admin조회에서UnknownTopicOrPartitionException을관측했다. 초기화Admin과실험Admin의준비시점차이를다루도록같은일시오류만10초내재조회하고횟수를기록한다. 과거실패원본은보존한다.

- #29 최종01e943e의6노드10단계모두통과했다. mainACK171=broker171=DB171·이행20일치·소유컨테이너/볼륨0. controller과반손실시metadata쓰기실패와기존topic1ACK를구분했다. 실패3회·성공1회를함께보존하고리팩토링소유권/정책경계를문서화했다.
