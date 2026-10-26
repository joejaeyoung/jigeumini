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

## 2026-10-26 · 부하·편향·복구 #11 · F10 → T06/T07/T08/T09/T11/T12

- 고정 합성 입력, 별도 topic/schema/group, 실제 DB commit 지연·partition lag·JVM/서비스 자원·고정 replay를 기록했다. 실패 실행을 포함한 원본과 hash·HTML을 보존했다.
- 리팩토링: 프로필 상한을 ExperimentPlan, 두 실험의 HTML을 ExperimentReport, 진행 중 계측 종료를 ExperimentSampler로 분리했다. 수집·발행·DB의 가변 수명은 Session이 소유한다.
- 검토에서 정렬된 manifest의 인덱싱은 기존 생성자 계약으로 안전함을 확인했으나 번호 조회로 모듈 간 가정을 줄였다. 종료 시 계측 interrupt와 입력 후 실패 판정 누락은 실제로 보완했다.
- 비동기와 shard는 실험 후보이며 제품 적용 #27·복제 HA #29·ACK 대조 #31과 분리했다. [검증 해석](pipeline/EXPERIMENTS_VERIFICATION.md)에 수치·실패·미확인 근거를 연결했다.


### #11 CI 초기 DB 경계 보완

- 8d533ee의 push40209091989/PR40209094885에서 새 부하 스모크1개가 시작0.16초에 실패했다. 나머지78개 실제 통합 검증은 통과했다. 기존 결과는 원인/단계 출력이 부족해 특정 SQL 예외라고 확정하지 않는다.
- 코드 확인에서 public 운영 테이블이 이미 있다는 가정을 발견했다. 새 CI DB에는 실수집 namespace를 만들지 않으므로 테이블 존재를 조회하고 없는 상태를 그대로 기록하도록 바꿨다. migration이나 운영 테이블 생성으로 맞추지 않는다.
- 빈/일부/존재하는 테이블을 실제 격리 schema로 대조하는 회귀와 단계·예외 타입·SQLSTATE만의 진단을 추가했다. 최종 CI로 수정 효과를 확인한다.

### #11 최종 계측 보완 재실행과 결과 정리

- steady 및 영향받은12개 결과를 추가해 원본39회/정제JSON/HTML을 재생성했다. 과거실패2회·계측이슈9회는 보존했다. 새12개는 계수/DB/replay/정리 통과, 직렬500/s만 목표 미달이며 다른11개는 목표 완료다.
- 전역3partition/consumer확장·키분산결정·복구 관측상한을 STATUS의 T06~T09에 연결했다. 제품공정성 #27·HA #29·ACK/커밋 #31의 남은 범위를 유지한다.
- 코드가 다른 실행은 commit/class hash별로 구분했다. final steady의7d7b87d와 aab0b76 사이에는 기준값 조회/진단 보완이 있고, 동일 바이너리 전체 재시험으로 표시하지 않는다.

- 자원 정리 감사에서 Kafka 이미지의 VOLUME3개가 docker rm만으로는 정리 증거에 포함되지 않음을 확인했다. 새 실험은 소유자label이 있는 명시적 data volume을 쓰고 나머지 두 경로는 tmpfs로 둔다. 정리 시 정확한 컨테이너·volume의 소유자label을 검사한다. 과거 익명 volume은 소유권을 기록하지 않아 임의로 삭제하지 않으며 과거 cleanupComplete를 컨테이너 확인 범위로 한정한다.

## 2026-10-26 · #11 마지막 자원 정리 검증

- cc0a1c1에서 실제broker4사례와소유컨테이너/볼륨정리통과. 과거3개의익명볼륨정리근거없음을정제표에명시하고원본40회로갱신했다. 기존계측문제9개와실패2개도보존한다. 실제발행후callback조기실패는최대용량미도달거절을허용하며계수는보존한다.

### #27 발행 경로 구현과 리팩토링 점검

- 전용 dispatcher가 채널별 queue를 한 건씩 순환하고 ACK callback이 대기 건수를 해제한다. Kafka send 자체의 metadata/buffer 대기는 수신 callback 밖에서 처리한다.
- admission·ACK·계수 snapshot은 같은 잠금으로 보존 관계를 유지하고 종료 join은 잠금 밖에서 실행한다. 종료 기한의 결과를 고정해 늦은 ACK가 저장된 결과를 바꾸지 않는다.
- 공개 EventSink/BufferState 계약을 유지하고 설정은 Limits로 묶었다. 발행 상태·공정 배분·계수를 단일 모듈 안에 두며 별도 thread pool/범용 scheduler를 추가하지 않았다.
- 채널 초과는 해당 연결을 terminal 실패로 끝내고 다른 채널의 정상 진행을 허용한다. Kafka 실패·전역 한도는 전체 수집을 중단한다. 어느 쪽이든 거절한 frame은 복구할 수 없고 최종 complete=false다.
- 느린 ACK·물리 중복·전량 거절·동시 admission·순환 순서·send 동기 실패/차단·종료 경합·늦은 ACK를 합성 단위12개로 확인했다. 실제 broker/DB와 성능 검증은 별도 기록한다.

- #27 비교 경계 점검: cf4049e 직렬 발행기를 테스트 전용 LegacySerialPublisher로 고정하고 두 실제 발행기의 실험 관측 어댑터를 뒀다. 제품 변경 뒤 baseline 이름이 새 구현을 가리키는 오류를 막는다. 수락 단계 실패와 실제 전달 실패를 분리해 전역 초과 후에도 이미 수락한 큐는 정상 broker로 drain한다. 이 경계를 추가 회귀로 검증한다.

- #27 첫 push CI40210203976에서 callback 실패 주입 직후 단언이 실패했다. mock send가 future를 반환하고 callback을 등록하기 전에 테스트가 future를 완료할 수 있는 경합을 확인했다. deliveryFailed 관측을 기다린 뒤 실패 계약을 검사하고, send가 이미 실패한 future를 반환하는 경우도 추가했다. 임의 sleep이나 제품 코드의 동기 ACK 대기를 넣지 않았다.

- 자원 정리 감사에서 Kafka 이미지의 VOLUME3개가 docker rm만으로는 정리 증거에 포함되지 않음을 확인했다. 새 실험은 소유자label이 있는 명시적 data volume을 쓰고 나머지 두 경로는 tmpfs로 둔다. 정리 시 정확한 컨테이너·volume의 소유자label을 검사한다. 과거 익명 volume은 소유권을 기록하지 않아 임의로 삭제하지 않으며 과거 cleanupComplete를 컨테이너 확인 범위로 한정한다.

## 2026-10-26 · #27 종료 리팩토링·재점검

- frame 내 채널 검사 순서를 입력 순서로 고정했다. send/dispatcher의 예상하지 못한 종료가 건강 상태 true를 남기지 않도록 고정 실패 코드와 미확인 보존으로 묶었다. 합성 Error 회귀는 원인 문자열 비노출·다음 send 차단을 검사한다.
- 실제 정상5회는1c2c3ba, product-hot과 broker 장애6경우는0791dc1에서 실행했다. 이후 변경은 예외 종료·결정적 admission 검사이며 기존 정상 경로 성능치를 새 commit 측정으로 주장하지 않는다.

- #11 최종40회 근거와 #27 제품 발행을 통합했다. 문서에는 최신 실험결과와 제품검증을 모두 보존하고 broker 하네스는 직렬/후보/제품6경우·소유볼륨정리를 유지했다.

- #11 최종1617bdb의Claude승인과push40211847614/PR40211851666성공을확인하고PR25를f94fc38로develop에병합했다. 이슈/보드완료와CI단위107·실제80근거를기록했다.

## 2026-10-26 · #28 수집기 임대 구현·리팩토링

- PostgreSQL 행잠금/DB시계와 요청시작 단조기한을 결합하고 owner/epoch/미만료 조건을 갱신·해제에 사용했다. 선정 분모·배정·연결·미관측 품질을 구분한다.
- DB소유권·로컬permit·연결수명·원천HTTP예산을 분리했다. 발행기는 기존 EventSink를 유지하며 admission의 동일permit을dispatch에서재확인한다. 원천READ/heartbeat프로토콜은재사용한다.
- 로컬단위128·하네스16·JAR를통과했다. 네트워크지연/정지후만료·기존permit차단·재획득거절과큐발행차단을검증한다. 실제두JVM·합성WS·DB/Kafka대조는진행중이다.
- broker강제fencing이나원천EOS를주장하지않으며이미발행한inflight와검사/send사이정지창,물리소켓잔존·DB시간점프·legacy비참여경로한계를계약에명시했다.

- #28 첫실제통합에서기존#16의V10 migration과번호가충돌해setup5개가실패했다. 새임대migration을V11로고치고초기화실패정리에서미생성topic의재삭제를피한다. 운영schema에는적용되지않았으며번호/실패를그대로기록했다.


### #28 실제 인계 검증과 #27 병합 반영

- 8425ebc에서 전체 실제 통합86개(임대5개)가 통과했다. 별도 JVM 정상 인계38·정지/만료16·DB 소켓 단절54건의 수락/ACK/broker/DB를 대조했다. 각 진단과 XML hash를 정제해 보관했다.
- 정상 종료 중 heartbeat interrupt와 채널 완료의 경합을 점검하고 정상 종료 complete/released 단언을 추가했다. 소유권·permit·연결·원천 예산의 책임 경계를 유지했다. 추가 범용 lease 프레임워크는 만들지 않았다.
- #27 최종8ad0817 Claude APPROVE와 push40212712923/PR40212715517 SUCCESS 뒤 PR33을597a162로 develop에 병합했다. CI artifact는 단위120·실제81·실패/skip0이며 웹29/31·하네스16은 CI 로그 근거다. browser-install 지연을 E2E 실패로 해석하지 않았다. 최종 병합과 이슈/보드 완료를 STATUS에 반영했다.

### #28 Linux CI의 종료 진단 파일 누락 보완

- 첫 push40213896363/PR40213900998에서 정상 SIGTERM 자식의 결과 파일이 없어 실제 통합2개가 실패했다. DB 단절/만료 인계 자체와 구분한다.
- main과 shutdown hook이 CAS로 쓰기 권한만 나누면 main이 JSON 저장 중일 때 hook이 반환하고 JVM이 종료될 수 있음을 확인했다. worker가 파일 저장까지 동기화하고 성공 후에만 persisted를 확정하도록 바꿨다.
- 별도 JVM의 main 결과 저장을5초 지연시키고 그 시작 marker를 본 뒤 SIGTERM을 보내는 회귀를 추가했다. hook이 저장 완료를 기다리는지 실제 파일·수락/ACK/DB로 검증한다. 기존 CI는 stderr를 버렸으므로 최초 누락의 내부 스택 자체는 확인하지 못했다.

- 추가 회귀는 첫 동기화 보완만으로도 실패했다. 정상 실행의 finally에서 파일 저장 전에 hook을 제거하는 별도 창을 실제 재현했다. hook 제거를 저장 완료 뒤로 옮기고 저장 전체를 동기화한다. 최종 CI 원인의 동일성은 최초 stderr 부재로 단정하지 않지만 두 종료 창은 코드·별도 JVM 회귀로 확인한다.

### #28 종료 경합 회귀와 재개 후 원천 차단 보완

최초 push40213896363/PR40213900998의 정상 SIGTERM 진단 누락을 조사했다. 결과 저장을5초 지연한 자식에 SIGTERM을 보내면 main의 CAS 선점 뒤 hook 반환·hook 조기 해제 창에서 파일이 없는 실패를 재현했다. 저장 완료까지 hook을 유지하고 동기화한 뒤1초 실행의 경계에서 CHANNEL_ENDED를 잘못 기록한 사례를 발견해 deadline 이후 검사를 제외했다. 첫 회귀는 파일 누락, 두 번째는 complete=false, 세 번째6개는 통과했다. SIGSTOP/CONT 시험은 기존 session의 재개 시각 이후 receivedAt 입력0을 broker에서 직접 단언하도록 강화했다. 이미 client에 전달한 이전 입력은 이 판정에 포함하지 않는다. worker 비활성 게이트를 DB 열기 앞으로 옮기고 preview/비활성 worker가 잘못된 JDBC 설정에서도 DB를 열지 않는 단위 사례를 추가했다. 리팩토링은 broker 재조회 helper와 결과 저장 단일 책임으로 한정했다.

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

### #29 전체 분석기 HA 정책 전달 보완

- 배포 코드 연결 점검에서 QualityRuntime의 trend DLQ 생성자가 기본 LOCAL 정책을 쓰는 호출을 발견했다. 기존 네 topic 초기화 검사는 통과해도 실제 HA 분석기 조립은 RF3 DLQ를 LOCAL로 재검사해 거부할 수 있었다.
- QualityConfiguration→QualityRuntime→DeadLetterRecovery에 같은 KafkaTopicPolicy를 전달하도록 수정했다. 6노드 하네스에 전체 Spring 분석기 기동, 합성10건 ranking/trend 각각 반영, 네 topic의 실제 HA 정책 대조를 추가한다. 초기 리뷰/기존 좁은 하네스가 놓친 범위를 숨기지 않는다.
- 실행 결과에 dirty tree 여부도 추가한다. 원인 진단은 원문 stderr를 보관하지 않고 고정 단계/타입을 유지한다.

- 결과 callback의 중복 CAS도 제거했다. worker의 synchronized 저장 계약이 성공 이후에만 persisted를 확정하므로 CLI가 먼저 CAS를 세우면 실제 파일쓰기 실패 후 두 번째 callback에서 성공으로 오인할 수 있었다. 성공 판정·재시도 책임을 worker 한 곳에 둔다.

## 2026-10-26 · #31 ACK·commit 비교 구현

- Spring Kafka container와 KafkaConsumer 공식문서에서 MANUAL 지연commit·BATCH·AUTO의poll조건을 확인했다. 같은고정합성입력에서처리전/DB직후/offset직후Runtime.halt를주입한다.
- AUTO_POLL_AHEAD는DB완료전다음poll을호출하는의도적으로잘못된경계다. 일반동기AUTO를같은경로라고표현하지않는다.
- ACK0/1/all은idempotence=false/retries0을통제하고RF3/minISR2/ELR1에서비교한다. ISR1쓰기와그leader손실뒤재조회/DB집합을대조한다. 제품의acks=all/멱등producer/BATCH기본은바꾸지않는다.

- #31 첫실행은commit10경계모두통과했다. AUTO동기는다음poll시commit,처리완료전에poll을진행한의도적오류는offset1/DB0으로재시작후에도누락1을재현했다. ISR1에서ACK0은API성공5/확인0,ACK1은확인5,all은실패5였다. 마지막ISR leader를잃은후다른replica가반드시즉시리더가된다는하네스조건에서중단됐다. ELR/lastKnownELR와리더없는구간을별도기록하고원래leader복귀뒤보존대조를완료하도록고쳤다. 실패원본/정리결과는보존한다.

### #31 ACK·commit 실제 대조와 리팩토링

- 472908f에서 commit10사례와 ACK0/1/all 복제 장애 대조가 통과했다. DB 전/직후/offset 직후의 별도 JVM 종료를 구분했고 정상9사례는 최종1건, DB 이전 다음 poll의 위험 순서는 누락1을 재현했다.
- ISR1에서 all은5건 모두 실패, ACK0/1은각5건 API 성공이었다. 마지막ISR상실 때 leader=-1/ELR[1]을 실제 기록했으며 원래디스크복귀 뒤25/25/20건 broker/DB 일치다. 디스크 영구 손실 내구성으로 확대하지 않는다.
- 첫실패의 가용성 가정을 제거하고 관측값/고정 단계 진단으로 바꿨다. 자원수명·별도 JVM commit 경계·대조 진행을 분리하고 기존 ProjectionConsumer를 재사용했다. 두 실행의 정리와 실패근거를 보존했다.

### #31 리뷰 후 실행 버전·계측 보완

- 검토 HEAD와 이전 실험 코드의 차이를 해소하려고 #27 및 #29 조립 수정까지 합친49a3834 clean tree에서 전체를 재실행했다. commit10/ACK3사례·정리 통과다.
- 실제topic설정·Java/PG버전·dirty여부,물리/고유건수·성공/실패입력별보존을추가했다. 25/25/20물리=고유=DB,누락/실패후보존0이다. 마지막leader부재는15.048초의timeout0정지관측이며기존범위를확대하지않는다.
- 중단용container와생산재시작consumer를구별하고첫실패는마지막checkpoint이후예외로표현을정정했다. AUTO위험순서는메커니즘시연이며확률측정이아니다.

- #31 최종cb5fa38 Claude APPROVE·push40215140028/PR40215143544 SUCCESS를 확인해 PR36을50cd6ad로 병합했다. CI artifact 단위124/실제81, 로그 하네스16·웹29/31이다. 문서의 원본2회를3회로 정정하고 요약의 대표 command/eventTimeBase를 최신third로 맞췄다.

## 2026-10-26 · 배포·복구 #30 구현과 경계 점검

- 이미지 commit/실제 ID·비밀 파일·비특권 앱·내부망·readiness·분리 migration과 runtime 계정을 구현했다. RuntimeEnvironment가 비밀/CLI 정책을, RecoveryPlan이 원천 identity/보존/DB 진행/Kafka offset 대조를 소유한다. 여섯 group의 복구는 새 namespace에만 설정하며 기존 group 변경을 금지한다.
- 실제 Kafka/DB의 시작 위치 불일치·새 group 복구·보존 만료·동명 topic 재생성·schema checksum/미래 migration 거부4개가 통과했다. 첫 실행은 테스트 Admin의 timeout 설정 충돌로 setup4개가 실패했다. XML에 남은 정확한 schema/원천ID16개를 대조해 소유 자원만 정리했고 설정/실패정리를 보완했다.
- fleet에 시청자 관측을 결합하며 임대한 채널만 활성화하고 채팅 HTTP 예산·권한을 공유한다. metadata 응답 뒤와 발행 직전에 권한을 확인한다. 공개live는 실행하지 않았다.
- 전체 이미지·롤백·백업 복원·브라우저 리허설과 최종 리팩토링/Claude/CI는 진행 중이다.

### #30 첫 이미지 리허설·수집기 통합·리팩토링

- 4f85f1d와88b30e6의 실제 image ID/JAR/buildId를 고정해16단계가 통과했다. 웹7경로 새로고침·375px overflow0·pageError0, DB 장애503/프로세스200, checksum 불일치 기동 거부·이미지 복귀, 새DB 복원과 Kafka 위치 차이 거부·새group6개 재개를 확인했다. 백업10→최종20건/알림2·미발행0·선택한 집계/ledger hash 차이0이며 관측복구17.105초다. 소유 컨테이너·볼륨만 정리했다.
- #28의 shutdown hook 결과 저장 보완을 통합했다. CLI 종료 callback에 관측/producer/telemetry 정리를 모아 SIGTERM에서도 기다리고, worker에 배포 schema VALIDATE를 연결했다. 관측 응답 후 권한 상실 검증은 종료 동작이 발행을 막는 오탐을 피하도록 실제 늦은 응답 future의 SourceFailure와 발행 경계를 각각 단언한다. 기존 ObservationCollector 수명을 재사용한다.
- 관리형/자체운영 후보와 비용 입력·수집 확대 중단·경보·복구 시작/끝의 정의를 DEPLOYMENT_OPERATIONS에 추가했다. 단가·전체채널 수·운영 RPO/RTO를 측정값 없이 확정하지 않았다. 이미지 CLI status/비활성 worker를 추가 리허설한다. 원천live/외부배포는 실행하지 않았다.

### #30 Claude 초기 변경 요청과 검증 보강

- Claude 6ca20c20은88b30e6에 REQUEST_CHANGES를 냈다. NGINX location별 add_header 상속 때문에 static의 nosniff가 빠졌고, 첫 browser는 API/실제 heading을 보지 않아 없는 /reports의 fallback도 통과했다. 응답·heading·실제20건 리포트·503/재시도·헤더 단언을 추가했다.
- startup 실패가 단순 exit!=0이던 시험에 허용 목록의 고정 실패코드와 OOM 여부를 추가하고 DDL은42501을 확인한다. 복구 gap/DB분리/기존group불변·초기 group/source 수는 실제 조회로 계산한다. 첫 두 원본과 검증 한계를 삭제하지 않는다.
- migrate를 maintenance profile로 옮겨 API 시작이 migration을 자동 실행하지 않게 했다. 동일V12 이미지 rollback·checksum 손상을 실제 새 migration/파괴적 변경 복구로 확대하지 않으며 T16은부분검증으로 표시한다.
- 배포 비밀 파일 누락은 fail-closed, init SQL의 비밀 argv를 제거했다. cleanup 성공 후 생성 비밀/dump를 삭제한다. fleet 관측은 opt-in으로 바꾸고 offline fallback의두 HTTP 모두 공유 예산에 반영한다. SourceFailure/고정 진단·HTTP예산 회귀를 추가했다.

- 230852b의 보강 이미지 리허설17단계가 통과했다. SCHEMA_NOT_READY/RECOVERY_ALIGNMENT_REQUIRED·DDL42501·실제 기존groupoffset 불변과8개 정상경로+리포트 상세20건·API2xx/503재시도·헤더를 단언했다. 생성 secret/dump도 정리했다. 후속 안정화는 fixture를120초 전으로 옮겨 완료된1분 구간의 경계에 따라 API참고건수가 달라지지 않게 하고 결과 디렉터리권한0700을 명시한다. 이는 정상 입력의 분석 시각 경계를 고정하는 하네스 변경이다.

- 89879bd clean 이미지/하네스17단계도 통과했다. 실제API26응답200·정상8경로+리포트20건·503재시도·복구18.387초·기존groupoffset불변·집계차이0·secret/dump삭제를 확인했다. Java139·하네스16·JAR도 통과했다. 최초두실행의검증한계와후속보강을 DEPLOYMENT_VERIFICATION에 함께 정리했다.

### #30 재리뷰 후 운영 진단·관측 의미 보완

89879bd diff는Claude APPROVE를받았고최종HEAD/CI는별도게이트다. 비차단의견중Kafka/fleet기동코드빈배열과HTTP요청생략의UPSTREAM_ERROR오인을수정했다. 고정코드목록과메시지없는예외타입으로진단하고FLEET_LIVE_DISABLED를실제단언한다. PublicObservationSource.Skipped는원천관측을만들지않고다음간격까지기다리는계약이며offline2회예산·다음interval회귀를연결했다. 종료/스케줄경계1회여유까지포함한관측186+최초채팅6≤500의예산식을기록하고남은상수BACKUP chat도DB에서계산한다.

- 로컬전체실제91개는통과했지만종료전14초에후속단위빌드가같은클래스/JAR를갱신한사실을mtime으로확인했다. 이실행을고정commit의최종회귀근거로사용하지않고격리된최종CI로대조한다. 후속단위140·하네스16·JAR는별도로통과했다.

- a0283bc clean이미지의다섯번째17단계도통과했다. 후보이미지의FLEET_LIVE_DISABLED·고정기동코드·DDL42501·실제DB/groupoffset/집계·실제웹20건을다시대조했고관측복구24.188초·소유자원/비밀삭제를기록했다.

### 2026-10-26 · #30 관측 생략 진단 보완

a0283bc/5차 증거는 Claude APPROVE다. 후속 비차단 검토에서 hook의 모든 RuntimeException을 생략하던 범위를 SourceFailure로 좁혔다. 관측 생략 횟수는 ObservationCollector에서 계수하고 LeasedObservations가 종료 후에도 합산해 FleetCli 최종 결과에 남긴다. hook 버그가 조용히 생략되지 않는 사례와 종료 후 계수 보존을 추가했다. 허용 코드/예외 타입 설명을 정정하고30분/500회 강제 범위를 명시했다. 새 이미지와 회귀 검증은 #28 시작 준비 보완을 통합한 뒤 진행한다.
### #28 Linux 후속 CI 연결/인계 실패 조사

1cd8476의 push40216093203/PR40216096733에서 저장 경합 회귀는 통과했지만 정상 두JVM 연결2개 대기와 SIGSTOP 인계의 owner 대조가 실패했다. 로컬87개 통과와 다르며 초기 파일누락과 같은 원인으로 단정하지 않는다. 당시 CI artifact에 자식진단JSON이 없어 실패/발행buffer 코드를 확보할 수 없다. 정제된 자식진단을 artifact와 JUnit 출력에 연결하고 임시로 수집기6개를 전체회귀보다 먼저 실행해 Linux의 실제실패코드를 수집한다. 정상경로의 작은 fixturebuffer30/20와 cold producer 지연은 가설이며 아직확정하지않았다.

### 2026-10-26 · #28 producer 준비 경계와 Linux 진단

1cd8476 CI의 인계2개 실패는 자식 결과 미보관으로 내부 원인을 확정하지 못했다. 진단 추가4603c53의 push/PR은 단독6개+전체87개 통과했으나 원인 해결로 기록하지 않는다. ProducerFactory 생성3초 지연을 주입해 채널 큐20 초과·거절1·CHANNEL_ENDED를 별도로 재현했다. CollectorWorker는 producer/metadata 준비 후 임대·소켓을 시작하도록 바꿨다. 실제7개와 단위134/하네스16/JAR 통과, 지연 동안 무임대/무수신·완료 후 계수 일치 회귀를 추가했다. 최종 fixture는 부모 해제 barrier를 사용한다. 책임은 worker의 수명 시작 경계에 두고 BufferedKafkaPublisher의 큐/실행 중 장애 계약은 유지했다. 중복 CI 단독 실행은 제거하고 정제 자식 JSON 업로드는 유지한다. 과거 CI와 재현 원인의 동일성, producer 생성 시간 상한은 주장하지 않는다.

### 2026-10-26 · #30 여섯 번째 통합 이미지와 리팩토링 점검

1bde2b7 clean tree에서 이미지 생성·단위142/하네스16/JAR·17단계 리허설을 통과했다. 수집 전 Kafka 준비와 관측 생략 계수까지 통합했고, 리포트20건/API26개/새 group6개·복구 후 차이0을 다시 확인했다. 마지막 복구 표본18.900초와 종료1.255초는 운영 SLA가 아니다. 로컬 복구6회(서로 다른 코드와 검사 강도)의 범위17.105~24.188초를 기록했다. 소유 컨테이너/볼륨과 생성 비밀/dump를 모두 정리했다.

리팩토링 점검에서는 관측 카운터를 기존 ObservationCollector에 두고 LeasedObservations가 종료 합계를 소유하도록 했다. HTTP/임대 예산과 원천 오류 분류, 운영 허용 코드 책임을 유지했다. 수집기 리뷰의 비차단 사항 중 깨진 자식 JSON이 테스트 자원 정리를 막는 경계는 고정 진단으로 보완했고 임시 재현 명령·로컬 검증 순서·빈 출력 파일 재실행 계약을 문서화했다. 이미지 생성 뒤 변경은 문서와 테스트 정리 코드이며 실제 이미지 입력은 그대로다.

## 2026-10-26 · 전체 이슈 병합 완료와 최종 기록

- #28의 정확한 HEAD `1aa8059`는 Claude APPROVE와 push40218155876/PR40218160268 SUCCESS를 확인한 뒤 PR35를 `79d0f8d`로 병합했다. CI 단위134·실제88(임대7)·하네스16·웹29/31이다. 과거 Linux 실패의 내부 원인 동일성은 미확정으로 유지하고 별도 producer 지연 재현/시작 준비 보완과 구분했다.
- #30의 정확한 HEAD `3b6d662`는 Claude APPROVE와 push40218491808/PR40218495927 SUCCESS를 확인한 뒤 PR37을 `d8b864d`로 병합했다. 최종 기능 CI 단위142·실제92·하네스16·웹29/31이다. 실제 배포 이미지/하네스는 clean `1bde2b7`의17단계이며 이후 이미지 입력은 바뀌지 않았다.
- 기존7건과 추가6건의 기능 이슈를 모두 닫고 Project 상태를 완료로 맞췄다. 조회 시 열린 이슈0건을 확인했다. STATUS에 이슈/기능/기술ID/PR을 모으고 두 최종 게이트의 XML hash·리뷰 세션·병합 커밋을 정제 근거에 저장했다.
- 마지막 리팩토링 점검에서는 임대/권한/수명, 원천 예산/관측, schema/복구/진단의 책임 분리를 유지했다. 검증 이후 새 추상화는 추가하지 않고 최종 문서의 오래된 진행 상태와 검증 범위를 정리했다. T13과T16의 미검증 운영 범위는 부분 검증으로 유지한다.
- 로컬 테스트 Kafka/PostgreSQL을 정지하고 데이터 볼륨을 보존했다. 리허설 소유 자원·생성 비밀·dump는 정리 완료다. 다른 worktree와 phone 프로젝트는 수정하지 않았고 main 릴리스·외부 배포는 실행하지 않았다. 원래 저장소에서 origin/develop을 fetch하고 문서 정리용 새 브랜치를 만들었다.

## 2026-10-26 · #39 전체 코드 구조 점검과 경계 추출

- 사용자 요청으로 최신 origin/develop c48a190에서 refactor/codebase-depth를 만들었다. 기존 기능13건 완료 이력은 보존하고 새 #39와 Project In Progress로 추적한다. 다른 develop worktree는 수정하지 않았다.
- codebase-design으로 서버102개·웹38개·scripts26개의 구성과 주요 호출/상태 소유를 검토했다. 공통 소비 설정·poll 관측, 수집기 실행 자원, 웹 시간축 검증/막대 표현, 관리 CLI JAR 수명을 추출했다. 영역별 변경/유지 이유와 보존 계약은 REFACTORING에 기록했다.
- generic Store/HTTP/CLI 프레임워크는 도입하지 않았다. 같은 모양의 코드라도 DB/DLQ/retention/품질 정책과 PoC의 exec 프로세스 수명은 다르므로 각 모듈에 유지했다. 임대·발행의 동시성 알고리즘과 SQL은 그대로다.
- 초기 회귀는 단위147·하네스20·웹29/Chromium31 통과다. 최초 actual JAR 테스트는 외부 원천 요청이 없는 빈 선정 계획에서 관측기 활성/비활성의 정상 결과와 초기화 실패의 종료코드·빈 파일·worker 미등록을 확인한다. System.exit 때문에 실행기 누수를 판정하는 근거로는 부족해 후속 보강했다. 실제 Kafka/DB 전체 결과와 최종 commit별 근거는 후속 기록에 남긴다.

### #39 리뷰 반영과 확정된 로컬 범위

- e5222e0의 전체 실제 Kafka/PostgreSQL94개와 단위147/하네스20/웹29/Chromium31이 통과했다. code-review의 Standards/Spec 두 정적 리뷰는 각각 지적0건, Claude Sonnet 세션91fc0a46-c2a7-4b9e-a1a7-377aa06bc55f도 APPROVE다. 리뷰어는 전달된 diff·문서를 읽었으며 테스트를 직접 실행하지 않았다.
- 비차단 의견에서 factory 속성의 불필요한 String cast를 원래의 equals 판정으로 돌리고, null initializer의 position 조회 의미를 명시했다. 루트 README에 기존 CLI 명령의 공통 실행 수명을 설명했다. 07021e4로 고정했다.
- 최초 JAR 시험은 main의 System.exit가 남은 실행기를 종료시킬 수 있었다. 별도 JVM fixture가 FleetCli.execute를 호출한 뒤 System.exit 없이 main을 반환하도록 추가했다. 정상/초기화 실패 모두 자연 종료하고 각각0/2 반환 marker를 남겼다. 단위147·하네스20·수집기 실제3개가 통과했다. 모든 native/daemon 자원의 무누수까지 확장하지 않는다.
- FleetRuntime의 callback/finally 반복 close는 기존 계약을 보존했다. 단순 AtomicBoolean 가드나 try-with-resources 전환은 종료 실패의 재시도·flush·예외 우선순위를 바꿀 수 있어 이번 변경에 섞지 않았다. 각 CLI의 JsonMapper와 화면 축 최대값 계산도 작은 지역 책임으로 유지했다.
- 로컬 기본 Node는26.3.1이었고 Homebrew node@24 경로도 같은26을 가리켰다. 초기 결과는 보조 기록으로 두고 fnm의 실제24.20.0으로 웹29/Chromium31을 재실행해 통과했다. CI도 .nvmrc 버전을 사용한다.
- 정제 근거에는 최초 전체94개와 후속 집중3개를 구분하고 XML/log hash, 현재 JAR/buildId, 실행환경·미검증 항목을 기록했다. 최초 JAR hash는 후속 빌드 전에 보관하지 않았으며 사후 생성하지 않았다. 최종 HEAD의 전체 CI/Claude/병합은 PR40의 통합 게이트 기록을 따른다.
- 검증 후 chat-pulse Kafka/PostgreSQL만 정지하고 데이터 볼륨을 보존했다. 다른 worktree HEAD707230a와 phone 서비스는 변경하지 않았다. 공개 원천·외부 배포·새 HA/고부하 실험은 수행하지 않았다.


## 2026-10-26 남은 기술 검증 등록과 trend 장애 경계 #41

- 사용자 요청으로 #41~#49를 등록하고 구현 가능한 로컬6건과 실제 인증/외부 환경이 필요한3건으로 나눴다. 각 이슈에 기존 F/T ID, 실패 주입과 대조 조건, 선행 조건을 연결했다. 진행 상태는 STATUS에서 관리한다.
- `origin/develop`의 PR #40 병합 `dcdc244`에서 `technicaltest/trend-rollback`을 분기했다. 다른 develop worktree를 변경하지 않았다.
- T02의 남은 경계는 `ranking-trend` 자체의 SQL rollback이다. 기존 ranking SQL 실패 시험과 trend 중복/독립 소비만으로 이 경계를 완료 처리하지 않았다.
- 최초 시험은 정상 입력의 배치 내부/간 중복·충돌, bucket 작성 뒤 progress SQLSTATE `P1002` 주입과 snapshot 대조였다. 아래 리뷰 후 commit 시점의 progress 쓰기 rollback, 같은 consumer의 실행 중 회복, 별도 JVM 종료/동일 offset 재수신으로 보강했다. broker 입력은 partition0에 한정하고 빈 partition1/2의 offset0 유지도 확인한다.
- 리팩토링 점검: 주입·snapshot·broker offset 확인은 이 장애 시나리오의 테스트 내부 helper로 모았다. 기존 제품 소비/저장 경계와 재시도 정책은 그대로 검증하며 공통 제품 코드에 새로운 시험용 분기를 넣지 않았다. 실행 결과는 실제 완료 후 근거에 기록한다.

- 최초 `6079690`의 단위147·하네스20·실제99개가 통과했다. Standards/Spec은 지적0건이었지만 Claude는 REQUEST_CHANGES로 progress 쓰기 자체의 rollback 입증과 DB commit 성공 뒤 같은 Kafka offset 재전달의 직접 근거를 요구했다. 최초 시험을 최종 완료 근거로 승격하지 않았다.
- 지연 constraint trigger로 commit 시점의 ledger3·bucket합3·partition별 progress2/1을 실제 조회한 뒤 `P1002`를 발생시킨다. 상태가 다르면 `P1003`으로 실패해 잘못된 주입 지점의 통과를 막는다. rollback되지 않는 시험 sequence로 실제 trigger 호출을 계측한다.
- 정상 입력 SQL 재시도는 같은 consumer에서 장애 제거 후 회복하도록 보강했다. 별도 JVM을 trend DB commit 직후 halt(91)하고 DB next1/Kafka commit0을 대조한다. 같은 group의 none 재시작 listener에서 실제 offset0 수신과 DB snapshot 불변을 확인한 뒤 새 offset의 중복/정상 입력을 대조한다. 최초 source_offset 보존도 확인한다.
- 보강 집중5개가 통과했다. 공식 PostgreSQL16 [constraint trigger](https://www.postgresql.org/docs/16/sql-createtrigger.html)와 [sequence](https://www.postgresql.org/docs/16/functions-sequence.html) 문서(2026-10-26 조회)로 commit 시점 검사와 abort 후 sequence 유지의 계측 범위를 확인했다. sequence를 DB 장애 후 영속 증거로 확장하지 않는다. 보강 commit의 전체 회귀·최종 리뷰/CI는 이어 확인한다.

- 보강 `43bedc2`의 `./scripts/check` 단위147·하네스20·JAR, `./scripts/integration` 실제100개가 모두 통과했다. 정제 근거에 suite/XML·명령 로그 hash·JAR/build ID와 실제 시각을 연결했다. Standards/Spec 각각 지적0·차단0이며 Claude 보강 재리뷰도 APPROVE다. session timeout6초와 Gradle classpath를 확인하고 partition 범위·이전 문단의 회복 순서를 정정했다. T02는 이 합성/로컬 장애 범위에서 검증됨으로 바꾸며, 원천 중복·전체 Exactly-once와 운영 장애는 확장하지 않는다. 최종 문서 HEAD 승인·push/PR CI·병합은 PR #50 통합 게이트에 남긴다.


### 2026-10-26 Kafka 디스크 영구 손실 하네스 #45

F15/F11 → T04/T10/T15. origin/develop에서 technicaltest/kafka-disk-loss를 분기했다. #43의30분 실행 동안 원격 CI 컴파일부터 확인하고 소유6노드 실험은 그 종료 뒤 실행한다. raw source를 사용하지 않는다.

기존 LocalKafkaCluster의 owner/project/service/volume mount/다른 참조 검사를 통과한 정지 broker 데이터 volume만 삭제·빈 volume으로 재생성한다. KafkaDiskProbe는 readonly mount에서 FileRecords CRC·offset·leader epoch·key/value digest와 안전한 metadata ID만 기록한다. 실제 classpath class를 실행별 복사/hash하고 매 probe 앞에 대조한다.

recover와 last-copy는 새 cluster를 각각 사용한다. 전자는 ISR2 지속·새 replica 내용 대조·ISR1의 실제 ACK all 거부·최종 ledger 집합을 확인한다. 후자는 stale BASE 디스크를 보존하며 TAIL을 가진 두 최신 디스크의 실제 내용 확인→각각 삭제→empty 상태를 연결한다. stale/빈 disk 복귀의 metadata·ELR·Replay 결과는 관측대로 보존하고, source 조회 불가를0건 복구로 바꾸지 않는다.

사전 Standards/Spec 리뷰는 serializer 누락과 재기동 중 metadata 조회 오류가 후속 복귀를 중단하는 문제를 찾아 수정했다. helper128MiB 예산·고정class/hash를 포함했다. 재리뷰 차단0이며 컴파일/실제 실험 통과를 뜻하지 않는다. 리팩토링 검토에서 소유 cluster 수명·디스크 읽기·시나리오의 책임을 분리하고 제품 producer/소비 계약은 재사용했다.


### #45 진입 리뷰 보완

Claude43b3474 진입 리뷰는 REQUEST_CHANGES였다. 불충분 ISR은 직접 KafkaTopicPolicy.NotReady 확인과 TOPIC_VISIBILITY_DEADLINE 원인을 함께 요구하고, 마지막 복사본 손실은 3개 실제 container Running/OOM 상태와 Admin 등록 broker ID1/2/3을 확인한 뒤 판정한다. SOURCE_UNAVAILABLE/OFFSET_TRUNCATED만 허용하고 다른 실패를 복구 불가 증거로 취급하지 않는다. readonly 디스크 helper도 소유 project/name으로 정리하고 실행 classpath는 고정한다. 빈 partition 초기 commit 지적은 기존 BatchConsumers.initialOffsets가 모두 commit하므로 해당 소스를 추가 제공해 재검토한다. 실제 손실 실험 전 진입 재검토를 수행하며 아직 결과를 PASS로 표시하지 않는다.


### #45 실제 디스크 유실 실행

2026-10-26 clean f992ef1에서 recover7단계와last-copy6단계를 각각새소유6노드로 실행했다. 전자는새directory ID와30건offset/hash 재구성·ISR1 실제거부 뒤최종ACK32=broker32=DB32·차이0이다. 후자는 최신tail10건의두물리copy를지우고3broker모두복귀했으나 source/oldmanifest가SOURCE_UNAVAILABLE였다. DB20은유지되고sourceRecoverySucceeded=false이며 읽기불가건수는null로기록했다. 실행종료정리모두true. [검증](pipeline/KAFKA_DISK_LOSS_VERIFICATION.md)·[정제근거](poc/evidence/kafka-disk-loss-2026-10-26/summary.json)에단계/시각/commit/hash를연결했다.

리팩토링은 소유 cluster 수명·readonly record probe·실험판정 경계를 유지한다. 실제디스크삭제와원천조회오류를구분하고 임의의일반예외로PASS하지않는다. 빈partitioncommit은기존공통소비계약을재사용했다. 진입Claude지적3개보완후APPROVE FOR EXPERIMENT를받았고 최종근거HEAD는다시리뷰/CI한다.

f992ef1의 `./scripts/check` 단위147·하네스20·JAR도 통과했다. 실제 디스크 실험13단계와 일반 integration CI는 서로 다른 검증으로 기록한다.

### 2026-10-26 수집기 장애 경계 #44 구현

F13 → T02/T11/T14. `origin/develop` c2aeffc에서 `technicaltest/collector-boundaries`를 분기했다. #43 soak의 이미지·probe class를 고정한 뒤 로컬 부하 시험을 겹치지 않고 구현한다. 공식 시간 API·PostgreSQL 시각과 기존 임대 경계를 [조사](research/COLLECTOR_FAILURE_BOUNDARIES.md)에 기록했다.

수신/관측/보고 Clock을 연결하고 기본 systemUTC를 유지한다. 임대 DB clock_timestamp와 System.nanoTime 기한은 그대로다. 합성 source marker로 부모와 skew된 child의 receivedAt을 직접 비교하던 인과 판정을 보완한다. 시계가 앞선 입력의 live 격리도 broker→정상 ledger/격리 offset 집합으로 구분할 계획이다. 독립 loopback DB/WS proxy와 실제 producer ID 관측은 테스트 내부에 둔다. 최종 permit 이전, permit 이후 send 이전, broker ACK 이후 callback 이전을 별도 실제 broker/DB 사례로 구현한다. 실행·리뷰 전 초안이며 결과로 주장하지 않는다.

사전 Standards/Spec 정적 검토에서 차단0을 확인했다. Spec의 시계 인과 장벽2건은 음수 Clock worker의 실제 INVALID_MESSAGE 관측 파일과 양쪽 Clock 적용 뒤 새 lease 기준값으로 보완했다. DB blackhole이 열린 transaction의 plan 행 잠금을 남길 가능성은 실제 backend state/transaction/차단 PID와 child 고정 진단을 기록해 판별한다. 새 장애 시험 실행 전이며 간헐 통과로 원인 해소를 주장하지 않는다.

## 2026-10-26 새 migration 복구 시험 #42

- F16/F08 → T04/T16. fetch한 origin/develop에서 새 `technicaltest/migration-recovery`를 만들고, 최종 CI 중인 #41의 검증 commit을 fast-forward로 포함했다. #41 게이트를 확인해 develop 병합 후 기준을 통합한다. 다른 worktree는 변경하지 않았다.
- 제품 V12 이미지 위에 시험 SQL만 얹은 V13 후보·SQL 실패·프로세스 중단 이미지를 분리한다. 같은 Git 코드 revision임을 명시하고 원래 JAR hash·SQL hash·시험 build ID·실제 image ID를 함께 기록한다.
- DDL/backfill 후 sequence 표시를 확인해 실제 실패/종료 지점을 입증한다. V13을 적용한 DB에서 V12 시작은 strict schema 검증으로 거부해야 한다. 복귀는 V12 백업을 새 DB에 복원하고 새 group6개를 정렬해 보존 입력을 재생하는 경로다.
- 리팩토링 점검: 기존 배포 리허설의 시나리오 순서를 `scenarios`로 분리해 자원 소유권·오류/정리 정책을 공유한다. 두 실제 사용처의 복원 대상 이미지를 인자로 받고, readiness 후 buildId도 manifest와 대조한다. 시험 SQL은 제품 migration과 분리한다. 실행 결과는 완료 후 기록한다.

- 첫 `47d5659` 실행은 제품 이미지 빌드 뒤 fixture FROM에 로컬 image ID를 전달해 실패했다. BuildKit이 이를 docker.io/library/sha256 태그로 해석했다. DB/Compose 시험은 시작하지 않았다. 로컬 manifest의 tag가 기록한 image ID와 같은지 빌드 전후 검사하고 그 tag를 base로 사용하도록 보완한다. 최초 출력은 ignored work에 보존한다.

- 두 번째 `0227330`은 기준 이미지와 fixture 이미지3개/manifest4개를 만들었으나 dynamic import가 scripts/__pycache__를 만들어 clean-tree 사전 검사에서 거부됐다. Compose를 시작하지 않았고 생성 secret 정리가 완료됐다. bytecode 생성을 끄고 소유한 pyc만 정리했다.
- Spec 리뷰의 차단1건: 별도 연결에는 미커밋 DDL도 보이지 않으므로 schema만 같다고 중단 transaction 종료를 단정할 수 없다. interrupted fixture에 연결 검사1초와 고정 application_name을 지정하고, kill137 뒤 해당 PostgreSQL backend가 사라진 것을 확인한 후 snapshot/history를 대조하도록 보완했다. [PostgreSQL16 연결 검사](https://www.postgresql.org/docs/16/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)는 기본0에서 다음 socket 상호작용까지 단절 감지가 지연될 수 있다고 설명한다. 시험 설정의 범위이며 운영 기본값의 즉시 종료 보장을 주장하지 않는다.
- Standards의 자원 생성 순서 지적에 따라 실패/중단 manifest를 비밀 생성 전에 읽도록 옮겼다. Flyway history의 version·checksum·success도 snapshot에 포함해 기존 history와 복원 결과를 비교한다.

- clean `bc2ff5b`의 V13 리허설16단계가 통과했다. 중단137/backend 종료/marker1, SQL 실패 뒤DDL/history복원, V13 backfill10/10, 구 V12 시작거부, 새DB V12복원·새group6개·집계20/알림2·기존group불변·선택snapshot차이0이다. 복원 관측25.163초, 실제웹8경로+리포트상세/26API200/JS오류0, 종료1.243초·OOM없음·소유Compose자원/secret/dump정리를 확인했다.
- 공유 리허설의 복원 대상 선택을 앞당긴 영향은 기존 V12 17단계로 별도 확인했다(기준0227330→후보bc2ff5b). 하네스20개·문서/ID검사 통과. 두 최초실패의 commit·원인·로그 hash와 성공 두 실행의 시각·manifest·hash를 정제근거에 보존했다. 최초 실패 시각은 별도 계측하지 않았다. 생성 이미지는 로컬 manifest로 남기며 Compose 자원정리와 구분한다.
- Standards의 초기heuristic1건과 Spec의 초기차단1건을 해소했고 bc2ff5b는 둘 다 남은지적0이다. Claude 정적리뷰도 APPROVE다. 비차단의10초JDBC timeout/느린호스트 kill창, 코드에서 파생된설정값/결과, 실제SQLSTATE미수집·marker판정범위를 검증문서에 명시한다. 최종문서HEAD Claude/push/PR CI는 PR51게이트에서 확인한다. T16은 WAL PITR·외부운영 등이남아 부분검증을 유지한다.

- #41 최종 bde3a47은 Claude APPROVE와 push40253684968/PR40253689010 SUCCESS를 확인했다. 두 CI XML은 각각 단위147·실제100·실패/오류/skip0, PR웹29/Chromium31/하네스20이다. PR50에 정확한HEAD 통합게이트를 남기고 c2aeffc로 develop 병합했다. #41과 보드를 완료 처리한다.


### 2026-10-26 PostgreSQL physical PITR 하네스 #46

F16/F08 → T02/T04/T16. origin/develop에서 technicaltest/postgres-pitr를 분기하고 선행 #42의 공통 리허설을 통합했다. #43의30분 부하 종료 전에는 로컬 중량 빌드/DB 실험을 겹치지 않고 CI 컴파일부터 확인한다.

base/T/C·named target pause/쓰기거부·resume/새timeline·Kafka 새group 초기offset/최종업무대조·필수WAL누락FATAL·retentiongap거부를 순차 검사한다. 원본 physical backup/archive와 모든 복구 PGDATA를 소유 volume으로 분리하고 성공/실패 모두 공통 정리 수명으로 회수한다. 새로운 overlay 쓰기도 run의 PREPARE 단계로 옮겨 파일 생성 실패 뒤 비밀 정리가 가능하도록 했다.

사전 Spec/Standards는 동적 import의 pyc가 clean preflight를 깨는 문제, WAL helper0644 실행 모드, 새group 초기offset 독립 검사 공백을 찾아 수정했다. import bytecode 억제·100755·최초 offset 없음→DB next==commit 직접 관측을 반영했다. 테스트용 DeploymentProbe만 실제 deleteRecords 음성 시나리오를 추가하고 제품 복구/소비 코드는 재사용했다. 정적 리뷰와 실제 성공을 구분하며 실행 결과는 후속 근거로 기록한다.


### #46 진입 승인 후 보완

Claude64537f8은 APPROVE FOR EXPERIMENT였다. 비차단 권고를 반영해 실제 retention gap 삭제 전에 이번 소유 Kafka cluster ID 일치를 요구하고, WAL 복사본 차이가 필요한 segment 하나뿐인지 hash inventory로 확인한다. 누락 WAL이 실제 restore helper 사용 목록에 없음을 검사하고 복원 서버 버전도 원본과 대조한다. maintenance profile의 helper를 cleanup에 포함하며 trim 전 복원 DB의 보존 로그 복구 가능성도 확인한다. 원본 backup/archive를 그대로 보존한 새 복원 디스크만 사용한다.


### #46 실제 physical PITR 완료

2026-10-26 clean4a6b7a8에서첫실행자원사전검사실패를수치trace로확인했다. 허용90%초과여서개발Kafka/DB만볼륨보존종료한뒤같은HEAD로재실행했다. 03:00:10.979~03:01:43.582 UTC12단계통과. baseSHA256/pg_verifybackup·필수WAL3개·target0/5051778pause·readonly25006·timeline1→2·새6group정렬·chat10재생·업무snapshot차이0을확인했다. 누락WAL은목표미달FATAL, 실제begin11>DBnext10은4projection RETENTION_GAP·새group/DB불변이었다.

[검증](POSTGRES_PITR_VERIFICATION.md)·[정제근거](poc/evidence/postgres-pitr-2026-10-26/summary.json)에시각/commit/image/probe/hash/LSN/집합을연결했다. archive failed_count4는숨기지않고원인미확정으로기록했다. 모든소유volume·container·비밀정리true이며다른서비스는유지했다. 단위147·하네스20·JAR통과. 기존배포소유권/그룹정렬경계를재사용하고물리복원책임만분리하는리팩토링을점검했다. 최종리뷰/CI는별도다.

## 2026-10-26 전체 제품 부하·지속 실행 #43

- F03/F04/F05/F06/F07/F10/F14 → T02/T07/T08/T11/T12. fetch한 origin/develop에서 `technicaltest/product-soak`를 만들고 최종 CI 중인 #42를 fast-forward로 포함했다. #42 병합 뒤 develop 기준을 통합한다.
- 제품 분석기와 부하 생성기를 별도 JVM·소유 Compose로 분리한다. 생성 입력의 UUID/내용/10초 합계를 DB 결과와 대조하고, 그룹6개·24개 group/topic/partition의 offset·lag·처리량과 API/DB 지연을 기록한다. 원천 세션/heartbeat와 outbox는 합성 입력이며 실제 원천·급상승 감지 정확도 부하 검증으로 확대하지 않는다.
- JMX는 시험 overlay의 내부망에서만 열고 분석기 heap을 읽는다. 분석기 RSS·컨테이너 CPU/메모리와 생성기 heap/RSS는 별도 표본이다. 기존 운영 compose를 바꾸지 않는다. 실행 클래스는 결과 디렉터리에 고정 복사하고 hash를 남겨 뒤의 빌드가 실행을 바꾸지 않게 한다.
- 균등/80% 편향 각120초100건/s, 단계50/100/200건/s 각60초, soak50건/s1800초를 고정한다. 100건마다 동일 UUID를 새 offset으로 중복 발행한다. smoke/soak은 분석기만20초 SIGSTOP/CONT하며 입력은 계속한다. 실행 전 설계이며 아직 통과 근거는 아니다.
- 최초 b91b8e7 smoke는 격리 인프라/분석기 시작 후 `OWNED_CONTAINER_MISSING`으로 중단했다. 생성기 one-off가 `api` service label을 공유해 API 소유권 조회에 컨테이너2개가 반환된 것이 원인이다. 생성기를 별도 `load-probe` service로 분리한다. 최초 결과와 정리 성공을 보존하며 완료로 세지 않는다.
- f71423a의60초 smoke는 고유1200·물리1212·모든 집계 대조·STOP/CONT·자원 정리를 통과했다. 다만 리뷰에서 입력 중 offset 조회 순서의 음수 lag 가능성과 정지 구간에 한정하지 않은 lag 단언을 지적해, 최종 근거로 쓰기 전에 commit→end 조회 순서/관측 시각과 STOP 구간 표본을 보강한다. 비밀 생성 뒤 파일 준비는 정리가 보장되는 phase로 옮기고 Python/Java 계획 일치를 장애 주입 전에 대조한다.
- 0904368 smoke도 고유1200·물리1212·정지중 양수lag1표본·재개5.023초 뒤 첫0lag·정리 통과다. Claude는 장시간 입력 중0lag에 의존한 복구 판정, API timeout과 묶인 offset 계측, 계측 커버리지 미검사로 REQUEST_CHANGES를 냈다. offset은 독립1초 JSONL로 기록하고 재개 뒤 첫 end를 고정해 그 위치까지의 복구를 잰다. API/DB/JMX는 별도5초 표본이며 정상 구간90% 계측 성공·최소 표본 수와 정상구간 API200을 완료 조건에 넣는다.
- 첫 uniform03은 계측 보강을 위해 소유 Python에 SIGINT로 중단했다. 앞부분에는 별도 로컬 통합 시험/원인 조사도 겹쳤으므로 최종 성능 비교로 사용하지 않는다. 이후 matrix는 다른 실험이 없는 상태에서 실행한다. 진행 파일을 작은 JSON으로 분리해1초 폴링의 전체 결과 재직렬화를 없앴다. 이전 실패/중단과 보강 전 결과를 보존한다.

### #43 장기 계측의 시간 공백 검사

`6799d11` smoke에서 offset59/59, 정상API64/64, 정지20.238초·고정offset복구1.948초와 모든 집계 대조·정리를 통과했다. Standards0/Spec1: 수집된 표본만으로 성공률을 계산하면 장기 실행에서 느린 계측이2개만 남아도 통과할 수 있었다. 입력 시작 epoch를 명시하고 정상10초 창의 완전한 API/DB/메모리/버퍼 표본 비율90%·정상 구간 최대15초 간격을 추가한다. 실제 실행값과 의도적으로 누락한 표본의 거부를 구분해 재검증한다.

### #43 정지 이후 준비 회복과 실패 시 계측 보존

Claude의 `6799d11` 후속 리뷰는 독립 offset/고정 복구/계측 누락 보완을 확인했으나, CONT 직후503을 정상구간 오류로 처리하는 조건을 지적했다. 고정 offset 도달과 첫 readyz200을 각각 기록하고 둘 다 완료한 뒤2초까지를 회복으로 분류한다. 회복 예산60초와 이후 readyz200을 별도로 요구하며, 구간 안 오류도 숨기지 않는다. 기존 smoke05 원본을 재분석해 Kafka1.067초·readyz5.049초를 구분했다.

느린 표본도 JSONL에 즉시 남기고, Docker 계측 실패는 표본의 고정 오류형과 커버리지로 판정한다. 정지 시간/표본 부족도 분석 결과에 실패 코드로 남긴다. 정상 시간 공백·재개 직후503·계측 누락·Docker 통계 누락의 하네스 회귀4개를 추가한다. 소유 probe JVM과 호스트의 시각을 호출 전후로 재고 불확실성을 기록한다. fault 해제 직전에는 최대10초짜리 자원 조회를 시작하지 않는다.

실행 중인 `f9a91f6` matrix는 이미 복사한 클래스/이미지와 로드한 분석 코드를 계속 사용한다. 균등/80%편향120초는 각각12000고유·12120ACK, 정상API192/192, 정리 통과; 생성기 최대RSS는 각각187342848/187015168bytes다. 마지막 단계 부하는 실행 중이며 완료 여부는 결과 파일로 판단한다. 새로운 하네스는 별도 smoke와30분 실행에서 검증한다.


### #43 30분 전체 제품 부하 완료

2026-10-26 · F03/F04/F05/F06/F07/F10/F14 → T02/T07/T08/T11/T12. `03eb75f` 입력1800.007582초·고유90,000/물리90,900, accepted=ACK90,900, 거절/미확인0, ranking/trend/category/report 각90,000 ledger와3,615 bucket·관측1,200·알림60을 독립 기대값과 대조했다. 24개 최종 lag0, 정상API2,784회200·STOP8회timeout·복구8회200, 정상10초window176/176이다. STOP20.303초 뒤 고정 end 도달2.245초·첫ready200표본5.091초를 기록하고 소유 container/volume/비밀을 정리했다.

`f9a91f6` uniform/hot/steps와03eb75f smoke/soak revision을 분리했다. 정상 profile 단회와 전체 실행 transaction timestamp 지연, GC 후 생존량이 아닌 heap used를 [검증](PRODUCT_LOAD_VERIFICATION.md)에 명시했다. 측정/입력/오케스트레이션/판정 모듈을 분리한 기존 리팩토링을 유지하며 의미 없는 공통 wrapper를 추가하지 않았다. 단위147·하네스24는03eb75f, 실제 Kafka/DB100개는0904368의 회귀다. 최종 통합 Claude·CI는 별도 게이트다.

### 2026-10-26 Outbox 거부 시험 준비 보완 #52

F07/F09 → T03/T08/T10. PR51 `44615d0`의 PR CI40254943726에서 거부 예외를 기대했으나 반환했다. 동일 HEAD push40254939425는 성공했다. 로컬 최소 하네스는 동적 변경20/생성 시 제한20 모두277byte 입력을64byte 한도에서 거부하여 원래 CI 실패를 재현하지 못했다. 실패 시 반환값/실제 producer 오류를 수집하지 않아 원인을 확정하지 않는다.

시험 topic 생성 시64byte 제한을 함께 지정하고, 동일 topic·같은 필드 구성의 payload를 사용한 별도 실제 producer 제어 발행에서 RecordTooLargeException·offset0을 확인한 뒤 outbox를 호출한다. Outbox 자체는 하위 원인을 숨기는 기존 일반 오류 계약으로 확인하며, pending/offset/반환값을 정제 진단에 남긴다. 기본 크기 복구 뒤에는 OUTBOX_DELIVERY_UNAVAILABLE만15초 한도에서 재시도하며 성공/잘못된 pending/다른 오류를 구분한다. 독립 schema/topic20회 반복으로 거부→복구→추가 발행 없음까지 대조한다. 제품 코드는 바꾸지 않는다.

사전 리팩토링 검토 Standards0/Spec1: 일반 Outbox 오류만으로 실제 broker의 크기 거부를 구별하지 못하는 근거 공백을 제어 발행으로 보완했다. [조사](research/OUTBOX_READINESS.md)에 공식4.1.2 소스와 추론/미재현 범위를 기록했다.

`cb098c0`에서 `./scripts/check` 단위147·하네스20·JAR와 실제 Kafka/PostgreSQL 집중20회가 통과했다(2026-10-26 02:09:52 UTC 시작, 12.692초). 매회 독립 topic/schema로 실제 크기 거부, pending 유지·end0, 복구 뒤 end1·pending 해제·추가 발행 없음을 확인했다. 이번 fixture payload는276byte이며 사전 최소 probe의277byte와 구분한다. 복구 재시도는20회 모두0회였다. [정제 근거](poc/evidence/outbox-readiness-2026-10-26.json)에 원본 hash를 남겼다. 보완 후 Standards/Spec은 각각 지적0·차단0이다. 최종 Claude와 push/PR CI는 병합 게이트로 별도 확인한다.

최종 리뷰 `b654ea4`는 Claude APPROVE였다. 비차단 의견의 근거 시각 중복 Z·숫자 타입을 정정하고, 제어 payload가 Outbox DB row를 직접 읽은 값은 아니라는 표현을 명확히 했다. 최종 commit은 다시 리뷰와 CI를 확인한다.


### 2026-10-26 수집기 자식 JVM 시작 진단 #54

F13 → T08/T14. PR51 push40254127191과 PR53 push40258124001의 기존 두 수집기 인계 시험이 연결2/실제1로 실패했다. PR53 최종65ff2d7은 Claude APPROVE와 PR CI 성공(실제119개)이지만 push 실패로 병합하지 않았다. 실패 시 한 자식의 정상 보고서만 있어 생성자 예외인지 실행 중 상실인지 아직 확정하지 않는다.

수집기 fixture에 시작 단계·단조 경과 시간·예외 타입 체인·허용 코드만 쓰는 진단을 추가하고, 부모는 종료코드와 보고서 유무를 함께 남긴다. 기존 인계 시험을 독립 topic/schema5회로 관측한다. 이 초기 단계에서는 producer 최초 metadata 조회2초 제한을 가설로 두고 정책을 유지했다. 이후 별도 실제 bootstrap 차단에서 관측한 timeout 경계에 근거해 시작 견고성을 보완했으며, 과거 CI 내부 원인 규명으로 취급하지 않는다. #43의 30분 부하 측정 중에는 로컬 중량 빌드를 겹치지 않고 원격 CI로 관측한다.


### #54 실제 초기 bootstrap 차단과 제한 재시도

2026-10-26 · F13 → T08/T14. `ae01064`에서 실제 Kafka bootstrap TCP 응답을 버려 metadata2,008ms timeout·자식 종료1·보고서 누락을 재현했다. `ea3eb34`는 임대 전 TimeoutException만 최대5회 시도한다. 경로 복구 후 ACK10/ledger 일치와 영구 차단 후 timeout5회·임대/원천0을 확인했다. 과거 CI의 내부 원인은 계속 미확정이며 동일 원인으로 단정하지 않는다.

`./scripts/check` 단위151·하네스20·JAR와 실제 Fleet13개(독립 인계5회)가 통과했다. 시작02:45:55.384 UTC·85.412초다. [조사](research/FLEET_STARTUP.md)와 [근거](poc/evidence/fleet-startup-2026-10-26.json)에 실패/성공 revision을 분리했다. Spec 지적0, Standards P2의 진단 오류로 cleanup 누락 가능성은 `f0b2fcf`에서 진단 오류 격리와 process/report 등록 순서로 수정했다. 제품 metadata 경계만 작게 추출하며 실행 중 발행 한도를 변경하지 않았다.

후속 `f0b2fcf`도 단위151·하네스20·JAR 통과, Standards 재검토 차단0이다. 실제 Fleet13 실행 revision은 ea3eb34로 유지한다. 최종 Claude·push/PR CI 확인 뒤 PR55에 포함된 #52까지 통합한다.


### #42 선행 시험 보완 통합

PR55 최종cb20ca6 Claude APPROVE·push40261118857/PR40261121946 성공, 두CI 단위151·실제125(Fleet13) 확인 후07f2444로 develop 병합했다. PR53도 포함된commit으로 GitHub에서병합상태가되었다. #54는완료·보드Done, #52는PR51최종CI조건을계속확인한다.

#42에07f2444를통합했다. STATUS/WORK_LOG의충돌은각실행의revision/근거를보존하고현재상태로합쳤다. migration코드·fixture는불변이다. 기존44615d0 승인과실제리허설결과를유지하되새HEAD의check·Claude·push/PR CI를다시확인한다.


### #43 최종 근거 리뷰 보완·선행 통합

164f66a의Claude REQUEST_CHANGES는문서/근거범위3건이었다. API는단일순차probe(명목1.6요청/s)이며webproxy·collector를실행하지않고 JMX/healthcheck실험overlay를사용함을명시했다. 실제경로는전역4개·고정채널3개·readyz이며모두단일채널이라는리뷰전제는코드와다르다. soak p95가uniform보다높은원인은미규명으로남기고, 전체quantile을정상구간quantile로바꾸지않는다. 정상/장애분해자료가없어지연안정성주장을하지않는다.

개별근거의files/partitions/apiLatencyMillis/memoryBytes/minuteSamples를색인에명시했다. 해당데이터와원본hash는기존파일에있었으나이전리뷰에첨부되지않았다. 사용한정제스크립트를scripts로보존하고원본5개실행에서공유JSON과동일한지대조한다. 순위/증가율·해시알고리즘자체·알림중복은독립검증에서제외한다. DB count7개는계측부하이며docker stats메모리와VmRSS도구분했다. #42 c34dc2b에포함된#52/#54를통합했으며문서충돌은양쪽이력을보존했다.


### #44 실제 경계 근거와 시작 진단 통합

94da3ad의push40260027568·PR40260031297에서실제Fleet14개(새경계7개)가모두통과했다. 전자는전체성공, 후자는기존Outbox1개실패였다. [검증](fleet/FAILURE_BOUNDARIES_VERIFICATION.md)·[근거](poc/evidence/collector-boundaries-2026-10-26/summary.json)에실제2JVM시계/분리장애와별도같은JVM의permit/send/ACK경합을구분했다. DBblackhole인계6.082초·이전프로세스생존·이전sessionmarker0, WS공백1/복원0, 검사후send는ACK가능을기록했다.

#43의420efa3(선행#42/#52/#54포함)을통합했다. Clockargs와bootstrapargs충돌은 mode/clock/bootstrap 위치를통일하고, 2종시작진단을#54의phase/예외타입/허용코드진단으로합쳤다. 실제producer관측과clock제어는보존한다. 진단쓰기실패가cleanup을막지않게했고 성공한process/report만함께등록한다. 제품Clock전달과임대전metadata재시도경계를모두유지하고기존한도는바꾸지않았다. 새통합HEAD의20개Fleet회귀와최종리뷰/CI를확인한다.

통합 `34e824e`에서 단위152·하네스24·JAR, 실제 Kafka/DB Fleet20개가 143.251초에 실패0으로 통과했다. XML·각 진단 hash와 permit 경계3개 결과를 보존했다. Standards/Spec은 각각 지적0·차단0이다. 원래 CI14개 수치와 로컬 통합20개 수치를 혼합하지 않는다.

### #43 환경 한계·통계 범위 재검토

최종 근거 리뷰420efa3의 문서 차단2건을 반영했다. CPU138개 점 표본·Kafka1CPU 한도 접근·입력 pacing 최대692.672ms와 계측8회 논리 Admin 호출을 명시한다. throttling/시간 상관과 지속 가능 용량은 미검증이다. partition분포는 STOP/catch-up을 포함한 전체 phase라고 한정했다. 분단위 메모리 항목, 중복의 한 채널 집중, SQL percentile_cont와 정제 계산의 출처도 구별했다. curator는 누락 containers 표본을 허용하며 harnessRevision/자체hash/재구성 명령 출처를 기록한다. 원본5개를 재정제하여 메타데이터 외 모든 숫자·원본hash가 기존과 일치함을 단언했다. 부하 하네스와 제품 실행 코드는 변경하지 않아 30분 재실행은 하지 않는다. 420efa3 check는 단위151·하네스24·JAR 통과다.

### #45 선행 검증 통합과 최종 리뷰 재요청

#42/#43/#44/#52/#54가 포함된 e90dc32를 통합했다. 코드 충돌은 없으며 README/STATUS/WORK_LOG의 양쪽 근거를 보존하고 오래된 실행 대기 문구를 실제 결과로 고쳤다. f992ef1에서 실행한 KafkaDiskLossExperiment/KafkaDiskProbe/LocalKafkaCluster와 replay/producer 제품 경로는 통합으로 변경되지 않았다. 단위·하네스·컴파일과 최종 CI를 확인하며 동일 디스크 실험은 반복하지 않는다. 초기 최종 Claude57f3024 요청은 14분 이상 출력이 없는 상태에서 소유 CLI만 종료했고 승인으로 취급하지 않는다. 통합 최종 HEAD는 전체 변경/실제 근거로 새 리뷰를 요청한다.

통합 `7e0ca6f`의 `./scripts/check` 단위152·하네스24·JAR가 통과했다. 실행 코드 불변은 f992ef1 대비 실험3종/스크립트/replay/pipeline 경로의 diff0으로 확인했다. 최종 CI와 정확한 HEAD Claude 판정은 PR56에 연결한다.

### #46 최종 근거 리뷰와 선행 통합

76ae909의 Claude는 로컬12단계 범위 APPROVE이며 실행4a6b7a8 이후 코드 불변과 미첨부 파일 확인을 요청했다. 알림 physical3/logical2·원래group SAFE_OVERLAP, 선택snapshot 범위, 목표미달 하네스 분류를 명확히 했다. 첫 사전 검사 실패 JSON도 hash만 남기지 않고 정제 근거에 함께 보관한다. 개발 서비스 종료는 부하/디스크 실험 종료 뒤 사용자 허용 범위에서 volume 삭제 없이 실행했고, PITR 뒤 #44 회귀용으로 재기동했다. volume 전후checksum과 archiver실패원인은 측정하지 않았다.

선행31e0773(#42~45/#52/#54)을 통합하고 README/STATUS/WORK_LOG의 양쪽 이력을 보존했다. PITR 하네스/DeploymentProbe/ops/compose의 실제 코드 불변을 대조하고 통합 check와 최종HEAD 리뷰/CI를 별도 확인한다. 공통 소유 자원 수명을 유지하며 의미 없는 추가 추상화는 만들지 않았다.

통합 `44449cd` check는 단위152·하네스24·JAR 통과다. 실행4a6b7a8 대비 PITR스크립트/helper·DeploymentProbe·PocApplication·operations/pipeline/db·compose 경로의 diff0과 파일 존재를 확인했다. 새 product코드 영향은 collector/Clock 경로로 한정되어 이 PITR 실행은 반복하지 않는다.

#42 최종c34dc2b Claude APPROVE·push40262248304/PR40262253338 SUCCESS와 각각단위151·실제125(실패0)를 확인해 PR51을03b9648로 develop 병합했다. #42와 #52의 모든 완료조건·보드를 완료했다. #52 원래 CI 원인 미확정 기록은 유지한다.


### #60 유휴 plan 잠금 실패 재현

F13 → T02/T08/T11/T14. PR58 e90dc32 push40263102345 성공/PR40263104263 실패를 확인하고 병합을 보류했다. 원래 실패의80개 DB 표본에서 이전 PID의 idle in transaction, 대기 PID의 transactionid 잠금17개 표본을 확인했다. 최종 SQLSTATE는 당시 보고서에 없어 확정하지 않는다.

실제 plan 행 FOR UPDATE 응답 직후 클라이언트를 멈추는 시험 전용 장벽을 넣고 대기 worker 등록 후 DB blackhole을 주입했다. 수정 전 로컬1개가 epoch2 기대/실제1로 실패했다. 표본 사이 수분 공백이 있어 이 실행의495초를 복구 시간으로 사용하지 않는다. 원본·hash는 work/remaining-validation/fleet-lock-red-01에 보존했다. 가설은 유휴 transaction 잠금 유지, 대기 worker 시작 지연, 단순 실행 속도 저하 순서이며 등록/잠금/대기 상태를 따로 확인한다.


#60의 두 번째 수정 전 실행은 장애 주입 전에 A가 종료하여 owner가 null인 진단 전제 실패였다. 첫 실행의 잔여 observed 파일을 두 번째 결과로 사용하지 않는다. 이어 유휴 timeout만 추가한 대조에서는 이전 잠금 PID가 사라졌으나 대기 worker가 이미 종료하여 epoch2 기대/실제1로 다시 실패했다(39.867초, 잠금 대기3개 표본). 이 분리는 유휴 잠금 제한과 미배정 충돌 재시도가 모두 필요함을 보여준다. 최초 CI에서 실제 SQLSTATE를 기록하지 않은 한계는 유지한다.

잠금 분류는 CollectorLeases의 transaction 완료 경계, 미배정 재시도 횟수와 기한은 CollectorWorker에 둔다. 보유 임대·연결/SQL취소·불확정 transaction을 동일하게 재시도하는 범용 도구를 만들지 않았다. 실제2JVM blackhole, 실제 대기/영구잠금/보유임대와 JDBC rollback 실패·불확정 commit의 합성 경계를 추가한다.

수정 후 집중 검증은 JDBC/worker 단위7개와 실제 Kafka/DB4개가 모두 통과했다. blackhole의 유휴 잠금 해제·epoch2 인계, 미배정 잠금 해제 후 수집, 영구 충돌5회 제한, 보유 임대 충돌의 재시도0을 확인했다. 이후 깨끗한 revision에서 check와 Fleet 전체 회귀를 실행해 통합 영향을 확인한다.

고정 ea7da9b의 check(단위159·하네스24·JAR)와 실제 Fleet23개가 385.072초에 실패0으로 통과했다. 기존 시계/분리 장애·발행 경합·인계5회·초기 metadata와 새 잠금 회귀를 함께 실행했다. Standards 하드/heuristic/차단0, Spec 지적/차단0이다. 원래 CI와 대조·전제 실패를 [검증 기록](fleet/DB_LOCK_VERIFICATION.md)에 보존했다. 이후 docs만 추가하며 최종 정확한 HEAD의 Claude·push/PR CI를 확인한다.


### #45 유휴 임대 잠금 수정 통합

31e0773의 push40263229427은 #44와 같은 blackhole 시험에서 실패했고 PR40263232084는 성공했다. 실패를 이유 없이 재실행하거나 병합하지 않았다. #60의14816f8(Claude APPROVE, 로컬 단위159·실제Fleet23 근거)을 통합한다. 문서 충돌은 #42/#43/#52의 완료와 #45 실제 디스크 근거, #60 실패/수정 이력을 함께 보존했다. 디스크 실험·probe·cluster·replay/pipeline 경로가 f992ef1 대비 불변인지 다시 확인하고 최종 HEAD의 check·Claude·두 CI를 확인한다.

## 2026-10-26 기술 검증 최종 기록 정리

이 정리 당시 #43~46의357435d/e90dc32/31e0773/3986b15는 각각 Claude APPROVE였다. 이후 #44/#45 CI 실패는 위 #60에서 따로 수정하며 과거 승인을 새 HEAD에 재사용하지 않는다. 이 문서 보완은 구현/실험 결과를 바꾸지 않고 리뷰의 비차단 설명을 정리한다. #43의 단기 CPU 해석 한계·장애 없는 profile 라벨·curator 출처, #44의 실행별 수치 출처·blackhole 인과 한계·미실행 경계, #45의 손실 경계 성공/원천 복구 실패·중간 container 생성 표본·미실행 D, #46의 코드 경로 대조/이미지 identity 한계를 명시했다. 원본5개를 정제 스크립트로 다시 계산해 공유 근거의 모든 필드 일치를 확인했다.

로컬 실험 종료 후 `./scripts/infra down`으로 개발 서비스만 종료했다. 소유 실행 container0·기존 named volume2개, 다른 프로젝트의 실행 container 이름6개를 이전 목록과 대조했다. volume 내용 checksum과 다른 container ID의 전후 비교는 하지 않았다. 정제 근거를 보관하며 PoC/API/web는 재기동하지 않았다. 각 PR 최종 CI/병합 결과와 외부 조건이 필요한 #47~49의 열린 상태는 후속 기록에 반영한다.


### #46 잠금 복구·문서 보완 통합

#60 승인14816f8과 #45 통합ac934e3을 반영했다. #46 이전3986b15의 PR40263423850은 성공했지만 push40263419936은 취소되어 두 최종 CI 성공으로 취급하지 않는다. 보존한 validation-closeout의 비차단 설명·한국어 근거·PITR 비밀 파일 위치 정정을 함께 통합했다. 문서 충돌은 각 실제 실행과 검증 이력을 모두 남겼다. 제품/시험 fleet 경로는14816f8과 동일하며 경로 감사에 나열한 PITR/배포/분석기/복구 파일을 실제 실행4a6b7a8과 다시 대조한다. 감사 밖의 fleet/poc 변경과 시험 JavaExec 추가는 후속 감사 파일에 명시한다. 이미지 byte 동일성을 주장하거나 디스크/PITR 실험을 새 코드에서 재실행했다고 쓰지 않는다.

#60 최종 Claude 비차단은 잠금1초<SQL3초의 순서 의존, 마지막 충돌과 재시도 수의 차이, DB 표본 수 해석으로 문서에 보완했다. 기존 승인 코드와 시험에는 변경이 없다.03:23 정리 뒤 #60 시험을 위해 개발 서비스를 다시 기동했고,04:30:28 UTC에 다시 종료했다. 새 정리에서는 다른6개 container의 ID·이름도 일치했고 개발volume2개를 보존했다. volume 내용 checksum은 측정하지 않았다.


## #60 통합 후 최종 근거 정리

최종 비차단 리뷰에 따라 Spring7.0.10 JdbcAccessor의 기본 번역기 선택과 내장 XML 매핑을 공식 소스로 대조하고 실제 JAR의 root override0을 확인했다. 자료를 확인하지 않은 리뷰의 버전 기억을 사실로 채택하지 않았다. #45 실패 아티팩트도 내려받아80개 유휴transaction/17개잠금 표본을 확인했으며 최종 SQLSTATE는 계속 미확정이다. blackhole 주입 시점이 plan 잠금 직후로 고정됐고 lock-free 구간은 별도 재검증하지 않았음을 명시했다. 유휴2초의 일시중단 가능성과 진단 코드 한계도 문서에 남겼다.

#45 ac934e3·#46 4e16f50의 check는 각각 단위159·하네스24·JAR 통과다. 디스크 감사에 Gradle/HA Compose/DB/migration을 추가해 모두f992ef1과 불변임을 확인했고, PITR 실행 경로도4a6b7a8과 대조했다. 증거의 과거 실행 값을 덮어쓰지 않고 새 검사·감사 파일로 연결했다.

## 2026-10-26 최종 게이트·이슈 마감

F03/F04/F05/F06/F07/F08/F10/F13/F14/F15/F16 → T02/T04/T07/T08/T10/T11/T12/T14/T15/T16. 다음 정확한 HEAD의 Claude APPROVE와 push/PR CI SUCCESS·테스트 XML 실패/오류/skip0을 다시 확인한 뒤 develop에 순차 병합했다. 각 XML SHA256과 리뷰 세션·병합 SHA는 STATUS의 최종 게이트에 연결했다.

| 이슈 | PR | 최종 HEAD | push / PR CI | 병합 |
|---|---|---|---|---|
| #43 | #59 | 357435d | 40263042346 / 40263046186 | a9722cf |
| #44/#60 | #58 | 14816f8 | 40269412503 / 40269417146 | 0b46c81 |
| #45 | #56 | ac934e3 | 40269571318 / 40269576801 | a21cb56 |
| #46 | #57 | 4e16f50 | 40269806695 / 40269809554 | 2f5b2da |

#43은 단위151·실제125, #44/#45/#46은 각각 단위159·실제135다. #46 최종 CI 로그의 하네스24·웹29·Chromium31도 대조했다. 각 이슈와 Project 상태를 완료로 갱신했다. 이전 실패/취소 run을 성공으로 바꾸지 않으며 #60 이전 CI의 최종 SQLSTATE는 여전히 미확정이다.

리팩토링 점검은 이슈별 검증 문서에 남겼다. #60은 DB 분류와 worker 수명 책임을 분리하고 시험 장벽을 test DataSource에 한정했다. #45는 소유 cluster/읽기 전용 disk probe/판정 경계, #46은 공통 소유 자원·snapshot·새 group 정렬을 재사용했다. 최종 비차단 문서 검토에서 PITR 경로 감사를 quality/categories/reports/notifications/observations/replay와 wrapper까지 확장하고, 실제 main source 변경5개 및 시험 JavaExec 차이를 명시했다. release의 lock 호출·비밀 파일0444/디렉터리0700·유휴2초의 정상 일시중단 영향·미배정 실패 코드 해석도 확인했다. 제품 코드와 시험은 마지막 승인 HEAD 이후 변경하지 않았다.

#47 공식 앱/동의/안전한 주입 경로, #48 계정/리전/AZ/예산, #49 배포 대상/도메인/운영 목표는 준비되지 않아 실제 운영 검증 이슈를 열어 둔다. 로컬 선행 #42/#43/#45/#46은 완료됐다. PoC/API/web와 개발 Kafka/PostgreSQL은 종료 상태이며 다른 worktree·다른 서비스는 변경하지 않았다.

## 2026-10-26 공개 방송 목록 대안 조사

F12/F13 → T13/T14/T08/T11. 사용자 요청에 따라 공식 API 등록 외의 발견 경로를 조사했다. 소유 저장소의 clean 상태를 확인하고 fetch한 origin/develop `128f3e6`에서 `technicaltest/public-catalog-research`를 만들었다. 별도 develop worktree는 변경하지 않았다.

현재 공개 웹 JavaScript와 고정 커밋의 클라이언트를 대조해 `/service/v1/lives` cursor, 카테고리별 목록, 채널별 상태 polling과 팔로워 합계 경로를 확인했다. 기존 `scripts/discover`도 공개 목록의 첫 10개만 읽고 있었다. [조사 문서](research/CHZZK_PUBLIC_CATALOG.md)에 근거 URL·소스 hash·추천 구조를 남겼다.

15:49:46~15:50:06 KST에 앱 키·쿠키 없이 목록 50개씩 20페이지와 채널 상세 1회를 호출했다. 목록 1,000행·고유 방송 995개·페이지 간 중복 5행, 상세의 `followerCount` 정수를 확인했다. 20페이지 예산에서 중단했고 다음 cursor가 남아 전체 순회가 아니다. 중복 원인은 확정하지 않았다. 원천 응답·식별자·이름은 메모리에서만 처리하고 [수치·필드·hash](poc/evidence/public-catalog-research-2026-10-26.json)만 공유한다. 초기 Python 로컬 CA 오류는 서비스 거절과 구분했고 curl 기본 TLS 검증으로 성공했다.

공식 앱이 모든 원천 조사의 선행이라는 해석을 바로잡고 #47의 범위를 공식 인증 경로로 한정했다. 제품 코드·수집 한도·인프라는 변경하지 않았다. 리팩토링 검토 결과 기존 `BroadcastCatalog`와 선정·임대 경계를 재사용하는 어댑터가 적합하며, 조사만을 위해 공통 계층을 추가하지 않았다. 문서 70개 링크·기술 목표 16개/기능 16개 매핑·diff 공백 검사와 정제 근거의 계수·종료 조건·공유 필드 검사가 통과했다. 공유 필드 검사에서 cursor 형식 설명의 `liveId: int`를 실제 ID 값으로 오인한 첫 검사식을 경로·형식으로 구분해 재검사했다. Java·Kafka/DB·웹 회귀는 문서만 변경한 이번 조사에서 실행하지 않았다.

## 2026-10-26 로컬 미리보기·공개 수집 재기동

F01/F03 → T02/T08/T13. 사용자가 실행 확인을 요청하여 기존 개발 volume을 보존한 채 Kafka/PostgreSQL·분석기·Node24.20.0 웹을 띄웠다. 현재 코드의 JAR 빌드는 UP-TO-DATE 성공이며 분석기와 수집기 실행 복사본의 SHA-256이 같다. 기존 Kafka group은 ranking/trend/notification 3개였으므로 관측/category/report 3개의 최초 시작만 earliest로 지정했다. 원천 목록의 첫 20개 응답에서 adult=false인 3채널을 메모리로 골라 `SPRING_PROFILES_ACTIVE=pipeline PIPELINE_ROLE=collector ./scripts/poc <3 IDs> 30m 0s`로 시작했다.

18:28:53 KST에 완료 1분 순위 3채널 HEALTHY, DB 처리 이력/집계 각각 7,100→9,304, 소비 실패 0을 확인했다. 브라우저 7경로와 320px 알림 화면의 JS/API 오류·alert·가로 넘침은 0이다. 실제 방송명·제목·이미지는 저장할 스크린샷에서 가리고 원천 목록은 저장하지 않았다. [정제 근거](poc/evidence/runtime-preview-2026-10-26.json)는 중간 관측으로 표시하며 진행 중 수집을 완료 PASS로 기록하지 않는다. 웹·서버·인프라를 사용자 확인용으로 유지하고 수집기만 기존 30분 상한에서 자동 종료한다.

도중 최대 채널 수 질문에는 기존 #43의 합성 20채널/50건/s 30분·200건/s 60초와 실연결 3채널을 구분해 답했다. 이 시험은 collector를 실행하지 않았으므로 실제 채널 최대 수용량으로 환산하지 않는다. 이번 기동에서 새 부하 실험이나 제품 코드 변경은 하지 않았다.

## 2026-10-26 용량 경계 측정 도구 #62

F01/F10/F13/F14 → T02/T07/T08/T11/T12/T14/T16. 사용자 지적에 따라 고정 부하의 성공과 인프라 산정에 필요한 포화 경계를 구분하고 #62를 등록했다. 이전 공개 목록/미리보기 문서를 보존하고 fetch한 origin/develop에서 `technicaltest/capacity-limits`를 만들었다. 다른 worktree는 변경하지 않았다.

유한 JSON 계획으로 채널 수·입력률·조회량을 지정하고 실제 ChannelProbe를 별도 합성 WebSocket 원천에 연결한다. 응답과 독립된 API 도착률, DB commit 가시성 표본, 지속 lag 추세, 컨테이너 CPU 제한·RSS·저장 크기를 추가했다. 원천/수집기/제품 자원과 생성기 한계를 구분하며 시험별 클래스·JAR·이미지·계획을 고정한다. 제품 코드와 공개 수집 한도는 바꾸지 않았다.

첫 하네스 회귀에서 HTTP client 종료만으로 종속 완료 콜백까지 기다린다고 가정한 오류가 드러났다. 실제 loopback HTTP 응답을 장벽으로 지연한 시험으로 재현했고, 발행한 요청의 완료 future를 기다리도록 수정했다. 지연된 응답 때문에 줄어든 요청 수와 실패 콜백을 누락하지 않도록 검증한다. 용량 결과는 깨끗한 commit의 이미지로 후속 실행한다.

0c8b456의 직접 입력 스모크는 20채널·50건/s 30초·API 5요청/s, 1,500개 고유/1,515개 물리 입력의 정합성과 정리를 확인했다. 실제 Kafka/DB 회귀와 일부 겹친 도구 점검이므로 인프라 산정에는 쓰지 않는다. 이 revision의 단위164·하네스29와 실제 Kafka/DB135가 통과했다.

Standards 검토의 계약 문제 2건·구조 판단 1건, Spec 검토의 오판 위험 3건을 반영했다. 비동기 소켓 큐 앞뒤의 수신 지연을 추가하고, 단계 구분을 계획 시각으로 통일했다. 100ms마다 첫 채널만 뽑는 편향은 시간 창 reservoir 표본과 partition 분포 검사로 바꿨다. 모든 소비자의 lag를 채팅 입력률로 비교하던 판정은 topic별 실제 입력률·partition end 도달 기한으로 바꿨다. 관측 consumer가 60초 동안 정지해도 통과하던 합성 시계열을 회귀 사례로 남겼다. 기본 profile 중복은 Python을 정본으로 두고 Java는 고정 JSON만 받도록 정리했다.

### #62 포화 경계·실패 진단 보완

연결 150개는 저부하에서 두 번 통과했고, 180개는 두 번 입력 전에 종료했다. 300개와 180개 반복에서 JVM heap 부족을 확인해 #64로 분리했다. 20채널 균등 2,000건/s는 60/120초 통과했고 2,500건/s는 두 번 카테고리 적체로 기준을 넘었다. 3,000건/s의 DB 2 CPU/API·DB 각 2 CPU 대조도 통과하지 못했다. 80% 편향 2,000건/s는 category/report가 적체됐다. 조회 100요청/s는 통과했고 200요청/s는 최대 동시 요청 32→100개 대조에서도 실패했다.

최소 원문만 측정한 한계를 보완하려고 UTF-8 1KiB 합성 한글 profile을 추가했다. 7bcc64d의 100채널·1,000건/s·API 50요청/s·80% 편향 5분은 30만 건 정합성·최종 0lag·정리에는 성공했지만 순위 가시성 p95 4,101ms·API 실패 3/미발행 22·여러 집계 적체로 SLO를 넘었다. 최종 drain은 71.478초다. 통과한 개별 축의 상한을 동시에 쓸 수 없다는 근거로 남기고, 100채널·500건/s·API 10요청/s 후보를 별도로 측정했다.

사후 리뷰는 누락 API 지연을 생성기 병목으로 오판한 사례와 DB 저장량 조회 실패가 종료 진단을 가리는 문제를 찾았다. 누락과 10초 histogram 초과를 구분하고 프로세스·원천·offset 분석을 먼저 checkpoint한다. 저장량 오류는 고정 코드로 보존하고 통과를 해제한다. 원본 분류와 수정 판정의 재분석을 정제기에 함께 남겨 과거 값을 덮어쓰지 않는다. 관련 Python 14개·전체 하네스 34개·Java 165개와 JAR가 통과했다. 제품 main source는 변경하지 않았다.

DB 증가량의 장기 보존 문제는 #63, 집계/공유 DB/계측 비용 분리는 #65에 후속으로 등록했다. 각 목표와 수용 조건을 연결했다.

### #62 복합 후보·정제·정리

c015e20의 100채널·500건/s·API 10요청/s·1KiB 합성 한글·80% 편향을 준비 30초+측정 270초로 실행했다. 고유 150,000/물리 151,500건을 모두 대조했고, 순위 가시성 p95 279ms·조회 76ms, 실패/미발행/지속 적체 0, 최종 24개 lag 0·drain 0.071초로 통과했다. 앞선 더 큰 조합의 실패를 함께 보존했다. 초기 비교 후보와 정책상 20% 여유 제안을 구분하며 fresh DB 5분 결과를 운영 SLA로 확대하지 않는다.

23회 원본을 정제기에 다시 넣어 원본/수정 분류·코드 hash·입력·이미지·자원·최종 대조를 연결했다. 누락 계측의 과거 오분류는 원본을 남긴 채 offline 재분석했다. 현재 EventCodec의 합성 1,000개 value 평균 426.594바이트·key 32바이트를 재현했고, 실제 Tomcat 11.0.27 JAR의 연결별 CharBuffer 할당 경로를 hash로 보존했다. Kafka 보존 크기가 partition별로 적용되고 segment 단위로 삭제됨을 공식 4.1 문서에서 재확인했다.

Standards/Spec 재검토의 진단 지적을 수정하고, 계획·원천·수신·API·commit 표본·판정을 나눈 책임을 점검했다. 원본 없이 결과를 새 실행으로 꾸미거나 제품 main source를 바꾸지 않았다. 소유 시험 컨테이너 0, 기존 8개 컨테이너 ID/이름/실행 상태 불변, 미리보기 API/web HTTP 200을 확인했다. 원래 공개 수집 PID는 종료됐다. 다른 worktree·main 릴리스·외부 배포는 변경하지 않았다. 최종 develop 통합은 PR66의 정확한 HEAD Claude 리뷰·CI·병합 기록으로 연결한다.

최종 Standards 검토에서 정제기에 예외 로그 4줄이 남은 부분을 찾아 고정 실패 코드별 건수로 바꿨다. 원본 diagnostic hash는 유지했다. 23회 원본 hash 대조와 정제 결과의 완전 재현·금지 필드 부재를 다시 확인했고, 최종 Standards/Spec의 남은 지적은 각각 0건이다.
