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

## 2026-10-26 전체 제품 부하·지속 실행 #43

- F03/F04/F05/F06/F07/F10/F14 → T02/T07/T08/T11/T12. fetch한 origin/develop에서 `technicaltest/product-soak`를 만들고 최종 CI 중인 #42를 fast-forward로 포함했다. #42 병합 뒤 develop 기준을 통합한다.
- 제품 분석기와 부하 생성기를 별도 JVM·소유 Compose로 분리한다. 생성 입력의 UUID/내용/10초 합계를 DB 결과와 대조하고, 그룹6개·24개 group/topic/partition의 offset·lag·처리량과 API/DB 지연을 기록한다. 원천 세션/heartbeat와 outbox는 합성 입력이며 실제 원천·급상승 감지 정확도 부하 검증으로 확대하지 않는다.
- JMX는 시험 overlay의 내부망에서만 열고 분석기 heap을 읽는다. 분석기 RSS·컨테이너 CPU/메모리와 생성기 heap/RSS는 별도 표본이다. 기존 운영 compose를 바꾸지 않는다. 실행 클래스는 결과 디렉터리에 고정 복사하고 hash를 남겨 뒤의 빌드가 실행을 바꾸지 않게 한다.
- 균등/80% 편향 각120초100건/s, 단계50/100/200건/s 각60초, soak50건/s1800초를 고정한다. 100건마다 동일 UUID를 새 offset으로 중복 발행한다. smoke/soak은 분석기만20초 SIGSTOP/CONT하며 입력은 계속한다. 실행 전 설계이며 아직 통과 근거는 아니다.
- 최초 b91b8e7 smoke는 격리 인프라/분석기 시작 후 `OWNED_CONTAINER_MISSING`으로 중단했다. 생성기 one-off가 `api` service label을 공유해 API 소유권 조회에 컨테이너2개가 반환된 것이 원인이다. 생성기를 별도 `load-probe` service로 분리한다. 최초 결과와 정리 성공을 보존하며 완료로 세지 않는다.
- f71423a의60초 smoke는 고유1200·물리1212·모든 집계 대조·STOP/CONT·자원 정리를 통과했다. 다만 리뷰에서 입력 중 offset 조회 순서의 음수 lag 가능성과 정지 구간에 한정하지 않은 lag 단언을 지적해, 최종 근거로 쓰기 전에 commit→end 조회 순서/관측 시각과 STOP 구간 표본을 보강한다. 비밀 생성 뒤 파일 준비는 정리가 보장되는 phase로 옮기고 Python/Java 계획 일치를 장애 주입 전에 대조한다.
- 0904368 smoke도 고유1200·물리1212·정지중 양수lag1표본·재개5.023초 뒤 첫0lag·정리 통과다. Claude는 장시간 입력 중0lag에 의존한 복구 판정, API timeout과 묶인 offset 계측, 계측 커버리지 미검사로 REQUEST_CHANGES를 냈다. offset은 독립1초 JSONL로 기록하고 재개 뒤 첫 end를 고정해 그 위치까지의 복구를 잰다. API/DB/JMX는 별도5초 표본이며 정상 구간90% 계측 성공·최소 표본 수와 정상구간 API200을 완료 조건에 넣는다.
- 첫 uniform03은 계측 보강을 위해 소유 Python에 SIGINT로 중단했다. 앞부분에는 별도 로컬 통합 시험/원인 조사도 겹쳤으므로 최종 성능 비교로 사용하지 않는다. 이후 matrix는 다른 실험이 없는 상태에서 실행한다. 진행 파일을 작은 JSON으로 분리해1초 폴링의 전체 결과 재직렬화를 없앴다. 이전 실패/중단과 보강 전 결과를 보존한다.
