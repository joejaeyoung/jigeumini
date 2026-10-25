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

### #27 발행 경로 구현과 리팩토링 점검

- 전용 dispatcher가 채널별 queue를 한 건씩 순환하고 ACK callback이 대기 건수를 해제한다. Kafka send 자체의 metadata/buffer 대기는 수신 callback 밖에서 처리한다.
- admission·ACK·계수 snapshot은 같은 잠금으로 보존 관계를 유지하고 종료 join은 잠금 밖에서 실행한다. 종료 기한의 결과를 고정해 늦은 ACK가 저장된 결과를 바꾸지 않는다.
- 공개 EventSink/BufferState 계약을 유지하고 설정은 Limits로 묶었다. 발행 상태·공정 배분·계수를 단일 모듈 안에 두며 별도 thread pool/범용 scheduler를 추가하지 않았다.
- 채널 초과는 해당 연결을 terminal 실패로 끝내고 다른 채널의 정상 진행을 허용한다. Kafka 실패·전역 한도는 전체 수집을 중단한다. 어느 쪽이든 거절한 frame은 복구할 수 없고 최종 complete=false다.
- 느린 ACK·물리 중복·전량 거절·동시 admission·순환 순서·send 동기 실패/차단·종료 경합·늦은 ACK를 합성 단위12개로 확인했다. 실제 broker/DB와 성능 검증은 별도 기록한다.

- #27 비교 경계 점검: cf4049e 직렬 발행기를 테스트 전용 LegacySerialPublisher로 고정하고 두 실제 발행기의 실험 관측 어댑터를 뒀다. 제품 변경 뒤 baseline 이름이 새 구현을 가리키는 오류를 막는다. 수락 단계 실패와 실제 전달 실패를 분리해 전역 초과 후에도 이미 수락한 큐는 정상 broker로 drain한다. 이 경계를 추가 회귀로 검증한다.

- #27 첫 push CI40210203976에서 callback 실패 주입 직후 단언이 실패했다. mock send가 future를 반환하고 callback을 등록하기 전에 테스트가 future를 완료할 수 있는 경합을 확인했다. deliveryFailed 관측을 기다린 뒤 실패 계약을 검사하고, send가 이미 실패한 future를 반환하는 경우도 추가했다. 임의 sleep이나 제품 코드의 동기 ACK 대기를 넣지 않았다.

- 자원 정리 감사에서 Kafka 이미지의 VOLUME3개가 docker rm만으로는 정리 증거에 포함되지 않음을 확인했다. 새 실험은 소유자label이 있는 명시적 data volume을 쓰고 나머지 두 경로는 tmpfs로 둔다. 정리 시 정확한 컨테이너·volume의 소유자label을 검사한다. 과거 익명 volume은 소유권을 기록하지 않아 임의로 삭제하지 않으며 과거 cleanupComplete를 컨테이너 확인 범위로 한정한다.
