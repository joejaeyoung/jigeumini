# 수집기 배정·임대·인계

2026-10-26 · #28 / F13 → T13/T14/T02/T11

## 활성화 경계

선정 미리보기는 수집을 시작하지 않는다. `fleet preview`는 파일을 검증하며 DB/원천에 접속하지 않는다. `apply`는 명시적으로 배정 계획만 저장하고 `worker`가 임대를 획득해야 연결한다. 공식 채팅 어댑터는 미구현이므로 OFFICIAL_AUTHORIZED 계획을 거부한다. UNOFFICIAL_POC는 `FLEET_LIVE_ENABLED=true`가 있어야 apply/worker를 허용하며 유효 배정 최대3개·각 실행최대30분이다. SYNTHETIC의20채널 상한은 테스트에서만 사용한다. 이를실제전체방송수집권한으로해석하지않는다.

```bash
./scripts/fleet migrate
./scripts/fleet preview work/selection.json 120
./scripts/fleet apply work/selection.json 120
./scripts/fleet status
# 실제 공개 PoC를 별도 실행할 때만 UNOFFICIAL_POC의 최신 선정파일을 apply한다.
# FLEET_LIVE_ENABLED=true ./scripts/fleet worker 1 60 work/fleet-result.json
```

선정파일 SHA256이 정책/선정 버전이다. 불완전 스캔·유효기간초과·중복/과다 채널을 거부한다. 선정 갱신은 별도 apply이며 자동 원천 순회가 아니다. 팔로워 하한과선정순서는[선정 계약](../discovery/README.md),접근범위는[조사](../research/CHZZK_SCALE.md)를따른다. 선정파일은안전한메타데이터만포함한다.

## 소유권과 유한 자원

- PostgreSQL의단일plan행 `FOR UPDATE` 안에서 선정·등록용량·획득/갱신/해제를직렬화한다. 채널PK당소유자UUID·증가epoch·DB만료시각을하나만저장한다.
- 프로세스는시작마다새UUID를쓴다. 갱신은동일owner/epoch·미만료·현재선정조건을모두통과해야한다. 만료한기존소유자는같은tick에서부활하지않는다. 해제도owner/epoch를검사해새소유권을지우지않는다.
- worker최대20개·worker당1~20채널·전체합성20/공개3개의유효배정상한이다. 현재제외된채널도기존임대가유효하면전역용량에포함한다. 등록채널20,000개를넘으면실패하며epoch이력을임의삭제하지않는다.
- 기본TTL10초(허용6~60초),1초간격갱신이다. 로컬권한기한은DB요청시작의단조시각+DB남은TTL−2초다. 늦은응답은기한을연장하지않고기한이이미지났거나DB호출이실패하면전체worker를중단한다.
- 원천HTTP호출은worker당기본500회·시작간격500ms,동시연결은배정용량이내다. 기존HTTP/WS본문·timeout·재시도상한은유지한다. 이예산은플랫폼공식quota가아니다. 발행전체1000·채널500·inflight64의[제품한도](../pipeline/PUBLISHER.md)를worker마다적용한다.

## 오래된 worker 차단의 정확한 범위

수신전HTTP단계와연결감독,frame admission에서임대를확인한다. 수락시받은epoch permit을Kafka send직전에다시검사하므로새임대가오래된큐의권한으로바뀌지않는다. 상실/만료 후 같은 프로세스의 재획득은 없으며 새 프로세스로 시작한다. 한 채널이 실행 기한 전에 종료해도 worker 전체를 중단하고 보유 채널 모두에30초 재시도 대기를 적용한다. 이는 현재 보수적인 PoC 운영 정책이다.

**이것은 Kafka broker의 강제 fencing이 아니다.** 이미client에넘긴최대inflight64건과마지막검사→send사이정지한한건은소유권상실뒤반영될수있다. Kafka내부재시도도이미넘긴입력범위다. 정지한프로세스의기존소켓은재개/서버timeout전물리적으로남을수있으므로유효임대수와물리연결수를혼동하지않는다. DB시계의갑작스런전진·호스트단조시계속도오차·악의적client의프로토콜우회는검증하지않았다. 운영은DB시간동기화와이상경보가필요하다.

원천메시지는안정적인중복키가없다. 다른수집기의같은원천메시지는다른eventId가될수있고DBledger가이를제거하지못한다. 인계공백의채팅도복원하지못한다. 검증은유효DB소유권·permit차단·수락/ACK/미확인·broker/DB집합을각각대조한다.

기존수신전용 `poc`와단일 `pipeline collector`는기존PoC경로다. fleet와동시에실행하지않는다. 다중배포에서는fleet진입점만사용한다. 임대에참여하지않는별도client까지하나의소유권으로통제했다는주장은하지않는다.

## 정상 종료·관측

정상종료는새획득중단→WS종료→최대10초발행drain→heartbeat종료→임대해제순서다. drain동안임대를계속갱신한다. DB장애/만료면permit을취소하고미확인을남긴다. DB해제실패시새소유자는원래만료를기다린다. 진단 파일은 정상 종료와 SIGTERM hook이 저장 전체를 동기화하고 파일 쓰기가 끝난 뒤 hook을 제거한다. CLI의 telemetry 마지막 flush와 producer 정리도 hook이 기다리는 저장 경로에 포함한다. released=true는 owner/epoch를 확인한 해제 명령이 DB에서 성공했다는 뜻이며, 이미 새 소유자라 실제 변경0행이어도 true다. 강제SIGKILL/전원손실의메모리입력과결과파일은복원하지못한다.

status의discovered/eligible/selected와유효assigned/connected를분리한다. connected는최근heartbeat의연결관측이고건강한집계구간의증명이아니다. healthyWindow는null이며[품질API](../api/QUALITY_RANKINGS.md)에서별도판정한다. 운영worker는CollectionTelemetry에수신·종료·손실·buffer관측을연결한다. 시청자메타데이터수집기는이새CLI에아직합성하지않았다. 원천API/토큰/채팅원문은파일과로그에기록하지않는다.

## 근거·리팩토링

- PostgreSQL16 [행 잠금](https://www.postgresql.org/docs/16/explicit-locking.html)과 [clock_timestamp](https://www.postgresql.org/docs/16/functions-datetime.html)를2026-10-26확인했다. 잠금후의DB현재시각을사용하며transaction시작시각을만료시계로쓰지않는다.
- CollectorLeases는DB소유권·용량,LeaseAuthority는로컬permit,CollectorWorker는연결/갱신/종료수명,PublishingAuthority는발행직전확인을소유한다. 원천프로토콜을복제하지않고ChannelProbe를재사용한다.
- 실제검증과남은범위는[STATUS](../STATUS.md)에서관리한다. 이번작업은외부배포나공개live확대실행을포함하지않는다.

## Kafka 준비 후 수집 시작

worker는 producer 생성과 `partitionsFor` 성공을 먼저 확인한 뒤 임대 획득·원천 연결을 시작한다. 준비 실패는 `FLEET_KAFKA_NOT_READY`로 거부하며 HTTP/소켓/임대를 만들지 않는다. metadata 대기는 producer의 `max.block.ms=2000` 설정을 따른다. 이 값이 JVM의 producer 생성 전체 시간을 제한한다는 뜻은 아니다. 실행 중 장애에는 기존 유한 큐·미확인 계수·실패 종료 계약을 적용하며 큐 크기를 늘려 감추지 않는다.
