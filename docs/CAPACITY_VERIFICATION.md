# 수집·집계·조회 용량 경계 측정

2026-10-26 · [이슈 #62](https://github.com/joejaeyoung/jigeumini/issues/62) · F01/F10/F13/F14 → T02/T07/T08/T11/T12/T14/T16.

## 측정 기준

인프라를 고르려면 고정된 자원에서 입력을 올려 **통과한 부하와 기준을 넘긴 부하**를 함께 알아야 한다. 채널 수만으로는 방송별 채팅량·조회량 차이를 표현할 수 없다. 기존 [전체 제품 시험](PRODUCT_LOAD_VERIFICATION.md)의 합성 20채널은 용량 상한이 아니다.

| 축 | 입력 | 관측 |
|---|---|---|
| 연결 | 합성 WebSocket 2~1,000개, 시작률 50개/s | 실제 ChannelProbe HTTP/READ/heartbeat, 연결/수신 대조, 스레드·FD·RSS |
| 처리 | 전체 10~10,000건/s, 균등 또는 한 채널 80% | Kafka ACK, 순위 commit 가시성, 독립 6개 group lag, 최종 모든 집계 대조 |
| 조회 | nginx를 통한 7종 API, 0~200요청/s | 응답 속도와 독립된 입력률, 동시성 초과 미발행, 실패, p95/p99 |
| 자원 | Compose 기본값, 한 역할만 CPU 2개로 변경 | cgroup CPU 사용·제한 시간, Docker RSS/네트워크/디스크, 저장 디렉터리 크기 |

제품 코드·개발 환경의 공개 수집 한도를 변경하지 않는다. 시험은 소유 표식이 있는 내부 Docker 망에 새 Kafka/PostgreSQL/API/web/probe를 띄우며 공개 원천을 호출하지 않는다. WebSocket 원천은 별도 JVM/컨테이너다. READ와 heartbeat 외의 명령은 실패로 기록한다.

## 실행과 자원 고정

루트 [README](../README.md)의 `scripts/product-load`가 정본이다. 깨끗한 commit으로 이미지를 빌드하고 JSON 계획과 시험 클래스/JAR hash를 실행마다 고정한다. 기본 API 1 CPU/640MiB, Kafka 1 CPU/768MiB, PostgreSQL 1 CPU/384MiB, collector-probe 1 CPU/640MiB, web 0.5 CPU/64MiB다. 별도 합성 원천은 1 CPU/256MiB다. JVM heap은 API/probe 384MiB, 원천 128MiB다.

```json
{
  "name": "capacity-smoke",
  "channels": 20,
  "stages": [
    {"name": "warmup", "seconds": 30, "eventsPerSecond": 100, "hot": false},
    {"name": "steady", "seconds": 60, "eventsPerSecond": 100, "hot": false}
  ],
  "faultAtSeconds": -1,
  "faultSeconds": 0,
  "sourceMode": "WEBSOCKET",
  "apiRequestsPerSecond": 10,
  "apiMaxInFlight": 32,
  "resourceProfile": "BASELINE"
}
```

`sourceMode=DIRECT`는 수집기 없이 정규화 이벤트부터 부하를 넣는 대조다. `resourceProfile`은 `BASELINE`, `API_2_CPU`, `DB_2_CPU`, `KAFKA_2_CPU`, `COLLECTOR_2_CPU`다. DB만 늘린 뒤 API 한도가 이어지는지 대조할 때 `API_DB_2_CPU`로 API와 DB를 각각 2 CPU로 고정한다. 한 시험은 최대 30분·고유 30만 건이며 실제 측정 단계는 30초 이상이다. 1% 동일 UUID 재전달, 30초 간격 채널 관측·outbox도 함께 입력한다.

## 임시 연구 기준과 해석

- 합성 원천의 예정 tick 시각부터 실제 collector 수신까지 전체 메시지의 p95 ≤1초·최대 ≤3초. 원천과 collector는 같은 Docker VM 시계를 쓴다. source의 비동기 송신 큐 적체도 포함하며, 단계별 수신량을 계획과 대조한다.
- 순위 ledger가 **commit 후 독립 질의에서 보일 때까지** p95 ≤2초. 각 100ms 창에서 reservoir 방식으로 한 건을 골라 최대 10표본/s를 읽고, 활성 partition별 최소 5표본과 p95도 검사한다. 200ms 폴링·질의·표본 선택 대기 시간이 더해진 상한이며 모든 projection의 commit 지연은 아니다. 다른 projection은 lag·최종 대조로 검사한다. `latency`의 quantile은 10초 안에 보인 표본만 집계한다. `notVisibleWithin10Seconds`가 하나라도 있으면 SLO 실패이며, 그 비율이 5%를 넘으면 전체 표본 p95는 10초 초과(정확한 값 미관측)로 설명한다.
- API p95 ≤500ms, 오류·동시성 초과 미발행 0. 요청 스케줄 지연 p95 >100ms 또는 입력률 95% 미달은 생성기/계측 한계로 구분한다.
- 최종 UUID·내용 hash·bucket·관측·알림·outbox 대조 및 24개 group/topic/partition 위치의 lag=0. 최종 drain 성공만으로 지속 처리 용량을 인정하지 않는다.
- 단계 앞 1/3 이후 group/topic별 lag 회귀 기울기가 **해당 topic에서 관측한** 입력률 2%/s(최소 0.01건/s)를 넘고, 마지막 평균이 첫 평균보다 입력 1초분 이상 증가하며 입력 2초분보다 크면 지속 증가로 판정한다(건수 하한 1). 별도로 관측한 partition별 end에 10초 넘게 도달하지 못한 경우도 실패다. 1초 표본은 원자 snapshot이 아니며 표본 수·간격도 함께 공개한다.
- 단계는 조정 루프의 실행 위치가 아닌 공통 시작 시각과 계획으로 계산한다. 메시지 지표는 예정 입력 단계, API와 offset은 실제 관측 시각에 귀속한다. `warmup` 단계는 기록하되 SLO 판정에서 제외한다. 생성 지연 최대 >1초, 계측 공백, 실패한 원천을 통과로 표시하지 않는다. 원천이 늦어진 수치를 제품의 최대치라고 쓰지 않는다.

결과의 `capacityAnalysis`는 `WITHIN_SLO`, `EXCEEDS_SLO`, `GENERATOR_LIMITED`, `MEASUREMENT_INCOMPLETE`를 구분한다. 실행 명령 성공은 하네스의 정합성 검사가 끝났다는 뜻이며 `withinSlo`와 별개다. 원본은 `result.json`, `probe-output/`, `source-output/source.json`에 보존한다. 종료 시 해당 시험 소유 자원을 정리한다.

## 검증 범위의 한계

- 공개 원천의 연결 제한·네트워크 지연·TLS·서비스 정책은 이 내부망 실험으로 알 수 없다. 공개 채널 live 검증은 기존 최대 3개다.
- 실제 ChannelProbe와 발행기를 실행하지만 fleet의 분산 임대·발견 루프 비용은 포함하지 않는다. 합성 원천·collector-probe·제품이 같은 Docker VM을 공유한다.
- collector-probe의 RSS/CPU에는 기대 결과 보관·API 부하·계측도 포함된다. 순수 collector 자원으로 환산하지 않는다. 큰 입력의 기대 결과 메모리가 먼저 한계가 되면 그 사실을 기록한다.
- 로컬 RF1 수치는 다중 호스트/AZ·RF3·운영 디스크의 수용량이 아니다. 외부 검증은 #48/#49에서 대상 자원을 고정한 뒤 재실행한다.
- 운영 상한은 측정한 정상/편중 부하 중 낮은 값에 명시적인 여유율을 적용한 제안이다. 실패 노드 재배치·retention·DB 보존 기간까지 고려해야 하며 측정하지 않은 구간을 선형 추정으로 통과 처리하지 않는다.


### 합성 원문 크기

`payloadProfile`은 생략 시 `MINIMAL`(9바이트 합성 ASCII)이다. WebSocket 계획은 `KOREAN_1K`로 지정하면 메시지마다 UTF-8 1,024바이트의 합성 한글을 파싱한다. 원문은 수치·hash 근거에 남기지 않으며, Kafka 이벤트 크기는 이 원문 크기와 다르다. 두 profile의 결과를 구분한다. 실제 원천의 필드 분포·TLS 비용은 포함하지 않는다.

## 2026-10-26 실측 결과

### 고정 환경

호스트는 macOS arm64, 논리 CPU 15개·메모리 48GiB, Docker Desktop VM은 CPU 8개·메모리 약 7.65GiB다. 기존 개발 컨테이너 2개·다른 프로젝트 컨테이너 6개와 미리보기 API/웹은 유지했다. 부하 시험은 하나씩 실행했다. 처음 DIRECT smoke는 실제 Kafka/DB 회귀와 겹쳐 산정에서 제외했다. 외부 원천 요청은 없다.

모든 실행의 제품 JAR SHA-256은 `e2f1d2b0eff4a4e5d9cd8bba61ca14c666e3ae643f893898cf52040a89ec2504`다. 실행마다 하네스 수정 commit·이미지 ID·fixture 클래스 manifest hash·계획·시각·원본 hash를 [정제 근거](poc/evidence/capacity-2026-10-26/summary.json)에 남겼다. 서로 다른 하네스 commit의 시험을 마지막 문서 HEAD에서 재실행했다고 취급하지 않는다. Kafka는 3 partition·RF1, 같은 채널은 같은 partition이고 제품 소비자·PostgreSQL 공유 풀 설정을 유지했다. 외부 메타데이터 조회는 비활성이다.

### 연결·처리·조회 경계

아래는 30초 준비 구간 이후의 결과다. 별도 표시가 없으면 메시지는 `MINIMAL`, API 10요청/s, 측정 60초, 위 기본 자원이다. `통과`는 해당 시간·입력의 임시 SLO와 최종 정합성이 충족됐다는 뜻이다.

| 축 | 입력 | 결과 | 관측 |
|---|---|---|---|
| 연결 | 100개, 전체 100건/s | 통과 | 순위 가시성 p95 227ms, API 49ms |
| 연결 | 150개, 전체 100건/s, 2회 | 둘 다 통과 | 가시성 272/223ms, API 40/43ms |
| 연결 | 180개, 2회 | 둘 다 입력 전 실패 | 최초 연결 최고 172/166개. 두 번째는 `JAVA_HEAP_SPACE`·exit3 |
| 연결 | 300개 | 입력 전 힙 부족 | 최고 연결 172개, 입력 0. 진단 후 소유 probe만 종료·정리 |
| 균등 처리 | 20채널, 2,000건/s, 60/120초 | 둘 다 단기 기준 통과·lag 증가 추세 | 가시성 547/360ms, API 84/113ms |
| 균등 처리 | 20채널, 2,500건/s, 2회 | 둘 다 기준 초과 | 카테고리 lag +764/+478건/s, watermark 21.4/12.1초 |
| 균등 처리 | 20채널, 3,000건/s | 기준 초과 | 카테고리 lag 기울기 +1,049건/s, 미도달 watermark 24.4초 |
| 자원 대조 | 3,000건/s, DB만 2 CPU | 기준 초과 | 카테고리 +485건/s, 순위 p95 2,142ms |
| 자원 대조 | 3,000건/s, API·DB 각각 2 CPU | 기준 초과 | 카테고리 +193건/s. 지연 감소만으로 지속 처리 통과 처리하지 않음 |
| 80% 편향 | 20채널, 2,000건/s, 120초 | 기준 초과 | 카테고리 +212건/s·watermark 13.2초, report +80건/s |
| 조회 | 20채널·100건/s, API 100요청/s | 통과 | 6,000요청, p95 39ms, 오류·미발행 0 |
| 조회 | API 200요청/s, 최대 동시 32개 | 기준 초과 | 12,000 예정 중 72개 미발행. 발행된 요청은 모두 성공 |
| 조회 | API 200요청/s, 최대 동시 100개 | 기준 초과 | p95 872ms, 실패 34·미발행 166, 순위 p95 2,020ms |

**연결 150개는 저부하에서 통과한 지점이고, 180개는 실패한 지점이다. 172개는 실패 도중 잠시 연결된 수이며 최대 운영 채널 수가 아니다.** 처리량과 조회량도 서로 다른 조건의 축별 경계다. 이들을 각각 최대값으로 동시에 운전할 수 있다고 해석하지 않는다. 2,000건/s의 카테고리 lag 기울기는 90.3/47.0건/s로 입력률 2% 기준을 넘었다. 마지막 평균이 입력 2초분보다 작아 단기 판정은 통과했지만, 반복의 마지막 평균은 3,478건으로 증가했다. 더 긴 지속 실행을 보증하지 않으며 운영 처리량으로 채택하지 않는다. 입력 전 종료한 180/300개 실행은 계측 미완료로 분류되지만, 연결 수 불일치와 종료 코드로 해당 계획의 연결 실패를 확인했다. 힙 부족 코드는 두 번째 180개와 300개 실행에서 확인했다.

### 복합 운영 후보

`7bcc64d`의 100채널·1,000건/s·API 50요청/s·1KiB 합성 한글·80% 편향은 30초 준비+270초 측정했다. 300,000건을 수신하고 최종 모든 집계가 일치했지만, 순위 가시성 전체 표본 p95 >10초(2,693개 중 296개가 10초 내 미가시; 보인 2,397개만의 p95는 4,101ms)·API 실패 3/미발행 22·ranking/trend/category/report 적체로 기준을 넘었다. source 예정→수신 p95는 9ms, 최댓값 145ms로 기준 이내였고, 최종 drain은 71.478초다. 이 조합은 운영 후보에서 제외한다.

`c015e20`의 **100채널·500건/s·API 10요청/s**는 같은 5분·80% 편향·1KiB 조합에서 통과했다. 전체 150,000개 고유/151,500개 물리 입력과 모든 집계가 일치했다. 준비 구간을 제외한 270초에서 source→수신 p95 10ms/최대 129ms, 순위 가시성 p95 279ms(2,698표본), API 2,700요청 전부 성공·p95 76ms였다. 모든 group/topic의 지속 증가·10초 watermark 초과가 없었고 최종 24개 위치 lag=0, drain 0.071초였다.

두 복합 실행은 입력률과 API 요청량을 함께 변경했다. 개별 실패 원인을 분리한 대조가 아니며, 후보 5분 실행도 1회다. 원문 크기만의 영향이나 편중의 정확한 처리 경계를 확정하지 않는다.

이 결과를 **현재 초기 비교 후보**로 사용한다. 연결만의 최대 통과 150개 대비 100개로 제한했고, 운영 admission을 더 보수적으로 시작한다면 20% 여유를 둔 80채널·400건/s·API 8요청/s를 제안한다. 이 20%는 정책상 여유율이며 새 측정값은 아니다. 통과 시험도 5분·fresh DB 범위이므로 하루 운영 성능을 보증하지 않는다. probe 표본 최대 RSS 약 558MiB·heap 약 367MiB·스레드 263개·FD 198개였고 전체 COUNT 질의 최대 301ms가 포함됐다. heap peak는 GC 후 생존량이 아니다. 힙 한도의 약 96%, container 메모리 한도의 약 87%인 표본이며 계측·oracle 비용을 포함한다.

### 실패 원인과 확정하지 않은 부분

- **연결 메모리:** 제품은 text message 최대치를 1MiB로 설정한다. 실제 포함된 Tomcat WebSocket 11.0.27의 `WsFrameBase` bytecode에서 그 크기로 `CharBuffer.allocate`하는 경로를 확인했다. UTF-16 배열 약 2MiB/연결 외에도 객체·HTTP/스레드·native 메모리가 필요하다. 300개 시험은 입력 전 `Java heap space`, cgroup PID 제한 `max`, 커널 OOM kill 0이었다. 이는 JVM heap 부족의 근거이며 OS FD 한도나 Kafka 최대치로 설명하지 않는다. heap dump는 저장하지 않았다. [실제 JAR·bytecode·코드 hash](poc/evidence/capacity-2026-10-26/connection-memory.json)를 보존했다. 개선은 [#64](https://github.com/joejaeyoung/jigeumini/issues/64)로 분리했다.
- **집계 적체:** 2,500/3,000건/s에서 source 수신은 정상이고 카테고리/report 소비가 뒤처졌다. CPU를 늘리면 카테고리 lag 기울기가 줄지만 3,000건/s는 계속 실패한다. 공유 DB·소비 배치·단일 consumer·계측 비용이 함께 들어가므로 DB CPU만이 유일한 원인이라고 확정하지 않는다. 같은 2,500건/s 반복의 기울기도 764/478로 달랐고 CPU 대조는 각각 1회라 효과 크기를 확정하지 않는다. DB만 2 CPU에서는 순위 가시성이 465→2,142ms로 악화돼 분석기 경합도 후속 대조가 필요하다. Kafka CPU는 이번 표본에서 먼저 포화된 지표가 아니며 Kafka의 최대 처리량을 찾았다는 뜻도 아니다.
- **조회:** 동시 요청 예산 32개를 늘리는 대조에서 지연·실패가 확인됐다. 200요청/s 실패를 첫 시험의 미발행만으로 서버 한계라고 단정하지 않았다. API/집계가 DB와 CPU를 공유하며 실제 데이터가 쌓인 장기 조회는 아직 시험하지 않았다.
- **계측:** 5초마다 전체 테이블 COUNT를 실행한다. 데이터 증가에 따른 계측 비용도 포함되며 순수 제품 한도와 분리한 대조는 없다. collector-probe에는 oracle·API 부하·계측 메모리가 포함된다. 자원 표본마다 실행하는 container 안의 `cat cpu.stat`과 Kafka/DB의 `du -sk` CPU·I/O도 측정 대상에 포함된다. API 응답 처리와 가시성 관측은 1 CPU probe에서 실행하므로 GC·throttling 영향을 제품 지연과 분리하지 못한다. 평균/표본만으로 순간 최대 CPU·메모리 또는 누수 부재를 주장하지 않는다.
- **도구 실패 보존:** 최초 WebSocket smoke는 Compose run DNS alias 누락으로 입력 0이었다. `--use-aliases` 수정 뒤 재실행했다. 입력 전 종료의 누락 API 표본을 생성기 병목으로 오분류한 판정은 수정하고 원본/재분석 분류를 함께 남긴다. 사후 저장량 질의 실패는 기존 offset·프로세스 진단을 지우지 않게 분리했다.

### 자원 계측의 해석

probe의 표본 구간 중 최대 throttled period 비율은 API200 10.45%, API200-wide 12.50%, 운영100 실패 13.43%였다. 각 전체 표본 구간의 throttled 시간 합은 2.704/2.891/4.052초다. 시작·drain도 포함한 근사 구간 통계이며 정상 측정 단계와 정확히 일치하지 않는다. 요청 발행 스케줄 지연만으로 응답 callback·계측 경합까지 배제하지 않는다. 실패를 순수 서버의 상한으로 환산하지 않는다.

VM 전체 CPU와 다른 8개 container의 사용량은 수집하지 않았다. API·DB 각 2 CPU 시험은 합성 원천까지 설정 한도 합이 7.5 CPU/VM 8 CPU이므로 공유 호스트 경합을 분리하지 못했다. 이러한 대조의 반복·API만 2 CPU 대조·COUNT/자원 표본 비용 분리는 #65에서 수행한다.

### 저장 용량 산정

81,000개 고유 이벤트의 fresh DB 혼합 입력 시험에서 데이터베이스가 **125,435,904바이트**, 약 **1,549바이트/고유 채팅** 증가했다. 인덱스를 포함하며 관측·알림 등 부수 입력도 섞여 있다. 주요 증가분은 `processed_event` 53.5MB, `report_event` 38.9MB, `category_event` 30.5MB다. 더 큰 입력 시험의 비율은 약 1.45~1.55KB였으며 채널/입력 분포에 따라 달라진다.

이를 초당 1,000건이 하루 종일 유지된다고 **외삽하면 약 134GB/일**이다. 이는 하루 실행의 실측도, WAL·복제·백업·장기 bloat를 포함한 최종 디스크 크기도 아니다. `processed_event`와 `chat_bucket_10s`에 자동 보존 정리가 없으므로 지금 작은 DB 디스크를 확정하면 안 된다. [#63](https://github.com/joejaeyoung/jigeumini/issues/63)에서 dedup/replay 하한을 보존하는 정리 정책과 저장량 상한을 먼저 검증한다.

Kafka에는 원문을 저장하지 않는다. 현재 `EventCodec`으로 합성 이벤트 1,000개를 직렬화한 값은 value 평균 **426.594바이트**, key **32바이트**였다([측정 근거](poc/evidence/capacity-2026-10-26/event-size.json)). record/batch/index·내부 topic·복제 비용은 이 값에 포함되지 않는다. 용량 검토용으로 **700바이트/record를 가정**하면 다음과 같다. 식의 초당 입력은 재전달을 포함한 Kafka 행 수다.

```text
Kafka 총 디스크 = 초당 입력 × record 바이트 × 86,400 × 보존 일수 × RF / 사용률 상한
1,000/s × 700B × 86,400 × 1일 × RF3 / 0.7 = 약 259.2GB (3 broker 합계)
```

균등 배치 시 broker당 약 86.4GB의 계산값이며 운영 디스크 선정값은 아니다. 관측/알림/DLQ·내부 로그와 장애 중 재복제 여유를 더해야 한다. 현재 기본 `retention.bytes=256MiB`는 **partition별 상한**이므로 24시간 설정만으로 24시간 보존이 보장되지 않는다. RF는 동일 partition의 시간 보존 범위를 늘리지 않는다. 가장 바쁜 partition의 입력 바이트/s로 실제 보존 시간을 계산하고 보존된 시작 offset을 관측해야 한다. 후보 500건/s·80% 편향에서 가장 바쁜 partition이 물리 입력 약 440건/s라고 가정하면, 256MiB를 700B/record로 나눈 크기 예산은 약 15분이다(460B 가정은 약 22분). 이는 segment·active log 동작을 제외한 계산이며 실제 보존 시간 실측이 아니다. #63/#49에서 시작 offset과 PITR 재생 가능 창을 함께 검증한다. partition별 크기 제한과 segment 삭제 동작은 [Kafka 4.1 공식 설정](https://kafka.apache.org/41/configuration/topic-configs/#retention.bytes)을 2026-10-26에 확인했다.

### 초기 인프라를 고르는 기준

외부 instance 상품이나 운영 HA 성능을 이 로컬 결과만으로 확정하지 않는다. 우선 **Docker에 CPU 8개·메모리 8GiB 수준을 배정한 단일 호스트**와 위 역할별 제한으로 100채널·500건/s·API 10요청/s를 재현하는 것이 비교 후보다. 실제 VM은 약 7.65GiB였으며 이 숫자는 클라우드 상품의 성능 보장이 아니다. 제품 5역할의 설정 한도 합은 4.5 CPU·2,496MiB이고, OS·page cache·실험 원천/계측 여유는 별도로 남긴다. 디스크는 아래 보존식과 #63 정책이 정해지기 전 확정하지 않는다. 공개 원천 연결/TLS·fleet 임대·외부 메타데이터·다중 호스트/RF3가 추가되는 비용은 [#48](https://github.com/joejaeyoung/jigeumini/issues/48)/[#49](https://github.com/joejaeyoung/jigeumini/issues/49)에서 측정한다.

- **수집기:** 연결 150개 성공 지점을 꽉 채우지 않고 검증한 후보 100개/worker를 비교 상한으로 잡는다. 조건은 Xmx384m·640MiB이며 oracle·API 부하·계측이 같은 JVM에 포함된다. 필요한 worker 수는 `ceil(선정 채널 수 / 검증된 worker당 채널 수)`이며 전체 채팅량·편향 한도를 함께 만족해야 한다. worker 1대 손실 시에도 남은 worker가 이 상한을 지키는 수를 추가한다. 이 산식이 fleet N대 실측을 대신하지 않는다.
- **분석기·DB:** 현재 비교 기준은 각각 1 CPU, API 640MiB/JVM384MiB, DB 384MiB다. 편향을 포함한 운영 후보 통과량에 여유를 두고 admission을 제한한다. 단순히 DB 2 CPU로 바꿔 3,000건/s를 수용한다는 계획은 대조 시험에서 탈락했다. consumer/SQL 비용 개선 후 같은 입력으로 재비교한다.
- **Kafka:** 3,000건/s 기준의 Kafka 표본 구간 평균은 약 0.422 CPU, 구간 최대 0.493 CPU였고 1 CPU 한도의 순간 부하는 알 수 없다. `KAFKA_2_CPU` 대조는 실행하지 않아 broker 증설 효과는 미검증이다. 운영에서는 기존 RF3/minISR2 및 controller quorum·장애 도메인 설계와 디스크/네트워크 보존식을 함께 적용한다. 로컬 RF1의 1 CPU/768MiB를 운영 broker 권장 사양으로 가져가지 않는다.
- **증설 조건:** 최종 운영 후보보다 큰 연결/입력/조회, 증가하는 lag·watermark age, 줄어드는 보존 시간, heap/RSS 여유 부족 중 하나라도 있으면 확대를 멈추고 같은 SLO로 재검증한다. 평균 채팅량만으로 채널 수를 정하지 않는다.

## 리팩토링·회귀 점검

입력 계획은 Python 한 곳에서 기본 profile을 만들고 Java는 고정 JSON을 읽는다. 합성 원천, 실제 collector 연결, API 부하, commit 표본, 판정을 별도 책임으로 나눴다. 기존 소유 Compose·정리 계약을 재사용하며 제품 수집 상한이나 frame 계약을 바꾸지 않았다. Standards/Spec 리뷰에서 수신 전 지연·표본 편향·단계 시계·저율 topic 적체·누락 계측 분류·종료 진단 순서를 보완했다.

### 최종 로컬 확인

실패를 포함한 23회 원본을 보존했다. 원본 판정과 현재 판정 코드 hash로 재계산한 분류를 함께 기록하며, 초기 DNS/연결 힙 부족 실행의 누락 지표가 `GENERATOR_LIMITED`였던 오분류는 `MEASUREMENT_INCOMPLETE`로 수정됐다. 당시 원본을 다시 쓴 것은 아니다. 부하 입력의 실제 실행과 offline 판정 재계산을 구분한다.

[로컬 회귀](poc/evidence/capacity-2026-10-26/local-gates.json)는 Java 165·하네스 34·JAR, 앞선 0c8b456의 실제 Kafka/DB 135다. 제품 main source/JAR는 그대로이며 하네스 후속 변경을 포함한 최종 CI는 [PR #66](https://github.com/joejaeyoung/jigeumini/pull/66)에서 확인한다. [정리](poc/evidence/capacity-2026-10-26/cleanup.json): 소유 시험 컨테이너 0, 기존 개발 2개·다른 서비스 6개의 ID/이름/실행 상태가 같다. 사용자 미리보기 API/웹은 HTTP 200으로 유지하고 원래 공개 수집 PID는 종료됐다. 볼륨 내용 checksum은 측정하지 않았다.
