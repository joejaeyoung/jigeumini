# 수집·집계·조회 용량 경계 측정

2026-10-26 · [이슈 #62](https://github.com/joejaeyoung/jigeumini/issues/62) · F01/F10/F13/F14 → T02/T07/T08/T11/T12/T14/T16.

## 측정할 값

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

`sourceMode=DIRECT`는 수집기 없이 정규화 이벤트부터 부하를 넣는 대조다. `resourceProfile`은 `BASELINE`, `API_2_CPU`, `DB_2_CPU`, `KAFKA_2_CPU`, `COLLECTOR_2_CPU`다. 한 시험은 최대 30분·고유 30만 건이며 실제 측정 단계는 30초 이상이다. 1% 동일 UUID 재전달, 30초 간격 채널 관측·outbox도 함께 입력한다.

## 임시 연구 기준과 해석

- 순위 ledger가 **commit 후 독립 질의에서 보일 때까지** p95 ≤2초. 200ms 간격으로 최대 10표본/s를 읽으므로 폴링·질의 시간이 더해진 상한이며 모든 projection의 commit 지연은 아니다. 다른 projection은 lag·최종 대조로 검사한다.
- API p95 ≤500ms, 오류·동시성 초과 미발행 0. 요청 스케줄 지연 p95 >100ms 또는 입력률 95% 미달은 생성기/계측 한계로 구분한다.
- 최종 UUID·내용 hash·bucket·관측·알림·outbox 대조 및 24개 group/topic/partition 위치의 lag=0. 최종 drain 성공만으로 지속 처리 용량을 인정하지 않는다.
- 단계 앞 1/3 이후 lag 회귀 기울기가 입력률 2%/s(최소 1건/s)를 넘고, 마지막 평균이 첫 평균보다 입력 1초분 이상 증가하며 입력 2초분보다 크면 지속 증가로 판정한다. 1초 표본은 원자 snapshot이 아니며 표본 수·간격도 함께 공개한다.
- `warmup` 단계는 기록하되 SLO 판정에서 제외한다. 생성 지연 최대 >1초, 계측 공백, 실패한 원천을 통과로 표시하지 않는다. 원천이 늦어진 수치를 제품의 최대치라고 쓰지 않는다.

결과의 `capacityAnalysis`는 `WITHIN_SLO`, `EXCEEDS_SLO`, `GENERATOR_LIMITED`, `MEASUREMENT_INCOMPLETE`를 구분한다. 실행 명령 성공은 하네스의 정합성 검사가 끝났다는 뜻이며 `withinSlo`와 별개다. 원본은 `result.json`, `probe-output/`, `source-output/source.json`에 보존한다. 종료 시 해당 시험 소유 자원을 정리한다.

## 검증 범위의 한계

- 공개 원천의 연결 제한·네트워크 지연·TLS·서비스 정책은 이 내부망 실험으로 알 수 없다. 공개 채널 live 검증은 기존 최대 3개다.
- 실제 ChannelProbe와 발행기를 실행하지만 fleet의 분산 임대·발견 루프 비용은 포함하지 않는다. 합성 원천·collector-probe·제품이 같은 Docker VM을 공유한다.
- collector-probe의 RSS/CPU에는 기대 결과 보관·API 부하·계측도 포함된다. 순수 collector 자원으로 환산하지 않는다. 큰 입력의 기대 결과 메모리가 먼저 한계가 되면 그 사실을 기록한다.
- 로컬 RF1 수치는 다중 호스트/AZ·RF3·운영 디스크의 수용량이 아니다. 외부 검증은 #48/#49에서 대상 자원을 고정한 뒤 재실행한다.
- 운영 상한은 측정한 정상/편중 부하 중 낮은 값에 명시적인 여유율을 적용한 제안이다. 실패 노드 재배치·retention·DB 보존 기간까지 고려해야 하며 측정하지 않은 구간을 선형 추정으로 통과 처리하지 않는다.

실제 실행 결과와 자원 산정은 후속 측정 기록에 추가한다.
