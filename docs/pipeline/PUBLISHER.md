# 유한 비동기 발행과 채널 과부하 제어

2026-10-26 · #27 / F14 → T02/T06/T10/T11/T12

## 채택 이유와 범위

#11의 합성 대조에서 기존 한 건씩 ACK를 기다리는 worker는 500건/초 입력을 끝내지 못했다. 실험용 비동기 후보는 같은 목표 입력을 완료했다. 정확한 수치·실패·계측 한계는 [부하 검증](EXPERIMENTS_VERIFICATION.md)에 기록한다. 제품에는 전용 dispatcher와 유한 ACK 대기 창을 적용한다. 실험 후보의 WebSocket callback 안 `send` 호출을 그대로 옮기지 않는다. Kafka `send` 자체도 metadata/buffer를 기다릴 수 있기 때문이다.

- Kafka key=channelId, timestamp=receivedAt, acks=all, idempotence=true, 연결당 요청 in-flight=1은 유지한다. 원문과 작성자 정보는 입력 계약에서 제외한다.
- 앱 발행 중 창의 기본값64와 Kafka 연결당 요청1은 서로 다른 한도다. client의 배치가 여러 이벤트를 담을 수 있다. ACK는 DB 반영을 뜻하지 않는다.
- 동일 채널의 수락 순서대로 dispatcher가 send를 호출한다. 다른 채널의 전역 순서와 eventTime 순서는 보장하지 않는다.

## 한도와 배분

| 설정 | 기본 | 허용 범위·효과 |
|---|---:|---|
| `pipeline.buffer-capacity` | 1000 | 1~1000. queued+발행 중+실패 미확인 전체 |
| `pipeline.channel-buffer-capacity` | 500 | 1~1000. 실제 한도는 전체 한도와의 최솟값 |
| `pipeline.publisher-in-flight` | 64 | 1~1000. 실제 창은 전체 한도와의 최솟값 |

환경 변수는 각각 `PIPELINE_BUFFER_CAPACITY`, `PIPELINE_CHANNEL_BUFFER_CAPACITY`, `PIPELINE_PUBLISHER_IN_FLIGHT`다. 프레임의 모든 이벤트를 검증·인코딩하고 모든 한도를 확인한 뒤 전량 수락한다. 최대16KiB의 정규화 이벤트만 담고 인코딩 중 임시 프레임도 남은 전역 슬롯 이내다. JVM 객체·Kafka client의 별도8MiB buffer·테스트 계측 메모리는 이 payload 상한과 별개다.

대기 채널 deque에서 한 건씩 꺼내고 남은 입력이 있는 채널은 뒤로 보낸다. 새 채널은 다음 순환부터 참여하며 이미 Kafka로 발행한 이벤트를 선점하지 않는다. 최대64건의 기존 발행 중 입력, 단일 dispatcher의 최대2초 send 대기와 broker 지연은 공정 배분의 한계다. 초당 동일 처리율이나 지연 SLA를 보장하지 않는다.

## 실패와 복구

| 상황 | 동작·고정 코드 |
|---|---|
| 한 채널의 한도 초과 | frame 전부 거절, 그 채널을 `PUBLISH_CHANNEL_LIMIT` terminal 실패로 중단. 이미 수락한 입력은 drain하고 다른 채널은 계속 진행 |
| 전역 한도 초과 | frame 전부 거절, `PUBLISH_BUFFER_REJECTED`로 전체 수집 중단. broker가 정상이면 기존 수락 큐는 drain |
| 잘못된 이벤트 | frame 전부 거절, `PUBLISH_INVALID_EVENT`로 전체 중단. 원문/예외 세부정보 출력 없음 |
| broker callback·동기 send 실패 | `KAFKA_DELIVERY_FAILED`, 이후 발행·수락 중단. 실패 입력과 미발행 큐는 미확인으로 남김 |
| dispatcher 내부의 예상하지 못한 종료 | `PUBLISH_WORKER_FAILED`, 건강 상태 실패·입력 중단·미확인 보존 |
| 진단 채널 수1000 초과 | `PUBLISH_CHANNELS_LIMIT` 전체 중단. 진단 map도 유한, 추가 거절은 untrackedRejected에 합산 |
| 종료 | 새 수락 중단, 최대10초 drain. timeout은 `PUBLISH_DRAIN_TIMEOUT`; 인터럽트는 `PUBLISH_INTERRUPTED` |

채널 초과 후 같은 publisher에서 자동 재개하지 않는다. 원인을 해결하고 해당 수집기를 다시 시작해야 한다. ChatConnection의 terminal 실패 경로가 연결 종료·품질 손실 범위를 기록한다. 전역/전달 실패 시 버퍼의 미확인 구간도 기존 품질 계측에 반영된다. 연결 공백의 채팅, 메모리 버퍼를 잃은 JVM 종료는 복원할 수 없다.

## 계수와 조회

`finish()`는 다음 관계를 보존한 불변 결과를 반환한다. 수락 단계 오류는 이미 수락한 큐의 drain을 막지 않고, 실제 전달 실패만 추가 send를 막는다. 종료 결과가 고정된 뒤의 호출·늦은 ACK는 해당 관측 결과를 바꾸지 않는다. 그 뒤 broker에 실제 반영될 수 있으므로 미확인은 손실 확정 건수도 아니다.

- `attempted = accepted + rejected`
- `accepted = acknowledged + unconfirmed`
- `unconfirmed = queued + inFlight + failedUnconfirmed`
- 동일 내부 eventId를 물리적으로 두 번 수락한 경우 발행 계수는2다. DB 멱등 반영은1이며 별도 검증한다.

기존 `bufferSnapshot()`의 채널별 pending/oldest는 품질 계측이 계속 사용한다. `channelSnapshot()`과 최종 delivery.channels에는 accepted/ACK/rejected/pending/oldest/stopped/reason이 있다. 전체 peakPending/peakInFlight, 한도·실패 코드도 남긴다. 살아 있는 프로세스의 per-channel rejected 전용 HTTP API는 추가하지 않았다.

## 리팩토링 점검

발행기의 공개 EventSink 계약을 유지하고 Admission→Dispatch→ACK→Finish 상태를 하나의 잠금으로 관리한다. Kafka 호출·worker join은 그 잠금 밖이다. 물리 발행 sequence를 따로 써 eventId 중복과 메모리 슬롯을 혼동하지 않는다. 설정 불변식은 Limits에 모으고 callback만 슬롯 해제와 계수 변경을 수행한다. 장치별 재시도 프레임워크나 범용 task queue는 추가하지 않았다.

## 공식 근거와 미검증

- 조회2026-10-26: [Kafka 4.1 Producer send](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html). 비동기 결과 callback과 metadata/buffer 대기, max.block.ms를 구분한다.
- 조회2026-10-26: [Kafka producer 설정](https://kafka.apache.org/41/configuration/producer-configs/). 앱 발행 창과 max.in.flight.requests.per.connection, ACK·멱등 설정은 별개다.
- RF1 로컬 비교는 Kafka HA 근거가 아니다. 복제 장애는 #29, ACK 수준·커밋 방식 대조는 #31이다. 실제 선정 채널 전체의 용량·외부 소스 완전성은 이 변경으로 입증하지 않는다.


## 성능·장애 대조 재현

`LegacySerialPublisher`는 cf4049e의 기존 직렬 구현을 테스트 소스에만 보존한다. 제품 변경 후에도 과거 기준선이 같은 이름의 새 구현으로 바뀌지 않는다. 각 결과의 publisherImplementation/codeCommit/runtimeClassesSha256을 확인한다.

```bash
./scripts/experiment serial-control work/publisher-serial-control.json
./scripts/experiment product-control work/publisher-product-control.json
./scripts/experiment baseline work/publisher-serial-baseline.json
./scripts/experiment product-baseline work/publisher-product-baseline.json
./scripts/experiment product-burst work/publisher-product-burst.json
./scripts/experiment product-hot work/publisher-product-hot.json
./scripts/experiment broker-fault work/publisher-broker-fault.json
```

통제 입력100/s·15초는 양쪽 모두 완료하는 범위에서 지연/요청/배치를 비교한다.500/s·30초의 미달 여부와2,000/s 순간 입력,80% hot 채널·중복, 실제 별도 broker PAUSE/STOP을 구분한다. broker-fault는 직렬 기준선/실험 후보/제품 비동기6경우며 공유 broker를 멈추지 않는다. 실제 결과와 검증 범위는 STATUS의 근거를 따른다.

프레임 안 여러 채널이 동시에 한도를 넘으면 입력의 첫 채널 순서로 중단 대상을 정한다. send의 RuntimeException/Error와 dispatcher의 예상하지 못한 종료 모두 고정 코드만 기록하고 원인 문자열은 출력하지 않는다.
