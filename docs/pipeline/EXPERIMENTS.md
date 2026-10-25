# 합성 부하·편향·복구 실험

#11 / F10 → T06·T07·T08·T09·T11·T12. 진행 상태는 [STATUS](../STATUS.md), 해석과 실제 수치는 [검증 기록](EXPERIMENTS_VERIFICATION.md)에 둔다.

## 실행 계약

`./scripts/experiment <프로필> <새 결과.json> [eventTime 기준 UTC]`는 로컬 Kafka19092·PostgreSQL15432를 사용한다. 공개 플랫폼에는 요청하지 않는다. `./scripts/infra up`으로 테스트 인프라를 준비한다. 결과 파일·`.progress.json`·`.html` 경로는 새 경로여야 하며 기존 결과를 덮어쓰지 않는다. 실행 중 progress는 마지막 표본, 기본 JSON은 실행 선언이고 `finishedAt`이 있는 최종 JSON만 완료 결과다.

```sh
./scripts/experiment steady work/steady.json
./scripts/experiment burst work/burst.json
./scripts/experiment recovery work/recovery.json
./scripts/experiment broker-fault work/broker-fault.json
python3 scripts/summarize-experiments.py work/report work/steady.json work/burst.json work/recovery.json work/broker-fault.json
```

| 프로필 | 입력·실패 조건 |
|---|---|
| smoke | 100/s·2초, 두 독립 group, 1% 같은 eventId 재발행 |
| serial-control / async-control | 100/s·15초, 동일 입력을 두 방식 모두 완료하는 비교 |
| baseline / async-baseline | 500/s·30초, 동일20채널·seed·eventTime을 선택해 직렬 발행과 bounded async 비교 |
| steady | 500/s·600초, A3명·B1명 독립 소비 |
| burst | 2,000/s·60초, A3명 |
| uniform-1~4 / hot-1~4 | 2,000/s·15초, 20채널 균등 / 한 채널80%, 소비자1~4명 |
| recovery | 500/s·100초, A 소비 종료 확인 후60초 중단·재시작, B 계속 처리 |
| db-delay / db-failure | 500/s·30초, 8~18초 A progress SQL에250ms 지연/예외, B 계속 처리 |
| membership | 500/s·40초, 소비자1→2→4→3, 두 producer·1% 재발행 |
| poll-stall | 500/s·30초, A2명, max.poll.interval6초에 DB commit 뒤8초 정지·heartbeat 스레드는 유지 |
| heartbeat | 500/s·35초, A 부모1명+별도JVM1명, 자식만SIGSTOP·재할당 확인 뒤SIGCONT·재가입 확인, B 계속 처리 |
| hot-duplicates / sharded-3 | 2,000/s·15초, 80% 편향·A3명·두 producer·1% 재발행, channelId / channelId+shard 후보 비교 |
| broker-fault | 별도512MiB Docker broker만 pause/stop, 직렬/async 각각30개 상한·20/10채널 입력·초과1건·종료 중 실패 |

목표 수치는 입력 계획이며 결과의 달성 주장과 별개다. frame 전체 거절/발행 실패/자원 경계에서 생성을 멈추고 실제 시도·수락·ACK·거절·미확인을 보존한다. `correct=true`는 계수·bucket 대조이며 `TARGET_INPUT_COMPLETED`를 함께 확인해야 목표 입력 완료다. 실패 결과도 보고서에 포함한다.

## 격리와 자원

각 실행의 UUID `exp.chat.*` topic, `exp.a.*`/`exp.b.*` group, `exp_*` schema만 만들고 정리한다. own schema에만 migration을 적용한다. 운영 ledger/bucket 존재 여부와 존재할 경우 계수의 전후 불변을 확인한다. 없는 테이블을 생성하거나0건으로 대체하지 않는다. 이 검사 중 다른 운영 입력이 들어오면 비교가 실패할 수 있으며 다른 작업을 중지시키지는 않는다. 원문·닉네임·시청자 사용자ID·토큰은 생성하거나 저장하지 않는다. 채널/이벤트/세션 ID는 고정 seed에서 생성한 합성 값이다.

- 생성 최대600초·400,000 unique, 2,000/s, 소비자 최대4명. 입력 뒤 drain180초, fixed-range replay300초. broker-fault는 broker 시작90초/명령10초/발행기 drain10초의 유한 대기를 사용한다.
- JVM heap768MiB. heartbeat 자식은128MiB이며 지연 A 표본은 부모JVM에서 처리한 이벤트만 포함한다. B 지연은 전체를 계측한다.
- 시작 전 호스트·Kafka·PostgreSQL 파일시스템 각각2GiB 여유 공간을 요구한다. 생성 표본마다 실험 topic+schema2GiB·여유 공간을 검사하고 replay 중에도 검사한다. 검사 사이 증가량까지 막는 디스크 quota는 아니다. Kafka 로그 segment 삭제·PostgreSQL WAL·공유 서비스 전체 사용량은 실험 데이터 크기와 다르다.
- 정상/실패/일반 종료에서는 자기 자원만 정리한다. 각 정리 단계를 독립 실행하며 실패는 `cleanupComplete=false`다. SIGKILL/호스트 종료는 hook을 실행하지 못하므로 최종 JSON의 UUID 자원을 운영자가 확인해 수동 정리한다. 재실행은 새 자원·출력 경로다.
- broker-fault는 공유 Kafka를 건드리지 않고 `exp-kafka-<UUID>` 컨테이너 한 개를 생성한다. 호스트 volume을 붙이지 않으며 마지막에 그 이름만 삭제한다. broker offset span 대조이며 DB 반영/복제 HA 검증으로 계산하지 않는다.

## 입력·집계·재처리 대조

seed20261025, eventId=`UUID(seed/index)`, eventTime=`base+index/rate`이며 매100번째 입력은120초 이전으로 보낸다. receivedAt은 실제 생성 시각이다. 같은 UTC base를 주면 논리 입력을 재현하지만 Kafka topic identity/offset manifest는 실행마다 다르다. 기본 base는 실행 당시 시각-2초로 결과에 기록한다. 두 producer는 frame 단위로 번갈아 보내며 중복 UUID의 물리 전송도 각각 계수한다.

실제 `ProjectionConsumer`·`ProjectionStore`의 BATCH·동기 commit·DB 멱등 트랜잭션을 사용한다. DB commit 직후 seam에서 receivedAt→DB p50/p95/p99를 기록한다. broker ACK는 DB 완료를 뜻하지 않는다. 입력 시도=수락+거절, 수락=ACK+미확인을 검사한다. 중복 때문에 ACK unique 하한은 `ACK-(acceptedPhysical-acceptedUnique)`이며 DB가 그 하한과 acceptedUnique 상한 사이인지 확인한다. ACK가 전부 확인되면 별도 생성기 기대 bucket과 DB가 정확히 같아야 한다.

모든 소비 종료 뒤 topic ID와 partition별 시작/끝을 고정한다. 운영 offset commit 없이 별도 UUID replay projection에 같은 범위를 읽고 FULL JOIN으로 bucket 차이를 센다. 오래된/변경된 topic·partition·offset gap은 성공으로 처리하지 않는다. shard 후보는 실험 전용 key 정규화 어댑터를 거친다. 운영 replay CLI가 shard key를 지원한다는 뜻이 아니다.

동일 채널/partition의 append 순서에서 eventTime이 앞선 최대 시각보다 작은 횟수와 채널별 실제 partition 집합을 기록한다. 늦은 시각/다중 producer가 있어도 eventTime bucket 합계는 재처리와 같아야 한다. `PartitionExpansionIntegrationTest`는 실제3→6증설에서 한 키 이동·기존 manifest 거부·새 범위 중복 집계2건 유지를 검증한다. 운영3partition 설정은 바꾸지 않는다.

## 관측과 해석

표본마다 partition별 source end/commit/lag·DB progress/계수·할당·버퍼 나이·JVM CPU/heap/RSS·Docker CPU/memory/IO·topic/schema 크기를 기록한다. 기본 간격5초에 명령 비용이 더해지며 각 표본 시작/종료 시각을 남긴다. Kafka와 DB의 원자적 snapshot이 아니므로 단일 표본의 end/DB 진행 시각은 어긋날 수 있다. 입력률/처리율은 표본 차분, 회복 시간은 재개 뒤 최초0lag 관측까지의 상한이다.

측정용 UUID 집합/지연 표본도 JVM 메모리를 쓴다. 실제 메모리를 발행 버퍼 단독 사용량으로 표현하지 않는다. Docker 자원은 공유 서비스 전체이며 독립 group은 자원까지 격리된 것이 아니다. consumer4명은3partition에서 한 명 이상 유휴지만 재할당 중 표본은 순간 상태다.

직렬 발행기는 운영 코드 그대로며 bounded async는 테스트 소스에만 둔다. async도 ACK 대기 포함1,000개·frame 전체 수락/거절·실패 후 수집 중단을 유지한다. 두 producer 프로필은 각1,000개로 총2,000개다. 전역 버퍼는 채널별 예약 용량이 없어서 먼저 온 채널이 공간을 차지할 수 있다. 생산 채택/키 분산/확장 판단은 실측 결과와 미검증 경계를 함께 적는다.

producer의 request-rate/record-send-rate는 Kafka client의 시간 창 평균이다. 짧은 실패 실행과30초 완료 실행의 raw rate를 직접 처리량으로 비교하지 않는다. `producerSettledSeconds`와 `acknowledgedPerSecondIncludingDrain`은 생성 시작부터 producer close 뒤 ACK 계수가 확정된 시각까지를 사용한다. 그 필드가 없는 초기 실행에는 사후 추정값을 채우지 않는다. `pendingObservedAfterRejection`은 거절 직후 읽은 값이며 그 사이 ACK가 진행될 수 있다. 주기적으로 표본한 highWater와 실제 최댓값을 구분한다.

계측은 ExperimentSampler가 실행 중 sample을 최대15초 기다리고 종료한다. 실제 실패는 abort와 고정 이벤트로 기록하고 producer/DB drain 뒤에도 확인한다. 이전 자료의 RESOURCE_OR_SAMPLE_FAILED는 정제 보고서에서 verificationComplete=false다. broker STOP은 콜백 조기 실패 때문에 상한30 이전 거절이 가능하므로 실제 수락/거절·한도 이내를 확인하고, PAUSE는30 도달을 요구한다.
