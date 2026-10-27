# 보존 하한·정리·저장량 검증

2026-10-27 KST · [#63](https://github.com/joejaeyoung/jigeumini/issues/63) · [PR #72](https://github.com/joejaeyoung/jigeumini/pull/72).
F01/F08/F10/F16 → T02/T04/T08/T16. [계약](RETENTION.md), [공식 조사](../research/DATA_RETENTION.md). 최종 진행 상태는 [STATUS](../STATUS.md)에 둔다.

## 경계 회귀

초기 구현은 단위 179개·하네스 38개·실제 Kafka/PostgreSQL 150개(새 경계 8개)가 통과했다. 보호 이력·진단·리뷰 보완 이후 보존 경계는 13개다. 전체 회귀와 최종 실행 HEAD는 아래 후속 기록을 따른다.

- receivedAt 72시간과 eventTime 8일 경계를 분리하고 equality는 보존한다. 삭제 뒤 동일 offset은 progress로 제외하고 같은 불변 UUID의 새 offset은 OUTSIDE_LIVE_RETENTION으로 격리한다. 실제 Kafka 재전달·DB 정합·시계 후퇴·namespace 분리를 대조했다.
- 삭제 trigger 실패는 batch를 rollback하되 이미 commit한 floor는 유지한다. 새 store로 재시도한 후 집계가 부활하지 않는다. FOR SHARE를 보유한 live writer와 정리의 100ms lock 예산을 확인했다.
- replay terminal 7일·metadata 30일, QUEUED/RUNNING 보호·active 32개 admission, 부분 정리 실패 뒤 EXPIRED/null 결과·재시도, stale 객체의 부모 없는 run 부활 거부를 확인했다.
- category/report/관측의 단조 하한, active report가 참조하는 품질 snapshot/revision/loss 보호, 종료 확정 뒤 해제, 10,000+1행 정리·실패 진단·다음 작업 진행을 확인했다.
- 8일 지연된 pending outbox의 실제 Kafka 첫 발행·소비는 receipt를 남기고 피드에는 노출하지 않는다. published 보존 창이 끝나면 outbox→receipt 순서로 정리한다. 자식 없는 만료 report 1,001개는 1,000/limited=true→1/false로 정리한다.

위 검사는 합성 입력·실제 단일 broker/로컬 PG이며 공개 원천·장기 운영·다중 AZ를 검증하지 않는다.

## 합성 DB 저장량

깨끗한 `93cf16f`에서 2026-10-26 20:14:38~20:15:02 UTC 실행. PostgreSQL 16.14, 별도 schema, 각 projection 60,000행×3 cycle이다. 실행 명령·원본 hash·schema 전체 합계·주요 relation/index bytes·삭제 batch는 [storage.json](../poc/evidence/retention-2026-10-27/storage.json)에 있다. 실행 당시 JAR/class hash는 기록하지 않았고 고정 commit/소스와 명령을 근거로 남긴다.

```sh
./scripts/retention-experiment storage work/retention-63/storage-first.json
```

V13의 합성 자료에 V14를 적용하는 데 253.110ms, cluster WAL delta 9,003,136bytes가 관측됐다. processed_event 120,000행의 relation 크기는 42,516,480→52,371,456bytes로 늘었다. 새 보존 색인의 저장 비용을 포함한 값이며 대규모 운영 migration 시간은 아니다.

| cycle | 정리 시간 ms | 정리 구간 WAL bytes | DELETE 후 전체 relation bytes | VACUUM 후 bytes | 동시 live ranking 누적 전→후 |
|---|---:|---:|---:|---:|---:|
| 0 | 1,533.557 | 26,213,672 | 171,597,824 | 171,728,896 | 0→600 |
| 1 | 1,638.363 | 67,589,272 | 260,988,928 | 261,332,992 | 2,000→2,800 |
| 2 | 1,418.614 | 26,069,888 | 263,176,192 | 263,217,152 | 4,200→5,000 |

각 cycle은 7회 호출로 정리했고 가장 긴 호출은 310.808ms다. cycle별 WAL에는 동시 live 입력과 공유 cluster의 다른 writer가 포함되므로 순수 DELETE 비용으로 해석하지 않는다. VACUUM은 `ANALYZE, TRUNCATE false`로 명시 실행했다. 자동 vacuum의 장기 처리율 시험은 아니다.

열린 report의 과거 60,000행 보호, 종료 확정 후 측정 대상 만료 ledger/event 0, 동시 입력 10,000건의 ranking/trend ledger·bucket 각 10,000을 확인했다. 공유 floor 잠금은 119.461ms 뒤 실패·재시도 조건으로 돌아갔다. 입력 목표 500/s는 최대 pacing 1,782.691ms로 밀렸고 두 집계의 100건 batch p95는 296.826ms, 최대는 341.792ms였다. 20.783초에 10,000건을 처리한 정합 시험이며 500/s 지속 운영 PASS로 세지 않는다.

DELETE로 파일 크기가 바로 줄지 않았고, cycle 0→1에서 색인이 약 88MB 추가 성장했다. cycle 1→2의 작은 증가만으로 장기 plateau를 확정하지 않는다. 오래된 시각의 자료를 직접 삽입한 시험으로, 72시간 실수신·7일 테이블·checkpoint/autovacuum/백업/복제 부하는 미검증이다.

## Kafka 시간·크기·파일 보존

최종 깨끗한 `8624c04`, 2026-10-26 20:24 UTC. Kafka 4.1.2, 1CPU/768MiB의 소유 단일 broker/RF1, 검사 주기 1초·파일 삭제 지연 1초다. [kafka.json](../poc/evidence/retention-2026-10-27/kafka.json)에 6개 실행의 실패·설정·입력·offset·segment 크기·시계열·원본 hash·정리를 함께 남겼다. 현재 제품의 5분 검사 주기와 구분한다.

```sh
./scripts/retention-experiment kafka work/retention-63/kafka-stable.json
```

| 대조 | 실제 입력/ACK | 관측 |
|---|---:|---|
| 시간 4초·크기 무제한·segment 1MiB | 각 partition 40, 총 120 | 입력 중단 후 전 partition earliest=end=40. 옛 manifest RETENTION_EXPIRED. 첫 삭제 표본 89,295bytes의 .deleted 파일이 이후 0으로 변함 |
| 시간 24시간·256MiB/partition·segment 32MiB | hot 480,010·나머지 각 60,010, 총 600,030 | hot earliest 0→90,314, cold 0 유지. 옛 manifest RETENTION_EXPIRED. hot 정상 log 289,167,144bytes·삭제 대기 log 0 확인 |

크기 대조는 700B payload·32B key·무압축·80% 편향의 가속 입력이다. 추가 600,000건 발행은 5.383초였고 마지막 Kafka 시각은 20:24:47.400Z다. 최종 hot의 첫 보존 표본은 20:24:43.133Z/offset 90,314였다. **24시간 설정에도 prefix가 몇 초 만에 삭제될 수 있음**을 확인했다. 이 가속 생성기를 실방송의 15분/22분 보존 창이나 최대 처리율로 환산하지 않는다. PITR에서 필요한 offset이 earliest보다 앞이면 복구가 완료될 수 없다.

시간 대조의 관측 완료는 추가 입력 직후 약 31.2초였다. broker 초기 정리 작업 지연이 포함돼 4초 뒤 정확히 삭제된다는 증거가 아니다. offset·파일은 동시에 읽는 snapshot이 아니므로 연속 3회 안정 표본을 요구했다. stat 사이 소멸한 파일만 별도 계수하며 최종 실행의 이 계수는 0이었다. 파일 소멸 분기 자체를 재현했다고 표현하지 않는다.

`logBytes`는 `.log` 파일 논리 크기이며 index·metadata·OS 할당량을 포함하지 않는다. 시간 만료 후에도 빈 active log와 index가 남는다. 크기 만료 후 289,167,144bytes가 256MiB를 넘는 것은 segment 단위 계약과 일치한다. 전체 디스크의 절대 상한은 아니다.

### 보존한 실패와 대조

| 실행/commit | 관측·처리 |
|---|---|
| kafka-first /93cf16f | topic 생성 실패, cause 미기록. 16KiB 설정이 공식 최소 1MiB 미만. 다음 실행에서 InvalidConfigurationException 확인 |
| kafka-corrected /3366272 | 초기 30 ACK/manifest 뒤 파일 조회 실패. 이미지 BusyBox의 미지원 find -printf 확인·stat로 교체 |
| kafka-verified /eccb5af | 30초 끝무렵 earliest 40/.deleted를 보았지만 파일 제거 확인 예산 부족. 초기 작업 지연 근거로 45초 예산 적용 |
| kafka-final /12c0c3e | 시간 대조 통과. hot 296,820,471bytes의 초과 28,385,015가 첫 segment 33,511,699보다 작아 45초 동안 미삭제. 삭제 조건 불충족 대조로 보존 |
| kafka-segment-boundary /901a7c0 | 시간 대조 통과 뒤 파일 명령 실패. 원본 stderr 미보존으로 직접 원인 미확정. 삭제 경합 때 사라진 파일만 계수하는 보완 적용 |
| kafka-stable /8624c04 | 시간/크기/만료 manifest/파일 제거/소유 자원 정리 통과 |

모든 실행은 소유 자원을 정리했다. 공유 broker·기존 8개 container·다른 worktree를 변경하지 않았다.

## 운영 판단과 남은 범위

[보존 계약의 경보·입력 제한](RETENTION.md#용량과-운영-조치)은 70/80/90%와 고갈 예상 시간을 초기 후보로 정의한다. 자동 디스크 차단이나 실제 경보 배선의 완료 근거는 아니다. active report·미발행/미확인 outbox·active replay는 보호하므로 TTL을 넘을 수 있다.

외부 장기 DB·RF3 복제·OS 여유·WAL 아카이브/백업/PITR 복구 창·실제 중앙 경보는 #48/#49에서 목표 입력/리전/예산과 함께 검증한다. 운영 24시간 Kafka 창에는 현재 256MiB를 그대로 쓰지 않고 실측 bytes와 최악 partition 입력률·복제·segment 여유를 재산정해야 한다.

## 후속 회귀·리뷰

`8624c04`의 `./scripts/check`는 단위 179개·하네스 38개·JAR, `./scripts/integration`은 실제 Kafka/PostgreSQL 155개가 통과했다. 실패·오류·skip은 모두 0이다. 이후 기존 보존 테스트에 projection_failure의 영향/격리 시각·report 보호와 spike_event의 outbox FK 보호/해제 대조를 추가했고, 보존 13개를 다시 통과했다. 제품 소스 변경은 없다. 부하 종료 후 웹 타입·단위 29개·제품 빌드와 Chromium 31개도 통과했다. [로컬 회귀](../poc/evidence/retention-2026-10-27/local-regression.json).

[CLI 검증](../poc/evidence/retention-2026-10-27/cli.json)은 고유 schema에서 실행했다. Kafka 주소를 접속 불가능한 loopback으로 설정한 채 `replay migrate`·`ops retention-status`·`replay cleanup`을 통과했다. 진단은 수치·상태를 읽고 replay 행을 바꾸지 않았다. 정리는 8일 된 합성 terminal run의 bucket 1행을 삭제하고 결과 만료/정리 완료를 기록했으며, 재실행은 삭제 0행이었다. 소유 schema와 CLI 실행 JAR 복사본은 정리했다.

`12c0c3e`의 [Claude 구현 리뷰](../poc/evidence/retention-2026-10-27/implementation-review.json)는 정적 bundle 검토 APPROVE이며 최종 HEAD 승인과 구분한다. 현재 group별 단일 `KafkaMessageListenerContainer`, 일치하는 ranking-trend namespace와 V1의 `chat_bucket_time` 색인을 코드로 확인했다. 실제 analyzer 배선·runtime grant는 아래 전체 제품 실행에서 확인했다. 오래된 NOTIFICATIONS/REPLAY 문구를 수정했고, 실제 Kafka 크기 시험의 입력량 보완은 최종 성공 실행에 반영됐다.

하한의 100ms 잠금 실패 시 해당 정리를 건너뛰고 다음 회차에 재시도한다. 여러 동시 writer가 같은 projection을 점유할 때의 지속 정리율은 미검증이다. report/history의 여러 statement가 8초 transaction 예산을 넘으면 앞선 삭제도 rollback될 수 있다. 장기 DB에서 진전 없는 반복이 관측되면 기존 하한 이하 삭제의 분리, 보호 계약을 유지하는 단계별 transaction, 필요한 색인을 대조한다. 생성자에서 window 행을 초기화하므로 analyzer에는 쓰기 권한이 필요하며, `ops retention-status`는 store를 만들지 않는 읽기 경로다. 이 범위를 다중 인스턴스·운영 저장 상한 검증으로 확대하지 않는다.

### 전체 제품 회귀

깨끗한 `d4aa22c`의 이미지로 2026-10-26 20:35:30~20:41:13 UTC 실행했다. [전체 제품 근거](../poc/evidence/retention-2026-10-27/product-regression.json)에 JAR/image/class hash·고정 계획·최종 대조·group별 추세·정리를 기록했다.

```sh
./scripts/deploy-images work/retention-63/images.json
./scripts/product-load work/retention-63/images.json work/retention-63/product-plan.json work/retention-63/product-operating100
```

- 100채널·총 500건/s·API 10요청/s·1KiB·80% 편향, 30초 준비+270초 측정이다. S1/P3·group별 consumer 1명·RF1·기존 BASELINE 자원을 유지했다.
- 고유 150,000/물리 151,500·관측 1,000·outbox 10을 전체 projection/관측/알림 oracle과 대조했다. 입력 300.007초·drain 0.061초, 최종 24개 lag 0, 판정 WITHIN_SLO다. 생성/측정/SLO 오류는 0개다.
- steady 순위 가시성 p95 236ms, API p95 62ms다. category/report 채팅 lag 기울기는 −0.014/−0.030건/s이며 관측된 미도달 watermark 나이 초과가 없었다. 앞선 #65와의 단회 수치 차이를 성능 개선율로 사용하지 않는다.
- 입력 약 118초에 `app_runtime`으로 작은 진단 SELECT를 한 번 추가했다. 정리 9종 모두 SUCCEEDED, 실패 누계 0, ranking/trend 하한 존재를 확인했다. fresh DB여서 이 표본의 삭제량은 0이다. aged 데이터 삭제를 포함한 전체 제품의 지속 용량 검증은 아니다.
- 소유 container·volume·network·생성 비밀을 정리했고 기존 container 8개 ID를 보존했다. 공개 원천·외부 배포·다중 AZ를 실행하지 않았다.

최종 HEAD Claude 승인과 CI 성공은 별도 병합 게이트로 남긴다. GitHub Actions의 최초 push 40395092953/PR 40395103827은 계정 결제 또는 사용 한도 때문에 runner가 시작되지 않았다. 코드 테스트 실패로 집계하지 않고 최종 CI 성공 게이트를 유지한다.
