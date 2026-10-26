# 전체 제품 집계·조회 부하 검증 #43

2026-10-26 · F03/F04/F05/F06/F07/F10/F14 → T02/T07/T08/T11/T12.

## 실행 범위와 입력

실제 배포 이미지의 Spring 분석기와 PostgreSQL·Kafka·웹을 소유한 단일 호스트 Compose에 띄웠다. ranking/trend/관측/category/report/알림의 consumer group6개, topic/partition24개 위치를 조회하고 API8경로를 함께 호출한다. 공개 채팅·인증·외부 배포는 사용하지 않는다. collector 역할은 실행하지 않았다.

API 계측은 단일 클라이언트가 5초 고정 지연마다 8경로를 순차 호출하는 가벼운 probe다(명목 약1.6요청/s, 실제 주기는 질의 소요만큼 길다). `http://api:8080`을 직접 조회해 nginx/web proxy를 거치지 않는다. status·ranking·spike·category는 전역 경로, timeline·report·notification은 한 고정 채널 대상이며 readyz는 준비 상태 경로다. 동시 사용자 조회 부하나 API 최대 처리량 시험이 아니다.

분석기에는 실험용 JMX 원격 옵션(인증/TLS 없음, 소유 내부망·호스트 미공개)과 별도 healthcheck를 overlay했다. 운영 이미지 기본 설정과 다른 계측 조건을 기록한다.

seed20261026·합성 채널20·category3개, 고유 chat100건마다 같은 event UUID/payload를 새 Kafka offset으로1회 추가한다. 균등 배정에서는 ordinal%100==0이 채널 index19에만 해당하여 중복900건이 한 채널에 집중된다(soak). hot 배정은 다르므로 균등한 중복 분포 시험으로 취급하지 않는다. 생성기는 예상 ledger UUID와 bucket별 count, 관측/분류/보고서 귀속, outbox/feed ID를 소비 구현과 별도로 누적한다. 기대 hash는 제품 EventCodec.hash를 재사용하므로 해시 알고리즘 자체의 독립 검증은 아니다. final drain 후 제품 DB와 전부 대조한다. accepted/ACK는 물리 발행 수, ledger는 고유 업무 수다.

채널별 관측은30초마다 갱신하고 TTL120초를 사용한다. 수집 품질 telemetry도 합성 connected/alive/flush 입력이다. outbox는30초마다 합성 SQL로 생성하므로 실제 급증 탐지 정확도 검증이 아니다. 실제 Outbox 발행·알림 소비와 최종 건수는 대조한다. 중복 입력 주입은 chat ledger에 한정하며 알림 중복은 주입하지 않았다.

| profile | 실행 revision | 입력 | 고유/물리 건수 | 결과 |
|---|---|---|---|---|
| uniform | f9a91f6 | 100건/s·120초·균등 | 12,000/12,120 | 정합성·계측·정리 통과 |
| hot | f9a91f6 | 100건/s·120초·80% 한 채널 | 12,000/12,120 | 정합성·계측·정리 통과 |
| steps | f9a91f6 | 50/100/200건/s 각60초 | 21,000/21,210 | 정합성·계측·정리 통과 |
| smoke | 03eb75f | 20건/s·60초·분석기 STOP/CONT | 1,200/1,212 | 정합성·장애/복구 계측·정리 통과 |
| soak | 03eb75f | 50건/s·1800초·분석기 STOP/CONT | 90,000/90,900 | 정합성·장애/복구 계측·정리 통과 |

각 profile은 단회다. f9a91f6의 세 정상 profile을 이후 버전으로 실행했다고 표기하지 않는다. 03eb75f는 장애 후 API 복구 구간 분류와 부분 samples JSONL, host/probe 시계 계측을 보강한 버전이다.

## 계측 의미

- 독립1초 offset 계측은 각 group의 commit을 먼저, topic end를 나중에 읽는다. 동시에 읽은 snapshot이 아니므로 lag는 관측 구간의 상한 성격이다. 각 partition의 실제 표본 시간 차로 입력/commit 처리량을 계산한다. 한 주기에는 group6개 commit 조회와 end/begin 조회의 논리 Admin API 호출8회가 단일 broker에 추가된다. 실제 네트워크 요청 수는 내부 batching·재시도에 따라 다를 수 있다. partitions.sampledByPhase는 생성기 phase별 전체 분포이며 soak의 STOP·catch-up도 포함한다. p99/max lag와 최대 commit 처리율을 정상 상태 값으로 해석하지 않는다.
- API/DB count query·JMX heap·생성기 RSS는5초 지연 주기, 컨테이너 CPU·docker stats 메모리 사용량과 host 여유 디스크는 별도 표본이다. 분석기 RSS는 별도로 /proc/1/status의 VmRSS를 읽는다. 누락/실패 표본도 보존한다. API latency는8경로별로 정상/STOP/복구 구간을 구분한다.
- `receivedToTransactionTimestampMillis`는 receivedAt부터 PostgreSQL transaction 시작 timestamp까지다. DB commit 완료 시각이나 네트워크 전체 지연이 아니다. 전체 실행의 분포이며 정상/장애별로 분해하지 않았다.
- JVM heap used는 GC 이후 생존 객체량이 아니다. RSS와 heap의 유한30분 표본은 메모리 누수 부재나 장기 SLA를 증명하지 않는다.
- API는 응답 status/지연을 측정한다. 응답 본문 전체 정합성·UI 사용자 흐름은 이 부하 실험의 범위가 아니다. 최종 업무 대조는 DB의 ledger/hash/bucket/귀속/알림으로 수행한다. 순위 순서와 증가율 산출값을 독립 기대값과 대조한 시험은 아니다.

계측 통과 조건은 offset 완전 표본90% 이상과 입력초수의80% 이상 표본, 정상 DB/JMX/RSS/API 기록90% 이상, 정상10초 window의90% 이상에서 전체 계측, 정상 구간 양 끝을 포함한 최대 공백15초 이하다. 정상 API는 모두200이어야 한다. HTTP 오류를 결과에서 제외하지 않는다.

## 장애와 회복 판정

해당 실행 소유 API 컨테이너만 SIGSTOP하고 입력은 계속한다. 실제 STOP20~30초, STOP~CONT 사이 lag>0을 필수로 확인한다. CONT 후 최초 end offset24개를 고정한 뒤 해당 위치까지 commit했는지 판단한다. 계속 들어오는 미래 입력을 목표로 삼지 않는다. 이 목표 도달과 별개로 첫 ready200 표본을 기록하고, 둘 중 늦은 시점 뒤2초까지를 복구 구간으로 둔다. 회복 한도60초와 이후 정상 ready200이 필요하다.

host의 요청 전/응답 후 시각과 probe JVM 시각으로 START/RESUME의 차이 범위·왕복 불확실성을 기록한다. 불확실성1초 이하, 차이 범위가 허용구간±250ms와 겹치는지 확인한다. 정확한 clock offset이나 절대 오차250ms 이내라는 보장은 아니다.

## 단기 관측 결과

| profile | ranking 지연 p50/p95/p99 ms | trend 지연 p50/p95/p99 ms | 정상 API200 | 10초 계측 window |
|---|---|---|---|---|
| uniform | 9.029 / 26.757 / 53.151 | 9.021 / 26.705 / 40.100 | 192 | 12/12 |
| hot | 8.600 / 69.742 / 148.688 | 8.626 / 70.005 / 146.904 | 192 | 12/12 |
| steps | 8.640 / 31.513 / 34.029 | 8.621 / 31.550 / 34.496 | 288 | 18/18 |

uniform/hot의 동일 입력률 단회에서 hot의 tail 지연이 더 컸다. 반복 실험의 분산이나 순서 효과를 통제한 인과 비교로 확장하지 않는다. 단계200건/s도60초 관측이므로 지속 가능 최대 용량으로 제시하지 않는다. 표의 지연 정의는 위 transaction timestamp 기준이다.

## 30분 지속 부하 결과

03eb75f의 실제 입력은1800.007582초, 고유90,000·물리90,900건이다. 최종 accepted=ACK=90,900, rejected/unconfirmed/queued/inflight=0이다. ranking/trend/category/report의 ledger는각90,000건, bucket은각3,615행이며 독립 예상값의 ID/hash/count·귀속 대조가 일치했다. 관측1,200건, spike/outbox/feed60건도 일치하고 최종24개 위치의 lag는0이다.

분석기 STOP은20.303초였다. STOP 중 양수 lag20표본을 관측했다. CONT 이후 고정 end까지 commit 관측은2.245초, 첫 ready200 표본은5.091초였다. 전자는약1초 offset 주기, 후자는약5초 API 주기에 따른 관측 상한으로 정밀도가 다르다. 정상 API2,784회는모두200, STOP 중8회는HttpTimeoutException, 복구 구간8회는200이다. 장애 오류를 정상 통계에서 별도로 표시하며 전체 기록에서 삭제하지 않았다.

전체 실행의 ranking 지연은p50/p95/p99=7.938/64.850/3507.609ms, 최대21403.527ms, trend는7.931/64.913/3574.918ms, 최대21564.276ms다. 모두 전체 실행의 quantile이다. STOP 중 입력은 약1.1%로 p99·최댓값에 큰 영향을 줄 수 있지만, 시간별 지연을 분해하지 않았으므로 정상 구간 p50/p95 값으로 따로 표시하지 않는다. 낮은 입력률50/s인 soak의 p95가 uniform100/s의26.757ms보다 높았던 원인은 미규명이다. 테이블 증가·DB 유지 작업·계측 count query·Kafka CPU 한도 접근 등이 후보지만 원인은 미규명이다. 처리 지연의 시간별 분해 자료가 없고 실행 DB를 정리해 사후 분해할 수 없다. API/관측용 DB count의 시계열은 원본 samples.jsonl에 남아 있으며 제품 처리 지연과는 구분한다. 정합성과 장애 회복·유한 자원 관측의 PASS이며 지연 안정성이나 원인별 비용을 입증한 결과가 아니다. 정상 DB count query는348표본, p50/p95/p99=18.523/36.647/42.836ms다. 이는7개 관측용 count(*) 질의 묶음의 시간으로 제품 DB 트랜잭션 지연이 아니며, 이 계측도 공유 DB에 부하를 더한다.

독립 offset1,780표본은모두완전했고 정상 느린 계측348표본과 정상10초window176/176이 충족됐다. 정상 최대 계측 공백5.780초, 최대 offset 표본 간격1.078초다. 분석기 heap used 최대52,227,464byte·RSS303,341,568byte, 입력기 heap59,588,976byte·RSS229,638,144byte, 표본상 버퍼 나이 최대64ms를 기록했다. heap은350표본, 분석기 RSS는138표본이다.

아래 CPU 표는 soak에 한정한다. 단기 uniform/hot/steps의 CPU는 이 문서에서 해석하지 않으며 단기 지연 수치를 CPU 여유나 지속 가능한 용량으로 읽지 않는다. CPU는 약13초 간격의 docker stats 점 표본138개다. 각 서비스의 p95/p99/max(%)는 다음과 같다.

| 서비스 | p95 | p99 | 최대 |
|---|---|---|---|
| Kafka | 83.647 | 99.766 | 103.300 |
| PostgreSQL | 18.276 | 22.558 | 98.620 |
| 분석기 | 40.733 | 58.588 | 82.580 |
| 입력기 | 4.519 | 11.589 | 94.740 |

Kafka는 1 CPU 한도 부근의 표본이 있었지만 cgroup cpu.stat의 throttling과 STOP/처리 지연의 시간 상관은 수집하지 않았다. 50건/s를 지속 가능한 운영 용량으로 결론내리지 않는다. 입력기 pacing 지연 최대692.672ms도 원인 미분리다. 같은 probe cgroup에서 시계 bracket용 JVM을 기동하는 비용은 가능한 후보일 뿐 검증한 원인은 아니다.

시계 차이 범위는 START[-10,396]ms(불확실성406ms), RESUME[-9,165]ms(174ms)다. 입력 종료 후 모든 대조·정상 종료·이번 실행 container/volume/비밀 제거가 통과했다. 원본 hash·표본 수·분포는 [정제 근거 색인](poc/evidence/product-load-2026-10-26/summary.json)에 연결한다. [soak 상세](poc/evidence/product-load-2026-10-26/soak.json)의 files는 result/load/offsets/samples 원본 hash, partitions는 group별 분포, apiLatencyMillis는 경로·구간별 응답, minuteSamples는 분단위 분석기 heap 최대·입력기 RSS 최대·버퍼 나이이며 분석기 RSS의 분단위 값은 없다. memoryBytes는 전체 메모리 분포다. 입력기 RSS 상승(약176→230MB)은 예상값 맵과 표본 리스트 누적의 영향을 포함하므로 제품의 메모리 증가로 해석하지 않는다. [정제 스크립트](../scripts/product-load-summary.py)는 표본 분포를 (N-1)·p 위치 선형 보간으로 계산한다. transaction 지연 quantile은 probe SQL percentile_cont 결과를 복사한다. harnessRevision은 실행 이미지 revision, curatorSha256은 정제 스크립트의 hash이며 command는 정제 인자로 재구성한 재현 명령이다.

## 환경·재현·남은 범위

호스트 Darwin25.6 arm64·CPU15·RAM48GiB, Docker29.5.3 linux/aarch64·CPU8·메모리8,217,145,344byte다. 전용 벤치마크 호스트가 아니며 다른 기존 서비스는 그대로 두었다. 입력기/분석기는 각1CPU·컨테이너640MiB·JVM-Xms64m/-Xmx384m이고 Kafka는1CPU·768MiB, PostgreSQL은1CPU·384MiB다. probe class는 실행별로 복사하여 hash를 기록하고 이미지/JAR build identity를 확인한다. Kafka는RF1이며 HA/AZ/디스크 손실은 별도 이슈다.

```sh
./scripts/product-load <고정 이미지 manifest.json> uniform <새 결과 디렉터리>
./scripts/product-load <고정 이미지 manifest.json> hot <새 결과 디렉터리>
./scripts/product-load <고정 이미지 manifest.json> steps <새 결과 디렉터리>
./scripts/product-load <고정 이미지 manifest.json> soak <새 결과 디렉터리>
```

공유 근거에는 실제 commit·명령·이미지/class hash·시각·입력·정합 대조·표본 범위·정리 결과를 남긴다. 작업 디렉터리 원본은 gitignored이며 생성된 비밀과 소유 DB/Kafka volume은 실행 종료 시 제거한다. 공개 source, multi-host/AZ, 실제 alert/TLS/secret 회전, 운영 SLO와 최대 용량은 별도 검증이다.

## 실패·리뷰·리팩토링 이력

최초 smoke는 probe 컨테이너 소유 자원 조회 실패로 중단했고 cleanup을 확인했다. 이후 무부하0lag 오인과 느린 API에 따른 offset 계측 공백을 발견하여 독립 offset loop·CONT 후 고정 end를 도입했다. 정상 시계열 시간 공백 검사, 장애 후 readiness503의 복구 구간 분류, 부분 samples JSONL과 시계 불확실성 기록을 보완했다. 중간 uniform 중단 실행은 최종 성능 결과에 포함하지 않는다.

입력·기대값은 ProductLoadFixture, 실측은 ProductLoadMeasurements, 오케스트레이션/대조는 ProductLoadProbe, 결과 판정은 product_load_results.py로 책임을 나눴다. 실제 제품 집계 구현을 부하 전용 구현으로 대체하지 않았다. 초기 Claude 지적을 반영한03eb75f는 soak 진입 APPROVE였으며, 최종 근거 HEAD 승인과 push/PR CI는 별도 병합 게이트다.


## #62 하네스 변경과 기존 결과의 관계

2026-10-26의 [용량 경계](CAPACITY_VERIFICATION.md) 작업에서 DIRECT 입력의 같은 tick 안 `eventTime + i ms` 차이를 제거하고, 공통 계획 시각으로 단계를 결정하도록 바꿨다. telemetry 시작 순서와 200ms commit 가시성 질의도 추가됐다. 기존 #43의 고정 입력·원본·revision은 그대로 보존하며 새 하네스로 동일 비용을 재현했다고 해석하지 않는다. 정본 `smoke`를 `8dd13ce`에서 다시 실행해 STOP 20.539초·고정 end 회복 2.263초·고유 1,200건 대조·최종 lag 0과 정리를 확인했다. [추가 회귀 근거](poc/evidence/capacity-2026-10-26/review-regression.json)는 기존 #43 기록과 별도다.
