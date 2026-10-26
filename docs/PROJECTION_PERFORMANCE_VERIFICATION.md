# 카테고리·리포트 비용 개선 검증

2026-10-27 KST · [#65](https://github.com/joejaeyoung/jigeumini/issues/65) · F05/F06/F10 및 F01/F13/F14 → T02/T07/T08/T11/T12/T14, F16 → T16.

## 변경과 유지한 계약

카테고리의 과거 시점 관측 결합과 INSERT를 합치고, 진행 위치는 배치 안의 topic/partition별 최대 offset+1을 한 번 저장한다. 리포트는 새 관측으로 기존 기록을 보정한 뒤 새 채팅을 바로 귀속한다. 채팅만 있는 배치에서는 세션 전체 이력을 재구성하지 않고 TTL 상태를 갱신한다. V13은 세션별 최신 관측을 위한 `(run_id,report_id,observed_at DESC,event_id DESC)` 색인으로 기존 두 열 색인을 대체한다.

동일 UUID의 멱등성·payload 충돌·늦은 관측 보정·채널 잠금 순서·품질·bucket delta·revision·진행 offset 트랜잭션을 유지했다. 만료 세션의 부분 삭제/재사용 방어와 별칭 계약도 유지한다. 영구 미래 입력 오류를 세션 재구성 전에 판정해 일시 재시도 오류로 가리지 않는다. Kafka 배정/파티션 수/소비 동시성/DB 풀 크기는 같은 조건이다.

## 측정 경계와 재현

- 수정 전 `d20a29a`, 첫 수정 `8a0fb76`, 색인·검증 보완 후 `7998bed`, 연결 진단을 추가한 `0cfa701`과 경합 보완 버전의 깨끗한 commit 이미지로 실행했다. 각 실행의 실제 commit·main/test tree·JAR·이미지·fixture hash·명령·시각은 [정제 근거](poc/evidence/projection-performance-2026-10-27/summary.json)에 있다. 마지막 문서 HEAD에서 과거 시험을 실행했다고 취급하지 않는다.
- 단일 호스트·P3/RF1·fresh DB·합성 WebSocket 원천·동시 API 7경로 혼합이다. 분석기/API JVM·DB·Kafka·probe 각각 1 CPU, API/probe heap384MiB가 기본이다. 정확한 제한과 별도 원천 자원은 [용량 기준](CAPACITY_VERIFICATION.md)을 따른다. CPU 대조는 표에 표시한 역할만 2 CPU다. API_2_CPU는 HTTP만이 아니라 같은 JVM의 여섯 소비 group·작업도 포함한다. partition과 consumer 수는 늘리지 않는다.
- Docker VM은 8 CPU·약7.65GiB다. 기존8개 container와 미리보기를 유지했으며 VM 전체/기존 workload 간섭을 통제하지 않았다. 실험끼리는 직렬이다.
- FULL은 주기 COUNT와 container의 cpu.stat/du를 포함한다. NO_COUNTS, NO_COUNTS_OR_DU도 최종 모든 projection 대조와 24개 offset 검사는 유지한다. 두 모드에서 COUNT를 생략했다는 표본을 남긴다.
- 실제 probe/API GC는 Serial(Copy/MarkSweepCompact)이다. SQL 서버 시간은 JDBC/pool/transaction 전체 시간이 아니다. 최대40 frame의 platform thread 순간 표본은 누적 대기 시간이 아니며 깊은 stack·virtual thread는 놓칠 수 있다.
- 양방향 시계 잔차의 절댓값250ms 초과/누락을 용량 판정에서 제외한다. 가시성은 성공한 표본만의 조건부 p95와 전체 선택 표본을 구분한다. 미관측이 하나라도 있으면 전체 선택 p95는 null이다. 원본 판정과 현재 재분석을 함께 보존한다.

```sh
# 해당 실행 commit의 깨끗한 checkout에서 새 manifest/output을 사용한다.
./scripts/deploy-images work/new-projection-images.json
./scripts/product-load work/new-projection-images.json work/plan.json work/new-projection-run
python3 -B scripts/projection-summary.py work/projection-performance-65 work/new-projection-summary.json
```

표의 채팅률은 전체 채널 합계다. `warmup`을 제외한 SLO와 최종 정합성을 따로 판정한다. 저부하 채널 수를 인기 방송 수용량이나 운영 최대치로 환산하지 않는다.

## 기준선과 중단 기록

최초 `baseline-rate2500`(`129a46d`)은 발행 버퍼 거절252와 +2.044초 시계 이동으로 입력을 마치지 못해 용량 대조에서 제외했다. accepted=ACK56,055·peakPending1,000이며 제품 process는 살아 있었다. 과거600채널 반복의 -2.045초 이동도 원인이 미확정이다. 호스트 시각을 보정해 통과 처리하지 않았다.

정상 시계의 `d20a29a` 기준선은 다음과 같다. 균등 입력은20채널·준비1,000/s30초+측정2,500/s60초·API10/s, hot은 측정2,000/s와 한 채널80%다.

| 실행 | 계측 | 결과 | category lag 기울기 | category watermark |
|---|---|---|---:|---:|
| before-full | FULL | SLO 초과 | +398.598건/s | 11.216초 |
| before-no-counts | NO_COUNTS | SLO 초과 | +538.548건/s | 13.132초 |
| before-diagnostic | NO_COUNTS+진단 | SLO 초과 | +315.343건/s | 10.168초 |
| before-no-counts-or-du-serial | NO_COUNTS_OR_DU | SLO 초과 | +356.032건/s | 10.174초 |
| before-hot-serial | NO_COUNTS+진단·hot2,000/s | 통과 | +0.785건/s | 0초 |

앞의 미완료129a46d 한 회와 위5회를 합쳐 역사적 [baseline.json](poc/evidence/projection-performance-2026-10-27/baseline.json)의6회다. 진단 추가 직후의 “기준선3회” 표현은 FULL/NO_COUNTS/진단 세 회만 가리켰다. 최종 근거에는 COUNT/du 대조와 hot 대조를 함께 구분한다.

COUNT·du를 제거해도 균등 실패가 남았다. 계측 비용이0이라는 결론은 아니다. hot2,000/s는 이번 수정 전에도 통과해 과거#62 실패를 확정 최대치로 쓰지 않는다. 잘못 겹친 `before-hot`/`before-no-counts-or-du` 두 회는 중단·비교 제외했고 원본 hash와 정리 보완을 보존했다. 원래8개 container는 유지했으며 이후 wrapper에 배타 잠금을 추가했다.

## SQL 비용과 색인

같은 NO_COUNTS+진단의 `before-diagnostic`과 `reviewed-diagnostic`은 각각 고유180,000/물리181,800개를 대조했다. 개별 statement 식별에는 정규화 template hash를 사용하며 query_id를 다른 DB 실행의 전역 식별자로 쓰지 않는다. 다음 두 행은 개별 statement다.

| statement | 수정 전 | 7998bed | 서버 실행 시간 |
|---|---:|---:|---:|
| category 진행 UPSERT, hash 8bbf20f2… | 181,860회 | 6,653회 | 2,448.397 → 92.458ms |
| 매 채팅 배치의 report 재귀속 CTE, hash 757294bc… | 20,216회 | 0회 | 18,832.301 → 0ms |

관측에 따른 report 보정 CTE `e40afe00…`는60회 남는다. 여러 statement를 묶은 family 집계는 다음과 같으며, 위 개별 statement 횟수와 다르다.

| family/operation | 호출 수 전→후 | 서버 실행 시간 전→후 |
|---|---:|---:|
| CATEGORY_PROGRESS/INSERT | 181,986 → 6,779 | 2,452.481 → 95.638ms |
| CATEGORY_OBSERVATION/SELECT | 182,115 → 299 | 1,412.605 → 5.575ms |
| REPORT_EVENT/CTE | 20,276 → 60 | 18,855.109 → 0.766ms |
| REPORT_EVENT/INSERT | 181,800 → 181,800 | 4,010.908 → 5,823.448ms |
| REPORT_SESSION/UPDATE | 20,276 → 34,240 | 177.899 → 334.387ms |

리포트 INSERT와 상태 UPDATE 비용은 늘었다. CTE와 INSERT의 합계 서버 시간은 약22.87초에서5.82초로 줄었지만 모든 transaction/JDBC/API 지연의 감소율을 뜻하지 않는다. 호출 수에는 probe·최종 oracle이 포함되고 SQL 값/원문은 남기지 않는다. 기존 CTE의 planner/JIT 내부가 원인이라고 확정하지 않았다.

SQL 순간 표본의 가장 오래된 transaction은 수정 전3,644.171ms, 색인 보완 후28.949ms였다. 두 실행 모두 풀 획득 위치가 관측됐으며 pg_stat_activity의 Lock 대기는 표본에서 발견하지 못했다. 최대40 frame·5초 간격 표본이므로 누적 잠금/풀 대기 시간이나 그 부재를 입증하지 않는다. 배치 진행 저장·불필요한 재귀속 호출 감소는 SQL 횟수와 코드 경로로 확인했고, 처리량 전체의 원인을 DB CPU 한 가지로 확정하지 않는다.

[색인 대조](poc/evidence/projection-performance-2026-10-27/report-index.json)는7일/30초 간격의 합성20,160행을 TEMP table에 넣은 별도 질의다. 정렬 색인 전 전체20,160행 scan/top-N sort(2.313ms)가 색인 후1행 index scan(0.022ms)으로 바뀌었다. 작은 local buffer 실험으로 제품 처리량 배수를 계산하지 않는다. 실제 장기 DB의 색인 생성 잠금/시간은 미검증이다.

## 고정 자원 부하 결과

### 처리량·계측·CPU 대조

| 실행·버전 | 입력 조건 | 판정 | category/report 최대 watermark(초) | 순위/API p95(ms) |
|---|---|---|---:|---:|
| after-diagnostic `8a0fb76` | 20채널·2500건/s·60초 | 통과 | 0/0 | 310/68 |
| after-full `8a0fb76` | 20채널·2500건/s·60초 | SLO 초과 | 2.07/3.078 | 964/146 |
| after-full-repeat `8a0fb76` | 20채널·2500건/s·60초 | 통과 | 1.007/1.031 | 347/68 |
| reviewed-full `7998bed` | 20채널·2500건/s·60초 | 통과 | 2.069/3.079 | 1224/126 |
| reviewed-diagnostic `7998bed` | 20채널·2500건/s·60초 | 통과 | 0/0 | 293/62 |
| final-full2500 `e4319e9` | 20채널·2500건/s·60초 | 최종 대조 미완료 | 미확정/미확정 | 미확정/미확정 |
| final-full2500-repeat `e4319e9` | 20채널·2500건/s·60초 | 통과 | 1.012/1.043 | 457/147 |
| after-no-counts `e4319e9` | 20채널·2500건/s·60초 | 통과 | 2.021/2.022 | 1050/98 |
| after-hot `8a0fb76` | 20채널·2000건/s·60초·80%편향 | 통과 | 0/0 | 268/56 |
| after-rate2000-soak `e4319e9` | 20채널·2000건/s·120초 | 통과 | 2.016/3.024 | 808/119 |
| reviewed-rate3000 `e4319e9` | 20채널·3000건/s·60초 | SLO 초과 | 5.07/13.14 | 3635/162 |
| after-rate3000-api2 `e4319e9` | 20채널·3000건/s·60초 | SLO 초과 | 1.013/5.056 | 465/125 |
| after-rate3000-kafka2 `e4319e9` | 20채널·3000건/s·60초 | SLO 초과 | 2.026/6.064 | 1479/142 |
| after-rate3000-db2 `e4319e9` | 20채널·3000건/s·60초 | 통과 | 1.069/2.02 | 1092/84 |
| after-hot3000 `e4319e9` | 20채널·3000건/s·60초·80%편향 | 통과 | 0/1.009 | 383/99 |

### 채널 수 저부하 대조

| 실행·버전 | 입력 조건 | 판정 | category/report 최대 watermark(초) | 순위/API p95(ms) |
|---|---|---|---:|---:|
| after-channels450-soak `7998bed` | 450채널·100건/s·270초 | 통과 | 0/0 | 228/57 |
| after-channels600 `7998bed` | 600채널·100건/s·60초 | 통과 | 0/0 | 268/50 |
| after-channels600-repeat `7998bed` | 600채널·100건/s·60초 | 통과 | 0/0 | 275/44 |
| after-channels600-soak `7998bed` | 600채널·100건/s·270초 | 통과 | 0/0 | 214/64 |
| after-channels1000 `0cfa701` | 1000채널·100건/s·60초 | 실행 기준 실패 | 30.425/29.418 | 388/197 |

### 복합 입력 대조

| 실행·버전 | 입력 조건 | 판정 | category/report 최대 watermark(초) | 순위/API p95(ms) |
|---|---|---|---:|---:|
| after-operating100 `7998bed` | 100채널·500건/s·270초·80%편향 | 통과 | 0/0 | 229/53 |
| after-operating100-rate1000 `7998bed` | 100채널·1000건/s·240초·80%편향 | 최종 대조 미완료 | 0/0 | 221/67 |
| after-operating100-rate1000-diag `e4319e9` | 100채널·1000건/s·240초·80%편향 | 통과 | 1.006/1.006 | 237/69 |

표의 시간은 warmup 뒤 측정 구간이다. watermark는 해당 group의 채팅·관측 토픽 중 최댓값이다. 순위 p95는 모든 선택 표본을 관측했을 때만 표시하며, 세부 미관측 건수·partition별 값·준비 구간은 JSON에 보존한다. DIRECT 기능 시험은 아래에서 별도 해석한다.

### 판정의 범위

동일 NO_COUNTS+진단의 수정 전/색인 보완 후 2,500건/s 비교에서 category 채팅 lag 기울기는 +315.343→-0.453건/s, 최대 watermark는10.168→0초였다. 개선 후에도 계측 조건·실행에 따른 변동이 있다. 아래 해석은 채팅 토픽 수치이며 표의 watermark는 채팅/관측 중 최댓값이다.

- 2,500건/s FULL은 `8a0fb76`의 최초 SLO 초과와 반복 통과, `7998bed`의 통과 모두를 보존했다. 후자의 category/report 채팅 기울기 +41.245/+67.600건/s도 남는다. 최종 `e4319e9` FULL 반복은 -8.833/-9.452건/s·watermark1.012/1.043초·drain0.170초였다. 단회 개선이나 짧은 통과를 안정 최대치로 채택하지 않는다.
- 같은 `e4319e9`, 20채널·2,500건/s·API10·진단false에서 FULL과 NO_COUNTS를 연속 대조했다. FULL의 순위/API p95는457/147ms, NO_COUNTS는1,050/98ms였다. NO_COUNTS의 category/report 채팅 기울기 +35.713/+70.106건/s·drain1.253초로, COUNT를 끄면 반드시 좋아진다는 근거가 없다. 모드별1회·공유 VM 변동·나머지 계측 비용이 남으며 “COUNT 비용0”으로 해석하지 않는다.
- 2,000건/s FULL 120초는 형식 통과이나 category/report 채팅 기울기 +24.991/+38.427건/s, 최대lag5,054/5,678이 남았다. 3,000건/s 기본은 +299.571/+696.858건/s로 초과했다. 분석기/API JVM2CPU와 Kafka2CPU도 lag 증가 기준을 넘었다. DB2CPU 단회는 형식 통과이나 +13.574/+31.212건/s였다. consumer 병렬화나 operating SKU 확정은 별도다.
- 80%단일채널 편향3,000건/s도60초에서 고유210,000/물리212,100·전체 대조·최종24개lag0·형식SLO 통과다. 채팅 category/report 기울기+3.275/+14.905건/s, 순위/API p95 383/99ms, source 최대6.766ms, 시계 잔차 최대95.766ms로250ms 기준 이내였다. 단순 bucket 샤딩과 달리 기존 S1/P3·group당1consumer의 전체 제품 결과이며, 더 긴 실행이나 S3 이득을 입증하지 않는다.
- 600채널은 총100건/s의 두 단기·5분 모두 통과했고 450채널5분도 통과했다. 1,000채널은 수신9,000/ACK9,090·최종 전체 대조/24개lag0이지만 readiness503 세 번으로 실행 기준 실패다. 부하 API600개는 전부200이었다. report 최대 watermark는 채팅28.410초/관측29.418초이며 category 채팅30.425초다. 입력은 완료됐으므로 연결 실패로 분류하지 않는다.
- 이 예산에서 검증한 저부하 하한은600채널이다. 실패1,000 사이의 정확한 경계는 미확정이며 700~900 중간 단계 반복과 더 긴 실행·실제 원천/fleet·장기 DB에서 재개한다. 채널 수만으로 worker 대수나 인기 방송 수용량을 확정하지 않는다.
- 100채널500/s·API10·1KiB·80%편향은5분 재통과했고, API10을 유지한1,000/s는30초 준비+240초 측정에서 순위/API p95 237/69ms, 고유255,000/물리257,550·최종24개lag0이었다. source 최대지연40.258ms·category/report 채팅 기울기 +0.600/+0.582건/s다. 앞선 준비 오류 실행은 제외하고 새 실행으로 따로 기록한다. 1,000/s는단회4분이며 초기 운영 비교는 반복 근거가 있는100채널500/s 후보를 유지한다.

### DIRECT 저율의 입력 스케줄 대조

같은20채널·20건/s·60초·API부하0·FULL+진단으로 정상/분석기 STOP을 각각 실행했다. 둘 다 고유1,200/물리1,212·모든 projection 대조·최종24개lag0·소유 자원 정리를 통과했다. 정상 실행의 형식 판정은 WITHIN_SLO이고, 장애 실행은 `FAULT_RUN_IS_NOT_STEADY_CAPACITY`로 용량 표에서 제외한다.

| 실행 | 최대 생성 지연·시점 | 첫 관측 ACK / outbox | 30초 관측 ACK / outbox |
|---|---:|---:|---:|
| control | 849.097ms·예정0초 tick0 | 775.870 / 6.491ms | 7.711 / 2.009ms |
| STOP | 703.969ms·예정0초 tick0 | 697.403 / 3.318ms | 8.280 / 1.284ms |

STOP은 입력17.078초 표본 뒤 시작해20.935초 유지했다. 두 실행의 최대 지연은 최초 관측 ACK와 outbox를 같은 입력 thread에서 기다린 직후 첫 tick이며 장애 전이다. 정상 실행은 side-input 전66.153ms도 포함한다. 따라서 이번 최대 지연은 STOP이 있어야 발생하는 현상이 아니며, 순차 side-input 경로의 초기 ACK 대기가 대부분을 차지했다. ACK 지연의 Kafka 초기 metadata/JIT/네트워크 내부 원인은 분리하지 않았다. 과거805.7ms 실행에는 발생 tick/side-input 시각이 없어 같은 원인으로 확정하지 않는다.

장애 중 양수 lag21표본, CONT 뒤 고정 end 도달 표본상한1.921초·ready200 완료5.108초를 확인했다. 정상 구간API56응답200·정지 중timeout8이었다. 가시성 선택592개 중115개(19.43%)는10초 내 미관측이라 전체 선택p95는null이다. 관측477개만의 조건부p958,283ms를 전체p95나 운영 SLO로 쓰지 않는다. 회복 시간은 로컬 합성 단회이고 운영RTO가 아니다.


## 시계·계측·자원 해석

과거600채널 실패의 DB 최대 throttle 비율99.26%는 입력10.851~24.314초의 준비 구간이었다. 리포트 watermark35.618초의 최대는66.398초에 관측됐으며 그 시각을 포함한64.810~78.243초의 DB 구간 평균은0.352CPU·throttle0이었다. readiness503은51.821/57.038초였다. 최대 throttle 하나로 해당 장애 시점의 원인을 확정하지 않는다. [공통 시간축 대조](poc/evidence/projection-performance-2026-10-27/alignment.json)는 독립 host/probe 표본의 근사 정렬이며 원자 계측이 아니다.

수정 후600채널 두 단기 실행에서는 준비 이후 report watermark0, readiness 실패0, DB 최대 throttle비율3.01%/2.24%였다. 기존 시계 무효 반복은 별도 표시해 보존했다. 당시2초대 시계 이동의 원인은 미확정이며 새 양방향 측정과 유효 반복으로 비교 가능성을 확보했다.

1,000채널의 report 채팅 watermark 최대는59.322초 표본에 있었다. 이를 포함한56.605~70.170초 구간 DB 평균은0.620CPU·throttle48.15%, 분석기/API는0.430CPU·6.62%, Kafka는0.342CPU·25.00%, probe는0.290CPU·0.74%였다. probe 최대 GC 증가 표본은83ms다. 실행 전체 DB 최대 throttle100% 한 값으로 해당 실패 원인을 확정하지 않는다. 독립 표본의 근사 시간 정렬이며 callback 진행 자체는 계측하지 못했다.

| 실행 | probe heap/RSS(MiB) | thread/FD 최대 | 누적 GC(ms) | COUNT 최대(ms) |
|---|---:|---:|---:|---:|
| reviewed-full | 79.3/254.0 | 106/116 | 609 | 497.612 |
| reviewed-diagnostic | 88.7/259.4 | 104/115 | 627 | 주기 조회 생략 |
| after-channels450-soak | 114.9/395.6 | 957/545 | 396 | 83.094 |
| after-channels600 | 128.5/437.2 | 1255/695 | 293 | 75.204 |
| after-channels600-repeat | 125.9/434.0 | 1256/695 | 330 | 64.107 |
| after-channels600-soak | 138.4/452.5 | 1258/695 | 579 | 95.265 |
| after-channels1000 | 184.7/576.4 | 2059/1095 | 474 | 73.299 |
| after-operating100 | 103.4/298.2 | 254/194 | 758 | 93.213 |
| after-operating100-rate1000-diag | 133.7/330.1 | 265/197 | 1116 | 278.659 |

| 실행 | 분석기/API 평균CPU·최대 throttle비율 | DB 평균CPU·최대 throttle비율 | Kafka 평균CPU·최대 throttle비율 |
|---|---:|---:|---:|
| reviewed-rate3000 | 0.5603 core / 44.85% | 0.685 core / 78.68% | 0.4076 core / 31.11% |
| after-rate3000-api2 | 0.6464 core / 9.63% | 0.7739 core / 88.97% | 0.4343 core / 33.09% |
| after-rate3000-kafka2 | 0.6568 core / 57.35% | 0.7482 core / 94.81% | 0.4656 core / 11.11% |
| after-rate3000-db2 | 0.6971 core / 62.5% | 0.8127 core / 7.41% | 0.4563 core / 34.81% |

CPU 평균은 준비·배출을 포함한 표본 구간 평균들의 산술평균이다. 최대 throttle 비율은 그 구간 중 최댓값이며 wall time 비율이나 실패 시각의 부하를 뜻하지 않는다. 각 역할만 2 CPU로 바꾼 단회 비교이며 공유 VM 전체 사용량은 미계측이다.


시험 코드에는 채널당 수집 실행 worker와 heartbeat scheduler가 각각 하나 있다. 소스 HTTP 어댑터는 채널들에 공유하고 WebSocket IO도 공용 수명을 사용한다. 600채널 첫 실행의 전체 thread peak1,255개는 이 구조와 다른 probe/API/측정 thread가 합쳐진 값이며 개별 component의 누적 CPU 비용은 분리하지 못했다.

JVM heap/RSS/thread 표본은 collector 외에 oracle·API 부하·측정을 포함한다. GC 이전 peak를 생존 heap으로 해석하지 않는다. scheduler/HTTP/WS 수명과 공유 VM 경합을 합친 값이므로 #64 독립G1/Xmx64 실험의 연결당 계수로 환산하지 않는다. CPU 대조도 평균·몇 초 표본이며 운영 cloud 성능이나 단독 자원의 최대치를 입증하지 않는다.

## 연결 실패 진단과 실패 실행 해석

7998bed의 `after-operating100-rate1000`은 입력 전 WS_CLOSED 1회가 있었고 재접속 뒤 수신255,000/ACK257,550을 마쳤으나 마지막 collector health에서 실패했다. 최종DB대조는 수행하지 못해 처리량 통과나 한계로 사용하지 않는다. 시작 오류를 입력 전에 거부하고 close 숫자코드·CONNECT/APPROVAL 대기 종류를 기록하도록 보완했다. 처음 소켓이 닫힌 원인은 미확정이다.

별도 회귀에서 timeout 뒤 cleanup의 close callback이 기존 실패를 덮는 결함을 재현했다. 후속 Spec리뷰는 timeout 처리와 transport callback의 경쟁에서 예외·observer 원인이 달라지는 경우를 발견했다. 다른 스레드가 진단 보고 중 WS_TRANSPORT를 먼저 확정하도록 한 회귀는 수정 전 실패했다. `fail`이 최초의 원자 완료 값을 반환하고 ready·예외·observer가 이를 공유하도록 보완했다. [cleanup 재현](poc/evidence/projection-performance-2026-10-27/handshake-reproduction.json)과 [경합 재현](poc/evidence/projection-performance-2026-10-27/handshake-race.json)은 위 실제소켓종료의 원인을 증명하지 않는다.

`e4319e9`의 `final-full2500`도 승인 단계에서20개 중19개만 준비돼 `APPROVAL_TIMEOUT`1회로 **입력 전에** 종료했다. 원천은 READ를 처리한 연결20개·수집 frame38개를 기록했고 원천 protocol/socket 오류·OOM은 없었다. 입력0이며 처리량 경계로 쓰지 않는다. 시작 전 오류 거부가 동작했으므로 재접속 뒤 오류를 숨긴 실행을 통과로 채택하지 않았다.

같은 제품 코드의 별도 호스트 loopback에서20채널×100회, 승인2,000/프레임4,000·대기 실패0을 확인했다. 1 CPU JVM processor hint이며 컨테이너 quota 실험이 아니다. 이 결과는 미재현이고 처음 실패의 해결 근거가 아니다. [승인 전용 정제 근거](poc/evidence/projection-performance-2026-10-27/startup-probe.json).

[공식 소스 조사](research/SYNTHETIC_APPROVAL_DELAY.md)에서 합성 원천 Java-WebSocket1.6.0의 쓰기 큐 등록과 selector 관심 덮어쓰기 사이에 가능한 경합을 찾았다. 실패 시 큐·interestOps·실제 쓰기 표본이 없어 이번 현상의 원인으로 확정하지 않는다. Spring/Tomcat handler 등록 순서는 READ 선송신 가설과 맞지 않았다. timeout 확대·heartbeat 추가로 통과시키지 않았다. 다시 발생하면 승인 대기 중 숫자 큐/쓰기 진행·수신 단계 관측으로 재개한다.

## 처리 계약·복구·리팩토링 확인

색인 보완 `7998bed`의 push40372282990/PR40372291574는 Java단위176·실제Kafka/PostgreSQL142개를 통과했다. 연결 경합까지 보완한 `e4319e9`의 push40380847007/PR40380852486 양쪽은 단위179·실제142·하네스38·웹29·Chromium31, 실패/오류/skip0을 XML과 job log로 확인했다. 로컬 `./scripts/check`도 단위179·하네스38·실행JAR를 통과했다. [구현 게이트](poc/evidence/projection-performance-2026-10-27/implementation-gates.json). 최종 문서HEAD의 Claude와 push/PR CI는[PR71](https://github.com/joejaeyoung/jigeumini/pull/71)의 별도 병합 게이트다.

새 계약2개는 개선 전후 모두 통과했다. 후속 TTL60/120/300초의 정확한 만료 경계, 별칭 후 채팅, SOURCE_CHANGED/OFFLINE_OBSERVED 종료 유지와 미래 입력 오류의 우선 판정을 실제 DB로 추가 검증했다.

제품V13 이후 시험 migrationV14를 생성해 [16단계 복구 리허설](poc/evidence/projection-performance-2026-10-27/migration.json)을 통과했다. 중단/실패 시 DDL·backfill rollback, V13 이미지의 V14 strict VALIDATE 거부, 새 DB에V13 복원 후6개 새group 재정렬·보존 입력10건 재생·선택 snapshot차이0·기존group 불변을 확인했다. 복구 후웹8경로/리포트20건·API26응답200, page error0·retry 복구도 확인했다. catchup25.210초는 로컬 합성 실행의 값이며 운영RTO가 아니다. 이 시험은 V14 fixture의 복원 절차이고 운영 크기의 V12→V13 색인 생성 시간을 측정한 것은 아니다.

파티션 사전 조건은 제품 초기화의 KafkaTopicPolicy.verify가 정확한P3을 검사한다. 합성 원천 서비스는 extends 없이 선언돼 비밀 환경을 상속하지 않으며, 실행 후 inspect의 secret mount/PIPELINE 환경0을 확인한다. 시작 전 resolved Compose 전체 검사 추가는 검토했지만 현재 직접 선언과 제품 검사·실행 후 검사에 별도 중복 검사를 늘리지 않았다.

동적 시험 migration 번호는 현재 제품 버전+1을 사용하되 정확한 이미지ID/revision/buildId와 이전 schema 목록을 검증한다. schema 목록 확인은 비밀 생성 전에 한다. 정제기는 미완료·중단 실행을 숨기지 않고 원본 hash·현재 분석기/정제 helper hash를 남긴다.

카테고리의 배치 진행 저장, 리포트의 관측 보정/채팅 귀속/TTL 갱신 책임을 검토했다. 별도 범용 계층을 늘리지 않고 기존 store의 transaction 안에 두었다. Standards/Spec의 초기 구현·색인 보완 검토는 지적0건이었다. 연결 진단 후 Spec P2의 실패확정 경합은 회귀와 재리뷰로 해결했다. Standards의 비차단 지적은1,000채널 watermark에 채팅/관측 토픽 범위를 명시해 반영했다. Claude8a0fb76 구현 APPROVE는 최종HEAD 승인으로 사용하지 않는다.

장기 보존·DB 크기는#63, 전체 제품 샤딩 판단은#68, 실제 원천/fleet·RF3/다중 호스트/AZ·외부 운영은#47/#48/#49에 남는다. 단기 합성 검증으로 운영 사양/비용/가용성 전체를 확정하지 않는다. 진행 상태는[STATUS](STATUS.md)를 따른다.
