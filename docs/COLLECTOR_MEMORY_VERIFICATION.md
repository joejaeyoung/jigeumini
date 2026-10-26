# 수집 연결의 메모리·종료 자원 검증

2026-10-27 KST · [#64](https://github.com/joejaeyoung/jigeumini/issues/64) · F01/F13/F14 → T02/T08/T11/T12/T14.
진행 상태는 [STATUS](STATUS.md), 원인 조사와 고정 라이브러리 근거는 [조사 문서](research/COLLECTOR_TEXT_MEMORY.md)에 둔다.

## 변경과 판정

연결 시 미리 잡던1,048,576문자 버퍼를8,192문자 청크로 바꿨다. 완성 메시지의 **UTF-8 전체1MiB 허용량은 유지**한다. `IncomingText`가 전체 한도·surrogate 경계·완성/실패 초기화를 소유하고, `ChatSocketClient`가 소유 Tomcat container를 종료한다. 재접속 정책·READ/heartbeat·Kafka key/ACK/offset·DB 집계 계약은 그대로다. 실패 확정 직후 수신을 비활성화해 socket close 전 후속 메시지가 처리·발행되지 않게 했다.

기존180채널의 입력 전 heap 실패는 같은 계획에서 해소됐다. 연결 저부하의 새 통과/실패 범위는 아래 전체 제품 결과로 판단한다. 단순히 연결된 수를 인기 방송 처리 가능 수로 읽지 않는다. 제품의 기존100채널·500건/s·API10/s 비교 후보도 별도로 재검증한다.

## 환경과 재현 구분

- 전: 전체 제품 `5048d88`, 독립 메모리 하네스 추가 `f9b7713`. 둘의 제품 main tree는 같다.
- 후: `876bb68bf1236e8ab52d47db5438998424f44019`. 이후 문서/정제 변경과 측정한 제품 코드를 구분한다.
- 독립 시험: macOS arm64·Java21.0.11·Tomcat11.0.27, 새 JVM Xmx64MiB/G1. 합성 loopback READ/heartbeat만 실행하고 HTTP/TLS/Kafka/DB/API/oracle을 제외한다. GC 완료를 관측한 heap·FD·스레드·IO 등록 수를 기록한다. RSS는 이 시험에서 측정하지 않았다.
- 전체 제품: Docker VM8 CPU·8,217,145,344bytes. collector/probe1 CPU·640MiB·Xmx384MiB, 분석기1 CPU·640MiB·Xmx384MiB, PostgreSQL1 CPU·384MiB, Kafka1 CPU·768MiB·P3/RF1, 원천1 CPU·256MiB·Xmx128MiB. 원천은 별도 합성 프로세스이며 내부망만 사용한다.
- 전체 제품 probe는 실제 HTTP/ChannelProbe와 발행기 외에 oracle·API client·계측도 포함한다. RSS/heap을 순수 수집기 비용으로 나누지 않는다. 같은 총 채팅/API 부하에서도 채널 수에 비례하는 방송 관측 입력은 늘어난다.
- 180채널 전후의 계획·fixture class hash 집합·fixture library hash는 모두 같다. 로컬 무거운 빌드/시험/부하를 동시에 실행하지 않았지만 기존8개 container와 호스트 전체 부하를 통제한 전용 장비는 아니다.

## 작은 재현과 자원 수명

| Xmx64MiB 시험 | 수정 전 | 수정 후 |
|---|---|---|
| 32연결×2회 | 첫 연결 구간 JAVA_HEAP_SPACE·자식exit3, JSON 미완료 | 두 회차 READ/heartbeat 통과, 연결 시 heap 약12.7MiB |
| 8연결×3회 | 연결 시 heap 약33.8~33.9MiB | 연결 시 heap 약10.0MiB |
| 매 회차 종료 IO 등록 | 8→16→24 | 0→0→0 |
| 매 회차 종료 IO 스레드 | 16→16→16 | 0→0→0 |
| FD 시작→종료 | 99→104 | 99→101 |

8연결의 연결 중 heap과 같은 회차 종료 후 heap의 차이를8로 나눈 관측값은 전 약3.09MiB/연결, 후 약0.116~0.120MiB/연결이다. 객체·공유 자원·GC 효과를 포함한 차이이며 char 배열만의 크기나 운영 RSS 계수가 아니다. 배열의 정적 크기 모델은 약2MiB→16KiB다. 전체 live thread는 새 JVM 시작10개에서 종료 뒤27~29개로 남으므로 JVM 공유 carrier 등까지 모두 회수됐다고 주장하지 않는다. 확인한 것은 소유 연결·IO 등록·IO 스레드의 종료다. 독립 시험은 실제 GC 이름(G1)을 기록했지만 당시 제품 probe/API의 GC 종류·region 크기·전체 VM flags는 수집하지 않았다. 관측 3.09MiB와 정적 2MiB의 차이를 특정 GC 현상으로 확정하거나 제품에 환산하지 않는다. #65에서 실행 중 GC·flags와 채널별 스레드 비용을 함께 계측한다.

완성 메시지가 실제로 큰 동안에는 유한 조립 버퍼가 필요하다. 동시에 모든 채널에서1MiB가 도착하는 부하나 TLS 비용의 상한은 검증하지 않았다.

## 전체 제품 대조

연결 축은 균등 총100건/s·API10/s·합성 최소 payload, 준비30초+측정60초다. source 시작률50연결/s, 무장애, fresh DB다. 모든 통과/실패와 기존 heap 종료를 [정제 근거](poc/evidence/collector-memory-2026-10-27/summary.json)에 보존한다.

| 실행 | 입력 시간 | 결과 | probe heap/RSS 표본 최대 | 순위 반영/API p95 |
|---|---|---|---|---|
| 전180채널 | 입력 시작 전 종료 | JAVA_HEAP_SPACE·최고172연결, 계측 미완료 | 미관측 | 미관측 |
| 후180채널 | 90초 | 통과 | 53.4/265.3MiB | 275/48ms |
| 후300채널 | 90초 | 통과 | 75.0/311.5MiB | 277/49ms |
| 후450채널 | 90초 | 통과 | 94.9/369.3MiB | 254/47ms |
| 후450채널 지속 | 300초 | 통과 | 113.6/396.5MiB | 240/60ms |
| 후600채널 | 90초 | 리포트 지연·readiness 초과 | 124.1/428.7MiB | 229/87ms |
| 후600채널 반복 | 90초 | 시계 기준 불일치로 용량 판정 제외 | 129.7/432.9MiB | 271/47ms(유효 판정 아님) |
| 후100채널 복합 | 300초 | 통과 | 98.4/302.0MiB | 263/65ms |

마지막 행만 총500건/s·1KiB 합성 한글·80% 편향이며 API는10/s다. 기존 #62와 같은100채널 복합 계획의 새 실행은 고유150,000/물리151,500개를 대조하고 source clock 오류0·API 부하 실패/미발행0·최종24개 lag0으로 통과했다. source 예정→수신과 모든 projection/관측/알림 기대값도 검사했다. 새 표본 heap/RSS가 이전 #62의367/558MiB보다 작지만 공유 호스트·JVM GC/실행 시각이 다른 단회이므로 정확한 절감률로 확정하지 않는다.

후180/300/450/600의90초 실행은 각각고유9,000/물리9,090개,450채널5분은고유30,000/물리30,300개를 대조했다. 수정 후 모든 실행의 최종 projection·offset 대조가 일치한다는 사실과, 중간 지연/계측 판정은 별개다. 모든 소유 시험 자원·생성 비밀을 정리하고 기존8개 container를 유지했다.

수정 후600채널 최초 실행은600개 모두 승인되고9,000개 채팅을 수신했다. source 예정→수신 p95 2ms, API 부하600회 모두200/p95 87ms, 최종 모든 projection·24개 offset lag0이었지만 리포트 채팅/관측 watermark가35.618/30.520초 뒤처졌다. 별도 readiness 표본2개가503이었다. 지속 증가로만 설명하지 않는다. 마지막에는 배출됐지만 중간의10초 지연 기준을 넘은 결과다. 이 실행의 heap 표본 최대124.1MiB/RSS428.7MiB로 heap 부족 재현과는 다르다.

600채널 반복은고유9,000/물리9,090·전체집계·최종lag0과readiness를 통과했지만 SOURCE_CLOCK_REFERENCE1,700건으로 용량 판정에서 제외했다. probe의 `벽시각 - 시작벽시각 - 단조경과시간`은71.484초까지 약0ms였다가76.591초 표본에서 약-2,044ms로 변했다. 단조 pacing과 벽시각이 달라진 관측이며, 누가 시계를 바꿨는지 또는 VM 동기화의 내부 원인은 확정하지 않았다. 음수 수신 지연을0으로 표시한 quantile만 보고 통과로 바꾸지 않는다.

600채널의 PostgreSQL 최대 throttled period 비율은 최초0.9926·반복0.304였다. 이 수치는 시작/배출을 포함한 자원 표본 구간이며 정확한 실패 시각과 정렬되지 않아 DB CPU만으로 원인을 확정하지 않는다. probe 전체 스레드 표본 최대는450채널 두 실행956개,600채널 최초1,257·반복1,259개다. scheduler·HTTP·기대 결과·API·계측 스레드를 포함하므로 순수 연결당 비용은 아니지만 #65의 다음 자원 진단 대상이다.

확실한 연결 저부하 통과는450채널90초 및5분이다.600채널은 최초 지연 초과·반복 계측 제외이므로 안정 운영 통과나 재현된 물리적 상한으로 채택하지 않는다.450은 이 자원·입력 조건에서 검증한 하한이며 정확한 최대치는 아니다. 원래 #64 완료 조건의 안정 pass/fail 경계 반복·정확한 최대치 검증은 미완료이며, 동일 조건450/600 반복·지속과 정상 시계 확보를 #65의 명시적 완료 조건으로 이관했다. 방송 관측/리포트 비용과 계측 시계 안정화도 #65에서 분리한다. 이 결과만으로 기존 복합 운영 후보100채널을450개 인기 방송으로 늘리지 않는다.

## 회귀와 리팩토링

- `IncomingTextTest`: 모든 UTF-16 분할 위치, surrogate 쌍, 누적 한도, 실패/종료 초기화.
- `ChannelProbeTextTest`: 실제1MiB 한글·이모지 수신, wire UTF-8 중간 분할, 누적1MiB 초과 거부, 부분 JSON 단절 후 재접속, client/server heartbeat. 완성 메시지당 이벤트·통계1회와 초과 시 손실 기록을 확인한다.
- `ChatSocketClientTest`: transport 등록 전 owner 종료와 늦은 연결 완료를 겹치고 다른 owner 보존·종료 후 재연결 거부·IO 등록 복귀를 확인한다.
- 기존 `ChatProtocolTest`·`ChzzkHttpSourceTest`·`ChannelProbeTest`를 유지하고 전체 회귀를 실행한다. HTTP/재접속/임대/발행 계약을 변경하지 않는다.
- [리팩토링 기록](REFACTORING.md): assembler와 transport 수명에 책임을 모았으며 공유 singleton이나 범용 transport interface를 추가하지 않았다.

[로컬 게이트](poc/evidence/collector-memory-2026-10-27/local-gates.json): 깨끗한876bb68에서 `./scripts/check` Java176·하네스34·JAR, `./scripts/integration` 실제 Kafka/PostgreSQL136개를 통과했다. XML의 실패/오류/skip은 모두0이다. Standards/Spec의 구현 diff 정적 검토는 각각 차단0·비차단0이었다. 최종 문서 HEAD의 Claude 승인·push/PR CI와 병합은 PR 게이트에 별도로 기록한다.

## 재현과 한계

정본은 루트 README의 `./scripts/collector-memory`, `./scripts/deploy-images`, `./scripts/product-load`다. 각 실행의 실제 command·시작/종료·코드/tree·image/JAR/fixture/log hash를 정제 근거에 연결했다. 독립 baseline 명령은 세션 실행과 clean commit을 재구성한 기록이며 이후 wrapper로 직접 캡처한 명령과 구분한다. 힙 실패 JSON을 완성된 시험으로 취급하지 않는다.

정제기는 원본과 해당 commit의 Git history가 있는 저장소 루트에서 실행한다. 원본 JSON/로그를 고치지 않고 허용 숫자·코드·hash만 출력한다. wrapper와 공통 요약기·판정기 hash를 모두 남기며 기존 판정과 재계산한 판정을 함께 보존한다.

```bash
python3 scripts/collector-memory-summary.py work/collector-memory-64 work/collector-memory-summary
```

실험 입력 계획도 정제 근거의 `plans/`에 보관한다. 이 명령은 결과 정제이며 실험을 재실행하지 않는다.

전체 제품의 일별 최대치·공개 원천 quota/소켓 정책·fleet 임대100개 이상·RF3/다중 호스트·장기 DB/보존·큰 메시지 동시 도착은 포함하지 않는다. heap peak는 GC 후 생존량이 아니며5초 COUNT와 container 자원 계측 비용도 포함한다. 검증된 입력률·분포·조회 조합 밖으로 운영치를 확장하지 않는다. 리포트/공유 DB 진단은 #65, 저장 보존은 #63, 전체 제품 샤딩 판단은 #68, 외부 운영은 #48/#49에서 이어간다.

### 최종 리뷰 보완

`afc1548`의 Claude APPROVE 이후 비차단 의견을 반영했다. Gradle `tasks`가 필수 실험 속성의 즉시 평가로 종료1을 내는 것을 재현해, 속성이 있을 때만 실험 인자를 설정하도록 수정했다. baseline Gradle 종료값은 별도 artifact에 캡처하지 않았으므로 정제 필드를 null로 바꾸고 기대값과 출처를 분리했다. 자식 exit3/2는 Gradle 실패 로그에서 추출한다. 정적 Tomcat IO 계수 회귀는 현재 순차 테스트 조건이며 병렬 테스트로 확장하면 격리가 필요하다.

최종129a46d의 명시적 `tasks`와 lifecycle 실행도 종료0이며8연결3회 매 종료 연결/IO등록/IO스레드0을 확인했다. [최종 게이트](poc/evidence/collector-memory-2026-10-27/final-gates.json)에 전후 명령·코드·종료·hash를 보존한다. buildId 계산은 build.gradle을 포함하므로 태스크 guard를 바꾼 최종 JAR/이미지 hash는876bb68 실측 산출물과 다르다. main/test source 동일성을 산출물 hash 동일성으로 해석하지 않는다.
