# 부하·편향·복구 검증 기록

2026-10-26 KST(UTC2026-10-25) · #11 / F10 → T06/T07/T08/T09/T11/T12. [실행 계약](EXPERIMENTS.md). [정제 비교 표](../poc/evidence/load-2026-10-26/report/README.md)·[독립 HTML](../poc/evidence/load-2026-10-26/report/index.html)·[수치와 hash](../poc/evidence/load-2026-10-26/report/summary.json). 진행 상태는 [STATUS](../STATUS.md)에서만 관리한다.

## 입력·환경·판정

합성20채널·seed20261025, UTC eventTime 기준과 UUID(seed/index), 매100번째120초 늦은 입력을 사용한다. RF1·Kafka4.1.2·PostgreSQL16.14·Darwin arm64·Java21, 3 partitions, DB pool4, acks=all/idempotence/inflight1이다. 실제 projection store의 DB commit 뒤 계측과 fixed offset replay의10초 bucket FULL JOIN을 대조한다. 기본 채널 키·shard 후보는 실험 어댑터로 분리했고 제품 코드는 변경하지 않았다.

원본마다 코드 commit·runtime class hash·시각·합성 입력 정의·환경·목표·실제 계수·partition별 표본·DB·replay·정리 여부를 보존한다. matrix 사이 진단 항목이 추가되어 commit이 다르며 빈 값을 사후 추정하지 않는다. 공유 호스트/서비스에서 실행했고 다른 작업 폴더의 단위 빌드도 있었다. 별도 DB 부하 실험은 직렬 실행했으나 전용 성능 장비나 통계적 반복 실험은 아니다. 지연 표본과 JVM 메모리에 하네스 자체 자료구조가 포함된다.

`correct`는 수락/ACK/미확인·DB·replay 대조이며 목표 입력률 달성과 별개다. 이전 하네스의 정상 종료 경합은 표본 실패를 기록하면서 correct=true로 끝날 수 있었다. 이 원본들은 수정하지 않고 비교 보고서에서 `measurementIssueCodes`와 `verificationComplete=false`로 표시한다. 정상 종료 계측 수명을 분리한 뒤 재검증한다. 원천 live·전체 제품 projection 동시 비용·복제 HA·영역 장애·장기 디스크 보존은 여기서 검증하지 않았다.

## 첫 steady와 발행 후보

첫d13fa6f steady는600.004초에300,000 unique를 생성했다. 수락=ACK=A3명DB=B1명DB=300,000, 거절/미확인0, replay153.466초·bucket차이0·정리true다. A p50/p95/p99=13.556/17.447/23.244ms, B=26.532/83.776/113.020ms, 각300,000표본이다. 후속 하네스와 같은 바이너리라는 주장은 하지 않는다.

직렬500/s 시도는 첫1,420건·후속1,380건 수락 후10건 frame을 거절하고 중단했다. 후속 ACK drain 포함 관측률148.67/s, 거절 직후pending997/상한1000, p95약6,247ms다. Kafka sampled max lag는5로 작아도 수집기에서 오래 기다릴 수 있음을 보여준다. async 후보의 첫 같은500/s·30초는15,000건 전량ACK/DB, p95/p99=52.27/77.91ms였다. 뒤에 추가한 같은1500건·100/s 대조와 측정 종료 보완 뒤 결과는 정제 표에서 구분한다. 목표를 못 마친 직렬과 완료한 async를 같은 입력 총량의 단순 배수로 비교하지 않는다.

직렬 records/request는1, async500/s는약10이었다. Kafka client의 record-send-rate/request-rate는 client의 이동창 통계여서 이 실행의 wall-clock throughput과 다르다. `acknowledgedPerSecondIncludingDrain`을 추가해 생성 시작부터 producer 종료까지의 ACK 수로 별도 계산했다. 애플리케이션 동시성·Kafka 연결당inflight1·broker batch를 혼동하지 않는다.

**결정:** bounded async는 제품 적용 후보로 채택하고 [#27](https://github.com/joejaeyoung/jigeumini/issues/27)에서 수신 thread와 send의 차단 가능성·실패 callback·종료 drain·채널별 상한을 보완한다. 이 PR은 실험 구현이다. Kafka `send`도 metadata/buffer 대기로 max.block.ms까지 막힐 수 있어 완전 비차단으로 부르지 않는다. [KafkaProducer 공식 계약](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

## 편향·consumer·복구 해석

균등20채널의 실제 partition 배치는25/30/45%였다. consumer1/2/3/4의 할당은[3]/[1,2]/[1,1,1]/[0,1,1,1]이며4번째가 추가 partition을 받지 못했다. consumer 수에 비례한 처리량 증가를 가정하지 않는다. 첫 uniform 계측 종료 경합이 있었던 행은 최종 완료 근거로 사용하지 않고 재검증한다.

한 채널80% 입력은 단일 partition에약88.4%가 몰렸다. hot-3의 p95=2,288.88ms였지만 hot-1은404.85ms였다. 같은 채널의 bucket을 합치면 SQL 작업이 줄 수 있고 공유 자원 변동도 있어 모든 hot 입력이 더 느리다고 단정하지 않는다. 일부 최초 행의 계측 종료 경합도 별도로 표시한다.

같은 hot·두producer·300개 동일UUID 재전달에서 channelId 키는partition2에26,824/30,300개, p95/p99=2,822.98/2,876.03ms였다. shard3 후보는p95/p99=270.58/330.99ms, DB30,000·replay차이0이었다. 다만 채널이 여러partition에 걸치고, 기존 품질/offset/카테고리 계약은 그 경로를 전제하지 않는다. 실험의 prefix 제거 어댑터를 제품으로 몰래 적용하지 않았다. **현재 제품 키는 channelId 유지**, shard의 성능 이점은 후속 계약 이행을 정당화할 자료로 남긴다. 서로 다른 키를 세어 partition이 균등하다고 설명하지 않는다.

A를60초 중단하는 동안 독립B는5,010→35,010건으로 진행했다. 최종양쪽50,000건, sampled max lag A29,450, 재개 뒤 최초0lag의 관측 상한32.82초다. DB 지연/실패의 회복 관측 상한은16.15/14.73초다. heartbeat STOP/CONT·재할당/재합류, poll8초 정지(max.poll6초), consumer추가/종료를 각각 기록했다. heartbeat A의 latency는 부모JVM 표본만 포함한다. 샘플 간격과 조회 비용 때문에 순간 최대lag나 정확한 회복 시각으로 해석하지 않는다.

**확장 판단:** partition별 입력/처리율·lag와 DB 지연, 수집기 pending/oldest를 함께 본다. 버퍼만 늙으면 producer 경로, 특정partition만 적체되면 키 편향, 전체partition과 DB가 함께 느리면 공유 DB 비용을 먼저 조사한다. 3partition에서consumer4개는 성능 확장 근거가 되지 않는다. 운영 자동확장과 알림 임계값은 [#29](https://github.com/joejaeyoung/jigeumini/issues/29)·[#30](https://github.com/joejaeyoung/jigeumini/issues/30)의 운영 계약으로 연결한다.

## 실패·보완·리팩토링

1. broker-fault 최초2회는async+BROKER_STOP의FILL_BUFFER에서 실패했다. 두 번째는IllegalStateException과단계를 남겼지만 당시 수락/콜백 세부 값은 없으므로 내부 원인을 확정하지 않는다. 하네스의 모든장애에서30건전량수락 가정은 틀릴 수 있다. callback이 먼저 실패하면 상한 전에 다음frame을 전량거절하는 회귀를 추가하고 실제 수락/거절/대기시간을 기록했다. 보완610aaf1 재시험4경우는대조/상한/정리true였다. pause직렬/async모두31ACK, stop은각ACK1/미확인30/거절1이며 재시작 broker에는1개가 남았다. 초기실패2회도 정제보고서에 포함한다.
2. 최초 자료9개에RESOURCE_OR_SAMPLE_FAILED가 입력후종료 시 기록됐다. shutdownNow가 실행중계측을 interrupt할 수 있고, 입력후의abort를최종판정에서놓치는 경로를 확인했다. `ExperimentSampler`로 현재계측을 기다린뒤종료하고 실제실패는대조완료로승격하지 않게 고쳤다. 진행중sample/정상종료와실제sample실패를 latch로 검증한다. 과거실패의정확한예외는저장되지않아 모두같은내부원인이라고소급단정하지 않는다.
3. Claude 초기 리뷰가 지적한 partition목록순서는 `ReplayManifest` 생성자에서이미정렬·연속번호검사를 수행했다. 실제기존결함으로기록하지 않는다. 비정렬입력 회귀를추가하고소비측도번호→range조회로바꿔정렬불변식의모듈간의존을줄였다.
4. 프로필/유한상한은`ExperimentPlan`, 공통HTML은`ExperimentReport`, 계측종료는`ExperimentSampler`로분리했다. 가변broker/DB/group수명은Session하나가소유한다. 예약장애작업의예외도고정실패이벤트로보존한다. 인프라정본은로컬broker id1,chat-pulse-kafka-1/postgres-1이다.

자원2GiB guard와실제자원조회는각수동프로필에서실행했다. 통합smoke는resources=false로빠르게동작하므로디스크부족주입을통과했다고쓰지않는다. 저용량/쿼터초과를실제디스크를채워시험하지않았다. UUID별topic/schema/group정리와공유live총계불변을대조한다. ACK/commit방식비교는[#31](https://github.com/joejaeyoung/jigeumini/issues/31),복제ISR는#29에서별도로검증한다.
