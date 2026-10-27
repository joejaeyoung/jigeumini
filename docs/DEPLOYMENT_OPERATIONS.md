# 배포·용량·복구 운영안

2026-10-26 KST · #30 / F16 → T04/T08/T16. [실행 패키지](../deploy/README.md), [최초 공식 조사](research/KAFKA_DEPLOYMENT.md), [실측 근거](poc/evidence/deployment-2026-10-26/summary.json)를 연결한다. 현재 진행 상태는 STATUS에만 기록한다.

## 배포 구성과 역할

| 구성 | 실행 단위와 경계 | 선택 기준 |
|---|---|---|
| 로컬·단일 호스트 실험 | NGINX 웹1, Spring API/분석기1, 별도 유한 fleet worker, PostgreSQL1, combined Kafka1/RF1 | 현재 이미지·복구 리허설. 호스트 장애 허용 없음 |
| 자체 운영 HA 후보 | gateway/정적 웹, API/분석기1, fleet worker별 임대·예산, AZ별 Kafka EC2 한 대 안에 별도 broker/controller 컨테이너 각1개(RF3/minISR2), PostgreSQL primary+standby·외부 백업 | Kafka용 EC2 총3대·프로세스6개. 한 호스트 손실은 broker와 controller 동시 손실이므로 결합 장애·복구 부하 검증 필요 |
| 관리형 후보 | 앱 컨테이너/VM, 관리형 Kafka3개 영역, 관리형 PostgreSQL standby·백업 | 운영 인력과 복구 책임 축소 여부를 비용에 포함. 서비스가 지원하는 Kafka 버전·ELR·토픽 설정·인증을 별도 호환성 시험 |

현재 분석기와 HTTP API는 한 JVM이며 scheduler도 포함한다. 트래픽 증가 시 무조건 API 복제 수를 늘리지 않는다. 조회 전용 역할과 scheduler 중복/DB 경합 계약을 분리한 뒤 증설한다. 일반 consumer는 partition3개라 같은 group의 네 번째는 유휴가 될 수 있다. 원천별 worker는 임대가 중복 소유를 제한하며, #28의 검사/send 창·이미 전송한 입력·인계 공백 제한을 그대로 가진다.

공식 목록 발견과 전체 채팅 접근은 다르다. [수집 조사](research/CHZZK_SCALE.md)와 [선정 정책](discovery/README.md)을 따라 접근 범위→선정→용량 배정 순서로 실행한다. 팔로워 하한은 설정값이며 1,000/5,000/10,000은 합성 민감도 비교값이다. 실제 권한·입력 분포·비용 없이 운영 하한을 확정하지 않는다. 현재 비공식 live 수집은 최대3개이며 공식 채팅 transport는 제공하지 않는다.

## 실행 순서

1. clean commit으로 서버/웹 이미지를 만들고 Git SHA·image ID·JAR SHA·buildId를 함께 보관한다. 실제 registry 전송 후에는 registry digest도 고정한다.
2. DB owner/runtime 계정의 비밀 파일을 서비스별 mount로 주입한다. runtime DDL을 허용하지 않는다. 웹만 gateway를 통해 공개하고 DB/Kafka는 내부망으로 둔다. 로컬 패키지는 loopback HTTP·내부 PLAINTEXT 범위다.
3. DB/Kafka를 준비하고 owner의 `ops migrate`→`ops bind-sources`→최초1회 `ops initialize-groups`를 실행한다. 쓰기 명령은 명시적 maintenance 모드다.
4. API/분석기는 schema VALIDATE와 recovery guard를 통과한 뒤 시작한다. `/livez`는 프로세스, `/readyz`는 DB/ISR/consumer 진행 표본을 확인한다. 원천 연결·정상 품질은 별도 운영 API에서 본다.
5. fleet는 동일 서버 이미지의 `fleet preview/apply/status/worker` 진입점을 사용한다. selector 입력과 worker 용량·실행 시간을 고정하며 legacy collector와 동시에 켜지 않는다. 외부 원천 연결이 필요한 worker에만 허용된 egress를 제공한다. 현재 리허설의 내부망에는 외부 수집을 켜지 않는다.
6. 새 API/web 이미지에서 revision·합성 집계·SPA 직접 경로/새로고침을 확인한다. 종료 시 수신 중지→발행 drain→consumer/DB 정리 후 교체한다. Compose API 유예60초, Spring phase45초이며 실제 종료 시간도 기록한다.
7. 동일 schema와 호환되는 이전 이미지로 복귀한다. 이번 두 이미지는 모두 V12이므로 파괴적 schema 변경의 rollback을 입증하지 않는다. 미래 migration·checksum 불일치는 자동 repair 없이 시작을 거부한다.

외부 실행 전 공급자/계정·지역·월 예산·도메인·TLS·원천 동의/호출 허용량·백업 저장소·RPO/RTO를 확정한다. 이 저장소 작업으로 외부 자원 생성이나 main 릴리스를 수행하지 않는다.

## 자원·비용 산정 입력

리허설의 Docker 메모리 한도는 API640MiB, migration/관리 명령640MiB, Kafka768MiB, PostgreSQL384MiB, 웹64MiB다. 동시에 관리 명령이 실행될 때 합계2,496MiB(2.4375GiB)다. 사전 검사는 Docker6GiB 이상, 현재 사용량+이 상한≤90%, 호스트 여유디스크2GiB를 요구한다. 이는 시험의 상한이며 운영 권장 VM 사양이나 Docker VM 내부 디스크 여유의 증명이 아니다. JVM 힙뿐 아니라 native memory·page cache·DB shared memory·로그·백업 공간도 포함해 관측한다.

입력은 `λ=선정 채널별 events/s 합`, `b=평균 직렬화 bytes`, `bc=실제 압축 후 저장 bytes/event`, `H=필요 보존 초`, `R=RF`, `μ=복구 중 DB 처리 events/s`로 둔다.

```text
논리 입력 bytes/s = λ × b
Kafka 저장 bytes ≈ λ × bc × H × R + segment/index/복구 여유
복제 전송 bytes/s 초기 추정 ≈ λ × b × (R-1)
복구 예상 초 ≈ backlog / (μ-λ)  (μ>λ인 경우만)
worker 수 하한 = ceil(선정 채널 수 / worker 채널 용량)
DB 연결 상한 예산 = 모든 프로세스의 pool 최대 합 + 관리/복구 여유
월 비용 = 실행시간 × (앱+broker+controller+DB 시간 단가)
         + 저장 GiB-month + IOPS/throughput + 백업/복구 보관
         + 영역 간 복제/consumer fetch/인터넷 전송 + gateway/NAT/관측/비밀 관리
```

worker 수 식만으로 연결 수를 늘리지 않는다. 실제 권한·HTTP 요청 예산·worker1000건 전역/500건 채널별/64inflight 한도와 품질을 먼저 확인한다. 500/s·2,000/s의 [발행기 합성 실측](pipeline/PUBLISHER_VERIFICATION.md)은 단일 broker/특정 DB projection 결과이며 전체 API/6개 group/HA/live 수용량이 아니다.

예시 계산(측정값 아님): λ=2,000/s, b=500bytes이면 논리 입력1MB/s. 압축 전24시간86.4GB·RF3이면259.2GB이며 여유/index/consumer 비용은 별도다. 현재 보존 용량256MiB×partition3은 한 복제본의 총768MiB 상한이다. 균등1MB/s라 가정해도 약805초 분량이고, hot partition은 더 짧아질 수 있다. segment 단위 삭제와 시간 한도가 함께 작동하므로 이 나눗셈을 실제 보존 시간으로 보장하지 않는다. 전체 방송 수집 전에 최소 보존 시작 시각/offset을 측정해 복구 소요보다 넉넉하게 확보해야 한다.

견적 입력표에는 지역, 월 실행시간(예:730시간은 계산 가정), 각 역할 수/CPU/메모리, storage/IOPS, 압축·분포 실측, 모든 consumer group fetch, standby, 보존/백업 기간, 외부 전송·NAT·로그를 넣는다. 단가는 견적일의 공급자 계산기로 채우고 세금/환율/약정 조건을 따로 적는다. 아직 지역·예산·실측 원천 부하가 없으므로 월 금액을 확정하지 않는다.

2026-10-26 공식 자료 추가 확인: [AWS MSK FAQ](https://aws.amazon.com/msk/faqs/)는 Standard broker의3개 영역 배치를 설명한다. [MSK 요금](https://aws.amazon.com/msk/pricing/)은 broker/저장/처리·전송 항목, [RDS PostgreSQL 요금](https://aws.amazon.com/rds/postgresql/pricing/)은 인스턴스/저장/백업/전송과 Multi-AZ 선택을 분리한다. [EC2 비용 구성](https://docs.aws.amazon.com/whitepapers/latest/how-aws-pricing-works/amazon-ec2-cost-breakdown.html)은 compute 외 gateway·네트워크 비용도 포함한다. 관리형 선택이 현재 Kafka4.1.2/ELR1 설정과 호환된다는 검증은 수행하지 않았다.

## 경보와 확대 중단 기준

아래는 초기 운영 제안이며 pager나 자동 확장 서비스가 설치됐다는 뜻은 아니다. 실제 지연 분포로 시간/비율 임계값을 조정한다. 진단은 payload 없이 코드·숫자·집계로 남긴다.

| 지표·조건 | 판단과 행동 |
|---|---|
| readiness503 또는 표본12초 초과 | 새 트래픽/배포 완료 판정 중지. liveness가200이면 DB/Kafka/consumer 원인을 분리 |
| ISR<minISR 또는 leader 없음 | 새 채널 추가 중지, 발행 미확인/누락 표시, replica 복구. ISR=minISR이면 rolling 작업 보류 |
| collector accepted-ACK 증가·가장 오래된 미확인 나이 증가 | producer/broker 경로 조사. buffer 상한 도달은 fail-stop이며 원천 입력을 되감을 수 없음 |
| selected>assigned, lease 만료/갱신 실패, connected/healthy 감소 | 원천 권한·예산·DB·인계·부분 관측을 구분. 분모가 없으면 정상률0으로 만들지 않음 |
| 한 partition만 입력률>처리율, lag 증가 | hot channel/SQL 비용 확인. consumer만 늘려 해결된다고 판단하지 않음 |
| 전 partition lag 증가 + DB pool waiting/transaction 지연 | DB 병목·잠금·IO 조사. 연결 수를 무조건 늘리지 않음 |
| 예상 backlog 회복 시간≥실제 남은 보존 시간 | 데이터 보존 위험 경보. 추가 수집 중지, 저장 용량/보존/복구 예산 조정 |
| 디스크 사용률70% 주의/85% 긴급(초안), 보존 시작이DB next 접근 | segment/로그/백업 공간 회수 계획과 실제 offset 범위 확인. 임의 topic 삭제 금지 |
| outbox 미발행 나이, API freshness, 격리/DLQ 증가 | 각각 publish·consumer·계약 오류 조사. 브로커 ACK를 DB/피드 반영 완료로 보지 않음 |

readiness는 현재 DB pool active/idle/waiting/maximum과 네 topic의 ISR/leader, 여섯 consumer 상태를 제공한다. 상세 partition lag·집계/품질·DLQ는 기존 운영 진단을 사용한다. Kafka 디스크·JMX, PostgreSQL IO/WAL/standby lag, 중앙 경보 수집기는 실제 인프라에 붙일 후속 운영 구성이다. 이번 이슈는 연결할 지표·행동을 명시하고 로컬 오류 반응을 검증한 범위다.

## 복구 절차와 측정 범위

API/수집/DB writer를 모두 멈춘 뒤 새 DB에 dump를 복원한다. 원천 clusterId/topicId/partition, 보존 begin/end, DB next와 Kafka committed를 대조한다. Kafka가 앞서면 시작을 거부한다. 보존이 남으면 새 prefix의 group6개를 DB next에 맞추고 시작한다. 기존 group은 수정하지 않는다. 보존 시작이 DB next를 넘거나 topic이 재생성되면 완전 복구를 거절한다.

첫 리허설은10건 시점 백업+pending outbox1개를 만들고 원래DB를20건/피드2개까지 진행했다. 새DB 복원 시 기존 group과의 차이를 감지해 시작을 거절했고, 새group6개로 재개 후 채팅20건·알림2개·미발행0·선택한 집계/ledger hash 차이0을 확인했다. pending outbox의 재발행으로 notification topic 물리 입력이3개가 되어도 피드는2개다. SQL로 합성 spike/outbox를 주입했으므로 실제 급상승 탐지 성능 검증은 아니다.

관측 복구 시간은 **17.105초**였다. 시작은 기존 API 정지 후 새DB 생성 직전, 끝은 restore·불일치 시작 거부·새group 준비·API readiness·집계 대조까지다. 장애 감지/사람 판단/외부 provisioning 시간은 제외한다. 백업 대비 누락10건이 보존 Kafka로0건이 된 **fixture의 결과**이며 실제 원천 RPO=0이나 운영 RTO 보장을 뜻하지 않는다. 삭제된 로그·수집 공백·미확인 메모리 입력은 복원하지 못한다.

첫 실행은 이미지 교체/복귀·DB 장애시503/프로세스200·schema checksum 실패/복귀·브라우저7개 URL의 SPA shell과375px 폭·최종 종료까지16단계가 통과했다. API 정상 종료 표본은1.144/1.326초, 종료코드143·OOM없음이다. [원본](poc/evidence/deployment-2026-10-26/first.json)에 시각·Git/image/JAR 식별·phase별 결과·cleanup을 보관한다. 백업 파일과 비밀은 gitignored work에만 두며 공유 근거에는 hash/수치만 포함한다.

## 리팩토링 점검

비밀/CLI 설정은 RuntimeEnvironment, schema 연결은 PostgresSupport, 복원 위치 판정은 RecoveryPlan, 주기 health와 HTTP 표현은 DeploymentHealth/Controller에 둔다. consumer별 progress table 차이는 RecoveryPlan 내부 Target으로 숨기며 새 group 준비와 readonly plan을 구분한다. 하네스의 Docker 소유권·호출 기한·정리는 한 Rehearsal에서 관리한다. 범용 배포 프레임워크나 새 외부 의존성을 도입하지 않았다. fleet 관측은 기존 ObservationCollector를 재사용하며 임대 응답 전후·발행 직전 검사와 공유 HTTP 예산만 연결한다. 실패·보완과 회귀 결과는 WORK_LOG에 남긴다.

### 최초 리뷰에서 드러난 검증 한계와 보완

첫16단계와 두 번째17단계 원본은 그대로 보존한다. 당시 `/reports`는 등록되지 않은 URL이므로 fallback200을 실제 리포트 기능 검증으로 계산하지 않는다. startup 비정상 종료만 확인해 checksum/복구 가드 원인을 직접 단언하지 않았고, 일부 진단 계수와 기존 group 불변 필드도 상수였다. 실제 hash 대조·DB 집계 결과까지 무효가 되는 것은 아니지만 그 추가 주장은 보류한다.

후속 하네스는 `SCHEMA_NOT_READY`/`RECOVERY_ALIGNMENT_REQUIRED` 허용 코드, DDL SQLSTATE42501·OOM 여부, 실제 기존 group offset 전후·DB 이름·계산한 누락 수를 단언한다. 브라우저는 실제 API2xx/요청 실패·각 화면 heading·채널별 리포트 목록/상세20건·503 오류/재시도·SPA/asset/revision nosniff를 확인하도록 보강한다. 결과는 후속 실행 원본으로 판정한다. strict VALIDATE와 동일 migration 집합의 이미지 rollback 한계는 배포 README를 따른다.
