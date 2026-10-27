# Google Cloud 서울 리전 일 비용 초안

2026-10-27 15:16 KST 조회 · 운영 후보 #49 · F16 → T08/T15/T16. 현재 작업 상태는 [STATUS](../STATUS.md)에서 관리한다. 이 문서는 **실제 배포 견적이나 처리량 보장치가 아닌**, 24시간 계속 켜 둔 후보의 정가 계산이다. 기존 설계의 `EC2`는 AWS 서비스명이며, 이 계산에서는 같은 역할의 **Google Compute Engine VM**을 뜻한다. Kafka는 VM 안의 Docker 컨테이너에서 직접 실행하며 관리형 Kafka 요금은 포함하지 않는다.

## 규모별 선택

현재 공개 수집은 실행당 최대 3채널·30분이며, GCP에서 24시간 연속 운전하거나 다음 VM 사양으로 부하 시험을 한 적은 없다. 따라서 현재 평가판 시험의 **첫 후보는 단일 `e2-standard-2`(2 vCPU·8 GiB), `pd-balanced` 80 GiB**다. 기존 [단일 호스트 Compose](../DEPLOYMENT_OPERATIONS.md)로 웹·Spring API/분석기·Kafka combined broker/controller·PostgreSQL을 같은 VM의 별도 컨테이너에 두고 fleet worker를 별도 실행한다. RF1·DB 1대이며 호스트 장애를 견디지 못한다. 현재 Docker 사전 검사에는 메모리 6 GiB 이상이 필요하므로 4 GiB `e2-medium` **한 대에 전체 서비스를 넣는 안은 통과 조건을 만족하지 않는다.** 80 GiB 저장량과 8 GiB 메모리는 운영 적합성을 측정하기 위한 시작값일 뿐이다.

| 24시간 가동 비교 | VM 메모리 | 할당 디스크 | VM+디스크 / 일 | 13일 기본료 (USD) | 판단 |
|---|---:|---:|---:|---:|---|
| 단일 호스트 PoC: `e2-standard-2` × 1 | 8 GiB | 80 GiB | **$2.41** | **$31.27** | 현재 Compose와 가장 가까운 시험 후보, HA 없음 |
| 역할 분리 8대: `e2-medium` × 8, 축소 디스크 | 32 GiB | 510 GiB | **$10.43** | **$135.62** | VM당 지속 CPU 1 vCPU 상당, 성능·HA 미검증 |
| 역할 분리 8대: `e2-medium` × 8, 기존 디스크 | 32 GiB | 1,760 GiB | **$15.77** | **$205.07** | VM만 축소한 비교 |
| 기존 8대 대형 사양 | 108 GiB | 1,760 GiB | **$35.38** | **$459.88** | 아래 상세 비용표의 HA 비교안 |

단일 후보 산식은 `24 × (0.08596572 + 80 × 0.000178082) = $2.40509472/일`이다. 8대 medium의 축소 디스크 510 GiB는 Kafka 50 GiB×3, PG 100 GiB×2, boot 20 GiB×8을 가정한다. 기존 디스크 1,760 GiB를 유지한 비교도 함께 보인다. 두 경우 VM 비용은 `8 × 0.04298286 × 24 = $8.25270912/일`이며 합계는 각각 **$10.4324328/일**, **$15.7748928/일**이다. 13일은 각 일 비용을 단순히 13배 한 값이다. 디스크 축소안은 24시간 Kafka 보존, PG 증가량, 복구 공간을 검증하지 않았다. [Google E2 사양](https://cloud.google.com/compute/docs/general-purpose-machines#e2_machine_types)에 따르면 `e2-medium`은 게스트에 2 vCPU가 보이지만 지속 CPU 용량은 총 1 vCPU 상당이며 짧은 burst에만 2 vCPU를 쓸 수 있다. 이 표의 모든 값은 네트워크·외부 IPv4·백업·세금 전 기본료다.

단일 후보에서 CPU·RSS·page cache·Kafka/PG 디스크 증가·API 지연·consumer lag·수집 연결 및 복구를 같은 기간에 계측한다. 8대 HA 구성으로 넘어갈 때는 3-zone RF3/minISR2와 PG 전환을 별도로 시험한다. 평가판 잔액과 환율은 계정·시점별 값이므로 이 공개 문서에 기록하지 않는다.

## 입력과 범위

- 리전: 서울 `asia-northeast3`, Linux 무료 OS 이미지, Compute Engine E2 온디맨드 정가, 할인·약정·세금·환율 제외.
- 아래 상세 비교의 VM: 웹 `e2-medium` 1대(게스트에 보이는 2 vCPU/4 GiB, 공유 코어의 지속 CPU는 1 vCPU 상당), API·분석기 `e2-standard-4` 1대(4/16), fleet `e2-standard-2` 1대(2/8), Kafka broker+controller 동거 `e2-standard-4` 3대(각 4/16), PostgreSQL primary/standby `e2-standard-4` 2대(각 4/16). 합계 **8대, 게스트에 보이는 28 vCPU, 108 GiB 메모리**다. 웹의 공유 코어를 2개의 전용 vCPU로 계산하지 않는다. [E2 머신 유형](https://cloud.google.com/compute/docs/general-purpose-machines#e2_machine_types). Kafka 3대는 서울의 서로 다른 zone에 한 대씩 놓는 후보이며, PG의 전환 자동화와 앱 다중화는 이 가격만으로 해결되지 않는다.
- 디스크: Kafka `pd-balanced` 200 GiB × 3, PG `pd-balanced` 500 GiB × 2, boot `pd-balanced` 20 GiB × 8 = **1,760 GiB 할당**. Kafka broker 데이터는 호스트별 200 GiB 디스크, controller metadata는 같은 호스트의 20 GiB boot 디스크 안에서 별도 경로를 쓰는 계산이다. 독립 물리 디스크 내구성을 가정하지 않으며 OS·이미지·metadata가 함께 쓸 boot 여유와 I/O는 운영 전에 검증한다. 스냅샷·Cloud Storage 백업·WAL의 별도 저장 공간은 포함하지 않았다.
- Kafka 200 GiB/호스트는 현재 토픽의 최대 24시간 보존 설정과 500건/s·압축 후 1 KiB 예시에서 한 복제본의 채팅 약 41.2 GiB/일을 참고한 임시 할당량이다. 다른 토픽·segment·재복제·실제 압축률을 측정하지 않았으므로 24시간 보존이나 500건/s 지속을 보장하지 않는다.
- 1일은 24시간, 30일 비교치는 720시간이다. 운영 중지·단축, 약정·결제 계정의 개별 할인은 적용하지 않았다. VM 크기는 운영 시험으로 승인한 사양이 아닌 비용 비교용 초기 후보이며 실제 GCP 자원은 생성하지 않았다.

## 확인한 서울 단가와 계산

| 자원 | 서울 온디맨드 단가 (USD) | 수량 | 24시간 비용 (USD) |
|---|---:|---:|---:|
| 웹 `e2-medium` | 0.04298286 / VM·h | 1 | 1.03 |
| fleet `e2-standard-2` | 0.08596572 / VM·h | 1 | 2.06 |
| API·분석기 + Kafka + PG `e2-standard-4` | 0.17193144 / VM·h | 6 | 24.76 |
| `pd-balanced` 용량 | 0.000178082 / GiB·h | 1,760 GiB | 7.52 |
| **VM + 할당 디스크** | | | **35.38** |

산식은 `24 × (0.04298286 + 0.08596572 + 6 × 0.17193144 + 1,760 × 0.000178082)` = **$35.37507696/일**이다. 동일 조건 30일은 **$1,061.25** (`× 30`; 월 청구액의 상한이 아님)이다. VM 부분 $27.85289328/일, 디스크 부분 $7.52218368/일이다.
표의 개별 행은 센트로 반올림했으므로 표시된 행의 합과 총액이 $0.01 다를 수 있다. 총액은 반올림 전 단가로 계산했다.

VM 단가는 [Google Cloud 일반 목적 VM 가격](https://cloud.google.com/products/compute/pricing/general-purpose?hl=en)의 지역 선택을 **Seoul (asia-northeast3)**로 바꾼 뒤 E2 standard 및 E2 shared-core 표의 `Default (USD)` 열에서 읽었다. 디스크 단가는 [Google Cloud 디스크 가격](https://cloud.google.com/compute/disks-image-pricing?hl=en)의 `Persistent Disk and Hyperdisk pricing` 지역 선택을 똑같이 바꾼 뒤 `Balanced provisioned space`의 **Hourly** 열에서 읽었다. 페이지의 초기 선택인 Iowa 값으로 계산하면 서울 견적이 틀린다. 공식 가격 페이지의 지역 선택 상태는 링크에 고정되지 않으므로 위 선택을 다시 해 확인해야 한다. [공식 SKU 카탈로그](https://cloud.google.com/skus/sku-groups/on-demand-e2-vms)는 E2 서울 core `9304-94C4-2117`, RAM `D715-4E57-BAFB`를 식별하지만, 이 표의 VM 단가는 위 완성 VM 가격표를 직접 읽은 값이다.

## 빠진 비용과 트래픽 민감도

위 **$35.38/일은 선택한 온디맨드 VM·할당 디스크의 부분 합계**다. 운영안에 따라 다음이 더해진다.

| 항목 | 공식 정가·계산 방식 | 이 초안의 처리 |
|---|---|---|
| zone 간 VM 송신 | 서울의 서로 다른 zone으로 나가는 트래픽 **$0.01/GiB**. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | Kafka 복제 및 앱→Kafka·소비·PG 경로의 실제 송신량 미측정이라 기본 합계에서 제외 |
| 인터넷 송신 | 한국 목적지 Premium Tier 첫 1 TiB 구간 **$0.19/GiB**. 인터넷 수신은 무료. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | 웹/API 응답·수집 프로토콜 송신량 미측정으로 제외 |
| 외부 Application Load Balancer | 처음 5개 글로벌 forwarding rule 묶음 **$0.025/h**, 처리량 별도. [Load Balancing 가격](https://cloud.google.com/load-balancing/pricing) | LB 종류·구성 미확정으로 제외. 글로벌 rule 1묶음만 놓으면 **$0.60/일**부터 |
| Public Cloud NAT | 할당 VM이 32대 이하일 때 **$0.0014/VM·h**, 처리량 **$0.045/GiB**, NAT 외부 IPv4 **$0.005/h**. 인터넷 송신 비용은 추가. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | NAT를 쓸 private VM 수·처리량 미확정으로 제외. 8대 모두 할당·IP 1개면 고정분 **$0.3888/일** |
| VM 외부 IPv4 | 일반 VM에서 사용 중인 IPv4 **$0.005/h·개**; forwarding rule에 연결한 IP는 별도 주소료 없음. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | 직접 공개 VM 수 미확정으로 제외 |
| PG 백업·복원, Cloud Storage, 스냅샷, 모니터링·로그, DNS, TLS/WAF, 갱신 중 임시 VM·디스크, 세금 | 선택한 서비스·보존량·호출량에 따라 달라짐 | 모두 제외 |
| Kafka·PostgreSQL 자체 운영 인력 | 패치·업그레이드·경보 대응·복구 리허설의 시간과 인건비 | 인프라 청구액과 별개로 제외 |

입력 규모의 감을 잡기 위한 **계산 예시**: 500건/s가 하루 내내 계속되고 Kafka에 쓴 이벤트의 실제 직렬화·압축 후 크기가 1 KiB라면, 논리 입력은 `500 × 1 KiB × 86,400 ≈ 41.20 GiB/일`이다. RF3에서 리더와 다른 zone의 replica 두 개로 이 바이트가 각각 한 번씩 송신된다고 단순화하면 복제 송신 **82.40 GiB/일 × $0.01 ≈ $0.82/일**이 추가된다. 이는 복제 송신 한 항목만의 추정이다. 요청, 응답, 제어 트래픽, 재복제, 압축 효과, 다른 zone의 producer·consumer·PG 경로와 인터넷 송신은 포함하지 않았다. [실험 기록](../PROJECTION_PERFORMANCE_VERIFICATION.md)의 100채널·500건/s 결과는 제한된 합성 실행이며 이 입력률의 24시간 지속, RF3 또는 실제 방송 수집을 검증하지 않았다.

## 확정 전 측정

디스크의 Kafka 실사용 크기·보존 창, PG/WAL 증가와 백업량, zone 간 송신 GiB, NAT 처리 GiB, 웹/API 인터넷 송신 GiB를 같은 기간에 재고 다시 계산한다. Kafka는 RF3/minISR2와 controller 3개를 한 호스트씩 동거시키는 후보이며, zone 1개 손실 뒤의 성능·quorum·ISR·복구 비용은 [HA 계약](../pipeline/KAFKA_HA.md)대로 별도 검증해야 한다. 가격표 단가는 변동될 수 있으므로 자원 생성 직전 [Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator)와 결제 계정의 실제 SKU·세금·할인을 다시 확인한다.
