# Google Cloud 서울 리전 일 비용 초안

2026-10-27 15:16 KST 조회 · 운영 후보 #49 · F16 → T08/T15/T16. 현재 작업 상태는 [STATUS](../STATUS.md)에서 관리한다. 이 문서는 **실제 배포 견적이나 처리량 보장치가 아닌**, 24시간 계속 켜 둔 8대 구성의 정가 계산이다. 기존 설계의 `EC2`는 AWS 서비스명이며, 이 계산에서는 같은 역할의 **Google Compute Engine VM**을 뜻한다. Kafka broker와 controller는 이 VM 안의 별도 Docker 컨테이너로 실행하며 관리형 Kafka 요금은 포함하지 않는다.

## 입력과 범위

- 리전: 서울 `asia-northeast3`, Linux 무료 OS 이미지, Compute Engine E2 온디맨드 정가, 할인·약정·세금·환율 제외.
- VM: 웹 `e2-medium` 1대(게스트에 보이는 2 vCPU/4 GiB, 공유 코어의 지속 CPU는 1 vCPU 상당), API·분석기 `e2-standard-4` 1대(4/16), fleet `e2-standard-2` 1대(2/8), Kafka broker+controller 동거 `e2-standard-4` 3대(각 4/16), PostgreSQL primary/standby `e2-standard-4` 2대(각 4/16). 합계 **8대, 게스트에 보이는 28 vCPU, 108 GiB 메모리**다. 웹의 공유 코어를 2개의 전용 vCPU로 계산하지 않는다. [E2 머신 유형](https://cloud.google.com/compute/docs/general-purpose-machines#e2_machine_types). Kafka 3대는 서울의 서로 다른 zone에 한 대씩 놓는 후보이며, PG의 전환 자동화와 앱 다중화는 이 가격만으로 해결되지 않는다.
- 디스크: Kafka `pd-balanced` 200 GiB × 3, PG `pd-balanced` 500 GiB × 2, boot `pd-balanced` 20 GiB × 8 = **1,760 GiB 할당**. 사용량이 아니라 할당량으로 과금하며, 스냅샷·Cloud Storage 백업·WAL의 별도 저장 공간은 포함하지 않았다. 디스크의 I/O가 이 사양에서 충분한지도 측정 전이다.
- 1일은 24시간, 30일 비교치는 720시간이다. 운영 중지·단축, 지속 사용 할인, 결제 계정의 개별 할인은 적용하지 않았다. VM 크기는 운영 시험으로 승인한 사양이 아닌 비용 비교용 초기 후보이며 실제 GCP 자원은 생성하지 않았다.

## 확인한 서울 단가와 계산

| 자원 | 서울 온디맨드 단가 (USD) | 수량 | 24시간 비용 (USD) |
|---|---:|---:|---:|
| 웹 `e2-medium` | 0.04298286 / VM·h | 1 | 1.03 |
| fleet `e2-standard-2` | 0.08596572 / VM·h | 1 | 2.06 |
| API·분석기 + Kafka + PG `e2-standard-4` | 0.17193144 / VM·h | 6 | 24.76 |
| `pd-balanced` 용량 | 0.000178082 / GiB·h | 1,760 GiB | 7.52 |
| **VM + 할당 디스크** | | | **35.38** |

산식은 `24 × (0.04298286 + 0.08596572 + 6 × 0.17193144 + 1,760 × 0.000178082)` = **$35.37507696/일**이다. 동일 조건 30일은 **$1,061.25** (`× 30`; 월 청구액의 상한이 아님)이다. VM 부분 $27.85289328/일, 디스크 부분 $7.52218368/일이다.

VM 단가는 [Google Cloud 일반 목적 VM 가격](https://cloud.google.com/products/compute/pricing/general-purpose?hl=en)의 지역 선택을 **Seoul (asia-northeast3)**로 바꾼 뒤 E2 standard 및 E2 shared-core 표의 `Default (USD)` 열에서 읽었다. 디스크 단가는 [Google Cloud 디스크 가격](https://cloud.google.com/compute/disks-image-pricing?hl=en)의 `Persistent Disk and Hyperdisk pricing` 지역 선택을 똑같이 바꾼 뒤 `Balanced provisioned space`의 **Hourly** 열에서 읽었다. 페이지의 초기 선택인 Iowa 값으로 계산하면 서울 견적이 틀린다. 공식 가격 페이지의 지역 선택 상태는 링크에 고정되지 않으므로 위 선택을 다시 해 확인해야 한다. [공식 SKU 카탈로그](https://cloud.google.com/skus/sku-groups/on-demand-e2-vms)는 E2 서울 core `9304-94C4-2117`, RAM `D715-4E57-BAFB`를 식별하지만, 이 표의 VM 단가는 위 완성 VM 가격표를 직접 읽은 값이다.

## 빠진 비용과 트래픽 민감도

위 **$35.38/일은 VM·할당 디스크의 바닥값**이다. 운영안에 따라 다음이 더해진다.

| 항목 | 공식 정가·계산 방식 | 이 초안의 처리 |
|---|---|---|
| zone 간 VM 송신 | 서울의 서로 다른 zone으로 나가는 트래픽 **$0.01/GiB**. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | Kafka 복제 및 앱→Kafka·소비·PG 경로의 실제 송신량 미측정이라 기본 합계에서 제외 |
| 인터넷 송신 | 한국 목적지 Premium Tier 첫 1 TiB 구간 **$0.19/GiB**. 인터넷 수신은 무료. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | 웹/API 응답·수집 프로토콜 송신량 미측정으로 제외 |
| 외부 Application Load Balancer | 처음 5개 글로벌 forwarding rule 묶음 **$0.025/h**, 처리량 별도. [Load Balancing 가격](https://cloud.google.com/load-balancing/pricing) | LB 종류·구성 미확정으로 제외. 글로벌 rule 1묶음만 놓으면 **$0.60/일**부터 |
| Public Cloud NAT | 할당 VM이 32대 이하일 때 **$0.0014/VM·h**, 처리량 **$0.045/GiB**, NAT 외부 IPv4 **$0.005/h**. 인터넷 송신 비용은 추가. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | NAT를 쓸 private VM 수·처리량 미확정으로 제외. 8대 모두 할당·IP 1개면 고정분 **$0.3888/일** |
| VM 외부 IPv4 | 일반 VM에서 사용 중인 IPv4 **$0.005/h·개**; forwarding rule에 연결한 IP는 별도 주소료 없음. [VPC 네트워크 가격](https://cloud.google.com/vpc/network-pricing) | 직접 공개 VM 수 미확정으로 제외 |
| PG 백업·복원, Cloud Storage, 스냅샷, 모니터링·로그, DNS, TLS/WAF, 갱신 중 임시 VM·디스크, 세금 | 선택한 서비스·보존량·호출량에 따라 달라짐 | 모두 제외 |

입력 규모의 감을 잡기 위한 **계산 예시**: 500건/s가 하루 내내 계속되고 Kafka에 쓴 이벤트의 실제 직렬화·압축 후 크기가 1 KiB라면, 논리 입력은 `500 × 1 KiB × 86,400 ≈ 41.20 GiB/일`이다. RF3에서 리더와 다른 zone의 replica 두 개로 이 바이트가 각각 한 번씩 송신된다고 단순화하면 복제 송신 **82.40 GiB/일 × $0.01 ≈ $0.82/일**이 추가된다. 이는 복제 송신 한 항목만의 추정이다. 요청, 응답, 제어 트래픽, 재복제, 압축 효과, 다른 zone의 producer·consumer·PG 경로와 인터넷 송신은 포함하지 않았다. [실험 기록](../PROJECTION_PERFORMANCE_VERIFICATION.md)의 100채널·500건/s 결과는 제한된 합성 실행이며 이 입력률의 24시간 지속, RF3 또는 실제 방송 수집을 검증하지 않았다.

## 확정 전 측정

디스크의 Kafka 실사용 크기·보존 창, PG/WAL 증가와 백업량, zone 간 송신 GiB, NAT 처리 GiB, 웹/API 인터넷 송신 GiB를 같은 기간에 재고 다시 계산한다. Kafka는 RF3/minISR2와 controller 3개를 한 호스트씩 동거시키는 후보이며, zone 1개 손실 뒤의 성능·quorum·ISR·복구 비용은 [HA 계약](../pipeline/KAFKA_HA.md)대로 별도 검증해야 한다. 가격표 단가는 변동될 수 있으므로 자원 생성 직전 [Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator)와 결제 계정의 실제 SKU·세금·할인을 다시 확인한다.
