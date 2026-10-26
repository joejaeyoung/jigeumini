# 수집기 시작 timeout 조사·검증 #54

2026-10-26 · F13 → T08/T14 · [이슈 #54](https://github.com/joejaeyoung/jigeumini/issues/54).

## 관측과 원인 구분

PR51 push40254127191와 PR53 push40258124001에서 연결된 임대2개를 기다렸으나1개만 관측했고, 자식 하나의 정상 보고서가 없었다. 당시 시작 단계·예외 진단이 없어 원인을 확정할 수 없다. 같은 HEAD의 다른 CI 성공은 원인 수정 근거가 아니다.

시작 단계와 monotonic 경과 시간, 고정 오류 코드, 예외 클래스만 별도 파일에 원자 기록한다. 원문·닉네임·사용자 ID·토큰·예외 메시지를 수집하지 않는다. 부모는 자식 종료 코드와 보고서 유무를 함께 남긴다.

## 공식 자료와 가설

Kafka4.1의 [producer 설정](https://kafka.apache.org/41/configuration/producer-configs/#max.block.ms)은 `partitionsFor`가 `max.block.ms`에 의해 제한됨을 설명한다. [4.1.2 KafkaProducer 구현](https://raw.githubusercontent.com/apache/kafka/4.1.2/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java)의 `partitionsFor`는 metadata 준비를 기다리다가 기한을 넘으면 TimeoutException을 전달한다. 확인일은2026-10-26이다.

현재 producer의 한도는2초다. 따라서 초기 bootstrap 응답이 지연되면 임대 획득 전 생성자가 실패할 수 있다는 가설을 실제 TCP proxy로 검사했다. 초기 bootstrap 통신 전체를 버리므로 특정 Kafka 요청 종류만 차단한 실험으로 해석하지 않는다.

## 실제 재현과 수정

- 수정 전 `ae01064`: 실제 bootstrap 차단에서 metadata 요청 후2,008ms에 TimeoutException·FLEET_KAFKA_NOT_READY, 자식 종료1·정상 보고서 없음. 연결을 복원해도 이미 종료된 수집기는 연결0이라 시험이 실패했다. 임대·원천 연결은 없었다.
- 수정 `ea3eb34`: 임대·원천 수신 전에 metadata TimeoutException만 최대5회 시도한다. 인증·기타 오류와 빈 partition은 즉시 실패한다. 실행 중 발행 큐·동시성·2초 producer 한도는 유지한다. producer 생성과 scheduling까지 포함한 절대 생성자 기한을 보장하는 것은 아니다.
- 같은 차단 후 복구: 첫 timeout 뒤 두 번째 조회가 성공했다. WORKER_READY2,560ms, ACK10·accepted10·거절0이며 실제 broker/DB ledger를 대조했다. SIGTERM 종료143에서 정상 보고서와 complete=true를 확인했다.
- 영구 차단: timeout5회·마지막10,437ms, 자식 종료1·정상 보고서 없음. 임대·원천 연결0으로 유한 실패를 확인했다.
- 실제 Fleet13개(독립 인계5회 포함)가2026-10-26 02:45:55.384 UTC부터85.412초에 통과했다. `./scripts/check` 단위151·하네스20·JAR도 통과했다.

[정제 근거](../poc/evidence/fleet-startup-2026-10-26.json)에 실행 revision·명령·XML hash·고정 진단을 남겼다. 과거 두 CI가 이 timeout과 같은 원인인지는 계속 미확정이다.

## 리팩토링 점검과 범위

metadata 준비 경계를 한 메서드로 모으고 timeout/인증/빈 partition을 구분하는 단위4개를 추가했다. Standards 리뷰에서 진단 읽기 실패가 정리를 중단하는 문제를 발견해, 성공한 process/report만 함께 등록하고 진단 실패를 고정 코드로 제한했다. Spec 리뷰 지적0이다. 후속 `f0b2fcf`의 단위151·하네스20·JAR도 통과했고 Standards 재검토 차단0이다. 최종 문서 HEAD의 Claude·push/PR CI는 별도로 확인한다.

진단 fixture 범위이며 공개 원천·다중 호스트 부하 결과로 확대하지 않는다. 원래 CI와 동일한 내부 지연 원인을 재현했다고 주장하지 않는다.
