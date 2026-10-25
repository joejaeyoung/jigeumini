# 토픽 생성 후 조회 준비 확인

2026-10-25 · F01/F07 · T10 기동 계약 보완. 기술 목표 전체 완료를 뜻하지 않는다.

## 확인한 실패

PR #20의 `3977d34` PR CI [run40196842128](https://github.com/joejaeyoung/jigeumini/actions/runs/40196842128)는 `DeadLetterIntegrationTest.setup`에서 실패했다. artifact의 stack은 `KafkaSupport.ensureTopic:32`, `describeConfigs(...).all().get(...)`의 `UnknownTopicOrPartitionException`을 가리킨다. 같은 HEAD의 push run40196838456은 통과했다. PR #18의 run40186994269도 사용자 기록상 같은 위치의 실패다.

실제 로컬 broker에 격리된 UUID 토픽을200개 생성하고 같은 순서로 생성→토픽 조회→설정 조회→삭제한 결과 실패0회였다. 이 결과로 broker 내부 원인을 확정하지 않는다. CI broker의 상태 전파, 내부 오류 또는 스케줄링 원인을 구별할 trace는 없다.

확인한 클라이언트 결함은 일시적인 조회 불가 응답 하나로 초기화 전체를 실패시킨다는 점이다. 생성 성공·토픽 조회 성공·설정 조회 첫 실패/다음 성공의 Admin fixture로 수정 전 실패를 재현했다. 이 fixture는 실제 broker의 내부 지연을 재현한 것이 아니라 동일한 응답 순서에서 클라이언트가 실패하는 경계를 고정한다.

## 선택한 처리

- 생성은 별도10초 안에 요청한다. 이미 존재하면 설정을 변경하지 않고 조회로 진행한다.
- 생성 뒤 토픽·설정 조회와 재시도에 공통10초 기한을 둔다. 각 Admin 요청과 Future 대기는 남은 예산을 쓴다.
- 두 조회의 `UnknownTopicOrPartitionException`만 최대50ms 간격으로 재조회한다. 토픽을 반복 생성하거나 offset을 바꾸지 않는다.
- 기한 초과·권한 오류·그 밖의 오류는 실패한다. partition3개·CreateTime 계약 불일치도 즉시 실패한다.
- 중단 신호를 복원하고 Admin 종료는1초로 제한한다. 이론적인 작업 예산은 생성10초+준비 확인10초+종료1초이며 프로세스 스케줄링 지연까지 보장하는 실시간 한도는 아니다.

Kafka 4.1.2 [Admin 계약](https://downloads.apache.org/kafka/4.1.2/javadoc/org/apache/kafka/clients/admin/Admin.html)은 토픽 생성 응답 뒤 브로커의 인식이 늦을 수 있음을 설명한다. [UnknownTopicOrPartitionException](https://downloads.apache.org/kafka/4.1.2/javadoc/org/apache/kafka/common/errors/UnknownTopicOrPartitionException.html)은 오래된 메타데이터 가능성과 재시도를 명시한다. 로컬 kafka-clients4.1.2의 `describeConfigs` 구현은 resource 오류를 Future의 예외로 완료한다. 해당 오류가 이 호출 안에서 자동으로 재시도된다고 가정하지 않는다.

## 검증 범위

`KafkaSupportTest`의 첫 사례는 기존 코드에서1/1실패했고 수정 후 통과했다. 추가로 공통 기한 소진, 권한/설정 오류의 즉시 거부, interrupt 복원과 Admin 종료를 검증한다. 가상 단조 시계를 사용해10초를 반복 대기하지 않고 기한 정책을 확인한다.

`KafkaSupportIntegrationTest`는 실제 broker에서20개 새 토픽 각각 최초/기존 초기화와 조회를 확인한다. 잘못된 partition 수 또는 timestamp 설정의 기존 토픽을 거부하며 그 설정을 변경하지 않는지도 확인한다. 완료한 전체 suite·코드 commit·JAR hash·초기 CI 실패는 [정제 근거](../poc/evidence/topic-readiness-2026-10-25.json)에 기록한다.

메타데이터가 영원히 사라진 경우를 성공으로 숨기지 않으며, 모든 Kafka 장애나 HA를 해결했다고 주장하지 않는다. CI broker 내부 원인은 미확정으로 남고, 이 변경은 알려진 일시 응답에 대한 클라이언트 기동 계약을 보완한다.
