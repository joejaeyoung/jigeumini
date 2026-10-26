# Outbox 거부 시험의 설정 준비 경계 조사

2026-10-26 · Kafka 4.1.2 · #52 · F07/F09 → T03/T08/T10.

## 사실과 한계

PR51 CI40254943726에서 예상 예외가 없었고 같은 HEAD push는 성공했다. 로컬40회(동적 변경20/생성 시 제한20)는 모두277byte payload가64byte 한도에서 거부되어 증상을 재현하지 못했다. CI의 publishOne 반환값·당시 broker log config·offset 기록이 없어 원인은 확정하지 않는다.

[Admin incrementalAlterConfigs 계약](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/Admin.html)은 resource 단위 설정 변경과 future를 설명한다. [4.1.2 QuorumController](https://github.com/apache/kafka/blob/4.1.2/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java)의 incrementalAlterConfigs는 appendWriteEvent로 기록하고, write event future는 metadata log의 offset 진행 뒤 완료한다. [DynamicConfigPublisher](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/server/metadata/DynamicConfigPublisher.scala)는 broker metadata update를 받아 topic handler의 processConfigChanges를 호출한다. 이 별도 적용 경로 때문에 Admin 완료만으로 실제 produce 거부 시각을 단정하지 않는 것이 합리적이다. 다만 이것이 해당 CI 실패 원인이라는 증거는 아니다.

## 선택할 보완

시험 topic을 생성할 때 max.message.bytes=64를 함께 고정한다. 원래 테스트가 검증할 outbox의 거부→pending→복구 계약은 유지하고, 정상 topic에서 제한으로 바꾸는 추가 비동기 전환을 준비 단계에서 제거한다. 동일 topic·같은 필드 구성의 payload를 사용한 별도 producer 제어 발행에서 RecordTooLargeException을 확인하고 broker end=0을 재확인한 뒤 outbox의 일반 오류·설정·offset·DB pending을 대조한다. Outbox 자체는 하위 원인을 숨기는 기존 계약이므로 그 예외에서 직접 RecordTooLarge를 확인했다고 표현하지 않는다. 복구 뒤에는 제한된 시간 동안 OUTBOX_DELIVERY_UNAVAILABLE만 재시도한다. 성공했거나 pending이 없어진 상태는 다시 발행하지 않는다. 제품 코드는 바꾸지 않는다.

새 시험이 통과하면 준비·진단 보강의 근거로 기록한다. CI 근본 원인 수정이나 Kafka 운영 장애 해결로 표현하지 않는다.

## 보완 검증

`cb098c0`의 실제 Kafka/PostgreSQL 독립20회에서276byte payload의 제어 발행은 모두 RecordTooLargeException이었다. Outbox는 모두 일반 전달 오류와 pending 유지·end0을 보였고, 제한 복구 뒤 end1·pending 해제·추가 발행 없음이 확인됐다. 복구 재시도 횟수는 모두0이었다. 단위147·하네스20도 통과했다. [정제 근거](../poc/evidence/outbox-readiness-2026-10-26.json)의 실행 시각·명령·원본 hash를 참조한다. 이는 변경한 시험 준비의 검증이며, 미재현한 최초 CI 실패의 원인 확정은 아니다.
