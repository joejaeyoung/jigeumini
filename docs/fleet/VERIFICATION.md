# 수집기 배정·인계 검증

2026-10-26(KST) · #28 / F13 → T13/T14/T02/T11

## 실행

`8425ebc`에서 `./scripts/integration`을 실행했다. 실제 Kafka4.1.2/RF1·PostgreSQL16.14와 합성 WebSocket, 별도 Java21 프로세스를 사용했다. 전체86개(임대5개)는 실패·오류·skip0이다. [정제 근거](../poc/evidence/fleet-2026-10-26/summary.json)에 XML 연결 hash, 시각·자식 프로세스 진단과 계수를 보관한다. 공개 원천에 접속하지 않았다.

| 사례 | 확인한 경계 | 수락/ACK/broker/DB |
|---|---|---|
| 두 DB 경쟁자 | 채널별 유효 owner 하나, 등록 용량 변경 거절, 만료 후 epoch 증가, 오래된 갱신/해제 차단 | DB 상태 단언 |
| 선정 변경·계획 만료 | 기존 유효 임대를 용량에서 즉시 빼지 않음, 제외/만료 후 갱신 실패 | DB 상태 단언 |
| 두 JVM 정상 인계 | 두 채널을 각각 수집, SIGTERM drain·해제 후 새 JVM이 epoch2로 획득 | 38/38/38/38 |
| SIGSTOP·SIGCONT | TTL6초 만료 후 새 소유자 획득, 재개한 기존 프로세스의 권한 상실·수신 중단 | 16/16/16/16 |
| 실제 DB 소켓 단절 | 해당 자식의 TCP proxy 연결/수신 소켓만 닫음, DB 해제 실패 후 만료 인계 | 54/54/54/54 |

세 프로세스 사례 모두 `accepted=acknowledged+unconfirmed`, broker UUID 집합=DB ledger 집합, bucket 합계=고유 입력이며 미확인0이다. broker 건수는 ACK 이상·수락 이하로 검사한다. 발행 결과가 개별 ACK eventId를 노출하지 않으므로 **개별 ACK ID 포함관계까지 검사했다는 뜻은 아니다**.

정상 SIGTERM 결과는 complete/released=true다. DB 단절의 기존 worker는 complete/released=false·COLLECTION_LEASE_LOST이며 이후 epoch2 worker가 정상 종료했다. SIGSTOP 기존 worker도 complete=false이고 새 worker는 정상 종료했다. 짧은 합성 연결의 ChannelProbe 결과는 CANCELLED/FAIL이며 heartbeat·재접속 PoC PASS로 바꾸지 않는다.

## 실패와 보완

처음5개는 기존 replay V10과 임대 V10의 migration 번호 충돌로 setup에서 실패했다. 임대 migration을 V11로 옮겼으며 실제 운영 schema에는 적용되지 않았다. 이후 임대5개 단독 실행과 전체86개가 통과했다. 정상 종료 때 heartbeat를 즉시 interrupt하면 정상 drain 뒤 실패로 오인할 수 있는 경계를 검토해 종료 순서를 유지하고 graceful shutdown으로 바꿨다. 실제 최종 프로세스 사례에서 정상 종료 상태를 단언한다.

## 리팩토링 점검과 한계

DB 소유권·만료와 로컬 단조시계 권한을 분리했다. 동일 epoch permit을 admission과 dispatch에 사용하고 source 프로토콜은 기존 ChannelProbe를 재사용한다. 테스트의 프로세스/소켓 proxy는 실수신 없이 장애 지점을 소유하며 공유 DB나 Kafka를 중단하지 않는다.

임대는 Kafka broker fencing이 아니다. 이미 client에 넘긴 입력과 마지막 검사/send 사이 정지 창, 정지 프로세스의 물리 소켓 잔존, 원천 중복과 인계 공백은 [계약](README.md)의 제한을 따른다. DB 시계 점프·다중 호스트 drift·장기 운영·전체 치지직 접근은 검증하지 않았다. 최종 리뷰와 CI 상태는 STATUS에서 관리한다.

## 종료 경합 수정 후 재검증

7fd3566의 컴파일 결과로 실제87개(임대6개), 단위134·하네스16·JAR가 통과했다. [후속 근거](../poc/evidence/fleet-2026-10-26/summary.json)의 정상인계39·정지21·DB단절54·진단저장중SIGTERM8건에서 수락=ACK=broker물리/고유=DB이며 집합차이0이다. SIGSTOP 재개 시각 이후 기존 session의 receivedAt 입력은 broker에서0건을 직접 확인했다. 이미 보내 둔 입력까지 broker fencing한 결과는 아니다.

최초 push40213896363/PR40213900998은 SIGTERM 자식 진단 파일 누락2개로 실패했다. 저장 지연5초+SIGTERM 회귀로 CAS 선점 뒤 hook 반환, 저장 전 hook 제거 창을 확인했다. 첫 보완 후 파일 누락, 두 번째 보완 후 deadline 경계의 CHANNEL_ENDED 오판을 재현했고 저장 완료 후 hook 제거·deadline 검사 보완 뒤6개가 통과했다. 최초 CI의 stderr는 없어 그 내부 스택과 재현의 동일성은 단정하지 않는다. 후속 CLI의 중복 CAS 제거는 성공 저장 책임을 worker에 모으는 수정이며 최종 check/CI로 다시 검증한다.

기능별 책임·공개 계약은 유지하고 broker 재조회 helper, 결과 저장 단일 책임, preflight live 비활성 게이트를 정리했다. preview/비활성 worker는 잘못된 JDBC 설정에서도 DB를 열지 않는다. 전체 결과와 이전 실패를 모두 보존한다.
