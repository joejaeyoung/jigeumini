# 수집 연결의 문자 버퍼와 자원 수명 조사

2026-10-27 KST · [#64](https://github.com/joejaeyoung/jigeumini/issues/64) · F01/F13/F14 → T02/T08/T11/T12/T14.
진행 상태는 [STATUS](../STATUS.md)에 둔다. 공개 원천을 호출하지 않은 합성 진단이다.

## 재현과 가설

기존 [연결 용량 근거](../CAPACITY_VERIFICATION.md)의180채널 계획을 깨끗한5048d88 이미지에서 다시 실행했다. collector Xmx384MiB/container640MiB·1 CPU, 입력100건/s·API10/s 예정이었으나 최고172연결 뒤 입력0에서 JAVA_HEAP_SPACE·exit3으로 종료됐다. container OOM kill은 false다. 계측 미완료이며 안정 운영 최대172채널이라고 해석하지 않는다. 실행은2026-10-26 15:21:36~15:22:14 UTC이며 소유 자원 정리와 기존8개 container 보존을 확인했다.

| 가설 | 분리할 예측·대조 |
|---|---|
| 연결마다 미리 잡는 큰 문자 배열 | 전체 메시지 상한을 유지한 작은 partial 청크가 연결 시 heap을 줄이고 같은 고정 heap의 실패를 해소해야 함 |
| transport 종료 시 공유 자원 등록 누수 | 접속/종료 반복 뒤 소유 IO group 등록 수가 시작값으로 복귀해야 함 |
| 하네스·스레드·HTTP/Kafka 비용 | 독립 loopback에서 DB/API/기대 결과 없이 재현하고 전체 제품 결과와 구분 |

별도 Xmx64MiB JVM의32연결에서도 heap 실패를 재현했다.8연결을3회 반복하면 소켓은 모두 닫혔지만 IO group 등록 수는0→8→16→24, 종료 뒤 IO thread는16/16/18개였다. 전체 live thread 증가에는 JVM의 공유 carrier 등도 포함되므로 모두 누수라고 단정하지 않는다. 정본 명령은 루트 README의 `scripts/collector-memory`다. 시험용 내부 계수는 고정 Tomcat 버전에서만 사용한다.

## 실제 라이브러리 계약

- 현재 의존성은 Spring WebSocket7.0.10·Tomcat11.0.27이다. [WsFrameBase](https://github.com/apache/tomcat/blob/11.0.27/java/org/apache/tomcat/websocket/WsFrameBase.java)는 세션 text buffer 크기로 CharBuffer를 연결 시 할당한다.1,048,576문자는 char 배열만 약2MiB이고 객체·GC 정렬·기타 버퍼는 추가다. 이를 전체 UTF-8 메시지의1MiB와 같은 자원 크기로 취급하면 안 된다.
- 같은 구현은 partial handler에서 문자 버퍼가 찼을 때 조각을 전달한다. UTF-8 decoder는 조각 사이의 byte 경계를 처리하고 잘못된 UTF-8을 거부한다. 작은 수신 버퍼와 전체 메시지 허용 크기를 분리할 수 있는 근거다.
- [Spring adapter](https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-websocket/src/main/java/org/springframework/web/socket/adapter/standard/StandardWebSocketHandlerAdapter.java)는 supportsPartialMessages가 true일 때 Jakarta Partial handler를 설치하고 isLast를 전달한다. 부분 JSON을 decode하거나 프레임/수신량으로 중복 계수하지 않아야 한다.
- [WsWebSocketContainer](https://github.com/apache/tomcat/blob/11.0.27/java/org/apache/tomcat/websocket/WsWebSocketContainer.java)는 최초 연결 때 공유 비동기 group을 등록하고 destroy에서 세션 종료·등록 해제를 수행한다. [AsyncChannelGroupUtil](https://github.com/apache/tomcat/blob/11.0.27/java/org/apache/tomcat/websocket/AsyncChannelGroupUtil.java)은 마지막 사용자가 해제될 때 group을 종료한다. 소켓 close만으로 container 등록이 해제되지는 않는다.
- [StandardWebSocketClient API](https://docs.spring.io/spring-framework/docs/7.0.10/javadoc-api/org/springframework/web/socket/client/standard/StandardWebSocketClient.html)는 container를 주입받지만 자동 종료 수명 인터페이스를 제공하지 않는다. 소유 container 수명은 수집 측에서 관리해야 한다.

## 변경 조건

수신 청크는8,192문자 후보로 줄이고, 완성 메시지는 UTF-8 누적1MiB를 유지한다. surrogate 쌍이 청크 경계에 걸리는 경우와 누적 초과를 처리하고, 완성/실패/종료 때 조립 버퍼를 해제한다. READ/heartbeat 이외 송신·원문 저장을 추가하지 않는다. 종료한 owner의 늦은 연결 완료도 닫고 소유 container 정리를 보장한다.

큰 메시지가 실제 도착한 동안의 유한 조립 메모리는 남는다. 모든 연결이 동시에1MiB를 받는 자원 상한과 평상시 연결 비용은 다르다. pure loopback은 HTTP/TLS/fleet/Kafka/DB 비용을 포함하지 않으므로 운영 채널 상한으로 환산하지 않는다. 개선 뒤 같은 전체 제품 계획·더 높은 연결 수·지속 실행·정리·Kafka/DB 계수와 품질을 따로 대조한다.
