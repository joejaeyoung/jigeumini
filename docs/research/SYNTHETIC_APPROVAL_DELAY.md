# 합성 WebSocket 초기 승인 지연의 공식 소스 조사

- 조회일: **2026-10-27 KST**. 관련 이슈: [#65](https://github.com/joejaeyoung/jigeumini/issues/65), 기술 목표 T07·T08의 부하 시험 유효성 확인.
- 목적: Java-WebSocket 1.6.0의 쓰기 큐와 selector 경합 가능성, 수정 이력, 제품 클라이언트 초기화 순서를 확인한다.
- 범위: 공식 소스·릴리스·이슈와 로컬 코드의 읽기. 재현 시험·빌드·제품 수정·운영 변경은 수행하지 않았다.
- 로컬 코드 기준: `e4319e9bfb567bc9db42814bb13c827fcf39d632`, `technicaltest/projection-bottlenecks`.
- 진행 상태는 [STATUS](../STATUS.md), 실행 판정은 [검증 문서](../PROJECTION_PERFORMANCE_VERIFICATION.md)와 [승인 전용 근거](../poc/evidence/projection-performance-2026-10-27/startup-probe.json)에 둔다.

## 1. 확인한 버전과 출처

| 대상 | 확인한 버전·기준 | 범위 |
|---|---|---|
| 합성 원천 | Java-WebSocket 1.6.0 | `server/build.gradle`의 test 의존성 및 fixture 라이브러리 복사 규칙 |
| 공식 태그 | `v1.6.0` → `da3cf2a777aed862f2f5b5cf060cae7969958667` | GitHub tag ref API로 commit 확인 |
| 공식 기본 브랜치 | `afeacbf8c0f6f6a761c9d9daed8c813dd3b8ed7d` | 조회 시점 master HEAD; 마지막 commit 시각 2026-01-13 |
| 제품 클라이언트 | Spring WebSocket 7.0.10, Tomcat WebSocket/Core 11.0.27 | 기존 실행 JAR 안의 `BOOT-INF/lib` 파일명 확인; 실행 중 컨테이너의 JAR 해시까지 대조하지 않음 |
| selector 계약 | Java SE 21 API | API 의미 확인; 이번 컨테이너의 JDK 구현 내부 동작을 계측한 것은 아님 |

출처: [로컬 의존성 선언](https://github.com/joejaeyoung/jigeumini/blob/e4319e9bfb567bc9db42814bb13c827fcf39d632/server/build.gradle#L26), [공식 태그 API](https://api.github.com/repos/TooTallNate/Java-WebSocket/git/ref/tags/v1.6.0), [공식 master commit](https://github.com/TooTallNate/Java-WebSocket/commit/afeacbf8c0f6f6a761c9d9daed8c813dd3b8ed7d).

## 2. 현재 관측이 말하는 범위

실행 결과의 정제 관측은 원천 연결 peak 20, 원천 protocol/socket errors 0, 수집기 승인 연결 19·수신 frame 38, `APPROVAL_TIMEOUT` 1, 원천 입력 0이다.
종료는 `1000 / REMOTE / BEFORE_START_REQUEST` 20이며, 원천·제품 OOM은 보고되지 않았다. 이 조사에서 실행 산출물을 재수집하지 않았다.

후속으로 실행한 승인 전용 호스트 loopback은 20채널 × 100회, 총 2,000 연결 모두 승인되었고 4,000 frame, handshake/source error 0, queue 잔류 0을 기록했다.
그 조건에서는 **미재현**이다. 실제 실패 당시의 queue·interestOps 증거가 없으므로 loopback 성공을 원인 배제나 수정 완료로 해석하지 않는다. 이 조사와 반복 시험은 같은 작업의 근거이며 독립 검증 기관의 재현을 뜻하지 않는다.

합성 원천은 READ 요청을 해석한 뒤 연결을 등록하고 approval, application ping 순서로 `send`를 호출한다.
따라서 peak 20은 READ 처리와 연결 등록의 근거다. **실패 연결의 승인 프레임이 큐에 들어갔는지, 소켓에 쓰였는지, 클라이언트에 도착했는지는 아직 확인되지 않았다.**
프레임 38은 정상 연결 19개의 초기 두 프레임과 양립하지만, 연결별 기록 없이 실패 연결의 수신 경로를 증명하지 못한다.
출처: [합성 원천 등록·송신 순서](https://github.com/joejaeyoung/jigeumini/blob/e4319e9bfb567bc9db42814bb13c827fcf39d632/server/src/test/java/io/chatpulse/operations/ProductLoadSocketServer.java#L43).

## 3. Java-WebSocket 1.6.0에서 확인된 구현

아래 항목은 태그 commit의 소스 사실이다.

1. `WebSocketImpl.write`는 `outQueue`에 버퍼를 넣고 listener의 `onWriteDemand`를 호출한다. `send` 반환은 소켓 쓰기 완료 통지가 아니다.
2. 여러 버퍼를 넣는 경로의 `synchronizeWriteObject` 잠금은 selector의 쓰기 완료 처리와 공유되지 않는다.
   출처: [WebSocketImpl의 enqueue](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/WebSocketImpl.java#L736).
3. selector의 `doWrite`는 `SocketChannelIOHelper.batch`가 true이고 key가 유효하면 관심 집합을 `OP_READ`로 설정한다.
4. `onWriteDemand`는 관심 집합을 `OP_READ | OP_WRITE`로 설정한 뒤 selector를 깨운다. 두 메서드 사이에 공통 잠금은 없다.
   출처: [doWrite](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/server/WebSocketServer.java#L583), [onWriteDemand](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/server/WebSocketServer.java#L848).
5. `batch`는 현재 버퍼를 다 쓰면 poll 후 다음 버퍼를 peek한다. 일반 SocketChannel에서 끝의 반환값은 동시 enqueue 여부에 묶여 있지 않다.
   마지막 `outQueue.isEmpty` 조건은 종료 처리용이다. 새 버퍼가 들어온 경우를 반환 false로 바꾸는 조건이 아니다.
   출처: [SocketChannelIOHelper.batch](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/SocketChannelIOHelper.java#L82).
6. selector는 읽은 데이터를 decoder worker에 넘긴다. worker가 메시지를 해석해 사용자 callback을 실행하므로 selector와 송신 요청은 다른 스레드에서 진행할 수 있다.
   합성 원천의 decoder 1개 설정도 selector와의 동시 실행을 제거하지 않는다.
   출처: [WebSocketServer의 run·doRead·WebSocketWorker](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/server/WebSocketServer.java).

## 4. 소스에서 가능한 실행 순서 — 이번 실패 원인으로는 미확정

다음은 앞의 구현에서 도출한 **경합 가설**이다. 이번 실행에서 이 순서를 관측했다는 뜻이 아니다.

| 순서 | 실행 주체 | 상태 변화 |
|---|---|---|
| 1 | selector | READ와 WRITE가 ready인 key에서 READ를 worker에 넘기고 selected set에서 key를 제거한다. |
| 2 | selector | 같은 반복의 WRITE 처리에서 `batch`가 빈 큐를 보거나 기존 마지막 버퍼를 소진한다. |
| 3 | decoder | READ callback이 approval과 ping을 enqueue하고 각각 쓰기 관심을 등록한다. |
| 4 | selector | 앞선 `batch`의 true 결과로 관심 집합을 READ만 남도록 덮어쓴다. |
| 5 | 다음 select | 큐에는 데이터가 있으나 WRITE 관심이 없다. 새로운 쓰기 요청이 없으면 전송이 지연될 수 있다. |

`interestOps` 설정은 주어진 집합으로 교체하는 연산이다. `wakeup`은 select의 반환을 유도하며, 덮어쓴 WRITE 비트를 복원하는 명령은 아니다.
한 key의 연산이 스레드 안전하다는 사실도 큐 확인부터 관심 집합 변경까지를 하나의 원자적 연산으로 만들지 않는다.
출처: [SelectionKey.interestOps](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/SelectionKey.html#interestOps(int)), [Selector.wakeup](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/Selector.html#wakeup()).

selected set에서 제거되지 않은 key는 이전 ready 비트를 유지할 수 있다. 따라서 모든 WRITE 덮어쓰기가 장기 정지로 이어지지는 않는다.
위 경로에서는 READ 처리 시 key가 제거되는 조건이 중요하다. 관심 집합과 ready 집합은 서로 다른 상태다.
출처: [Selector의 선택 절차](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/Selector.html), [서버 selector 반복](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/server/WebSocketServer.java).

두 초기 프레임 모두 남으려면 해당 빈 큐 관측 이후 enqueue가 이루어지고, 마지막 쓰기 요청까지 READ 설정보다 앞서야 한다.
enqueue가 뒤에 일어나거나 다음 쓰기 요청이 도착하면 WRITE 관심이 복구될 수 있다. approval이 이미 쓰인 경우 ping 지연만으로 approval timeout을 설명할 수 없다.
따라서 이 가설의 입증에는 **실패 연결에서 승인 대기 중 queue > 0, WRITE 관심 없음, 소켓 쓰기 진행 없음**의 동시 관측이 필요하다.

원천은 라이브러리 heartbeat를 끄고, 제품 heartbeat는 승인 완료 뒤 시작하며, 원천 본문 생성은 전체 연결 준비 뒤 시작한다.
이는 이후 쓰기가 정체를 풀어줄 기회를 줄이는 조건이라는 추론이다. 종료 close 프레임이 이후 전송을 유발할 수 있으므로 정상 close 코드도 승인 기한 내 전송의 증거가 아니다.
출처: [원천 heartbeat 설정](https://github.com/joejaeyoung/jigeumini/blob/e4319e9bfb567bc9db42814bb13c827fcf39d632/server/src/test/java/io/chatpulse/operations/ProductLoadSocketServer.java#L67), [제품 승인 대기·heartbeat 시작](https://github.com/joejaeyoung/jigeumini/blob/e4319e9bfb567bc9db42814bb13c827fcf39d632/server/src/main/java/io/chatpulse/poc/ChatConnection.java#L95).

## 5. 공식 수정 이력에서 확인한 것과 확인하지 못한 것

- 1.6.0 공식 릴리스 날짜는 2024-12-15다. 변경 기록은 Java 7 지원 종료, deflate 설정 복제 수정, inherited socket 지원을 담는다. 이 경합의 수정 설명은 찾지 못했다.
  출처: [1.6.0 릴리스](https://github.com/TooTallNate/Java-WebSocket/releases/tag/v1.6.0), [태그의 CHANGELOG](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/CHANGELOG.md).
- GitHub contents API로 태그와 조회 시점 master의 `WebSocketServer.java`, `SocketChannelIOHelper.java`, `WebSocketImpl.java`를 메모리에서 바이트 비교했다. **세 파일 모두 동일**했다.
  따라서 확인한 master에 이 세 경로의 직접 변경이 있다는 근거는 없다. 다른 파일의 모든 간접 영향을 전수 분석한 것은 아니다.
  출처: [master 서버](https://github.com/TooTallNate/Java-WebSocket/blob/afeacbf8c0f6f6a761c9d9daed8c813dd3b8ed7d/src/main/java/org/java_websocket/server/WebSocketServer.java), [master helper](https://github.com/TooTallNate/Java-WebSocket/blob/afeacbf8c0f6f6a761c9d9daed8c813dd3b8ed7d/src/main/java/org/java_websocket/SocketChannelIOHelper.java), [master impl](https://github.com/TooTallNate/Java-WebSocket/blob/afeacbf8c0f6f6a761c9d9daed8c813dd3b8ed7d/src/main/java/org/java_websocket/WebSocketImpl.java).
- 공식 이슈 검색어는 `interestOps`, `OP_WRITE`, `outQueue`, `write race`였다. 발견한 [#811](https://github.com/TooTallNate/Java-WebSocket/issues/811)은 일반적인 필드 가시성·스레드 안전성 논의로, 이번 경합의 확정 보고가 아니다.
- [#225](https://github.com/TooTallNate/Java-WebSocket/issues/225)는 selector의 높은 CPU 사용 보고다. 댓글이 가리킨 [#488](https://github.com/TooTallNate/Java-WebSocket/pull/488)은 새 SSL 채널 구현이며 이번 plain WebSocket의 승인 정지와 동일하다고 확인하지 못했다.
- [#1419](https://github.com/TooTallNate/Java-WebSocket/pull/1419)는 SSL handshake와 HTTP GET이 한 번에 읽힐 때 GET이 남는 문제를 고쳤다. 현재는 일반 `ws` 경로에서 애플리케이션 READ까지 처리됐으므로 해당 수정의 직접 적용 사례로 볼 근거가 없다.
- [#1367](https://github.com/TooTallNate/Java-WebSocket/pull/1367), [#1470](https://github.com/TooTallNate/Java-WebSocket/issues/1470)은 클라이언트 reconnect 경로다. 이번 서버의 쓰기 관심 덮어쓰기 수정 근거로 사용하지 않는다.

결론은 **동일 경합을 고쳤다는 공식 PR·릴리스를 이번 조사에서 확인하지 못함**이다. 검색 부재를 저장소 전체의 버그 부재나 영구 미수정 보증으로 확대하지 않는다.

## 6. Spring·Tomcat 초기 handler 등록 순서

Spring 7.0.10의 adapter `onOpen`은 native session 초기화 → text/binary handler 등록 → pong handler 등록 → 제품 `afterConnectionEstablished` 호출 순서다.
제품은 마지막 callback에서 READ를 보낸다. 따라서 이 코드에서 “READ를 먼저 보낸 뒤 text handler가 등록되어 초기 approval을 놓침”이라는 설명은 초기화 순서와 맞지 않는다.
출처: [Spring adapter v7.0.10](https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-websocket/src/main/java/org/springframework/web/socket/adapter/standard/StandardWebSocketHandlerAdapter.java), [제품 READ 송신](https://github.com/joejaeyoung/jigeumini/blob/e4319e9bfb567bc9db42814bb13c827fcf39d632/server/src/main/java/io/chatpulse/poc/ChatConnection.java#L47).

Spring client는 adapter를 생성해 `connectToServer`에 전달한다. Tomcat 11.0.27은 endpoint의 `onOpen`을 호출하고 session을 등록한 뒤 `startInputProcessing`을 실행한다.
이 처리에는 HTTP 응답 버퍼에 이미 들어온 초기 WebSocket 데이터도 포함된다. 이후 읽기는 비동기 채널 경로로 이어진다.
출처: [Spring StandardWebSocketClient](https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-websocket/src/main/java/org/springframework/web/socket/client/standard/StandardWebSocketClient.java), [Tomcat WsWebSocketContainer](https://github.com/apache/tomcat/blob/11.0.27/java/org/apache/tomcat/websocket/WsWebSocketContainer.java), [Tomcat WsFrameClient](https://github.com/apache/tomcat/blob/11.0.27/java/org/apache/tomcat/websocket/WsFrameClient.java).

이 사실은 런타임의 스레드 정체·소켓 수신·callback 지연을 배제하지 않는다. 클라이언트 자체가 정상이라고 결론 내려면 실패 연결의 실제 수신 단계 관측이 더 필요하다.

## 7. 후속 입증에 필요한 최소 관측과 설계 후보

아래는 **미실행 제안**이다. 본문·닉네임·사용자 ID·토큰을 수집하지 않고 합성 연결의 실행 내 순번과 코드·숫자만 사용한다.

| 관측 지점 | 제한된 값 | 판별할 내용 |
|---|---|---|
| 원천 READ callback | 등록·approval send 반환·ping send 반환의 횟수와 상대 시각 | 등록과 enqueue 시도의 구분 |
| 승인 대기 중 원천 | 연결별 queue 버퍼 수·잔여 byte 수, key 유효성·interestOps·readyOps | 미전송 큐와 WRITE 관심 소실 여부 |
| selector 실제 쓰기 | 호출 횟수·쓴 byte 수·마지막 진행 시각 | enqueue와 소켓 쓰기의 구분 |
| 수집기 | text callback·완성 frame·approval 해석의 횟수와 상대 시각 | 수신·조립·프로토콜 판정 단계의 분리 |
| 스레드 상태 | selector·decoder·client callback 진행 여부 | queue 경합과 스케줄링 정체의 구분 |

서로 다른 시점의 큐 크기와 관심 집합 스냅샷만으로 원자적인 상태를 관측했다고 주장하지 않는다. 승인 대기 중 반복 관측과 상태 전환 시각을 함께 남긴다.
원천 전체의 `bufferedConnections` 값만으로는 실패 연결의 잔여 두 버퍼인지 판별하기 어렵다. 라이브러리 TRACE는 payload를 출력할 수 있으므로 사용하지 않는다.
출처: [hasBufferedData·write의 TRACE](https://github.com/TooTallNate/Java-WebSocket/blob/da3cf2a777aed862f2f5b5cf060cae7969958667/src/main/java/org/java_websocket/WebSocketImpl.java#L708).

결정적 재현 후보는 실제 1.6.0 경로에서 빈 큐 관측과 READ 설정 사이를 제어하고, 두 버퍼 enqueue 이후 WRITE 관심 소실·미전송을 확인하는 것이다.
이후 명시적 새 쓰기 요청으로 진행이 재개되는지도 대조한다. 임의로 만든 큐 모델만 통과시키는 시험은 라이브러리 경합 재현으로 계산하지 않는다.

수정 후보는 selector가 관심 변경을 소유하도록 요청을 직렬화하거나, 관심 제거 뒤 큐 재확인과 재등록을 올바르게 조정하는 방식이다. 검증 전 채택하지 않는다.
한쪽을 `interestOpsOr`로만 바꾸거나 READ 설정 **이전** 큐 확인을 한 번 더 추가하면 이후 덮어쓰기 간격이 남는다. timeout 확대와 주기적 ping도 원인 제거의 증거가 아니다.
출처: [SelectionKey의 OR·AND 계약](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/SelectionKey.html#interestOpsOr(int)).

서버의 `onWriteDemand`는 final, `doWrite`는 private이다. 기존 fixture subclass에서 두 메서드만 override하는 수정은 불가능하므로 구현 위치와 라이브러리 변경 비용도 검토해야 한다.
현재 판정: **소스상 경합 가능성 확인, 이번 timeout과의 인과관계 미입증, 승인 프레임의 실제 전송 여부 미확인**.
