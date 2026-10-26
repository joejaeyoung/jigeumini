# 카테고리·리포트 비용 조사

2026-10-27 · #65 · F05/F06/F10 및 F01/F13/F14 → T02/T07/T08/T11/T12/T14, F16 → T16

## 기존 근거와 첫 재현

#62에서는 고정 1 CPU의 전체 제품에서 균등 2,500건/s와 80% 편향 2,000건/s에 카테고리 적체가 있었다.
#64 메모리 수정 후 `129a46d`의 첫 2,500건/s 재현은 약 41초에 발행 버퍼 상한에 닿았다.
ACK 56,055·거절 252·최대 pending 1,000이며 제품 process는 살아 있었다. 이 실행에서는 probe의
벽시계와 단조 경과 차이도 약 +2,044ms 움직였다. 입력이 끝나지 않았으므로 제품 최대치가 아니다.
기존 용량·메모리·이번 시험 27회의 표본 감사에서 절댓값 250ms 초과는 이 실행과 이미 제외한
600채널 반복(-2,045ms) 두 건이었다. 원본과 이전 판정은 보존한다.

## 가설과 구분할 대조

1. 카테고리의 건별 SQL 왕복이 주원인이면 partition 진행 위치를 배치당 한 번 저장하는 변경 등으로
   해당 호출 수와 같은 입력의 처리 시간이 함께 줄어야 한다.
2. 리포트 세션 재구성이 주원인이면 관측 없는 채팅 배치의 이력 조회·세션 계산을 줄였을 때,
   450/600채널의 관측 입력과 채팅 입력을 나눈 대조에서 개선 범위가 달라야 한다.
3. 공유 pool/CPU 경쟁이 주원인이면 SQL 서버 실행 시간 외에 풀 대기 표본과 API 지연이 늘고,
   API만·DB만 CPU를 바꾼 대조가 다른 결과를 보여야 한다.
4. 계측이 주원인이면 같은 제품·입력에서 전체 COUNT만 끈 대조와 `du`를 추가로 끈 대조가 개선돼야 한다.
   Kafka polling과 다른 container의 간섭은 이 대조만으로 제거되지 않는다.

처리 코드를 먼저 바꾸지 않고 진단 하네스를 깨끗한 commit으로 고정한다. 각 대조는 fresh DB,
같은 입력/자원/계측으로 실행하며 실제 수집 원천을 호출하지 않는다.

## 공식 자료와 적용 범위

- [PostgreSQL 16 pg_stat_statements](https://www.postgresql.org/docs/16/pgstatstatements.html):
  preload와 DB별 extension이 필요하다. 정규화된 query별 호출 수·서버 실행 시간·블록·WAL을 읽는다.
  JDBC 왕복·pool 대기·전체 transaction 시간과 같지 않다. query ID는 서로 다른 DB 실행의 전역 식별자가 아니다.
  진단 DB에서 utility 추적과 종료 시 저장을 끄고, 결과에는 고정 family/operation·template hash·숫자만 남긴다.
- [PostgreSQL 16 통계](https://www.postgresql.org/docs/16/monitoring-stats.html):
  누적 통계의 갱신/조회 시점에 제약이 있다. `pg_stat_activity`의 wait는 순간 표본이다.
  wait 표본 수를 누적 대기 시간이나 병목의 원인으로 단정하지 않는다.
- [HikariCP JMX](https://github.com/brettwooldridge/HikariCP/wiki/MBean-%28JMX%29-Monitoring-and-Management):
  pool MBean은 `registerMbeans=true`가 필요하며 각 수치는 원자적으로 함께 읽는 값이 아니다.
  이번에는 제품 pool 설정을 바꾸지 않고 아래 JVM stack의 풀 획득 위치만 분류한다.
- [Java 21 ThreadMXBean](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/ThreadMXBean.html):
  platform thread만 관측한다. 최대 40개 frame을 메모리에서 분류하고 상태·카테고리/리포트·pool/JDBC/소켓 위치의
  개수만 출력한다. 이름과 stack은 저장하지 않는다. 5초 표본에서 대기가 안 보인다고 대기가 없었다고 결론내리지 않는다.

## 계측 계약

`scripts/product-load`의 `measurementProfile`은 FULL, NO_COUNTS, NO_COUNTS_OR_DU다.
후자의 두 모드는 주기 COUNT를 의도적으로 생략했다는 표본을 남긴다. 이를 DB 계측 성공으로 위장하지 않고
선언한 계획과 표본의 일치를 검사한다. 입력 종료 뒤 COUNT·모든 projection 대조·최종 24개 offset 검사는 유지한다.
`diagnostics`는 기본 false이며 true일 때 SQL 통계와 JVM stack 개수를 추가한다. 실제 GC 이름과 허용 VM 옵션은
probe/API에서 읽는다. 설정 인수 전체를 저장하지 않는다.

시계 검증은 입력 시작 epoch와 probe의 단조 경과에 대한 벽시계 잔차를 양방향으로 검사한다.
독립 합성 원천도 100ms tick마다 잔차의 최소·최대를 기록한다. 이는 관측된 시계 이동을 검출하는 장치이며
두 process의 절대 시계 동기화나 물리 네트워크의 지연을 증명하지 않는다. 이번 2초 이동의 원인은 미확정이다.
호스트 시각 변경이나 공유 Docker 재시작은 하지 않는다.

가시성 histogram은 관측에 성공한 표본만의 분포다. 선택/관측/미관측 수와 비율을 함께 보고하며
미관측 표본이 하나라도 있으면 전체 선택 표본의 p95는 보수적으로 `null`로 남긴다.
기존 조건부 quantile은 모집단을 표시해 보존한다. 5% 초과 미관측이 유한 전체 p95로 보이는 일을 막는다.

## 계측 비용 대조와 코드 검증

`d20a29a`의 균등 2,500건/s는 FULL, NO_COUNTS, NO_COUNTS_OR_DU 모두 카테고리 적체로 실패했다.
마지막 모드의 증가율은 356.0건/s·watermark 10.174초였다. COUNT와 `du`를 제외해도 실패가 남지만
두 계측의 비용이 0이라는 뜻은 아니다. `cpu.stat` 실행·Kafka offset 조회와 공유 VM 간섭은 남는다.
80% 편향 2,000건/s의 NO_COUNTS+진단은 이번에 통과했다. 과거 #62 실패가 같은 조건의 확정된
한계라고 주장하지 않고 전후 비용과 더 높은 입력을 비교한다.

최적화 코드 `4f48aee`의 실제 Kafka/PostgreSQL 전체 138개·단위 176개·하네스 37개가 통과했다.
가시성 모집단 보완은 별도 Python 회귀로 검증한다. 기능 통과와 성능 향상은 구분한다.

## 실제 대조 뒤 판단

[검증 문서](../PROJECTION_PERFORMANCE_VERIFICATION.md)와 [정제 실행](../poc/evidence/projection-performance-2026-10-27/summary.json)에 같은 입력의 SQL·CPU·가시성·계측 대조를 연결했다.

- 카테고리 진행 주 SQL은181,860→6,653회, 리포트 매 채팅 재귀속 주 CTE는20,216→0회로 줄었다. 새 관측 보정 CTE는 남았다. SQL 서버 시간이 줄어드는 경로를 특정했지만 JDBC/pool/전체 처리 지연을 같은 비율로 환산하지 않는다.
- 리포트 세션별 TTL 상태 갱신용 최신 관측 조회에는 정렬 색인이 필요했다. 20,160행 scan/sort가1행 index scan으로 바뀌는 대조와 제품 V13을 추가했다. 새 채팅 귀속의 채널별 시각 조회는 기존 `report_observation_time`을 사용한다. 실제 운영 크기 색인 생성 잠금·시간은 미검증이다.
- 순간 표본의 최고 transaction 나이는3,644.171→28.949ms였고 두 실행 모두 풀 획득 위치가 있었다. Lock 대기는 표본에서 발견하지 못했다. 누적 대기 시간/잠금 부재를 뜻하지 않는다.
- COUNT/du를 제거해도 수정 전 실패가 남았다. 수정 후에도 모드별 비용과 단회 변동을 함께 남겼다. API2CPU는 HTTP만이 아니라6개 소비 group/작업이 같은 JVM에서 받는 CPU 변화다. 어느 한 CPU 지표만으로 병목을 확정하지 않는다.
- 정상 시계600채널 반복과5분, 복합100채널500/s5분을 통과했다. 1,000채널의 readiness/watermark 실패와2,000/s 균등의 양수 lag 기울기를 한계로 남겼다. 정확한 최대치는 미확정이다.
- 연결 준비 실패가 최초 원인을 가리는 결함은 회귀로 수정했다. 별도 실제 승인 지연은 입력0에서 거부했고, 2,000연결 반복에서 미재현이다. [공식 소스와 남은 가설](SYNTHETIC_APPROVAL_DELAY.md)을 분리했다.

## 아직 검증하지 않은 것

원천/TLS/fleet, 장기 보존 DB, RF3 및 다중 호스트/AZ는 이번 시험의 범위 밖이다. 공유 VM/기존 workload의 전체 사용량과 순간 대기 시간을 측정하지 않아 성능 원인 전체를 분리하지 못했다. 다음 비교는 운영 후보 반복·장기 DB(#63), 전체 제품 샤딩 판단(#68), 실제 장애 영역/운영(#48/#49)이다. 진행 상태는 [STATUS](../STATUS.md)에서 관리한다.

idle/fetch·1초 offset 조회의 개별 비용과 VM 전체/동거 container 관측은 미실행으로 #49에 명시적으로 이관한다. 현재 하네스는 이 호출들의 독립 토글과 VM 전체 원본이 없어 사후 분석으로 대체할 수 없다. 운영 대상·예산이 정해진 뒤 SKU 확정 전에 무입력/idle consumer/offset 조회 켜기·끄기/100·500건/s를 같은 자원과 DB 크기로 반복한다. [후속 대조와 판정 경계](../PROJECTION_PERFORMANCE_VERIFICATION.md#미분리-계측의-후속-조건)를 따른다.
