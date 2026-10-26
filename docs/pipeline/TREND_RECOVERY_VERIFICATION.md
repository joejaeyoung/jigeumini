# 급상승 집계 SQL 실패·재전달 검증

2026-10-26 KST · [#41](https://github.com/joejaeyoung/jigeumini/issues/41) · F03/F09 → T02/T03/T05/T08.
진행 상태는 [STATUS](../STATUS.md), 처리 계약은 [M1](README.md)·[DLQ](DLQ.md)를 따른다.

## 입력과 대조 경계

기준은 PR #40 병합 `dcdc244`, 보강 시험 코드는 `43bedc2`다. 제품 코드 변경 없이 실제 Kafka 4.1.2·PostgreSQL 16.14에서 합성 채널과 UUID topic/group/schema를 사용했다. 공개 방송에 접속하지 않았다.

`TrendRecoveryIntegrationTest`는 `ranking-trend` namespace를 직접 검사한다. snapshot은 해당 run의 ledger·10초 bucket·진행 위치의 모든 열을 비교한다. 최초 event ID가 가진 `source_offset`도 확인한다. 각 시험이 만든 토픽·그룹·schema만 정리한다.

| 사례 | 주입·예상값 | 확인하는 결과 |
|---|---|---|
| 배치 내부/간 중복 | a,a,b 뒤 a,b | UUID 2개·bucket 1/1·DB next5·최초 offset a0/b2; ranking snapshot 불변 |
| 기존 ID payload 충돌 | a commit 뒤 신규 b와 변경된 a | `EVENT_ID_CONFLICT`; 신규 b를 포함한 변경 rollback; 기존 snapshot 불변; 정상 재시도 뒤 2건 |
| SQL commit 실패 | 기존 a에 b@p0:1,c@p1:0 추가 | 지연 trigger가 ledger3·bucket합3·progress2/1을 읽은 경우만 `P1002`; 실패 후 세 저장 영역과 ranking snapshot 복원; 주입 제거 후 3건·재적용 0건 |
| 독립 소비·실행 중 복구 | a 뒤 trend progress 쓰기만 실패시키고 b,b,c | trigger 2회 이상·재시도 상태; ranking commit4/3건, trend commit1/1건, DLQ0; 같은 consumer에서 주입 제거 뒤 commit4/3건. 이후 `none` 재시작과 새 offset의 a,b에도 3건 유지·commit6 |
| DB commit과 Kafka commit 사이 종료 | 별도 JVM을 실제 DB commit callback에서 halt(91) | 종료91·DB next1/1건·Kafka commit0; 같은 group의 `none` 재시작 callback에서 실제 offset0 수신·DB snapshot 불변. 이후 중복 a+신규 b로 commit3/2건 |

배치 내부의 서로 다른 payload 충돌은 SQL 전 검증의 거부 사례다. 실제 rollback 근거는 기존 ledger와 충돌하는 배치와 SQL commit 실패 사례다. DB 직접 시험에는 논리 partition0/1을 넣지만, broker 시험의 입력은 partition0에만 발행한다. 빈 partition1/2는 commit0 유지를 확인한다. 다중 활성 partition의 동시 장애를 입증하지 않는다.

## 실패 주입의 근거

SQL 시험의 `AFTER ... DEFERRABLE INITIALLY DEFERRED` constraint trigger는 commit 시점에 실행한다. 세 저장 영역의 새 값을 조회하고 기대값과 다르면 `P1003`, 일치하면 `P1002`로 실패시켜 단순히 쓰기 전 실패한 시험이 통과하지 못하게 한다. [PostgreSQL 16 CREATE TRIGGER](https://www.postgresql.org/docs/16/sql-createtrigger.html)

시험용 sequence는 trigger가 실제 실행됐는지를 센다. `nextval`은 transaction abort에서 되돌리지 않으므로 rollback된 시도를 확인할 수 있다. 이는 살아 있는 PostgreSQL에서의 계측이며 DB crash 뒤 sequence 값의 영속 증거가 아니다. [PostgreSQL 16 sequence 함수](https://www.postgresql.org/docs/16/functions-sequence.html)

JVM 종료 시험은 기존 `afterDbCommit` 경계를 사용한다. 자식의 실제 종료91과 DB/Kafka 간 위치 차이, 재시작 listener가 받은 동일 offset까지 함께 검사한다. Kafka session timeout은 6초, 재가입 대기는 최대30초다. 테스트 classpath는 기존 Gradle `integration.classpath`를 사용한다. 자식이 제한 시간 안에 종료하지 않으면 실패 처리하고 강제 정리한다.

## 검증과 리뷰 이력

최초 `6079690`의 단위147·하네스20·실제 Kafka/DB99개는 통과했다. Claude는 progress 쓰기 자체의 rollback 증거와 동일 Kafka offset의 재전달 증거를 요구했다. 이후 지연 trigger·실행 중 회복·별도 JVM 종료로 보강했다. 최초 통과를 보강 코드의 결과로 사용하지 않는다.

보강 집중5개 이후 `43bedc2`의 `./scripts/check` 단위147·하네스20·JAR와 `./scripts/integration` 실제100개가 통과했다(실패/오류/skip0). [정제 근거](../poc/evidence/trend-recovery-2026-10-26.json)에 suite별 XML hash·JAR/build ID·명령·실제 시각을 남겼다. Claude 보강 재리뷰는 APPROVE다. 최종 문서 HEAD의 리뷰·CI는 [PR #50](https://github.com/joejaeyoung/jigeumini/pull/50)에 정확한 commit/run과 함께 연결한다.

## Standards

`dcdc244`부터 최초 시험 및 `43bedc2` 보강까지 독립 정적 검토에서 문서 규칙 위반0·차단0·구조 개선 지적0건이다. 주입·snapshot·offset 확인은 시험 내부 helper로 모으고, 실제 두 실패 시점은 `FailurePoint`로 표현했다. 새 제품용 시험 분기는 없다.

## Spec

같은 범위의 독립 정적 검토에서 추가 지적0·차단0건이다. commit에서 진행 위치를 쓴 사실, 같은 consumer의 회복, 별도 JVM 종료와 동일 offset 재수신, 최초 source offset 보존을 확인했다. 리뷰어는 테스트를 실행하지 않았다.

## 해석 한계

- 단일 호스트·broker1대·RF1·합성 입력 범위다. 원천 중복 제거, 전체 Exactly-once, 다중AZ/디스크 손실 또는 DB 서버 crash의 증거가 아니다.
- 기존 `P1002` trigger 실행과 제거 후 회복으로 SQL 재시도 원인을 연결한다. 운영 오류 코드 자체는 여러 일시 처리 오류에 공통이다.
- 새 live·고부하·웹 검증을 이 시험의 결과로 주장하지 않는다. 전체 제품 동시 부하는 #43에서 별도로 측정한다.
- 리팩토링 점검 결과, 제품 저장/소비 경계를 바꿀 근거는 없었다. 공유 테스트 추상화를 추가하지 않고 이 시나리오의 주입과 대조를 한 fixture에 유지했다.
