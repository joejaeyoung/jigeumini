# 새 migration 릴리스의 실패·중단·백업 복원 검증

2026-10-26 KST · [#42](https://github.com/joejaeyoung/jigeumini/issues/42) · F16/F08 → T04/T16.
[실행 명령](../README.md#새-migration-릴리스-복구-시험) · [공식 자료 조사](research/POSTGRES_RECOVERY.md) · [정제 근거](poc/evidence/migration-recovery-2026-10-26/summary.json).
진행 상태와 최종 게이트는 [STATUS](STATUS.md)·[PR #51](https://github.com/joejaeyoung/jigeumini/pull/51)에서 확인한다.

## 실행 산출물

clean `bc2ff5b`에서 제품 V12 이미지와 정상·SQL 실패·프로세스 중단 V13 이미지를 만들었다. 같은 코드 revision이며 시험 SQL만 이미지의 classes에 추가한다. 제품 migration 디렉터리는 V12로 유지한다. 각 manifest는 원래 JAR hash·시험 SQL hash·별도 build ID·실제 image ID를 구분하고, 실행 `/livez`의 build ID도 대조한다.

V13은 기존 ranking/trend의 합계를 별도 `release_migration_probe` 테이블에 backfill한다. 그 테이블은 시험 시점의 값이며 새 제품 집계 기능이 아니다. Flyway 11.14.1·PostgreSQL16.14·Kafka4.1.2/RF1·Node24.20.0·합성 채널1개/채팅20건·관측2건·합성 outbox2개를 사용했다.

## 실제 16단계 결과

| 경계 | 주입과 결과 |
|---|---|
| V12 기준선 | 그룹6개·원천4개 초기화, runtime DDL은 SQLSTATE42501, 채팅10건·피드1개 |
| 이전 schema 백업 | API 정지 뒤 pending outbox1개 생성, V12 history와 업무 snapshot을 함께 보관 |
| migration 프로세스 중단 | 새 DDL/backfill을 확인한 sequence1 뒤 컨테이너 KILL137. 해당 PostgreSQL backend 종료 후 새 테이블 없음·기존 history/업무 snapshot 동일 |
| SQL 실패 | DDL/backfill 확인 후 sequence1과 고정 오류, migrate 비정상 종료. 새 테이블/성공 history 없음·기존 값 보존·V12 재기동 정상 |
| V13 적용 | 기존12개 history 보존·V13 추가·backfill ranking10/trend10. 후보 runtime의 build ID 확인 |
| 새 입력 | 채팅10건 추가 후 ranking/trend20·관측/category/report·알림2개 대조 |
| 구 이미지 거부 | V13 DB에 V12 runtime을 시작하면 `SCHEMA_NOT_READY`, 종료2·OOM없음·schema/업무 snapshot 불변. V13 이미지 재기동 정상 |
| V12 새 DB 복원 | 원래 V13 DB를 보존하고 새 `restored` DB에 V12 백업 복원. V12 history와 신규 테이블 부재를 백업과 대조 |
| Kafka 위치 검사 | 기존 최신 group은 `RECOVERY_ALIGNMENT_REQUIRED`로 시작 거부. 새 group6개를 복원 DB 진행 위치에 준비 |
| 보존 입력 재생 | 이전 V12 이미지에서20건·알림2개, 선택한 업무 snapshot 차이0·기존 group offset 불변. 백업 이후 차이10건→0건 |
| 실제 웹 | 8개 경로 새로고침·heading·375px 넘침 없음, 리포트 상세20건 렌더링, API26응답 모두200·JS오류0 |
| 종료·정리 | 마지막 정상 종료1.243초·종료143·OOM없음, 소유 컨테이너/볼륨·생성 secret/dump 제거 확인 |

복원 측정25.163초는 API 정지 후 새DB 생성 직전부터 restore·위치 불일치 거부·새 group 준비·readiness·업무 대조까지다. 원래 V13 schema/backfill 확인을 포함한 전체 복원 phase는27.253초다. 장애 감지·사람 판단·외부 자원 생성 시간은 포함하지 않는다.

복원된 pending outbox 재발행으로 공유 Kafka spikes partition2의 end는2→3으로 증가했다. 논리 피드는2개를 유지했고 기존 알림 group의 committed는2로 불변이다. 원래 group을 재시작하면 이 물리 중복을 추가 소비할 수 있다.

중단 시험의 연결 검사1초는 `SET LOCAL`로 시험 transaction에만 적용했다. PostgreSQL 기본 설정의 즉시 단절 감지를 입증하지 않는다. 미커밋 DDL이 다른 연결에 보이지 않는다는 사실만으로 rollback을 선언하지 않고, 실제 backend 소멸을 먼저 대기한다. [PostgreSQL 연결 검사](https://www.postgresql.org/docs/16/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)

업무 snapshot은 `DeploymentProbe`가 선택한 ledger UUID/source offset·bucket·관측 ID·알림/outbox 상태의 hash와 총계다. 모든 DB 열의 동일성을 의미하지 않는다. 브라우저의503 UI/재시도는 요청 모의이며, API26개200은 복원된 실제 서버의 응답이다. 실패 후보의 SQLSTATE는 직접 수집하지 않았고, backfill 확인 후 marker 도달·migrate 비정상 종료·rollback을 함께 판정했다. `P1300`은 fixture에 지정한 주입 코드다. 결과 JSON의 연결 검사1000ms 등은 설정값 또는 단언에서 파생한 값이다. pgjdbc socket timeout10초 전에 kill해야 하므로 느린 호스트에서는 시험이 실패할 수 있으며, 그때 timeout을 중단137 성공으로 계산하지 않는다.

## 기존 배포 절차 회귀와 초기 실패

공유 리허설 변경을 확인하기 위해 V12 baseline `0227330`→V12 candidate `bc2ff5b`의 기존17단계도 통과했다. 두 이미지는 제품 build ID가 같고 Git revision은 다르다. 새 V13 시험과 별개로 기존 DB 장애·이미지 복귀·checksum 거부·새 DB 복원·브라우저·정리 경로를 보존했다. 하네스20개도 통과했다. `bc2ff5b`의 Claude 정적 리뷰는 APPROVE이며 리뷰어는 도구나 테스트를 실행하지 않았다.

이 호환 실행의 복원 측정은26.540초, 전체 복원 phase는27.799초다. 이전 여섯 실행과 이번 두 실행은 코드·검사 강도가 달라 시간 동등성이나 지속적인 운영 RTO를 입증하지 않는다. 관측한 복원 측정8회의 범위는17.105~26.540초다.

- 첫 `47d5659`는 fixture FROM에 로컬 image ID를 사용해 BuildKit이 외부 저장소 이름으로 해석했다. DB 실행 전 실패했다. 기록한 로컬 tag가 실제 ID와 같은지 빌드 전후 확인하도록 수정했다.
- 두 번째 `0227330`는 dynamic import가 pyc를 만들어 clean-tree 사전 검사에서 거부됐다. DB 실행 전 중단했고 생성 secret은 정리했다. bytecode 생성을 끄고 소유 pyc만 제거했다.
- 두 실패를 최종 성공으로 덮지 않고 [정제 근거](poc/evidence/migration-recovery-2026-10-26/summary.json)에 실행 commit·원인·보완을 보존했다.

## Standards

초기 heuristic1건은 비밀 생성 전 manifest 파싱 순서였다. failed/interrupted manifest를 먼저 읽도록 옮겼고, 보강 `bc2ff5b`의 남은 규칙 위반0·heuristic0·차단0이다. 독립 리뷰어는 실행하지 않았다.

## Spec

초기 차단1건은 미커밋 DDL의 비가시성만으로 transaction 종료를 판정한 부분이었다. 실제 backend 종료 대기와 제한된 연결 검사 설정으로 보완했고, 남은 지적0·차단0이다. 실제 실행에서도 해당 phase가 통과했다.

## 리팩토링과 범위

기존 `Rehearsal`의 자원 소유권·시간 한도·실패 기록·정리는 공유하고, 실행 순서는 `scenarios()`로 분리했다. 복원 대상 이미지 인자는 기존 V12 후보 복원과 새 시험의 V12 이전 이미지 복원이라는 두 사용처를 지원한다. 시험 코드와 SQL은 제품 migration에 들어가지 않는다. 복원 대상 이미지 선택 시점은 DB 복원 전으로 앞당겼고 기존17단계로 영향을 확인했다. 생성한 Docker 이미지는 manifest와 함께 로컬에 남기며, 자동 정리는 소유 Compose 컨테이너/볼륨·secret·dump에 한정한다. `migration-images`가 중간 실패하면 일부 manifest가 남으므로 다음 실행은 새 출력 경로를 쓴다.

T16은 부분 검증을 유지한다. 이번 결과는 transactional V13 fixture와 이전 schema 백업·보존 Kafka 입력의 복구 경로다. 일반적인 파괴적 migration의 가역성, 여러 파일 중 일부만 성공한 릴리스, WAL PITR, 실제 원천 인증, 다중AZ, 외부 배포·비밀 회전·운영 SLA는 별도 범위다.
