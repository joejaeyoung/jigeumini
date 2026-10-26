# 새 migration 복귀와 PostgreSQL 16 PITR 시험 조사

2026-10-26 KST · #42 → T04/T16, #46 → T02/T04/T16.

조사한 코드 기준은 `43bedc2c07922a2a30647d8050848175e9389f35`다. 이 문서는 공식 자료와 시험 설계이며 실행 통과 근거는 아니다. 실제 구현·실행·리뷰 결과는 [STATUS](../STATUS.md)와 [작업 로그](../WORK_LOG.md)에 연결한다. #42는 논리 백업으로 이전 schema를 복원하고, #46은 physical base backup과 이후 WAL로 복구 목표를 재현한다.

§2는 #42 구현 전 설계다. 최종 구현은 같은 시험 DB에서 DDL/backfill 도달 marker·migrate 비정상 종료·rollback을 대조하며 실제 SQLSTATE를 수집하지 않는다. 아래의 `22012`·별도 실패 DB 제안은 채택하지 않았다. 실제 적용 범위는 [migration 검증](../DEPLOYMENT_MIGRATION_VERIFICATION.md)을 따른다. §3 이후의 PITR도 실행 전 설계다.

외부 자료는 PostgreSQL 16와 Redgate Flyway 공식 문서만 조회했다. PostgreSQL 문서는 16 계열의 현재 문서이며 일부 표시는 16.15다. 실제 시험 이미지는 저장소의 16.14를 고정하고 `SELECT version()`·image ID·도구 버전을 기록해야 한다. Flyway 문서도 현재 배포본을 설명하므로 실행 JAR에 포함된 Flyway 버전을 별도 기록한다.

## 1. 현재 구현에서 재사용할 것과 남은 간격

| 현재 구현 | 확인한 계약 | 이번 시험에서 추가할 것 |
|---|---|---|
| `PostgresSupport.openMigrated` | 명시적 Flyway migrate, 실패 시 pool close | 실제 신규 DDL migration의 성공·실패·부분 변경 rollback |
| `PostgresSupport.openValidated` | `ignoreMigrationPatterns(new String[0])`, validate, pending/current 검사. 실패는 `SCHEMA_NOT_READY` | 구 이미지가 실제 새 migration 적용 DB를 거부하고 DB를 바꾸지 않는지 |
| `deploy-rehearsal.py.schema_failure` | V1 checksum을 SQL로 바꾼 뒤 startup 거부와 checksum 복원 | 이 시험은 DDL 실패가 아니다. 신규 SQL 실행 중 오류 사례가 따로 필요 |
| `backup` / `restore` | writer 정지, pending outbox를 둔 `pg_dump -Fc`, 별도 DB restore, 새 group 6개, 집계 비교 | #42는 구 schema를 실제 복원. #46은 논리 dump와 별개로 전체 PG cluster의 physical base backup/WAL 복구 |
| `RecoveryPlan` | cluster/topic UUID·partition·보존 begin/end·DB next·committed 대조. Kafka가 앞서면 시작 거부. 정지한 원래 group을 보존하고 새 prefix 사용 | PITR 직후의 읽기 전용 상태와 promotion 후 DB/Kafka 정렬을 연결 |
| `DeploymentProbe.snapshot` | 정규화 ledger·bucket·관측·알림·outbox의 일부 열 hash와 총계 | schema/history·PITR marker·WAL/LSN/timeline·실제 새 group 전후를 추가. 모든 DB 열 비교로 확대 해석하지 않음 |

코드 근거: [PostgresSupport](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/server/src/main/java/io/chatpulse/db/PostgresSupport.java), [리허설](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/scripts/deploy-rehearsal.py), [RecoveryPlan](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/server/src/main/java/io/chatpulse/operations/RecoveryPlan.java), [DeploymentProbe](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/server/src/test/java/io/chatpulse/operations/DeploymentProbe.java), [배포 계약](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/deploy/README.md).

## 2. #42: migration의 트랜잭션 경계를 분리한다

Flyway는 기본적으로 **migration 파일 하나씩** 트랜잭션을 적용한다. 실패한 파일은 가능한 경우 rollback하고 다음 파일을 진행하지 않는다. 앞에서 성공한 다른 파일까지 자동으로 되돌리는 계약은 아니다. `group`의 기본값은 false다. [Flyway transaction handling](https://documentation.red-gate.com/fd/migration-transaction-handling-273973399.html), [Flyway group](https://documentation.red-gate.com/flyway/reference/configuration/flyway-namespace/flyway-group-setting)

`executeInTransaction` 기본값은 true지만 script별 설정과 SQL의 transaction 가능 여부가 영향을 준다. `CREATE INDEX CONCURRENTLY`나 `CREATE DATABASE`처럼 transaction block 안에서 실행할 수 없는 명령을 이번 정상적인 transactional DDL rollback fixture에 섞지 않는다. 이런 migration은 별도의 복구 계약이 필요하다. [Flyway executeInTransaction](https://documentation.red-gate.com/fd/flyway-execute-in-transaction-setting-277578997.html), [PostgreSQL CREATE INDEX](https://www.postgresql.org/docs/16/sql-createindex.html), [PostgreSQL CREATE DATABASE](https://www.postgresql.org/docs/16/sql-createdatabase.html)

### 2.1 실패한 신규 DDL migration fixture

아래는 **시험용 SQL 설계 예시**다. production migration 위치에 섞거나 실제 운영 DB에 적용하지 않는다. 실제 번호는 실행 당시 최대 migration 다음으로 결정한다.

```sql
CREATE TABLE release_fixture_change(id integer PRIMARY KEY, value integer NOT NULL);
INSERT INTO release_fixture_change VALUES (1, 42);
ALTER TABLE release_fixture_before ADD COLUMN must_rollback integer;
SELECT 1 / 0;
```

`release_fixture_before`는 실패 시험 전 만들어 둔 합성 표다. 이전 schema 백업으로 만든 별도 DB에 실제 candidate migration runner로 위 파일을 실행한다. SQL 파싱 전에 실패한 경우를 DDL rollback 성공으로 계산하지 않도록, 실행 실패 SQLSTATE와 최종 catalog를 함께 확인한다. PostgreSQL의 `division_by_zero`는 `22012`다. [PostgreSQL 오류 코드](https://www.postgresql.org/docs/16/errcodes-appendix.html)

필수 관측값:

- migration 명령의 비정상 종료, 허용된 migration 실패 코드, 원인 SQLSTATE `22012`.
- `release_fixture_change`가 없고 `must_rollback` 열도 없음. 기존 fixture row·schema 정의·업무 snapshot은 전후 동일.
- Flyway history의 기존 version/checksum/success는 유지되고 새 migration의 성공 행은 없음. 실패 행 여부도 실제로 기록한다.
- `repair`, history 삭제·checksum 변경, 수동 DROP을 실행하지 않아도 구 이미지의 strict validate와 조회가 성공.
- 실패가 난 같은 파일에 쓰인 변경만 rollback됐음을 명시. 여러 pending 파일이 있으면 먼저 성공한 migration이 남는 사례를 별도로 기록하거나 이번 시험에서 pending을 정확히 한 파일로 제한.

원문 SQL 오류 전체를 공유 로그에 저장하지 않는다. 위 합성 파일의 SHA, 단계, SQLSTATE, object 존재 여부, history의 안전한 필드·hash만 남긴다.

### 2.2 실제 새 schema와 구 이미지의 거부

Flyway validate는 적용된 migration과 로컬 파일의 이름·종류·checksum·존재를 대조한다. 기본 ignore pattern은 `*:future`지만 이 프로젝트는 이를 비워 미래 migration도 거부한다. 따라서 새 migration 적용 뒤 구 이미지를 같은 DB에 기동해 **`SCHEMA_NOT_READY`로 종료하는 것**이 현재 정책의 기대값이다. [Flyway validate](https://documentation.red-gate.com/flyway/reference/commands/validate), [ignoreMigrationPatterns](https://documentation.red-gate.com/fd/flyway-ignore-migration-patterns-setting-277579002.html)

실제 candidate artifact에 성공하는 시험 migration을 넣어 적용하고 다음을 단언한다.

| 단계 | 관측·기대 결과 |
|---|---|
| 구 schema + 구 이미지 | validate/readiness 성공, baseline snapshot A |
| 구 schema + 후보 이미지의 runtime | pending migration 때문에 거부, 자동 migrate 없음 |
| 후보의 명시적 migrate | 새 table/column 실제 존재, 새 history version/checksum/success 확인 |
| 새 schema + 후보 이미지 | readiness 성공, 추가 Kafka 입력을 반영한 snapshot B |
| 새 schema + 구 이미지 | `SCHEMA_NOT_READY`, exit!=0, OOM=false, schema/history hash 불변 |

기존 `schema_failure()`의 checksum 변조 결과와 이 결과를 서로 다른 phase로 보관한다. 후보 image ID·Git SHA·JAR SHA·migration 파일 SHA·migration 목록을 명시한다. 시험용 migration을 덧붙인 artifact는 일반 clean production 이미지와 구별해 식별한다. 이미 적용한 파일 내용을 나중에 바꾸거나 checksum을 수동 조정해서 성공시키지 않는다.

### 2.3 이전 schema 복원 후 Kafka 입력 재생

다음은 현재 `Rehearsal.restore`를 확장할 수 있는 순서다.

1. 구 이미지의 schema에서 합성 입력 A를 처리한 후 writer를 모두 정지한다. pending outbox 1개를 만들고 **구 schema/history를 포함한** backup과 A snapshot을 저장한다.
2. 별도 실패 fixture DB에서 2.1을 수행한다. 성공 fixture DB/원래 실행 DB와 실패 잔여 상태를 섞지 않는다.
3. 원래 DB에는 성공하는 새 migration을 적용하고 후보 이미지를 기동한다. 합성 입력 B를 추가하고 최종 B snapshot·committed offset을 얻은 뒤 writer를 정지한다.
4. 구 이미지를 새 schema에 기동해 strict 거부를 확인한다. 원래 DB와 Kafka group은 그대로 보존한다.
5. 구 schema backup을 **새 DB**로 복원한다. 구 migration 목록/checksum, 신규 object 부재, A snapshot을 대조한다. 이 복원 DB에 후보의 migrate를 실행하면 구 schema 복귀 시험의 의미가 사라진다.
6. 구 이미지 + 복원 DB + 기존 최신 group은 `RECOVERY_ALIGNMENT_REQUIRED`로 거부해야 한다. schema 오류와 offset 오류를 단계별로 구분한다.
7. 원래 모든 consumer가 정지했음을 확인하고, 보존 입력이 남았을 때 새 prefix group 6개를 복원 DB next로 준비한다. 구 이미지로 재생해 B의 업무 snapshot과 대조한다.
8. 원래 DB/schema와 원래 group committed는 변화가 없어야 한다. 신규 group만 전진하고 outbox 재발행이 있어도 논리 알림 수는 기대값을 유지해야 한다.

`pg_dump`는 실행 중에도 일관된 snapshot을 만들 수 있지만, 이 서비스의 여러 단계 snapshot·outbox·Kafka 경계는 별개다. 이번 시험은 writer 정지와 같은 고정 입력 범위로 대조를 단순화한다. [PostgreSQL pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html)

이 시험은 새로운 migration이 있는 릴리스에서 **백업 schema 복원 + 보존 이벤트 재생**을 검증한다. 일반적인 SQL downgrade, 모든 파괴적 migration의 가역성, 보존 밖 입력 복구는 입증하지 않는다.

## 3. #46: base backup과 이후 WAL을 모두 사용하는 PITR

`pg_basebackup`은 DB 하나가 아니라 **전체 PostgreSQL cluster**를 physical 파일로 백업한다. replication 권한/접속 허용이 필요하며 `-X stream`은 별도 WAL 연결을 사용한다. plain 형식의 빈 목적지를 쓰고, 이번 시험은 복원에도 같은 16.14 image ID·도구를 사용한다. `-R`은 standby 연결 설정과 `standby.signal`을 만들므로 archive-only PITR 시험에서는 사용하지 않는다. [PostgreSQL pg_basebackup](https://www.postgresql.org/docs/16/app-pgbasebackup.html)

따라서 #42의 `CREATE DATABASE restored` + `pg_restore` 단계를 그대로 PITR이라고 부를 수 없다. #46은 별도의 소유 PGDATA·PostgreSQL 프로세스·WAL archive로 복구한다. physical 복제본은 원래와 system identifier·DB 이름·role이 같을 수 있다. 분리 증거는 container/volume/PGDATA 소유권으로 기록하고 DB 이름 차이를 필수 조건으로 두지 않는다.

### 3.1 격리 환경과 설정

아래는 테스트 설정 초안이다. 기존 공유 PostgreSQL 설정을 변경하지 않는다.

```conf
wal_level = replica
archive_mode = on
archive_command = '/test-tools/archive-wal "%p" "%f"'
hot_standby = on
full_page_writes = on
fsync = on
```

base backup 전에 archiving을 활성화하고 동작을 확인한다. archive helper는 완료된 파일만 성공으로 응답해야 한다. 동일 이름이 있으면 내용이 같고 저장이 완료된 경우만 재성공하고, 다른 내용이면 실패해야 한다. 실패를 숨기면 복구할 수 없는 WAL 공백이 생긴다. [PostgreSQL WAL archiving](https://www.postgresql.org/docs/16/continuous-archiving.html#BACKUP-ARCHIVING-WAL)

시험 helper는 `%f`를 허용된 WAL/history/backup 파일명으로 검사하고, 소유 archive 디렉터리 안에서 임시 파일→완료 파일 전환·hash/크기 검증·파일과 디렉터리 동기화를 마친 뒤 성공을 반환하는 방식이 적절하다. 이것은 하네스 설계 제안이며 클라우드 객체 저장소 내구성을 검증한 것이 아니다. WAL·base backup에는 DB 내용과 role 비밀이 포함될 수 있으므로 공유 근거에는 원본을 넣지 않는다.

실행 전후에 `wal_level`, `archive_mode`, `hot_standby`, `full_page_writes`, `fsync`, `wal_segment_size`, `max_wal_senders`, image/tool 버전을 실제 조회한다. `pg_stat_archiver`의 archived/failed count와 마지막 archive 파일·시각도 기록한다. `last_archived_wal`보다 앞선 파일이 모두 보관됐다고 단정할 수 없으므로 필요한 각 파일의 존재·크기·hash를 별도로 확인한다. [PostgreSQL WAL 설정](https://www.postgresql.org/docs/16/runtime-config-wal.html), [PostgreSQL archiver 통계](https://www.postgresql.org/docs/16/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW)

### 3.2 입력·복구 목표를 만드는 순서

기존 10건씩 두 batch fixture를 그대로 활용하려면 다음 세 상태로 나눌 수 있다.

| 상태 | DB/Kafka 의미 | 목적 |
|---|---|---|
| B0 | migration·source identity·초기 group 완료, 업무 입력 0건 | physical base backup |
| T | batch 0의 10건·관측 반영, writer 정지, pending outbox 1개와 before-target marker commit | 복구 목표 snapshot |
| C | writer 재개, batch 1 추가 후 업무 20건·피드 2개, after-target marker commit | 복구에서 제외돼야 할 최신 상태와 최종 Kafka 재생 기대값 |

fixture 표는 B0 전에 `recovery_fixture_marker(id integer primary key, stage text not null)`처럼 만들고 runtime의 DML 권한을 확인한다. 값은 `before_target`, `after_target`, `after_promotion` 같은 합성 코드만 사용한다.

제안 실행 순서:

1. B0에서 plain base backup을 새 디렉터리에 만든다. 예시 명령 형태는 `pg_basebackup -D <owned-base> -F plain -X stream --checkpoint=fast --manifest-checksums=SHA256`다. 비밀은 argv에 넣지 않는다. backup 종료코드·시작/끝 시각·manifest SHA·WAL range를 보관한다.
2. `pg_verifybackup <owned-base>`를 실행한다. 검증되지 않은 copy를 복구 원본으로 삼지 않는다. 도구 통과가 실제 restore와 업무 검증을 대체하지는 않는다. [PostgreSQL pg_verifybackup](https://www.postgresql.org/docs/16/app-pgverifybackup.html)
3. base backup 완료 이후 새로운 WAL segment로 전환하고, 그 다음 T의 업무 입력·marker를 commit한다. 목표는 backup의 최소 복구 끝보다 뒤에 있어야 한다. manifest의 `Timeline`, `Start-LSN`, `End-LSN`을 남긴다. [PostgreSQL backup manifest WAL range](https://www.postgresql.org/docs/16/backup-manifest-wal-ranges.html)
4. writer가 멈춘 상태에서 유일한 이름 `pitr_<owner>_target`으로 `pg_create_restore_point`를 호출하고 반환 LSN을 기록한다. 같은 이름을 재사용하지 않는다. 목표 직전 snapshot T와 marker를 함께 고정한다. 이후 C를 만들고 원래 writer를 정지한다.
5. `pg_switch_wal()`로 목표와 C가 들어간 segment의 archive를 촉진하고 필요한 파일의 완료·hash를 기다린다. restore point와 WAL 전환 함수는 공식 관리 함수다. [PostgreSQL backup control functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADMIN-BACKUP)

**핵심:** `-X stream`은 backup 도중 필요한 WAL을 backup에 포함한다. 목표와 실패 주입 대상 segment를 **backup 완료 후** 생성하고, 해당 파일이 base의 `pg_wal` 목록에 없음을 확인해야 archive 사용과 archive 누락을 제대로 시험한다. base 안에 이미 있는 파일만 archive에서 지우면 fallback 때문에 음성 시험이 통과할 수 있다. [PostgreSQL pg_basebackup의 WAL 포함 범위](https://www.postgresql.org/docs/16/app-pgbasebackup.html), [복구의 pg_wal fallback](https://www.postgresql.org/docs/16/continuous-archiving.html#BACKUP-PITR-RECOVERY)

### 3.3 새 PGDATA에서 지정 목표로 복구

검증한 base는 불변으로 두고 시험별 새 소유 디렉터리에 복사한다. PostgreSQL을 처음 시작하기 전에 recovery 설정과 signal을 둔다. PostgreSQL 16은 `recovery.conf`를 사용하지 않는다. [PostgreSQL recovery 설정 변경](https://www.postgresql.org/docs/16/recovery-config.html)

```conf
restore_command = '/test-tools/restore-wal "%f" "%p"'
recovery_target_name = 'pitr_<owner>_target'
recovery_target_timeline = '<기록한 원본 timeline의 10진 ID>'
recovery_target_action = 'pause'
hot_standby = on
```

`recovery.signal`만 둔다. `standby.signal`이 함께 있으면 standby가 우선해 WAL을 계속 기다릴 수 있다. streaming의 우연한 보충을 막기 위해 원본 `primary_conninfo`·slot 설정도 이 시험에 상속하지 않는다. restore helper는 파일이 실제 복사됐을 때만 0을 반환하고 없는 파일에는 비정상 종료해야 한다. [PostgreSQL archive recovery 설정](https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-ARCHIVE-RECOVERY)

복구 프로세스는 원본 archive를 읽기 전용으로 사용한다. promotion 후 새 timeline의 WAL이 원본 archive를 오염시키지 않도록 복구 쪽 archiving을 끄거나 별도 owned archive에 보관한다. config 파일에는 secret 값을 포함하지 않고 로그도 allowlist만 남긴다. 실제 서비스·consumer는 아직 연결하지 않는다.

### 3.4 pause와 promotion 전후 검증

복구 중 hot standby는 SELECT 연결을 허용하지만 DML/DDL은 허용하지 않는다. `pg_isready` 성공은 writable 또는 복구 목표 도달의 증거가 아니다. [PostgreSQL hot standby](https://www.postgresql.org/docs/16/hot-standby.html)

**목표에 도달한 pause에서 필수 관측:**

- `pg_is_in_recovery() = true`.
- `pg_get_wal_replay_pause_state() = 'paused'`. `pg_is_wal_replay_paused()`는 pause 요청 여부이므로 이것만 단언하지 않는다.
- `pg_last_wal_replay_lsn()`·`pg_last_xact_replay_timestamp()`·recovery target 설정값·원본 timeline을 기록한다.
- 업무 snapshot = T, before-target marker 있음, after-target marker 없음, 미래 migration을 넣었다면 그 history/object도 목표 시점에 맞음.
- 원본에서 DML 권한을 확인했던 같은 role의 marker INSERT가 SQLSTATE `25006`으로 실패. `42501`이면 권한 문제이므로 읽기 전용 상태 검증으로 계산하지 않음.

관리 함수 의미: [PostgreSQL recovery control](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL). 오류 코드: [PostgreSQL SQLSTATE](https://www.postgresql.org/docs/16/errcodes-appendix.html).

**목표 확인 후 한 가지 복구 종료 경로를 명시적으로 사용한다.** `recovery_target_action=pause`에서 `pg_wal_replay_resume()`를 호출하면 해당 목표에서 복구가 끝난다. 명시적인 promotion을 시험한다면 별도 동일 fixture 또는 같은 pause 상태에서 `pg_promote(true, 60)`을 선택하고 완료 반환을 검사한다. 둘을 이미 끝난 같은 인스턴스에서 연속 호출하지 않는다. 목표 확인 전에 promote하여 누락 WAL을 우회하지 않는다. [PostgreSQL recovery target 동작](https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET), [PostgreSQL pg_promote](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL)

promotion/복구 종료 이후 새 연결에서 `pg_is_in_recovery=false`, `SHOW transaction_read_only=off`, marker INSERT/COMMIT·다른 연결의 SELECT 성공을 확인한다. 업무 snapshot은 시험 marker와 분리해 T와 비교한다. `CHECKPOINT` 후 `pg_control_checkpoint().timeline_id`, 이전 timeline, 새 history 파일을 기록한다. system identifier는 base와 같은지 대조한다. [PostgreSQL control data functions](https://www.postgresql.org/docs/16/functions-info.html#FUNCTIONS-CONTROLDATA)

### 3.5 LSN·시각·timeline의 해석

- 기본 목표는 **named restore point**로 한다. 서버 시간 오차나 transaction commit 순서에 덜 의존하는 재현 가능한 fixture를 만들기 위한 선택이다.
- LSN 목표도 시험하려면 같은 base의 **새 copy**에 `recovery_target_lsn=<기록 LSN>`과 `recovery_target_inclusive=on`을 적용하고 named-target 업무 결과와 비교한다. target name/time/LSN/XID를 동시에 설정하지 않는다. replay LSN이 특정 문자값과 같다는 것만으로 업무 범위를 판정하지 않는다.
- timestamp 사례를 추가하면 UTC offset을 명시하고 commit 경계의 포함/제외를 별도로 검증한다. transaction 시작 시각·eventTime·Kafka receivedAt을 PostgreSQL commit 복구 목표로 바꾸어 읽지 않는다.
- timeline은 원본의 숫자 ID로 고정한다. 기본 `latest`를 그대로 두면 이전 반복 복구에서 만든 child timeline을 따라갈 수 있다. 종료 후 생성된 새 timeline/history를 원본과 구별한다.

설정 의미와 상호 배타 조건: [PostgreSQL recovery target](https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET). timeline의 분기·history: [PostgreSQL timelines](https://www.postgresql.org/docs/16/continuous-archiving.html#BACKUP-TIMELINES).

## 4. 누락·실패를 성공으로 오인하지 않는 음성 시험

| 사례 | 실패 주입 방법 | 통과 판정에 필요한 관측 |
|---|---|---|
| 필수 archive segment 누락 | 원본 archive는 보존하고 **별도 복사본**에서 backup 이후·target 이전 필수 WAL 하나 제외. base pg_wal에도 없음을 증명 | 명시 target에 도달하지 못하고 FATAL/비정상 종료. writable 성공 0회, 업무 snapshot T 달성 실패, 하네스가 PITR 실패로 기록 |
| restore point 이름 오타 | 존재하지 않는 유일 이름 지정, 정상 archive 사용 | target 미달로 실패. `pg_isready`가 잠깐 성공해도 PASS 금지 |
| base 파일 손상 | 새로운 base copy의 작은 fixture data 파일을 변조 | `pg_verifybackup` 비정상 종료, DB 시작하지 않음 |
| archive helper 실패 | owned archive 목적지의 의도된 쓰기 실패 | 완료 성공으로 응답하지 않음, archiver 실패/지연 관측, fixture 입력·디스크 상한 안에서 종료 |
| target 전에 조기 promote | 정상 경로에서는 실행하지 않는 금지 동작 | 하네스가 target pause·marker 검증 전에는 promotion 단계를 호출하지 않도록 상태 순서 검사 |

PostgreSQL은 명시한 recovery target에 도달하기 전에 archive recovery가 끝나면 FATAL로 종료한다. `standby.signal` 때문에 계속 기다리거나 단순 timeout만 난 결과는 이 실패 원인을 검증한 것으로 계산하지 않는다. [PostgreSQL recovery target 실패](https://www.postgresql.org/docs/16/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET)

`.history` 또는 끝 이후 WAL을 찾지 못하는 요청은 정상 복구에서도 나타날 수 있다. restore helper의 모든 not-found를 즉시 치명적 실패로 해석하지 않는다. **필수 파일 부재 + 목표 미달 + writable 불가**를 함께 판정한다. [PostgreSQL archive 복구](https://www.postgresql.org/docs/16/continuous-archiving.html#BACKUP-PITR-RECOVERY)

`pg_verifybackup`은 manifest가 요구하는 backup 복구 WAL을 검사하며, 그 이후 모든 WAL의 유효성이나 목표 T까지의 복구를 보장하지 않는다. 따라서 base 검증 통과와 이후 archive 누락 실패는 동시에 성립할 수 있다. [PostgreSQL pg_verifybackup 범위](https://www.postgresql.org/docs/16/app-pgverifybackup.html)

archive가 빠진 target 복구를 통과시키려고 원본 PGDATA의 이후 WAL을 복사하거나 target을 없애거나 `recovery.signal`을 지우는 작업은 하지 않는다. 이런 변경은 시험 실패를 숨긴다. 보완 후 새 owned copy로 다시 실행하고 실패 근거를 보존한다.

## 5. PITR 뒤 Kafka 위치 정렬

이 절은 PostgreSQL의 보장에 더해 적용하는 **현재 프로젝트 코드 계약**이다. PostgreSQL WAL 복구는 별도 Kafka group의 committed offset을 되돌리지 않는다. 현재 RecoveryPlan은 아래 값을 이미 대조하므로 재사용한다. [RecoveryPlan](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/server/src/main/java/io/chatpulse/operations/RecoveryPlan.java)

1. 원래 수집·consumer·outbox·평가·관리 writer를 정지하고, 원래 group committed C와 topic bounds를 고정한다. 원래 DB와 복원 DB를 동시에 같은 projection writer로 실행하지 않는다.
2. PITR target T의 snapshot을 **promotion 전·직후** 각각 확인한다. 그다음 replay로 C까지 따라잡는 단계는 별도 결과다. final C만 보면 PITR가 T에서 멈췄는지 알 수 없다.
3. promotion된 복원 DB에서 `ops recovery-plan`을 조회한다. clusterId/topicId/partitions 동일, `begin <= databaseNext <= end`를 확인한다. existing committed > databaseNext이면 `KAFKA_AHEAD_DATABASE`가 기대값이다.
4. 기존 group으로 startup하면 `RECOVERY_ALIGNMENT_REQUIRED`를 확인하고 DB/schema/기존 offsets가 바뀌지 않았는지 대조한다.
5. `ops recover-groups <새-prefix>`로 DB next의 group 6개를 준비한다. 기존 group은 정지·불변, 새 group은 처음 존재하지 않았음, 초기 새 offset이 **실제 restored DB next와 일치함**을 조회한다.
6. 복원 앱을 기동해 보존 입력을 C까지 재생한다. ranking/trend, observation/category/report, notification/outbox의 선택한 ID·hash·총계와 기존 expected C를 비교한다. pending outbox 재발행의 물리 Kafka 중복과 논리 알림 중복을 분리한다.
7. 새 group plan이 `ALIGNED` 또는 계약상 `SAFE_OVERLAP`인 상태인지 검사하고 기존 group 전후 committed map은 그대로인지 비교한다.
8. 별도 폐기 가능한 topic에서 보존 begin이 restored DB next를 넘게 만든 경우 `RECOVERY_RETENTION_GAP`으로 새 group 준비와 정상 시작을 거부한다. schema/restore 성공을 전체 데이터 복구 성공으로 표시하지 않는다.

현재 prepareNewGroups는 여러 group을 한 번에 원자적으로 만드는 transaction이 아니다. 부분 실패하면 만들어진 새 group을 기록하고 새 prefix로 재계획한다. 유지 보수 동안 source identity·bounds·group inactivity가 바뀌지 않게 통제한다. 해당 조건은 [배포 복구 계약](https://github.com/joejaeyoung/jigeumini/blob/43bedc2c07922a2a30647d8050848175e9389f35/deploy/README.md)을 따른다.

## 6. 정제 근거 필드와 종료 조건

### 공통 식별·안전 범위

- issue/기술 ID, 실행 시각·host 환경, Git SHA·JAR SHA·image ID, PostgreSQL 서버/pg_basebackup/pg_verifybackup/Flyway 버전.
- synthetic seed·input UUID 집합 hash, owned project/container/volume ID, 단계별 제한 시간·메모리·디스크 상한.
- 시험을 수행한 source/restore PGDATA의 소유권과 분리, 원래 실행의 writer 정지 관측.
- 원본 채팅·닉네임·viewer ID·토큰·DB 비밀·backup/WAL 원본은 공유 증거에서 제외. 소유 자원만 정리하고 secret/base/archive 생성 파일은 성공적인 정리 후 제거.

### #42 필수 결과

`baselineMigrationSet`, `candidateMigrationSet`, 파일별 SHA, 실제 history schema version/checksum/success, 신규 object 전후, DDL 실패 SQLSTATE, failed migration 전후 업무 snapshot, 구 이미지 startup 거부 원인, 복원 schema/history 검증, 새 group 초기 offset 대조, 최종 업무 차이, 기존 DB/group 불변.

### #46 필수 결과

`baseBackupStartedAt/FinishedAt`, `backupManifestSha256`, WAL range/timeline, archive 파일명·크기·SHA, 목표 이름/LSN, 원본 control system/timeline, `restore_command`의 실제 성공 파일 집합, backup 이후 archive 사용 증거, pause/recovery/read-only 상태, marker 포함·제외, promotion 이후 writable·새 timeline, 누락 archive 실패, PITR-T와 replay-C의 별도 snapshot, DB/Kafka offset map.

wall clock의 복구 시작·완료 외에 단계 시간은 monotonic clock으로 잰다. `물리복구 시작→target pause`, `promotion→writable`, `새group 준비→정합 catch-up`을 분리한다. 이 표본은 로컬 합성 환경의 관측 시간이며 운영 RTO/SLA가 아니다. target 이후 DB 전용 marker의 제외는 의도된 PITR 결과이고, Kafka에 남은 입력의 재생 가능 범위와 따로 보고한다.

### 완료 판정

- #42: 실제 신규 migration 성공·transactional DDL 실패 rollback·구 이미지 strict 거부·이전 schema 복원·보존 Kafka 재생이 모두 연결되고 최종 업무 결과가 맞아야 완료다.
- #46: 유효 base + **이후 archive WAL**로 목표 T를 복원하고 목표 이후 입력 배제·promotion 전후 쓰기 가능성·필수 WAL 누락 실패·Kafka 재정렬 후 C 결과를 모두 검증해야 완료다.
- 이 문서 작성만으로 어떤 phase도 통과 처리하지 않는다. 실제 실패·수정·재실행과 리팩토링 점검은 해당 이슈의 WORK_LOG·STATUS·정제 근거에 남긴다.

## 7. #42 리뷰에서 보강한 클라이언트 중단 판정

미커밋 DDL은 다른 연결에서 보이지 않으므로, catalog가 원래대로 보인다는 사실만으로 migration transaction이 종료됐다고 판정하지 않는다. 시험 연결에 고정 application_name을 두고 실제 backend PID를 조회한다. 클라이언트 컨테이너 KILL137 뒤 해당 PID가 사라진 후 catalog·history·업무 snapshot을 대조한다.

PostgreSQL의 `client_connection_check_interval` 기본0은 실행 중 연결 검사를 끈다. 이 경우 다음 socket 상호작용까지 연결 상실 감지가 지연될 수 있다. interrupted fixture에만 `SET LOCAL ... = '1s'`를 설정하고 그 범위를 결과에 기록한다. 이는 운영 기본값의 즉시 종료 보장이 아니다. [PostgreSQL16 연결 검사](https://www.postgresql.org/docs/16/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)
