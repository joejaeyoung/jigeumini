# PostgreSQL 시점 복구 검증 #46

F16/F08 → T02/T04/T16. 구현·사전 정적 검토 단계이며 실제 실행 결과는 아직 없다. [조사](research/POSTGRES_RECOVERY.md)의 PostgreSQL16 physical backup/WAL/명시 target 계약을 따른다.

## 시나리오

1. 소유 source DB에 archive를 켜고 physical base backup·SHA256 manifest를 `pg_verifybackup`으로 확인한다.
2. base 이후 별도 WAL의 T에서 업무10건·pending outbox1개·before-target marker를 commit하고 유일한 named restore point를 만든다. C까지 업무20건·알림2개·after-target marker를 추가한 뒤 writer를 정지한다. 필요한 모든 segment의 base/archive 포함 여부와 hash를 기록한다.
3. 새 PGDATA와 원본 archive의 읽기 전용 mount로 T를 복구한다. 실제 recovery paused·snapshot T·marker0/1·runtime 쓰기25006 거부·restore_command가 사용한 target WAL을 확인한다.
4. 이 확인 뒤 `pg_wal_replay_resume` 한 경로로 복구를 종료한다. 쓰기 가능·별도 연결 marker commit·같은 system identifier·새 timeline/history hash를 확인한다.
5. 원래 Kafka group의 ahead 상태로 startup 거부를 확인한다. 새 prefix의 최초 offset 부재와 준비 직후 전 partition의 DB next==commit·ALIGNED를 확인한 뒤 실제 분석기를 시작한다. C까지 재생하여 모든 업무 snapshot을 대조하고 원래 group commit 불변을 확인한다.
6. 별도 archive 복사본에서 base에 없던 필수 target WAL 하나를 제외한다. 명시 target 미달 FATAL과 비정상 종료를 확인한다. 원본 archive는 그대로 보존한다.
7. 또 다른 새 PGDATA를 T까지 복구·검증·종료한 뒤 실제 Kafka chat begin11>DBnext10을 만든다. 네 chat projection의 RETENTION_GAP, 새group 준비의 RECOVERY_RETENTION_GAP, startup 거부, 새group offset 부재·기존group 불변·DB snapshot 불변을 확인한다.

## 자원·정리

원본 PGDATA, base, archive, restored/missing/gap PGDATA와 누락용 archive copy는 서로 다른 소유 label volume이다. physical DB/WAL이나 생성 비밀을 host 공유 근거로 복사하지 않는다. helper는 network none, 원본 archive는 복구 컨테이너에서 readonly다. 기본 Compose 예산 외에 복구PG3×384MiB와 helper256MiB를 사전 여유 계산에 넣는다. 다른 프로젝트와 worktree는 수정하지 않는다.

정제 결과는 backup manifest hash, 필수 WAL 이름/hash, 시각·LSN·timeline, 업무 snapshot hash/count, 실제 오류 코드, 단계 시간, group 위치를 포함한다. probe class는 실행별 고정/hash 대조한다. 종료 시 이번 실행의 container/volume/비밀을 제거하고 정리 확인을 남긴다.

## 실행

```sh
./scripts/deploy-images work/pitr-images.json
./scripts/postgres-pitr work/pitr-images.json work/pitr-result
```

clean commit과 같은 image revision을 요구한다. 실제 실행일·환경·입력·결과·검증 범위는 실행 뒤 이 문서와 근거에 추가한다. 물리 복원→목표 pause, 복구 종료→writable, 새group 준비→검증된 catchup 시간을 구분한다. 로컬 단회·동일 호스트 archive이며 외부 archive 내구성·운영 RTO/RPO/SLA를 확정하지 않는다.
