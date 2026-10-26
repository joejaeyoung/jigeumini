# PostgreSQL 시점 복구 검증 #46

F16/F08 → T02/T04/T16. clean `4a6b7a82af1396ba80207dd3698687bc35ac0714`의 실제12단계가2026-10-26에 통과했다. [조사](research/POSTGRES_RECOVERY.md)의 PostgreSQL16 physical backup/WAL/명시 target 계약을 따른다.

## 시나리오

1. 소유 source DB에 archive를 켜고 physical base backup·SHA256 manifest를 `pg_verifybackup`으로 확인한다.
2. base 이후 별도 WAL의 T에서 업무10건·pending outbox1개·before-target marker를 commit하고 유일한 named restore point를 만든다. C까지 업무20건·알림2개·after-target marker를 추가한 뒤 writer를 정지한다. 필요한 모든 segment의 base/archive 포함 여부와 hash를 기록한다.
3. 새 PGDATA와 원본 archive의 읽기 전용 mount로 T를 복구한다. 실제 recovery paused·snapshot T·marker0/1·runtime 쓰기25006 거부·restore_command가 사용한 target WAL을 확인한다.
4. 이 확인 뒤 `pg_wal_replay_resume` 한 경로로 복구를 종료한다. 쓰기 가능·별도 연결 marker commit·같은 system identifier·새 timeline/history hash를 확인한다.
5. 원래 Kafka group의 ahead 상태로 startup 거부를 확인한다. 새 prefix의 최초 offset 부재와 준비 직후 전 partition의 DB next==commit·ALIGNED를 확인한 뒤 실제 분석기를 시작한다. C까지 재생하여 선택한 업무 snapshot 항목을 대조하고 원래 group commit 불변을 확인한다.
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

clean commit과 같은 image revision을 요구한다. 실제 실행일·환경·입력·결과는 [정제 근거](poc/evidence/postgres-pitr-2026-10-26/summary.json)에 있다. 물리 복원→목표 pause, 복구 종료→writable, 새group 준비→검증된 catchup 시간을 구분한다. 로컬 단회·동일 호스트 archive이며 외부 archive 내구성·운영 RTO/RPO/SLA를 확정하지 않는다.


## 실제 실행 결과

2026-10-26 03:00:10.979~03:01:43.582 UTC. PostgreSQL16.14·wal_level=replica·archive_mode=on·fsync/full_page_writes=on, WAL segment16MiB, 원본timeline1이다. 첫 실행은 자원 사전 검사에서 중단됐다. 실제 trace에서 사용량3,363,517,235.2byte+최대4,093,640,704byte가Docker8,217,145,344byte의90%를 넘었다. 30분 부하와 Kafka 디스크 실험이 모두 끝난 뒤, 사용자가 허용한 개발Kafka/DB를 `./scripts/infra down`(volume 삭제 옵션 없음)으로 종료하고 같은HEAD로 재실행했다. 개발 volume의 전후 checksum은 측정하지 않았다. PITR 종료 뒤 #44 통합 시험을 위해 `./scripts/infra up`으로 다시 기동했다. 다른 서비스와 worktree는 변경하지 않았다.

physical base는1,399파일·SHA256 manifest·pg_verifybackup을 통과했다. base에 포함된 WAL은000000010000000000000003, targetLSN은0/5051778이다. base 시작0/3000028부터 필요한3개 segment(끝3/4/5)가 모두 base/archive에 있고 hash를 기록했다. target WAL(끝5)은 base에 없어 archive를 실제 사용하는 복원을 검증한다.

| 단계 | 실제 결과 |
|---|---|
| 필수WAL 누락 | 별도 archive copy에서끝5만 제거, 나머지hash동일. restore는끝3/4를 사용했으나 목표미달로exit1·하네스 분류 RECOVERY_ENDED_BEFORE_TARGET(목표미달 로그 문구 일치, PostgreSQL SQLSTATE 아님). 원본archive보존 |
| 목표pause | PGDATA복사·설정 뒤 기동부터2.785초. replayLSN0/5051778·paused·marker0/1만 존재·T snapshot 일치·runtime쓰기SQLSTATE25006 |
| 승격 | pg_wal_replay_resume 뒤2.190초에쓰기·별도연결commit확인. 같은systemIdentifier·timeline2·history hash, T snapshot유지 |
| Kafka재정렬·재생 | 원래group으로시작거부, 새6group의최초commit없음→모든partition DBnext==commit/ALIGNED 확인 후분석기시작. 21.083초에chat10건추가복원·C snapshot차이0·원래group commit불변 |
| 보존공백 | 별도복원T에서chat partition0을begin11로삭제. DBnext10인4projection은RETENTION_GAP. 새group준비RECOVERY_RETENTION_GAP, offset생성없음·기존group/DB불변·시작거부 |

시간은 각 표의 시작/종료 경계에 한정하며 전체 재해 복구 SLA가 아니다. 목표T의chat10·pendingoutbox1에서 최신C의chat20·알림2로 재생해 업무snapshot을 대조했다. 선택 snapshot 항목(ID/hash/count·귀속) 기준의 관측 누락은0이지만 임의 wall-clock RPO초수나 삭제된 원천까지0손실을 보장하지 않는다. T의 pending outbox 재발행으로 알림 Kafka 물리 레코드는3개, 논리 알림은2개다. 기존 deploy-live-notifications의 commit2/DBnext3은 SAFE_OVERLAP이며 재생 후 ALIGNED로 표현하지 않는다. 새 group 준비 직후의 ALIGNED와 원래 group commit 불변은 별도 관측이다.

named restore point로 고정한 목표이며 임의 timestamp target은 실행하지 않았다.

pg_stat_archiver의 archived_count6·failed_count4를 함께 기록했다. 이번 실행은 필요한segment3개를 모두 확보하고 실제사용·hash를 확인했으나, 앞선archive실패의 원인 로그는 수집하지 않아 미확정이다. archive오류0회나 장기 archive 안정성을 주장하지 않는다.

physical base/archive/PGDATA와 생성비밀은 전부 소유volume에만 두었다. 종료시volume·container·비밀제거를 확인했고 공유근거는설정/LSN/파일명/hash/count/코드만 남겼다. `./scripts/check` 단위147·하네스20·JAR도 통과했다.

## 리팩토링·리뷰

기존 배포 리허설의 소유권·이미지identity·업무snapshot·그룹정렬을 재사용하고, WAL archive/restore와 물리복원단계만 분리했다. probeclass를실행별고정하고 실제 삭제전clusterID를검사한다. 소유helper cleanup도maintenance profile을포함한다. 최초64537f8은Claude APPROVE FOR EXPERIMENT였고 후속비차단의소유권/WAL차이검사를반영했다. 최종실제근거HEAD의Claude·push/PR CI는별도게이트다.

최종 보완에서 실제 cluster ID 불일치 가드와 maintenance cleanup의 실패 경로를 별도 음성 실행한 것은 아니다. 코드 가드 존재와 성공 실행의 정리 통과 범위로만 기록한다. archive 실패4회의 원인은 여전히 미확정이며 오류 없는 archive 운영의 근거로 사용하지 않는다.
