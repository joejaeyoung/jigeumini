# 로컬 배포·복구 패키지

#30 / F16 → T04/T08/T16. 공급자 계정·외부 자원·main 릴리스는 포함하지 않는다. 이 Compose는 단일 호스트/RF1 절차 리허설이며 [HA 구성](../docs/pipeline/KAFKA_HA.md)의 실제 다중 장애 영역 배포를 대신하지 않는다.

## 실행 역할과 설정

- 서버/웹 이미지는 clean git commit으로만 만들고 OCI revision·실제 image ID·JAR hash·buildId를 manifest에 묶는다. 서버 USER10001, 웹 USER101, 읽기 전용 root·capability 제거·tmpfs·메모리/CPU 상한을 사용한다.
- web만 loopback의 임의 포트를 공개한다. web/API service network와 API/DB/Kafka data network를 분리하고 DB/Kafka host port를 공개하지 않는다. Kafka PLAINTEXT는 이 격리 로컬망에 한정한다. 외부 운영은 인증/TLS·권한·egress 정책을 먼저 적용한다.
- 비밀은 `/run/secrets/` 파일로 읽는다. 이미지·Git·환경변수 값에 넣지 않는다. Compose 파일 mount는 클라우드 secret manager의 보장을 대신하지 않는다.
- migration 서비스는 owner 계정으로 명시 실행한다. 로컬 owner는 PostgreSQL 초기 superuser이며 runtime은 DML과 조회만 가능한 별도 계정이며 Flyway checksum·미적용·미래 migration을 검사해 실패하면 종료한다. 소스 topicId/clusterId/partition과 복원 DB 진행 위치·Kafka group offset도 consumer 기동 전에 검사한다.
- API와 분석기는 현재 한 JVM이다. 조회만 별도 증설할 역할 분리는 아직 제공하지 않는다. 수집은 별도 `fleet` CLI를 사용하며 legacy collector와 함께 켜지 않는다. fleet의 시청자 관측은 `OBSERVATIONS_ENABLED=true`로 명시 활성화한다. live-detail 뒤 offline 확인을 위해 두 번째 HTTP가 필요한 경우까지 매 요청에서 임대·공유 예산을 검사하며 HA 정책을 전달한다.
- `/livez`는 프로세스, `/readyz`는 최근 DB·topic ISR/leader·consumer 상태 표본이다. stale 표본은 실패하며 원천 접속 성공/채팅 품질 정상과는 별개다. shutdown 중 readiness를 내리고 Spring/consumer 종료를 기다리도록 구성한다. Compose는60초 유예를 주고 healthcheck는15초 간격이다. 리허설의 종료 시간/코드만으로 진행 중 HTTP 요청 완료까지 검증했다고 보지 않는다. 기동 실패는 원문 없이 허용된 고정 원인 코드와 예외 타입명만 stdout에 남긴다.

## 이미지 만들기

```bash
./scripts/deploy-images work/images.json
# 서로 다른 두 clean commit에서 만든 manifest를 사용한다.
./scripts/deploy-rehearsal work/images-baseline.json work/images-candidate.json work/deploy-result
```

Dockerfile의 Java/NGINX base는 digest로 고정했다. PostgreSQL/Kafka는 버전 태그이며 그 이미지의 root/쓰기·초기 권한은 앱 hardening과 별개다. `deploy-images.py`를 직접 호출하지 않고 빌드부터 수행하는 정본 래퍼를 사용한다. 동일 commit의 bit-for-bit 재현을 보장하는 대신 manifest로 실행 산출물을 추적한다. 빌드 manifest는 로컬 image ID를 기록하며 registry push를 하지 않는다. 실제 배포는 동일 이미지를 registry digest로 고정해야 한다. Compose 환경에는 SERVER_IMAGE/WEB_IMAGE, UUID DEPLOYMENT_OWNER, KAFKA_CLUSTER_ID, 비밀 파일 경로를 지정한다. 기본 DB는chatpulse, group은deploy-live다.

migrate는 maintenance profile의 명시적 작업이며 API의 dependency에서 제외했다. 새 설치는 DB/Kafka 시작→`migrate`→`ops bind-sources`→`ops initialize-groups`→API/web 순서다. 쓰기 관리 명령은 `DEPLOYMENT_MAINTENANCE=true`를 명시한다. 초기화와 복구는 자동 runtime 동작이 아니다. 재시작 때 initialize를 다시 실행하지 않는다.

## 복구 계약

`ops recovery-plan`은 읽기 전용이다. 원래 topic identity가 없거나 달라지면 실패한다. 각6개 group/8개 topic 관계에 대해 보존 시작/끝·DB next·Kafka committed를 대조한다. Kafka가 DB보다 앞선 상태는 시작을 거부한다. 보존 시작이 DB next를 넘었으면 완전 복구 가능으로 표시하지 않는다.

원래 환경의 모든 consumer와 수집/DB writer를 정지하고, 백업을 **새 DB**에 복원한다. 같은 Kafka 보존 범위가 남아 있는지 확인한 뒤 `ops recover-groups <새-prefix>`로 기존 group을 바꾸지 않고 복원 DB 진행 위치의 새 group6개를 만든다. 같은 prefix의 재사용과 활성 group을 거부한다. 전 작업은 maintenance/quiescence 전제이며 Kafka/DB를 포괄하는 원자적 복구 트랜잭션이 아니다. 중간 실패 시 일부 새 group이 생길 수 있으므로 새 prefix로 다시 계획한다.

전체 원천 입력이 없으면 복원할 수 없다. 과거 데이터만 복원한 DB를 기존 최신 group과 바로 기동하지 않는다. outbox의 ACK/DB 완료 사이 중복은 알림의 멱등 consumer가 처리한다.

## 운영 구성·용량·검증

[배포·용량·복구 운영안](../docs/DEPLOYMENT_OPERATIONS.md)에 초기/HA/관리형 후보, 역할별 자원 상한·비용 입력식, 경보/확대 중단 조건과 실제 복구 시간의 범위를 기록했다. 수집기는 동일 서버 이미지의 fleet 진입점을 사용하며 공개 원천 호출은 기본 비활성이다. 배포 worker도 schema VALIDATE를 사용하고 SIGTERM 결과 저장 전에 관측/telemetry/producer를 정리한다.

## 공식 근거

2026-10-26 조회. [Docker secrets](https://docs.docker.com/compose/how-tos/use-secrets/)의 서비스별 파일 mount, [Flyway validate](https://documentation.red-gate.com/flyway/reference/commands/validate)의 migration 일치 검사, [PostgreSQL pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html)의 논리 백업과 일관 snapshot, [NGINX 비특권 이미지](https://github.com/nginx/docker-nginx-unprivileged)의 실행 경로를 확인했다. 복원 DB의 Kafka 위치 검사는 이 프로젝트의 추가 계약이다. 운영 후보·비용 산정 입력은 [조사](../docs/research/KAFKA_DEPLOYMENT.md)를 따른다.

실제 실행·실패·브라우저·종료·RPO/RTO 근거는 docs/STATUS.md에서 관리하며 실행 전 구성만으로 검증 완료라 표시하지 않는다.

## migration이 포함된 릴리스와 비밀 수명

이번 이미지 교체/복귀는 동일 V12 migration 집합만 검증했다. checksum 손상 시험은 실패한 DDL migration을 재현한 것이 아니다. strict VALIDATE는 새 migration이 적용된 DB에서 이전 이미지를 거부한다. 따라서 migration 포함 릴리스의 복귀는 같은 DB의 이미지 교체로 처리할 수 없으며, 이전 schema 백업을 새DB에 복원하고 보존 입력/진행 위치를 검사해 재생하는 절차가 필요하다. 이 경로에서 현재 제품 schema 다음 번호를 사용하는 시험 전용 migration 리허설은 루트 README의 `migration-images`/`migration-rehearsal` 명령으로 실행한다. 시험 migration은 기존 집계의 합계를 별도 점검 테이블에 backfill하며 제품 동작은 바꾸지 않는다. 실제 실행 결과는 STATUS를 따른다. 파괴적 변경은 별도 forward fix/복구 계획이 필요하다.

배포 또는 VALIDATE 모드에서 비밀 파일이 없으면 `SECRET_FILE_REQUIRED`로 거부한다. 로컬 리허설은 생성한 secret 파일을0700 디렉터리 안에0444로 두어 UID10001/컨테이너DB가 읽게 한다. 실제 운영은 비밀 관리자의 서비스별 UID/GID·접근 권한으로 주입한다. init-db.sh는 새 PG 데이터 디렉터리에서만 실행되며 기존 볼륨 역할 생성이나 비밀 회전을 수행하지 않는다. 비밀은 psql argv에 전달하지 않는다. 성공적인 소유 자원 정리 뒤 생성 secret과 dump도 삭제하고 hash/숫자만 남긴다. 정리 실패 시 로컬 work를0700 경계 안에서 보관하고 소유 자원 확인 후 수동 정리한다.

현재 `/readyz` 상세 본문은 loopback 로컬 진단용이다. 외부 gateway에서는 내부 운영 접근으로 제한하거나 상태 코드만 공개한다. Kafka 인증/TLS·PG standby·중앙 지표·pager·비밀 회전은 실제 공급자 환경의 별도 운영 구성이다.

최신 실행의 명령·이미지·원인코드·실제API/화면·복구 대조·리뷰 보완은 [배포 검증](../docs/DEPLOYMENT_VERIFICATION.md)을 참조한다.

관측 요청 예산은 `채널수 × (ceil(실행초/관측간격초)+경계여유1회) × 최대2HTTP + 채팅방/토큰/재접속 요청`으로 계산한다. 현재최대3채널·30분·60초 간격의관측은경계여유를포함해186회이며 최초채팅준비6회를합쳐192회, 총500회에서308회가재접속 등에남는다(간격/연결패턴에따른계획값). 실제총량은BudgetedChatSource카운터가제한한다. 로컬예산/임대거절로HTTP를생략하면원천오류관측을만들지않고다음간격까지기다린다. 관측은명시opt-in이다.

init SQL은로컬세션의statement/error statement로깅을끄지만CREATE ROLE의실제SQL에는비밀이포함된다. 실제관리형DB의감사로그정책과비밀회전은별도설정이필요하다. healthcheck15초간격은이번로컬합성기동범위이며고부하시probeRSS/CPU/플래핑은미검증이다. UI의503은브라우저요청모의,실제프록시503은DB중단HTTP검증으로구분한다.

관측 로컬 생략은 `SourceFailure`(예산/임대)만 처리하며 worker 최종 결과의 `observations.skippedRequests`에 누적한다. 예상하지 못한 hook 오류는 기존 오류 관측 경로로 드러난다. `CollectorWorker.Limits.defaults`는 총500회를 지정하고 생성자가30분 초과 실행을 거부하므로 예산 식의 실행시간 상한을 강제한다. 관측 활성 FleetCli의 실제 원천 연결·종료 조립은 이번 리허설에서 실행하지 않았으며 활성화 전 해당 환경에서 확인한다.

2026-10-27 #65의 제품 V13은 리포트 세션별 최신 관측용 정렬 색인이다. V12까지의 실제 검증 기록은 당시 근거이며, 현재 `migration-images`는 제품 migration 다음 번호와 이전 version 목록을 manifest에 기록한다. migration 적용 DB에서 이전 이미지로 단순 복귀할 수 없다는 계약은 같다.
