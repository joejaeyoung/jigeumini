# 로컬 배포·복구 패키지

#30 / F16 → T04/T08/T16. 공급자 계정·외부 자원·main 릴리스는 포함하지 않는다. 이 Compose는 단일 호스트/RF1 절차 리허설이며 [HA 구성](../docs/pipeline/KAFKA_HA.md)의 실제 다중 장애 영역 배포를 대신하지 않는다.

## 실행 역할과 설정

- 서버/웹 이미지는 clean git commit으로만 만들고 OCI revision·실제 image ID·JAR hash·buildId를 manifest에 묶는다. 서버 USER10001, 웹 USER101, 읽기 전용 root·capability 제거·tmpfs·메모리/CPU 상한을 사용한다.
- web만 loopback의 임의 포트를 공개한다. web/API service network와 API/DB/Kafka data network를 분리하고 DB/Kafka host port를 공개하지 않는다. Kafka PLAINTEXT는 이 격리 로컬망에 한정한다. 외부 운영은 인증/TLS·권한·egress 정책을 먼저 적용한다.
- 비밀은 `/run/secrets/` 파일로 읽는다. 이미지·Git·환경변수 값에 넣지 않는다. Compose 파일 mount는 클라우드 secret manager의 보장을 대신하지 않는다.
- migration 서비스는 owner 계정으로 명시 실행한다. runtime은 DML과 조회만 가능한 별도 계정이며 Flyway checksum·미적용·미래 migration을 검사해 실패하면 종료한다. 소스 topicId/clusterId/partition과 복원 DB 진행 위치·Kafka group offset도 consumer 기동 전에 검사한다.
- API와 분석기는 현재 한 JVM이다. 조회만 별도 증설할 역할 분리는 아직 제공하지 않는다. 수집은 별도 `fleet` CLI를 사용하며 legacy collector와 함께 켜지 않는다. fleet의 시청자 관측도 임대·공유 HTTP 예산을 따르고 HA 정책을 명시 전달한다.
- `/livez`는 프로세스, `/readyz`는 최근 DB·topic ISR/leader·consumer 상태 표본이다. stale 표본은 실패하며 원천 접속 성공/채팅 품질 정상과는 별개다. shutdown 중 readiness를 내리고 Spring/consumer 종료를 기다린다. Compose는60초 유예를 준다.

## 이미지 만들기

```bash
./scripts/deploy-images work/images.json
# 서로 다른 두 clean commit에서 만든 manifest를 사용한다.
./scripts/deploy-rehearsal work/images-baseline.json work/images-candidate.json work/deploy-result
```

Dockerfile의 Java/NGINX base는 digest로 고정했다. 빌드 manifest는 로컬 image ID를 기록하며 registry push를 하지 않는다. 실제 배포는 동일 이미지를 registry digest로 고정해야 한다. Compose 환경에는 SERVER_IMAGE/WEB_IMAGE, UUID DEPLOYMENT_OWNER, KAFKA_CLUSTER_ID, 비밀 파일 경로를 지정한다. 기본 DB는chatpulse, group은deploy-live다.

새 설치는 DB/Kafka 시작→`migrate`→`ops bind-sources`→`ops initialize-groups`→API/web 순서다. 쓰기 관리 명령은 `DEPLOYMENT_MAINTENANCE=true`를 명시한다. 초기화와 복구는 자동 runtime 동작이 아니다. 재시작 때 initialize를 다시 실행하지 않는다.

## 복구 계약

`ops recovery-plan`은 읽기 전용이다. 원래 topic identity가 없거나 달라지면 실패한다. 각6개 group/8개 topic 관계에 대해 보존 시작/끝·DB next·Kafka committed를 대조한다. Kafka가 DB보다 앞선 상태는 시작을 거부한다. 보존 시작이 DB next를 넘었으면 완전 복구 가능으로 표시하지 않는다.

원래 환경의 모든 consumer와 수집/DB writer를 정지하고, 백업을 **새 DB**에 복원한다. 같은 Kafka 보존 범위가 남아 있는지 확인한 뒤 `ops recover-groups <새-prefix>`로 기존 group을 바꾸지 않고 복원 DB 진행 위치의 새 group6개를 만든다. 같은 prefix의 재사용과 활성 group을 거부한다. 전 작업은 maintenance/quiescence 전제이며 Kafka/DB를 포괄하는 원자적 복구 트랜잭션이 아니다. 중간 실패 시 일부 새 group이 생길 수 있으므로 새 prefix로 다시 계획한다.

전체 원천 입력이 없으면 복원할 수 없다. 과거 데이터만 복원한 DB를 기존 최신 group과 바로 기동하지 않는다. outbox의 ACK/DB 완료 사이 중복은 알림의 멱등 consumer가 처리한다.

## 운영 구성·용량·검증

[배포·용량·복구 운영안](../docs/DEPLOYMENT_OPERATIONS.md)에 초기/HA/관리형 후보, 역할별 자원 상한·비용 입력식, 경보/확대 중단 조건과 실제 복구 시간의 범위를 기록했다. 수집기는 동일 서버 이미지의 fleet 진입점을 사용하며 공개 원천 호출은 기본 비활성이다. 배포 worker도 schema VALIDATE를 사용하고 SIGTERM 결과 저장 전에 관측/telemetry/producer를 정리한다.

## 공식 근거

2026-10-26 조회. [Docker secrets](https://docs.docker.com/compose/how-tos/use-secrets/)의 서비스별 파일 mount, [Flyway validate](https://documentation.red-gate.com/flyway/reference/commands/validate)의 migration 일치 검사, [PostgreSQL pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html)의 논리 백업과 일관 snapshot, [NGINX 비특권 이미지](https://github.com/nginx/docker-nginx-unprivileged)의 실행 경로를 확인했다. 복원 DB의 Kafka 위치 검사는 이 프로젝트의 추가 계약이다. 운영 후보·비용 산정 입력은 [조사](../docs/research/KAFKA_DEPLOYMENT.md)를 따른다.

실제 실행·실패·브라우저·종료·RPO/RTO 근거는 docs/STATUS.md에서 관리하며 실행 전 구성만으로 검증 완료라 표시하지 않는다.
