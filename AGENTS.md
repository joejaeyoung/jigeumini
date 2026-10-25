# Chat Pulse 작업 안내

이 저장소는 치지직 채팅 분석 프로젝트다. 현재 구현은 Spring 익명 수신과 Kafka·PostgreSQL 10초 집계 파이프라인이다. 시작점은 [README.md](README.md)다.

## 작업 경계

- 문서·설명·커밋 제목은 한국어, 코드 식별자는 영어로 쓴다.
- 변경 전 `git status --short --branch`를 확인한다. 실험 브랜치는
  `technicaltest/<주제>`, 제품 구현은 `feat/<주제>`, 수정은 `fix/<주제>`를 쓴다.
- 이 저장소의 규칙을 적용한다. 참고한 gromo의 Jira·배포·원격 리뷰 자동화는 이식하지 않는다.
- 진행 상태는 [docs/STATUS.md](docs/STATUS.md) 한 곳에서 관리한다.

## 변경별 확인 자료

- 수집 프로토콜·재접속 변경: [docs/poc/PROTOCOL.md](docs/poc/PROTOCOL.md)를 먼저 읽고
  `ChatProtocolTest`, `ChzzkHttpSourceTest`, `ChannelProbeTest`의 해당 사례를 갱신한다.
- Kafka 발행·DB 집계·offset 변경: [M1 처리 계약](docs/pipeline/README.md)을 읽고 `./scripts/integration`으로 실제 broker/DB 검증을 실행한다.
- 제품 범위·Kafka·DB 설계 변경: [docs/design/README.md](docs/design/README.md)를 먼저 읽는다.
- 하네스 변경: 실행 명령은 `scripts/`가 정본이며 사용법은 루트 README에 둔다.

## 검증과 기록

- Java 변경 후 `./scripts/check`로 테스트와 실행 JAR 빌드를 확인한다.
- live 검증은 `./scripts/poc`로 최대 3개 공개 채널에서 수행하고, 로컬 fixture 통과와 구분한다.
- 채팅 송신·최근 이력 요청은 구현하지 않는다. 익명 READ 연결과 heartbeat만 송신한다.
- 원문·닉네임·사용자 ID·토큰은 파일·로그·테스트 결과에 남기지 않는다.
  fixture는 합성 데이터를 사용하고 진단은 허용된 코드와 숫자로 기록한다.
- 임시 결과는 gitignored `work/`, 공유할 정제 결과는 `docs/poc/evidence/`에 둔다.
  실제 실행일·입력·명령·결과·검증하지 못한 항목을 함께 남긴다.
