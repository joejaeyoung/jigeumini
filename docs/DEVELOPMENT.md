# 개발 구조와 Git Flow

## 모노레포 경계

```text
jigeumini/
  server/               # Spring Boot, 수집·집계·조회 API와 서버 테스트
    build.gradle
    src/
  web/                  # React·TypeScript·Vite
    src/app/            # 상단 메뉴, 경로, 공통 레이아웃
    src/features/       # 방송 조회·상세, 관심 방송
    src/shared/         # API 계약·polling, 공통 UI
    src/styles/         # 확정 디자인의 토큰과 제품 스타일
    tests/              # 합성 API 응답 기반 브라우저 검증
    design-preview/     # 확정 디자인의 원본, 제품 API와 별도
  scripts/              # 서버·웹 설치·실행·검증 명령의 정본
  docs/                 # 설계, 검증 근거, STATUS.md
  compose.yaml          # 로컬 Kafka·PostgreSQL
  settings.gradle       # server Gradle 서브프로젝트
  package.json          # npm workspace, 웹 의존성은 web/package.json
```

실행법은 [루트 README](../README.md), 진행 기록은 [STATUS](STATUS.md)에 둔다. 서버와 웹은 서로의 소스 파일을 가져오지 않으며 HTTP JSON 계약으로 연결한다. 서버의 기존 수집·집계 코드를 이동했고 처리 계약은 바꾸지 않았다. 기존 운영 확인 화면은 `server/src/main/resources/static/index.html`에 보존했다.

## Git Flow

| 브랜치 | 시작점 | PR 대상 | 용도 |
|---|---|---|---|
| `main` | 릴리스 기준 | — | 배포 가능한 릴리스 기준선 |
| `develop` | `main` | `release/<버전>` 분기 | 다음 릴리스 통합 |
| `feat/<주제>` | `develop` | `develop` | 제품 기능 |
| `fix/<주제>` | `develop` | `develop` | 일반 수정 |
| `technicaltest/<주제>` | `develop` | `develop` | 검증 실험 |
| `release/<버전>` | `develop` | `main`, 이후 `develop` 동기화 | 릴리스 안정화 |
| `fix/hotfix-<주제>` | `main` | `main`, 이후 `develop` 동기화 | 긴급 수정 |

기존 저장소의 `fix/` 규칙을 유지하기 위해 긴급 수정은 `fix/hotfix-` 접두사를 쓴다. 릴리스는 `main` 병합 후 `v<버전>` 태그를 만들고 `develop`에도 반영한다. 일반 기능을 `main`으로 바로 병합하지 않는다. 릴리스·긴급 수정의 병합과 배포는 별도 작업이다.

초기 서버 커밋 `44b6321`에서 `main`·`develop`을 만들었다. `feat/chat-pipeline`은 기존 이력으로 보존하고 첫 웹 작업은 `feat/monorepo-web`에서 수행한다. 저장소 기본 브랜치는 `main`이다. CI는 `main` 대상 PR이 `release/*` 또는 `fix/hotfix-*`인지 검사한다. 이는 PR 검사이며 관리자 직접 push를 막는 서버 측 브랜치 보호 규칙은 별도로 설정해야 한다.

앞선 기능 PR의 미병합 코드가 필요한 경우 그 `feat/*`를 기반으로 후속 `feat/*`를 분기하고, PR도 앞선 기능 브랜치를 대상으로 열어 변경 범위를 분리한다. 선행 PR을 `develop`에 병합한 뒤 후속 PR의 기준을 `develop`으로 전환한다.

## GitHub Projects

[지금이니 구현 보드](https://github.com/users/joejaeyoung/projects/1)는 [저장소 Projects](https://github.com/joejaeyoung/jigeumini/projects)에 연결한다.

- 구현 단위로 저장소 이슈를 만들고 보드에 추가한다.
- 작업 전 `Todo` → `In Progress`, 검증과 PR 병합이 끝나면 `Done`으로 옮긴다.
- PR 본문에 이슈와 검증 결과를 연결한다. PR이 열려 있는 동안 완료로 표시하지 않는다.
- 현재 진행 상태의 저장소 기록은 `docs/STATUS.md`에만 둔다.

## 웹과 서버 계약

제품 웹은 `/api/status`, `/api/buckets`와 `/api/channels` 계열을 조회한다. 방송 정보는 [메타데이터 계약](api/BROADCAST_METADATA.md)을 따른다. 개발·로컬 빌드 미리보기는 Vite가 `/api`를 Spring 서버로 프록시하므로 브라우저에 서버 자격 증명을 두지 않는다. `API_PROXY_TARGET`은 개발 서버 프로세스에만 적용되는 주소다. 외부 배포에서는 정적 파일을 제공하는 호스트가 `/api`를 조회 서버로 전달하고, `/channels/...` 등 문서 요청은 `index.html`로 fallback해야 한다. API의 오류를 HTML fallback으로 바꾸지 않는다.

API 응답은 Zod로 실행 시 검사한다. 조회 기간 변경 시 이전 기간 데이터를 지우며, 같은 기간의 갱신 실패는 마지막 데이터·조회 시각과 함께 오류를 표시한다. 요청은 8초 제한과 취소 신호를 사용하고 응답 이후 5초 뒤 다음 요청을 보내 중첩 polling을 막는다.

현재 M1 응답은 진행 중인 10초 구간을 포함하며 수집 공백을 측정하지 않는다. 합계는 수집된 건수이며 검증된 전체 순위·방송 여부·급상승 배수가 아니다. 방송 여부는 독립 메타데이터 조회 결과로 표시한다. 메타데이터가 없거나 만료되면 대체 화면을 쓰며, 관심 채널의 최근 기록이 없어도 0건을 만들지 않는다. 검증된 급상승 판정은 후속 구현이다.

참고한 공식 문서: [Vite 구성](https://vite.dev/guide/), [React Router 경로 구성](https://reactrouter.com/start/declarative/installation). 의존성 버전은 workspace manifest와 루트 lockfile로 고정한다.
