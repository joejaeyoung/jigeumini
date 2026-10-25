# 웹

확정한 밝은 방송 탐색 디자인을 React·TypeScript·Vite로 구현한다. 실행 명령은 [루트 README](../README.md), 모노레포와 Git Flow는 [개발 구조](../docs/DEVELOPMENT.md), 현재 진행 상태는 [STATUS](../docs/STATUS.md)를 따른다.

| 위치 | 책임 |
|---|---|
| `src/app/` | 상단 메뉴, 경로, 공통 안내 |
| `src/features/broadcasts/` | 서버 집계 조회, 검색, 카드, 채널 상세 |
| `src/features/saved/` | 브라우저 관심 채널 저장·복원 |
| `src/shared/api/` | 응답 스키마, HTTP, polling·취소·오류 상태 |
| `src/shared/ui/` | 모달, 아이콘, 배수별 리본 |
| `src/styles/` | 확정 디자인 토큰과 반응형 제품 스타일 |
| `public/images/` | AI 제작 서비스 일러스트 |
| `tests/` | 합성 응답 기반 Playwright 시나리오 |
| `design-preview/` | 이전 디자인 시안 원본, 합성 방송 데이터 |

제품 경로는 `/`, `/explore`, `/surges`, `/saved`, `/search?q=...`, `/channels/:channelId`다. 관심 방송은 실제 형식의 채널 ID만 저장하며 시안의 가상 ID는 옮기지 않는다.

현재 서버가 제공하지 않는 방송 제목·이미지·카테고리·급상승 수치를 만들어 표시하지 않는다. `ReactionRibbon`은 향후 검증된 배수를 전달할 때 사용하는 디자인 컴포넌트다. 썸네일은 대체 화면이며 홈 이미지는 서비스 일러스트로 표시한다. 이미지 출처와 생성 프롬프트는 [원본 기록](design-preview/assets/README.md)을 따른다.

상단 메뉴와 공통 레이아웃은 유지하고 새 기능은 `features/`와 `App.tsx`의 경로 정의에 추가한다. 브라우저용 코드에서 수집 프로토콜이나 Kafka를 직접 다루지 않는다. 테스트의 API 대체 응답은 테스트 실행에만 존재하고 제품 빌드에는 포함하지 않는다.
