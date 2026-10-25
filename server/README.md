# 서버

기존 Spring Boot 수집·집계·조회 구현의 Gradle 서브프로젝트다. 소스·테스트·리소스와 `build.gradle`을 이 디렉터리로 이동했고 Kafka·DB 처리 계약은 유지했다.

루트 Gradle wrapper를 사용한다. 실행과 검증 명령은 [루트 README](../README.md), 이벤트·트랜잭션·offset 계약은 [파이프라인 문서](../docs/pipeline/README.md)를 따른다. 빌드 출력은 `server/build/`이며 루트 `scripts/`가 실행 JAR를 `work/runtime/`로 복사한다.

기존 서버의 정적 운영 확인 화면은 보존한다. 사용자용 제품 웹은 `web/`에서 별도로 빌드하며 `/api/status`, `/api/buckets`를 호출한다.
