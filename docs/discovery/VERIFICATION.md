# 방송 선정 검증

2026-10-26 · #26 / F12 → T13/T08/T11. 새 공개 수집이나 공식 자격 증명 호출을 수행하지 않았다.

`SelectionPolicyTest`5개, `OfficialBroadcastCatalogTest`9개, `SelectionCliTest`2개가 합성 입력으로 통과했다. 실제 loopback HTTP의 페이지 이동·채널 합계 누락·중복·원문 필드 제거·opaque cursor encoding·cursor 반복·방송 ID 충돌·요청/페이지 상한·429·401/500/redirect 거부·잘못된JSON·깨진 계약·느린 body·응답 크기 제한을 확인했다. 순수 정책은 하한 경계·null/0·상한·안정 동률·명시 제외·성인/미확인·TTL·빈 분모를 대조했다. CLI는 같은 입력 결과 일치·덮어쓰기 거부·비활성 기본값·미허용 fixture 필드 거부를 확인했다.

실행 JAR의 `selection fixture`는 Spring 서버·Kafka·DB 없이 예제 입력을 읽고 정제 JSON을 출력했다. 리뷰 보완b200134의 ./scripts/check에서 Java101·하네스16·JAR가 통과했다. [정제 근거](../poc/evidence/selection-2026-10-26.json)에 JAR/XML hash와 명령을 남겼다. 실행JAR로 cutoff미설정/0/1000/5000/10000을 같은 예제에 적용해 적격4/3/2/1/0, 선정3/3/2/1/0을 확인했다. Claude 승인·최종 CI는 STATUS에서 기록한다. 이 파일의 단위 통과를 실제 원천 검증이나 서비스 용량으로 확대하지 않는다.

리뷰에서 비동기 HTTP handler의 단언 전달 누락을 확인해 query/헤더를 테스트 본문에서 대조하도록 수정했다. nanoTime 초기값·출력 부모 선검사·정책5종을 보완하고 Java101/하네스16/JAR 및 정책별 실제 CLI를 다시 통과했다. 기존fae9531 근거는 JSON previousVerification에 보존한다.
