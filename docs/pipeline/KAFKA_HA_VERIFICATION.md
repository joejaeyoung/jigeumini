# Kafka HA 장애·복구 검증

#29 / F15 → T04/T08/T10/T15 · 2026-10-26(KST)

## 실행과 환경

```bash
./scripts/kafka-ha work/ha-fourth.json
```

01e943e에서 apache/kafka4.1.2 전용controller3개·broker3개,RF3/minISR2,ELR1,unclean=false,acks=all/idempotence,Java21/PostgreSQL16.14로 실행했다. 호스트하나·합성입력·소유schema/cluster만 사용했다. [원본4회와 정제요약](../poc/evidence/kafka-ha-2026-10-26/summary.json)에 코드commit·시각·partition별leader/replica/ISR·ACK·offset·DB·정리를 남겼다.

## 실제 결과

| 단계 | 결과 | 단계 소요초 |
|---|---|---:|
| 정상RF3 | 100/100ACK·DB,lag0 | 1.457 |
| 현재leader broker1대중단 | 나머지ISR2·leader교체,20/20ACK·DB,lag0 | 9.002 |
| broker재합류 | ISR3 회복 | 3.437 |
| broker2대중단/controller정상 | ISR1·앱HA시작거부·1발행ACK0. end-offset조회불가를미관측으로기록 | 29.495 |
| ISR복구 | ISR3,20/20ACK·DB·lag0 | 4.757 |
| 현재controller1대중단 | controller리더교체·새토픽생성·10/10ACK | 4.458 |
| controller2대중단 | 새metadata쓰기ACK실패,기존topic의1건은ACK성공 | 5.617 |
| quorum복구 | 새토픽쓰기회복·20/20ACK·DB | 3.479 |
| 기존RF1→RF3 | HA시작거부→실제replica재배치→ISR3/minISR2,전후각10ACK·topicId불변 | 1.861 |
| 최종읽기대조 | main시도172/ACK171/broker171/DB171,이행topic20/20/20/20 | 0.473 |

모든ACK 입력이복구broker에존재했고broker와DB집합차이0·bucket총합일치다. ISR1에서미확인1건은최종broker에없었다. 이를모든미확인의손실보장으로일반화하지않는다. 단계시간은정지호출·감지·대기·검증을포함하며순수선출시간이나운영RTO가아니다. metadata과반손실이기존데이터요청을즉시모두막는다는가정은실제1ACK 관측과맞지않는다.

## 실패와 계측 보완

첫세원본도보존했다. 모두소유자원정리는통과했다.

1. 첫실행은ISR1진단의ExecutionException으로중단했다. 세부조회가없어당시원인을확정하지않는다.
2. 두번째에서는GROUP offsets조회가성공하고END_OFFSETS_READ가TimeoutException임을확인했다. 해당구간lag는0으로채우지않는다. controller전환뒤새토픽준비실패는별도원인체인관측을추가했다.
3. 세번째는controller전환뒤TimeoutException/DisconnectException1회를기록한후유한재시도로준비됐다. 과반손실/복구까지통과하고새RF1토픽의별도Admin조회에서UnknownTopicOrPartitionException을관측했다. 실험Admin도이일시오류만10초내재조회하며횟수를기록한다. 영구설정불일치는재시도하지않는다.
4. 최종실행은10단계모두통과했다. 이전실패원인을나중실행의예외로소급확정하지않았다. 제품의초기화기한은변경하지않았다.

## 정리·리팩토링·한계

LocalKafkaCluster가소유UUID·6개컨테이너·전용네트워크·명시적데이터볼륨을관리한다. 이미지의다른VOLUME두경로는tmpfs이며정리전소유자라벨을검사한다. 최종조회에서실험컨테이너/볼륨0개,기존chat-pulse와다른프로젝트는유지됐다. KafkaTopicPolicy는생성/검사불변식을모으고각기능이같은정책을전달한다. 장애실험코드는테스트소스에만있다.

실제3AZ·호스트손실·디스크손상·장기부하·원천수집완전성은미검증이다. ACK0/1/커밋대조는#31,이미지배포/DB복원은#30이다. 최종단위/실제통합/Claude/CI/병합상태는[STATUS](../STATUS.md)에서관리한다.
