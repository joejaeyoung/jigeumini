#!/usr/bin/env python3
"""완료한 합성 실험 JSON을 정제된 비교 표와 독립 HTML로 만든다."""
import argparse
import hashlib
import html
import json
from pathlib import Path


def maximum(samples, key, default=None):
    values = [s.get('resourceUsage', {}).get(key) for s in samples]
    return max((v for v in values if isinstance(v, (int, float))), default=default)


def summarize(path):
    raw = path.read_bytes()
    data = json.loads(raw)
    if 'finishedAt' not in data or 'correct' not in data:
        raise ValueError(f'완료 결과가 아닙니다: {path.name}')
    row = {'file': path.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'codeCommit': data.get('codeCommit'),
           'runtimeClassesSha256': data.get('runtimeClassesSha256'), 'startedAt': data['startedAt'],
           'finishedAt': data['finishedAt'], 'environment': data.get('environment'), 'outcome': data['outcome'],
           'correct': data['correct'], 'cleanupComplete': data.get('cleanupComplete')}
    row['measurementIssueCodes'] = sorted({event['code'] for event in data.get('events', [])
                                          if event['code'] == 'RESOURCE_OR_SAMPLE_FAILED' or event['code'].endswith('_EXPERIMENT_FAILED')})
    row['cleanupIssueCodes'] = (['UNTRACKED_IMAGE_VOLUMES'] if data.get('profile') == 'broker-fault'
                                and data.get('cleanupEvidenceScope') != 'CONTAINER_AND_LABELLED_DATA_VOLUME' else [])
    row['cleanupEvidenceScope'] = data.get('cleanupEvidenceScope')
    row['verificationComplete'] = bool(data['correct'] and data.get('cleanupComplete')
                                       and not row['measurementIssueCodes'] and not row['cleanupIssueCodes'])
    if data.get('profile') == 'broker-fault':
        return row | {'profile': 'broker-fault', 'cases': data['cases'], 'scope': data['scope']}
    profile = data['profile']
    samples = data.get('samples', [])
    producer = data.get('producer', {})
    metrics = data.get('metricsA', {})
    row.update(profile=profile['name'], plan=profile, seed=data['seed'], eventTimeBase=data.get('eventTimeBase'),
               targets=data['targets'], attemptedRecords=data.get('attemptedRecords'), generatedUnique=data.get('generatedUnique'),
               generatorSeconds=data.get('generatorSeconds'), generatorScheduleLagMillis=data.get('generatorScheduleLagMillis'),
               producer=producer, publisherDrainSeconds=data.get('publisherDrainSeconds'), producerSettledSeconds=data.get('producerSettledSeconds'),
               acknowledgedPerSecondIncludingDrain=data.get('acknowledgedPerSecondIncludingDrain'), pendingObservedAfterRejection=data.get('pendingObservedAfterRejection'), producerMetrics=data.get('producerMetrics'), secondProducerMetrics=data.get('secondProducerMetrics'),
               databaseA=data.get('databaseA'), databaseB=data.get('databaseB'), metricsA=metrics, metricsB=data.get('metricsB'),
               metricsAScope=data.get('metricsAScope', 'ALL_GROUP_MEMBERS'), postInputDrainSeconds=data.get('postInputDrainSeconds'),
               expectedBucketsMatch=data.get('expectedBucketsMatch'), replayManifest=data.get('replayManifest'),
               replayBucketDifferences=data.get('replayBucketDifferences'), replaySeconds=data.get('replaySeconds'),
               appendOrderAudit=data.get('appendOrderAudit'), events=data.get('events'), failureCode=data.get('failureCode'))
    row['countConservation'] = (data.get('attemptedRecords') == producer.get('accepted', 0) + producer.get('rejected', 0)
                               and producer.get('accepted') == producer.get('acknowledged', 0) + producer.get('unconfirmed', 0))
    row['resourcePeaks'] = {k: maximum(samples, k) for k in ['clientHeapUsedBytes', 'clientRssBytes', 'clientCpuLoad',
                                                          'clientCpuNanos', 'kafkaTopicBytes', 'databaseSchemaBytes']}
    row['maximumPending'] = max((s.get('pending', 0) for s in samples), default=None)
    row['maximumOldestUnackedMillis'] = max((s.get('oldestUnackedMillis', 0) for s in samples), default=None)
    row['sampleCount'] = len(samples)
    row['maximumSampleCostSeconds'] = max((s.get('sampleFinishedSeconds', s['seconds']) - s['seconds'] for s in samples), default=None)
    row['groupMeasurements'] = {}
    for group in ['groupA', 'groupB']:
        valid = [s for s in samples if group in s]
        if not valid:
            continue
        intervals = []
        for first, last in zip(valid, valid[1:]):
            seconds = last['seconds'] - first['seconds']
            if seconds <= 0:
                continue
            intervals.append({'fromSeconds': first['seconds'], 'toSeconds': last['seconds'],
                              'inputPerSecondByPartition': [(b-a)/seconds for a, b in zip(first['sourceEnd'], last['sourceEnd'])],
                              'commitPerSecondByPartition': [(b-a)/seconds for a, b in zip(first[group]['committed'], last[group]['committed'])]})
        row['groupMeasurements'][group] = {
            'maximumLagByPartition': [max(s[group]['lag'][p] for s in valid) for p in range(3)],
            'maximumLagTotal': max(sum(s[group]['lag']) for s in valid),
            'observedAssignments': sorted({tuple(sorted(c['assignedPartitions'] for c in s[group]['consumers'])) for s in valid}),
            'rates': intervals}
    recovery = []
    for event in data.get('events', []):
        if event['code'] not in ['ANALYZER_RESTARTED', 'DB_FAULT_END', 'POLL_STALL_END', 'HEARTBEAT_RESUMED', 'CONSUMER_REMOVED']:
            continue
        caught = next((s for s in samples if s['seconds'] >= event['seconds'] and sum(s['groupA']['lag']) == 0), None)
        recovery.append({'after': event['code'], 'eventSeconds': event['seconds'],
                         'firstObservedZeroLagAfterSeconds': None if caught is None else caught['sampleFinishedSeconds']-event['seconds']})
    row['recoverySamples'] = recovery
    return row


def number(value, digits=1):
    return '—' if value is None else f'{value:,.{digits}f}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path, help='새 출력 폴더')
    parser.add_argument('inputs', nargs='+', type=Path)
    args = parser.parse_args()
    rows = [summarize(path) for path in args.inputs]
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / 'summary.json').write_text(json.dumps({'formatVersion': 1, 'experiments': rows}, ensure_ascii=False, indent=2)+'\n')
    header = ['실행 파일', '결과', '입력/목표', 'ACK', 'DB A/B', 'p95/p99 A (ms)', '최대 lag A', '입력 후 drain (s)', 'replay 차이']
    table = []
    labels = {'TARGET_INPUT_COMPLETED': '목표 완료', 'TARGET_INPUT_NOT_COMPLETED': '목표 미달', 'RECONCILIATION_FAILED': '대조 실패', 'EXPERIMENT_FAILED': '실험 실패', 'CLEANUP_FAILED': '정리 실패', 'RECONCILED': '대조 완료'}
    for row in rows:
        if row['profile'] == 'broker-fault':
            table.append([row['file'], ('정리 확인 필요' if row['correct'] and row['cleanupIssueCodes'] else labels.get(row['outcome'], row['outcome']))] + ['—'] * (len(header)-2))
            continue
        latency = row.get('metricsA', {}).get('receivedToDbMillis', {})
        db_a, db_b = row.get('databaseA') or {}, row.get('databaseB') or {}
        outcome_label = '계측 확인 필요' if row['measurementIssueCodes'] else labels.get(row['outcome'], row['outcome'])
        table.append([row['file'], outcome_label, f"{row['generatedUnique']}/{row['targets']['uniqueEvents']}",
                      str(row['producer'].get('acknowledged', '—')), f"{db_a.get('processedEvents', '—')}/{db_b.get('processedEvents', '—')}",
                      f"{number(latency.get('p95'))}/{number(latency.get('p99'))}",
                      str(row['groupMeasurements'].get('groupA', {}).get('maximumLagTotal', '—')),
                      number(row.get('postInputDrainSeconds')), str(row.get('replayBucketDifferences', '—'))])
    notes = [
        '단일 로컬 broker·RF1·공유 PostgreSQL의 합성 실험이다. 공개 플랫폼에 부하를 주지 않았다.',
        '목표 입력 완료, 수락/ACK/거절/미확인 보존, DB 정합성, 자원 정리를 각각 판정한다. 실패도 결과에 포함한다.',
        '과거 broker-fault의 cleanupComplete는 컨테이너 제거만 확인했다. 이미지 익명 볼륨의 삭제 근거는 없으며 소유권 없는 잔여 볼륨을 임의 삭제하지 않는다.',
        '이전 하네스는 입력 종료 후 계측 실패를 correct에 반영하지 못했다. 해당 원본을 보존하고 계측 확인 필요로 별도 판정한다.',
        '지연은 receivedAt부터 실제 DB commit 뒤 계측 시각까지다. heartbeat 실험의 A 지연은 부모 JVM 표본만 포함한다.',
        '입력률/commit률은 표본 차분이다. Kafka·DB 조회는 원자적 snapshot이 아니며 표본 간 비용과 오차가 있다.',
        '회복 시간은 재개 뒤 최초 0 lag 표본까지의 관측 상한이다. 표본 사이 순간 최대 lag/정확한 회복 시각은 모른다.',
        'JVM 메모리는 계측용 eventId/지연 표본도 포함한다. Docker CPU/메모리/IO는 공유 서비스 전체 값이다.',
        '균등 채널이 균등 partition을 뜻하지 않는다. 3 partitions에서 4번째 consumer의 유휴 할당을 실제 표본으로 확인한다.',
        '복제 HA·외부 네트워크·다중 호스트·실방송·장기 디스크 보존·전체 제품 projection의 동시 비용은 검증하지 않았다.',
    ]
    totals = f"전체 {len(rows)}회 · 원본 입력 목표 완료 {sum(r['outcome']=='TARGET_INPUT_COMPLETED' for r in rows)}회 · 목표 미달 {sum(r['outcome']=='TARGET_INPUT_NOT_COMPLETED' for r in rows)}회 · 대조 실패 {sum(not r['correct'] or not r['cleanupComplete'] for r in rows)}회 · 계측 확인 필요 {sum(bool(r['measurementIssueCodes']) for r in rows)}회 · 볼륨 정리 근거 없음 {sum(bool(r['cleanupIssueCodes']) for r in rows)}회"
    markdown = '# 지금이니 합성 부하·복구 실험\n\n' + totals + '\n\n' + '\n'.join('- '+n for n in notes) + '\n\n'
    markdown += '| '+' | '.join(header)+' |\n|'+ '|'.join(['---']*len(header))+'|\n'
    markdown += '\n'.join('| '+' | '.join(line)+' |' for line in table)+'\n\n'
    markdown += '환경·버전·seed·eventTime·입력 manifest·표본별 partition 처리율·자원·장애 시각은 [summary.json](summary.json)에 있다. 상세 원본 JSON의 파일명과 SHA256으로 대조한다.\n'
    (args.output / 'README.md').write_text(markdown)
    html_table = '<table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in header)+'</tr></thead><tbody>'
    html_table += ''.join('<tr>'+''.join('<td>'+html.escape(cell)+'</td>' for cell in line)+'</tr>' for line in table)+'</tbody></table>'
    details = ''.join('<details><summary>'+html.escape(row['profile'])+'</summary><pre>'+html.escape(json.dumps(row, ensure_ascii=False, indent=2))+'</pre></details>' for row in rows)
    document = '<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>지금이니 부하·복구 실험</title><style>body{margin:32px;font:15px/1.7 system-ui;color:#173c30}h1{font-size:28px}table{border-collapse:collapse;white-space:nowrap}th,td{padding:10px;border-bottom:1px solid #cdded5;text-align:right}th:first-child,td:first-child{text-align:left}thead{background:#e6f5ee}.table{overflow:auto}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f1f6f3;padding:16px}summary{padding:12px;cursor:pointer}</style><h1>합성 부하·복구 실험</h1><p>'+html.escape(totals)+'</p><ul>'+''.join('<li>'+html.escape(n)+'</li>' for n in notes)+'</ul><div class="table">'+html_table+'</div><h2>프로필별 근거</h2>'+details+'</html>'
    (args.output / 'index.html').write_text(document)
    print(f'{len(rows)}개 결과: {args.output / "index.html"}')


if __name__ == '__main__':
    main()
