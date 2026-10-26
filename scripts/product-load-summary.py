#!/usr/bin/env python3
"""소유한 부하 실행 JSON에서 비교 가능한 숫자와 원본 hash만 추출한다."""
import json,hashlib,math,sys,collections,datetime
from pathlib import Path

def distribution(values):
    v=sorted(values)
    def quantile(p):
        if not v:return None
        at=(len(v)-1)*p;lo=int(at);hi=math.ceil(at)
        return round(v[lo]+(v[hi]-v[lo])*(at-lo),3)
    return {'samples':len(v),'min':min(v,default=None),'p50':quantile(.5),'p95':quantile(.95),'p99':quantile(.99),'max':max(v,default=None)}

def summarize(directory,manifest):
    r=json.loads((directory/'result.json').read_text());l=r['load'];fault=r.get('fault')
    recovery_end=(r['analysis'].get('recovery') or {}).get('recoveryWindowEndEpochMillis',fault['resumedAtEpochMillis'] if fault else None)
    def overlaps(a,b):return fault and a<=recovery_end and b>=fault['stoppedAtEpochMillis']
    def scope(a,b):
        if fault and a<=fault['resumedAtEpochMillis'] and b>=fault['stoppedAtEpochMillis']:return 'duringStop'
        if fault and a<=recovery_end and b>=fault['resumedAtEpochMillis']:return 'duringRecovery'
        return 'normal' 
    s={k:r.get(k) for k in ['scope','profile','startedAt','finishedAt','correct','cleanupComplete','sensitiveArtifactsRemoved','probeExitCode','probeOomKilled','fixtureClasses','loadContract']}
    s['revision']=r['candidate']['revision'];s['images']=r['candidate'];s['command']=f'./scripts/product-load {manifest} {r["profile"]} {directory}'
    s['analysisRevision']=r['candidate']['revision'];s['clockBrackets']=r.get('clockBrackets');s['clockBracketMeaning']='INTERVAL_BOUND_WITH_ROUND_TRIP_UNCERTAINTY; NOT_EXACT_SKEW_OR_PROOF_OF_ABSOLUTE_SKEW_UNDER_250MS'
    s['files']={name:{'bytes':(directory/name).stat().st_size,'sha256':hashlib.sha256((directory/name).read_bytes()).hexdigest()} for name in ['result.json','probe-output/load.json','probe-output/offsets.jsonl','probe-output/samples.jsonl'] if (directory/name).exists()}
    s['phases']=[{k:p[k] for k in ['name','startedAt','passed','seconds','result'] if k in p} for p in r['phases']]
    for k in ['plan','seed','channels','inputContract','inputSeconds','maximumPacingDelayMillis','expectedUnique','expectedPhysical','expectedObservations','expectedOutbox','drainSeconds','reconciliation','receivedToTransactionTimestampMillis','healthyRankingEntries']:
        s[k]=l[k]
    s['publisherFinal']={k:v for k,v in l['publisherFinal'].items() if k!='channels'}
    s['measurementCoverage']=r['analysis']['measurementCoverage'];s['apiStatus']=r['analysis']['api'];s['fault']=fault
    recovery=r['analysis']['recovery'];s['recovery']=None if recovery is None else {k:v for k,v in recovery.items() if k not in ['target','recovered']}
    # 원래 sample은 5초 고정 지연이므로 actual interval과 N을 함께 보관한다.
    api=collections.defaultdict(list);errors=collections.defaultdict(collections.Counter)
    for sample in l['samples']:
        for response in sample['api']:
            key=response['path'];stage=scope(response['startedAtEpochMillis'],response['finishedAtEpochMillis'])
            api[(key,stage)].append(response['millis']);errors[(key,stage)][str(response.get('status',response.get('error')))]+=1
    s['apiLatencyMillis']=[{'path':key,'scope':scope,**distribution(values),'status':dict(errors[(key,scope)])} for (key,scope),values in sorted(api.items())]
    normal=[x for x in l['samples'] if not overlaps(x['startedAtEpochMillis'],x['finishedAtEpochMillis'])]
    s['normalDatabaseCountQueryMillis']=distribution([x['databaseQueryMillis'] for x in normal if 'database' in x])
    s['memoryBytes']={k:distribution([x[k] for x in l['samples'] if k in x]) for k in ['analyzerHeapUsedBytes','generatorHeapUsedBytes','generatorRssBytes']}
    s['memoryBytes']['analyzerRssBytes']=distribution([x['analyzerRssBytes'] for x in r['resourceSamples'] if 'analyzerRssBytes' in x])
    s['maximumSampledBufferAgeMillis']=max(x['publisher']['oldestAgeMillis'] for x in l['samples'])
    s['minimumHostFreeDiskBytes']=min(x['hostFreeDiskBytes'] for x in r['resourceSamples'])
    cpu=collections.defaultdict(list)
    for sample in r['resourceSamples']:
        for c in sample['containers']:
            service=c['Name'].removeprefix(r['project']+'-').removesuffix('-1');cpu[service].append(float(c['CPUPerc'].removesuffix('%')))
    s['containerCpuPercent']={k:distribution(v) for k,v in cpu.items()}
    raw=[json.loads(line) for line in (directory/'probe-output/offsets.jsonl').read_text().splitlines()]
    groups=collections.defaultdict(list)
    for sample in raw:
        for p in sample.get('partitions',[]):groups[(p['group'],p['topic'],p['partition'])].append((sample,p))
    summaries=[]
    for final in l['finalOffsets']:
        key=(final['group'],final['topic'],final['partition']);rows=groups[key]
        rates=collections.defaultdict(lambda:{'input':[],'committed':[],'lag':[]})
        for sample,p in rows:
            if p.get('lag') is not None:rates[sample['phase']]['lag'].append(p['lag'])
        for (sa,a),(sb,b) in zip(rows,rows[1:]):
            dt=(b['observedAtEpochMillis']-a['observedAtEpochMillis'])/1000
            if dt>0 and sa['phase']==sb['phase']:
                rates[sb['phase']]['input'].append((b['end']-a['end'])/dt)
                if a.get('committed') is not None and b.get('committed') is not None:rates[sb['phase']]['committed'].append((b['committed']-a['committed'])/dt)
        summaries.append({k:final[k] for k in ['group','topic','partition','begin','end','committed','lag']}|{'sampledByPhase':{stage:{metric:distribution(v) for metric,v in values.items()} for stage,values in rates.items()}})
    s['partitions']=summaries
    s['minuteSamples']=[]
    for minute in range(math.ceil(l['inputSeconds']/60)):
        rows=[x for x in l['samples'] if minute*60<=x['elapsedSeconds']<(minute+1)*60]
        if rows:s['minuteSamples'].append({'minute':minute,'samples':len(rows),'maxAnalyzerHeapUsedBytes':max((x.get('analyzerHeapUsedBytes',0) for x in rows)), 'maxGeneratorRssBytes':max(x.get('generatorRssBytes',0) for x in rows),'maximumBufferAgeMillis':max(x['publisher']['oldestAgeMillis'] for x in rows)})
    return s

if __name__=='__main__':
    directory=Path(sys.argv[1]);dest=Path(sys.argv[2]);dest.write_text(json.dumps(summarize(directory,sys.argv[3]),ensure_ascii=False,indent=2)+'\n')
