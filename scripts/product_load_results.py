"""저장된 부하 표본의 커버리지와 고정 offset까지의 복구를 판정한다. 실행 자원은 변경하지 않는다."""
import hashlib,json

def slow_time_coverage(load,normal,fault):
    start=load['inputStartedAtEpochMillis'];end=start+load['inputSeconds']*1000
    intervals=[(start,end)]
    if fault:
        intervals=[(start,min(end,fault['stoppedAtEpochMillis'])),(max(start,fault['resumedAtEpochMillis']),end)]
    intervals=[(a,b) for a,b in intervals if a<b]
    required=('database','analyzerHeapUsedBytes','generatorHeapUsedBytes','generatorRssBytes','publisher')
    complete=[s for s in normal if all(k in s for k in required) and len(s['api'])==8
              and all('status' in r or 'error' in r for r in s['api'])]
    observed=sorted(s['startedAtEpochMillis'] for s in complete if start<=s['startedAtEpochMillis']<=end)
    windows=[]
    for i in range(int((end-start)//10000)):
        a=start+i*10000;b=a+10000
        if any(left<=a and b<=right for left,right in intervals):windows.append((a,b))
    covered=sum(any(a<=at<b for at in observed) for a,b in windows)
    maximum_gap=0
    for a,b in intervals:
        points=[a,*[at for at in observed if a<=at<=b],b]
        maximum_gap=max(maximum_gap,max((right-left)/1000 for left,right in zip(points,points[1:])))
    return {'windowSeconds':10,'completeNormalWindows':covered,'normalWindows':len(windows),
            'completeNormalWindowRatio':covered/len(windows) if windows else 0,
            'maximumNormalGapSeconds':maximum_gap,'allowedNormalGapSeconds':15,
            'scope':'FULL_INPUT_WINDOWS_EXCLUDING_FAULT_OVERLAP; GAPS_INCLUDE_NORMAL_EDGES'}


def analyze(load,offset_file,fault,resources,duration):
    offsets=[json.loads(line) for line in offset_file.read_text().splitlines()]
    def complete(sample):
        rows=sample.get('partitions',[])
        return len(rows)==24 and all(isinstance(p.get('committed'),int) and isinstance(p.get('lag'),int) and p['lag']>=0 for p in rows)
    good=[sample for sample in offsets if complete(sample)]
    def observed(sample):return max(p['observedAtEpochMillis'] for p in sample['partitions'])
    def overlaps(start,end):
        return fault is not None and start<=fault['resumedAtEpochMillis'] and end>=fault['stoppedAtEpochMillis']
    normal=[sample for sample in load['samples'] if not overlaps(sample['startedAtEpochMillis'],sample['finishedAtEpochMillis'])]
    def ratio(success,total):return success/total if total else 0
    coverage={
        'offsetSamples':len(offsets),'offsetCompleteSamples':len(good),'offsetSuccessRatio':ratio(len(good),len(offsets)),
        'normalSlowSamples':len(normal),
        'databaseSuccessRatio':ratio(sum('database' in s for s in normal),len(normal)),
        'analyzerHeapSuccessRatio':ratio(sum('analyzerHeapUsedBytes' in s for s in normal),len(normal)),
        'generatorHeapSuccessRatio':ratio(sum('generatorHeapUsedBytes' in s for s in normal),len(normal)),
        'generatorRssSuccessRatio':ratio(sum('generatorRssBytes' in s for s in normal),len(normal)),
        'resourceSamples':len(resources),'analyzerRssSuccessRatio':ratio(sum('analyzerRssBytes' in s for s in resources),len(resources)),
        'requiredSuccessRatio':0.90,'minimumOffsetSamples':int(duration*0.8),'minimumResourceSamples':max(2,int(duration/20))}
    responses=[response for s in load['samples'] for response in s['api']]
    api={'duringFault':{},'outsideFault':{}}
    for response in responses:
        scope='duringFault' if overlaps(response['startedAtEpochMillis'],response['finishedAtEpochMillis']) else 'outsideFault'
        key=str(response.get('status',response.get('error','UNRECORDED')))
        api[scope][key]=api[scope].get(key,0)+1
    coverage['apiRecordedRatio']=ratio(sum('status' in r or 'error' in r for r in responses),len(load['samples'])*8)
    coverage['slowTimeCoverage']=slow_time_coverage(load,normal,fault)
    temporal=coverage['slowTimeCoverage']
    coverage['evidenceComplete']=(temporal['completeNormalWindowRatio']>=0.90 and temporal['maximumNormalGapSeconds']<=15 and all(v>=0.90 for k,v in coverage.items() if k.endswith('Ratio') and k!='requiredSuccessRatio')
        and len(good)>=coverage['minimumOffsetSamples'] and len(resources)>=coverage['minimumResourceSamples'] and len(normal)>=2)
    api['healthyOutsideFault']=bool(api['outsideFault']) and set(api['outsideFault'])=={'200'}
    recovery=None
    if fault is not None:
        if not 20<=fault['actualStoppedSeconds']<=30:raise RuntimeError('LOAD_STOP_DURATION')
        lagged=[s for s in good if fault['stoppedAtEpochMillis']<=observed(s)<fault['resumedAtEpochMillis'] and any(p['lag']>0 for p in s['partitions'])]
        after=[s for s in good if observed(s)>=fault['resumedAtEpochMillis']]
        if not lagged or not after:raise RuntimeError('LOAD_FAULT_LAG_NOT_OBSERVED')
        # 재개 뒤 첫 end를 고정한다. 계속 들어오는 입력의 일시적인 lag=0 여부에 의존하지 않는다.
        target=after[0];key=lambda p:(p['group'],p['topic'],p['partition'])
        ends={key(p):p['end'] for p in target['partitions']}
        recovered=next((s for s in after if all(p['committed']>=ends[key(p)] for p in s['partitions'])),None)
        if recovered is None:raise RuntimeError('LOAD_FIXED_WATERMARK_NOT_DRAINED')
        intervals=[(observed(b)-observed(a))/1000 for a,b in zip(good,good[1:])]
        recovery={'scope':'FIRST_POST_RESUME_KAFKA_END_FIXED_WHILE_INPUT_CONTINUES','stoppedLagSamples':len(lagged),
            'targetObservedAtEpochMillis':observed(target),'recoveredAtEpochMillis':observed(recovered),
            'secondsResumeToFixedWatermark':(observed(recovered)-fault['resumedAtEpochMillis'])/1000,
            'maximumOffsetSampleIntervalSeconds':max(intervals,default=0),'target':target['partitions'],'recovered':recovered['partitions']}
    return {'measurementCoverage':coverage,'api':api,'recovery':recovery,
        'offsetEvidence':{'file':offset_file.name,'bytes':offset_file.stat().st_size,'sha256':hashlib.sha256(offset_file.read_bytes()).hexdigest()}}
