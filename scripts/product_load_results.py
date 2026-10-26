"""저장된 부하 표본의 시간 커버리지와 고정 offset까지의 복구를 판정한다. 실행 자원은 변경하지 않는다."""
import hashlib,json

def counts_enabled(load):
    return load.get('plan',{}).get('measurementProfile','FULL') not in ('NO_COUNTS','NO_COUNTS_OR_DU')

def database_observed(load,sample):
    return 'database' in sample if counts_enabled(load) else sample.get('databaseSampling')=='INTENTIONALLY_DISABLED'

def slow_time_coverage(load,normal,unavailable):
    start=load['inputStartedAtEpochMillis'];end=start+load['inputSeconds']*1000
    intervals=[(start,end)]
    if unavailable:
        intervals=[(start,min(end,unavailable[0])),(max(start,unavailable[1]),end)]
    intervals=[(a,b) for a,b in intervals if a<b]
    required=('analyzerHeapUsedBytes','generatorHeapUsedBytes','generatorRssBytes','publisher')
    complete=[s for s in normal if database_observed(load,s) and all(k in s for k in required) and len(s['api'])==8
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
            'scope':'FULL_INPUT_WINDOWS_EXCLUDING_STOP_AND_RECOVERY; GAPS_INCLUDE_NORMAL_EDGES'}

def analyze(load,offset_file,fault,resources,duration):
    offsets=[json.loads(line) for line in offset_file.read_text().splitlines()]
    def complete(sample):
        rows=sample.get('partitions',[])
        return len(rows)==24 and all(isinstance(p.get('committed'),int) and isinstance(p.get('lag'),int) and p['lag']>=0 for p in rows)
    good=[sample for sample in offsets if complete(sample)]
    def observed(sample):return max(p['observedAtEpochMillis'] for p in sample['partitions'])
    responses=[response for sample in load['samples'] for response in sample['api']]
    failures=[];recovery=None;unavailable=None
    if fault is not None:
        if not 20<=fault['actualStoppedSeconds']<=30:failures.append('LOAD_STOP_DURATION')
        lagged=[s for s in good if fault['stoppedAtEpochMillis']<=observed(s)<fault['resumedAtEpochMillis'] and any(p['lag']>0 for p in s['partitions'])]
        after=[s for s in good if observed(s)>=fault['resumedAtEpochMillis']]
        if not lagged or not after:failures.append('LOAD_FAULT_LAG_NOT_OBSERVED')
        if after:
            # 재개 뒤 첫 end를 고정한다. 계속 들어오는 입력의 일시적인 lag=0 여부에 의존하지 않는다.
            target=after[0];key=lambda p:(p['group'],p['topic'],p['partition'])
            ends={key(p):p['end'] for p in target['partitions']}
            recovered=next((s for s in after if all(p['committed']>=ends[key(p)] for p in s['partitions'])),None)
            ready=next((r for r in responses if r['path']=='/readyz' and r.get('status')==200 and r['startedAtEpochMillis']>=fault['resumedAtEpochMillis']),None)
            if recovered is None:failures.append('LOAD_FIXED_WATERMARK_NOT_DRAINED')
            if ready is None:failures.append('LOAD_READINESS_NOT_RECOVERED')
            if recovered is not None and ready is not None:
                # 준비 상태와 고정 offset 복구 후 2초까지 별도 기록한다. 이 구간을 무제한 연장하지 않는다.
                recovery_end=max(observed(recovered),ready['finishedAtEpochMillis'])+2000
                if recovery_end-fault['resumedAtEpochMillis']>60000:failures.append('LOAD_RECOVERY_EXCEEDS_TEST_BUDGET')
                unavailable=(fault['stoppedAtEpochMillis'],recovery_end)
                intervals=[(observed(b)-observed(a))/1000 for a,b in zip(good,good[1:])]
                recovery={'scope':'FIRST_POST_RESUME_KAFKA_END_FIXED_WHILE_INPUT_CONTINUES','stoppedLagSamples':len(lagged),
                    'targetObservedAtEpochMillis':observed(target),'recoveredAtEpochMillis':observed(recovered),
                    'secondsResumeToFixedWatermark':(observed(recovered)-fault['resumedAtEpochMillis'])/1000,
                    'secondsResumeToReadiness200':(ready['finishedAtEpochMillis']-fault['resumedAtEpochMillis'])/1000,
                    'recoveryWindowEndEpochMillis':recovery_end,'recoveryGraceSeconds':2,'maximumRecoveryBudgetSeconds':60,
                    'maximumOffsetSampleIntervalSeconds':max(intervals,default=0),'target':target['partitions'],'recovered':recovered['partitions']}
                if not any(r['path']=='/readyz' and r.get('status')==200 and r['startedAtEpochMillis']>recovery_end for r in responses):failures.append('LOAD_POST_RECOVERY_READY_UNOBSERVED')
    def overlaps(start,end):return unavailable is not None and start<=unavailable[1] and end>=unavailable[0]
    normal=[s for s in load['samples'] if not overlaps(s['startedAtEpochMillis'],s['finishedAtEpochMillis'])]
    def ratio(success,total):return success/total if total else 0
    coverage={
        'offsetSamples':len(offsets),'offsetCompleteSamples':len(good),'offsetSuccessRatio':ratio(len(good),len(offsets)),
        'normalSlowSamples':len(normal),
        'databaseSamplingMode':'COUNTS' if counts_enabled(load) else 'INTENTIONALLY_DISABLED',
        'databaseContractRatio':ratio(sum(database_observed(load,s) for s in normal),len(normal)),
        'analyzerHeapSuccessRatio':ratio(sum('analyzerHeapUsedBytes' in s for s in normal),len(normal)),
        'generatorHeapSuccessRatio':ratio(sum('generatorHeapUsedBytes' in s for s in normal),len(normal)),
        'generatorRssSuccessRatio':ratio(sum('generatorRssBytes' in s for s in normal),len(normal)),
        'resourceSamples':len(resources),'analyzerRssSuccessRatio':ratio(sum('analyzerRssBytes' in s for s in resources),len(resources)),
        'containerStatsSuccessRatio':ratio(sum(len(s.get('containers',[]))==len(s.get('containerRoles',{'api':0,'kafka':0,'postgres':0,'probe':0})) for s in resources),len(resources)),
        'requiredSuccessRatio':0.90,'minimumOffsetSamples':int(duration*0.8),'minimumResourceSamples':max(2,int(duration/20))}
    api={'duringStop':{},'duringRecovery':{},'normal':{}}
    for r in responses:
        stopped=fault is not None and r['startedAtEpochMillis']<=fault['resumedAtEpochMillis'] and r['finishedAtEpochMillis']>=fault['stoppedAtEpochMillis']
        scope='duringStop' if stopped else 'duringRecovery' if overlaps(r['startedAtEpochMillis'],r['finishedAtEpochMillis']) else 'normal'
        key=str(r.get('status',r.get('error','UNRECORDED')));api[scope][key]=api[scope].get(key,0)+1
    coverage['apiRecordedRatio']=ratio(sum('status' in r or 'error' in r for r in responses),len(load['samples'])*8)
    coverage['slowTimeCoverage']=slow_time_coverage(load,normal,unavailable);temporal=coverage['slowTimeCoverage']
    coverage['evidenceComplete']=(temporal['completeNormalWindowRatio']>=0.90 and temporal['maximumNormalGapSeconds']<=15
        and all(v>=0.90 for k,v in coverage.items() if k.endswith('Ratio') and k!='requiredSuccessRatio')
        and len(good)>=coverage['minimumOffsetSamples'] and len(resources)>=coverage['minimumResourceSamples'] and len(normal)>=2)
    api['healthyNormal']=bool(api['normal']) and set(api['normal'])=={'200'}
    if not coverage['evidenceComplete']:failures.append('LOAD_EVIDENCE_INCOMPLETE')
    if not api['healthyNormal']:failures.append('LOAD_API_ERRORS_OUTSIDE_FAULT_OR_RECOVERY')
    return {'measurementCoverage':coverage,'api':api,'recovery':recovery,'failures':failures,
        'offsetEvidence':{'file':offset_file.name,'bytes':offset_file.stat().st_size,'sha256':hashlib.sha256(offset_file.read_bytes()).hexdigest()}}
