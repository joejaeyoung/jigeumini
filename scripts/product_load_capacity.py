"""용량 시험의 보수적 판정. 부하 생성 실패와 제품 SLO 초과를 구분한다."""
import json,statistics

LIMITS={'visibilityP95Millis':2000,'apiP95Millis':500,'generatorMaximumDelayMillis':1000,
        'apiScheduleP95Millis':100,'minimumStageSeconds':30,'minimumOffsetCoverage':0.7,
        'maximumOffsetGapSeconds':5,'lagGrowthRateFraction':0.02}

def lag_summary(samples,stage,begin):
    end=begin+stage['seconds'];series={};times=[]
    for sample in samples:
        at=sample.get('elapsedSeconds',-1);rows=sample.get('partitions',[])
        if not begin<=at<end or len(rows)!=24 or not all(type(p.get('lag')) is int and p['lag']>=0 for p in rows):continue
        times.append(at);totals={}
        for row in rows:totals[row['group']]=totals.get(row['group'],0)+row['lag']
        for group,value in totals.items():series.setdefault(group,[]).append((at,value))
    groups={};growing=[]
    for group,points in series.items():
        # 처음 1/3은 단계 전환의 여파를 허용한다. 나머지 구간의 추세와 양 끝 평균을 함께 본다.
        points=[(t,v) for t,v in points if t>=begin+stage['seconds']/3]
        if len(points)<6:continue
        mean_t=statistics.mean(t for t,_ in points);mean_v=statistics.mean(v for _,v in points)
        variance=sum((t-mean_t)**2 for t,_ in points)
        slope=sum((t-mean_t)*(v-mean_v) for t,v in points)/variance if variance else 0
        width=max(2,len(points)//3);first=statistics.mean(v for _,v in points[:width]);last=statistics.mean(v for _,v in points[-width:])
        threshold=max(1,stage['eventsPerSecond']*LIMITS['lagGrowthRateFraction'])
        growth=slope>threshold and last-first>stage['eventsPerSecond'] and last>stage['eventsPerSecond']*2
        groups[group]={'samples':len(points),'slopeRecordsPerSecond':round(slope,3),'firstMean':round(first,1),'lastMean':round(last,1),
                       'maximum':max(v for _,v in points),'sustainedGrowth':growth}
        if growth:growing.append(group)
    ordered=[begin,*sorted(times),end]
    gap=max(b-a for a,b in zip(ordered,ordered[1:]))
    return {'completeSamples':len(times),'coverage':len(times)/stage['seconds'],'maximumGapSeconds':gap,'groups':groups,'growingGroups':growing}

def analyze_capacity(plan,load,offset_file,source=None):
    invalid=[];generator=[];failures=[];stages=[]
    if not load.get('correct'):failures.append(load.get('failureCode','RUN_DID_NOT_RECONCILE'))
    if plan['faultAtSeconds']>=0:invalid.append('FAULT_RUN_IS_NOT_STEADY_CAPACITY')
    if offset_file.is_file():
        try:offsets=[json.loads(line) for line in offset_file.read_text().splitlines()]
        except (ValueError,OSError):offsets=[];invalid.append('OFFSET_FILE_INVALID')
    else:offsets=[];invalid.append('OFFSET_FILE_MISSING')
    if plan['sourceMode']=='WEBSOCKET':
        if not source or not source.get('finished'):invalid.append('SOURCE_NOT_FINISHED')
        elif source.get('maximumPacingDelayMillis',float('inf'))>LIMITS['generatorMaximumDelayMillis']:generator.append('SOURCE_PACING')
        collector=load.get('collector',{})
        if collector.get('connectedOnce')!=plan['channels']:failures.append('COLLECTOR_CONNECTIONS')
        if any(count for reason,count in collector.get('reasons',{}).items() if reason!='CANCELLED'):failures.append('COLLECTOR_UNEXPECTED_STOP')
    elif load.get('maximumPacingDelayMillis',float('inf'))>LIMITS['generatorMaximumDelayMillis']:generator.append('DIRECT_GENERATOR_PACING')
    begin=0
    for stage in plan['stages']:
        name=stage['name'];row={'name':name,'eventsPerSecond':stage['eventsPerSecond'],'hot':stage['hot'],'seconds':stage['seconds'],'warmup':name=='warmup'}
        stage_invalid=[];stage_failures=[]
        visibility=load.get('commitVisibility',{}).get('phases',{}).get(name,{})
        row['visibility']=visibility;p95=visibility.get('latency',{}).get('p95UpperMillis')
        if visibility.get('sampled',0)<stage['seconds']:stage_invalid.append('VISIBILITY_SAMPLES')
        if visibility.get('queryErrors',0) or visibility.get('notQueued',0):stage_invalid.append('VISIBILITY_OBSERVATION_ERRORS')
        if visibility.get('notVisibleWithin10Seconds',0) or p95 is None or p95>LIMITS['visibilityP95Millis']:stage_failures.append('VISIBILITY_SLO')
        if plan['apiRequestsPerSecond']:
            api=load.get('apiTraffic',{}).get('phases',{}).get(name,{});row['api']=api
            expected=plan['apiRequestsPerSecond']*stage['seconds']
            if api.get('offered',0)<expected*0.95:stage_invalid.append('API_ARRIVAL_COUNT')
            delay=api.get('scheduleDelay',{}).get('p95UpperMillis')
            if delay is None or delay>LIMITS['apiScheduleP95Millis']:generator.append(name+':API_SCHEDULER')
            latency=api.get('latency',{}).get('p95UpperMillis')
            if api.get('failed',0) or api.get('notIssuedConcurrentLimit',0) or latency is None or latency>LIMITS['apiP95Millis']:stage_failures.append('API_SLO')
            if api.get('successful',0)+api.get('failed',0)!=api.get('issued',0):stage_invalid.append('API_UNFINISHED')
        row['lag']=lag_summary(offsets,stage,begin);begin+=stage['seconds']
        if row['lag']['coverage']<LIMITS['minimumOffsetCoverage'] or row['lag']['maximumGapSeconds']>LIMITS['maximumOffsetGapSeconds']:stage_invalid.append('OFFSET_COVERAGE')
        if row['lag']['growingGroups']:stage_failures.append('SUSTAINED_LAG_GROWTH')
        if stage['seconds']<LIMITS['minimumStageSeconds']:stage_invalid.append('STAGE_TOO_SHORT')
        row['measurementProblems']=stage_invalid;row['sloViolations']=stage_failures;stages.append(row)
        if name!='warmup':
            invalid.extend(name+':'+reason for reason in stage_invalid);failures.extend(name+':'+reason for reason in stage_failures)
    if all(s['warmup'] for s in stages):invalid.append('NO_MEASURED_STAGE')
    classification='GENERATOR_LIMITED' if generator else 'MEASUREMENT_INCOMPLETE' if invalid else 'EXCEEDS_SLO' if failures else 'WITHIN_SLO'
    return {'classification':classification,'withinSlo':classification=='WITHIN_SLO','provisionalLimits':LIMITS,
            'scope':'THIS_FIXED_RESOURCE_PLAN_ONLY; RANKING_COMMIT_VISIBILITY; ALL_SIX_GROUP_LAG; NOT_CLOUD_OR_PUBLIC_SOURCE_CAPACITY',
            'generatorProblems':generator,'measurementProblems':invalid,'sloViolations':failures,'stages':stages}
