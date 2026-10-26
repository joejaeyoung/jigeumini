"""용량 시험의 보수적 판정. 부하 생성 실패와 제품 SLO 초과를 구분한다."""
import json,statistics

LIMITS={'visibilityP95Millis':2000,'apiP95Millis':500,'sourceToReceivedP95Millis':1000,'sourceToReceivedMaximumMillis':3000,'generatorMaximumDelayMillis':1000,
        'apiScheduleP95Millis':100,'minimumStageSeconds':30,'minimumOffsetCoverage':0.7,
        'maximumOffsetGapSeconds':5,'maximumWatermarkAgeSeconds':10,'lagGrowthRateFraction':0.02}

def lag_summary(samples,stage,begin):
    end=begin+stage['seconds'];series={};times=[]
    for sample in samples:
        at=sample.get('elapsedSeconds',-1);rows=sample.get('partitions',[])
        if not begin<=at<end or len(rows)!=24 or not all(all(type(p.get(k)) is int for k in ('lag','end','committed')) and p['lag']>=0 for p in rows):continue
        if len({(p['group'],p['topic'],p['partition']) for p in rows})!=24:continue
        times.append(at);topics={}
        for row in rows:topics.setdefault(row['group']+'/'+row['topic'],[]).append(row)
        for group,partitions in topics.items():series.setdefault(group,[]).append((at,partitions))
    groups={};growing=[]
    for group,all_points in series.items():
        # 처음 1/3은 단계 전환의 여파를 허용한다. 나머지 구간의 추세와 양 끝 평균을 함께 본다.
        points=[(t,sum(p['lag'] for p in rows)) for t,rows in all_points if t>=begin+stage['seconds']/3]
        if len(points)<6:continue
        mean_t=statistics.mean(t for t,_ in points);mean_v=statistics.mean(v for _,v in points)
        variance=sum((t-mean_t)**2 for t,_ in points)
        slope=sum((t-mean_t)*(v-mean_v) for t,v in points)/variance if variance else 0
        width=max(2,len(points)//3);first=statistics.mean(v for _,v in points[:width]);last=statistics.mean(v for _,v in points[-width:])
        input_rate=(sum(p['end'] for p in all_points[-1][1])-sum(p['end'] for p in all_points[0][1]))/(all_points[-1][0]-all_points[0][0])
        threshold=max(.01,input_rate*LIMITS['lagGrowthRateFraction'])
        growth=slope>threshold and last-first>max(1,input_rate) and last>max(1,input_rate*2)
        max_age=0
        for at,rows in all_points:
            commits={p['partition']:p['committed'] for p in rows}
            for before,target in all_points:
                if before>at:break
                if any(commits[p['partition']]<p['end'] for p in target):max_age=max(max_age,at-before)
        stale=max_age>LIMITS['maximumWatermarkAgeSeconds']
        groups[group]={'samples':len(points),'slopeRecordsPerSecond':round(slope,3),'firstMean':round(first,1),'lastMean':round(last,1),
                       'observedInputRecordsPerSecond':round(input_rate,3),'maximum':max(v for _,v in points),'sustainedGrowth':growth,
                       'maximumUnreachedWatermarkAgeSeconds':round(max_age,3),'staleWatermark':stale}
        if growth or stale:growing.append(group)
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
        if collector.get('sourceClockErrors',0):invalid.append('SOURCE_CLOCK_REFERENCE')
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
        partition_samples=visibility.get('sampledByPartition',{})
        for partition in visibility.get('inputPartitions',[]):
            if partition_samples.get(str(partition),partition_samples.get(partition,0))<5:stage_invalid.append('VISIBILITY_PARTITION_SAMPLES')
        if not visibility.get('inputPartitions'):stage_invalid.append('VISIBILITY_PARTITIONS_UNRECORDED')
        for latency in visibility.get('latencyByPartition',{}).values():
            if latency.get('p95UpperMillis') is None or latency['p95UpperMillis']>LIMITS['visibilityP95Millis']:stage_failures.append('VISIBILITY_PARTITION_SLO');break
        if plan['sourceMode']=='WEBSOCKET':
            flow=load.get('collector',{}).get('sourceScheduledToReceived',{}).get(name,{})
            row['sourceToReceived']=flow
            if flow.get('samples')!=stage['seconds']*stage['eventsPerSecond']:stage_invalid.append('SOURCE_STAGE_COUNT')
            if flow.get('p95UpperMillis') is None or flow['p95UpperMillis']>LIMITS['sourceToReceivedP95Millis'] or flow.get('maximumMillis',float('inf'))>LIMITS['sourceToReceivedMaximumMillis']:stage_failures.append('SOURCE_TO_RECEIVED_SLO')
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
