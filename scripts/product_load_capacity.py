"""용량 시험의 보수적 판정. 부하 생성 실패와 제품 SLO 초과를 구분한다."""
import json,math,statistics

LIMITS={'visibilityP95Millis':2000,'apiP95Millis':500,'sourceToReceivedP95Millis':1000,'sourceToReceivedMaximumMillis':3000,'generatorMaximumDelayMillis':1000,
        'apiScheduleP95Millis':100,'minimumStageSeconds':30,'minimumOffsetCoverage':0.7,
        'maximumOffsetGapSeconds':5,'maximumWatermarkAgeSeconds':10,'lagGrowthRateFraction':0.02,
        'maximumClockResidualMillis':250}

def clock_reference(load):
    """입력 기준 벽시계와 probe 단조 경과의 양방향 차이. 원천 간 동기화를 증명하지는 않는다."""
    epoch=load.get('inputStartedAtEpochMillis');samples=load.get('samples',[]);residuals=[];missing=0
    for sample in samples:
        wall=sample.get('startedAtEpochMillis');elapsed=sample.get('elapsedSeconds')
        if type(epoch) is not int or type(wall) is not int or type(elapsed) not in (int,float) or not math.isfinite(elapsed):
            missing+=1;continue
        residuals.append(wall-epoch-elapsed*1000)
    maximum=max((abs(value) for value in residuals),default=None)
    return {'samples':len(residuals),'missingSamples':missing,
            'minimumResidualMillis':round(min(residuals),3) if residuals else None,
            'maximumResidualMillis':round(max(residuals),3) if residuals else None,
            'maximumAbsoluteResidualMillis':round(maximum,3) if maximum is not None else None,
            'valid':len(residuals)>=2 and missing==0 and maximum<=LIMITS['maximumClockResidualMillis']}

def visibility_summary(visibility):
    """관측 성공 표본의 quantile과 전체 선택 표본의 미관측을 구분한다. 원본은 바꾸지 않는다."""
    selected=visibility.get('sampled');visible=visibility.get('latency',{}).get('samples')
    valid=type(selected) is int and type(visible) is int and 0<=visible<=selected
    missing=selected-visible if valid else None
    return {**visibility,'latencyPopulation':'OBSERVED_VISIBLE_SAMPLES_ONLY_INCLUDING_PER_PARTITION',
            'sampleCoverage':{'valid':valid,'selected':selected,'observedVisible':visible,'unobserved':missing,
                              'unobservedRatio':missing/selected if valid and selected else None},
            'allSelectedP95UpperMillis':visibility.get('latency',{}).get('p95UpperMillis') if valid and missing==0 else None,
            'allSelectedP95Rule':'UNKNOWN_WHEN_ANY_SELECTED_SAMPLE_IS_UNOBSERVED'}

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
    # 기존 원본은 변경하지 않는다. 초기 profile 포함 계측은 v1, 그 이전은 v0로 읽는다.
    schema=plan.get('measurementSchemaVersion',1 if 'measurementProfile' in plan else 0)
    if type(schema) is not int or schema not in (0,1,2):invalid.append('MEASUREMENT_SCHEMA_VERSION');schema=2
    clock=clock_reference(load)
    if not clock['valid']:invalid.append('PROBE_CLOCK_REFERENCE')
    if not load.get('correct'):failures.append(load.get('failureCode','RUN_DID_NOT_RECONCILE'))
    if plan['faultAtSeconds']>=0:invalid.append('FAULT_RUN_IS_NOT_STEADY_CAPACITY')
    if offset_file.is_file():
        try:offsets=[json.loads(line) for line in offset_file.read_text().splitlines()]
        except (ValueError,OSError):offsets=[];invalid.append('OFFSET_FILE_INVALID')
    else:offsets=[];invalid.append('OFFSET_FILE_MISSING')
    if plan['sourceMode']=='WEBSOCKET':
        if not source or not source.get('finished'):invalid.append('SOURCE_NOT_FINISHED')
        elif source.get('maximumPacingDelayMillis') is None:invalid.append('SOURCE_PACING_UNRECORDED')
        elif source['maximumPacingDelayMillis']>LIMITS['generatorMaximumDelayMillis']:generator.append('SOURCE_PACING')
        collector=load.get('collector',{})
        if collector.get('connectedOnce')!=plan['channels']:failures.append('COLLECTOR_CONNECTIONS')
        if any(count for reason,count in collector.get('reasons',{}).items() if reason!='CANCELLED'):failures.append('COLLECTOR_UNEXPECTED_STOP')
        if collector.get('sourceClockErrors',0):invalid.append('SOURCE_CLOCK_REFERENCE')
        source_clock=(source or {}).get('clockReference')
        if schema>=1 and source_clock is None:invalid.append('SOURCE_MONOTONIC_CLOCK_UNRECORDED')
        if source_clock is not None:
            bounds=[source_clock.get(k) for k in ('minimumResidualMillis','maximumResidualMillis')]
            if not source_clock.get('samples') or any(type(v) not in (int,float) or not math.isfinite(v) or abs(v)>LIMITS['maximumClockResidualMillis'] for v in bounds):invalid.append('SOURCE_MONOTONIC_CLOCK_REFERENCE')
    elif load.get('maximumPacingDelayMillis') is None:invalid.append('DIRECT_PACING_UNRECORDED')
    elif load['maximumPacingDelayMillis']>LIMITS['generatorMaximumDelayMillis']:generator.append('DIRECT_GENERATOR_PACING')
    begin=0
    for stage in plan['stages']:
        name=stage['name'];row={'name':name,'eventsPerSecond':stage['eventsPerSecond'],'hot':stage['hot'],'seconds':stage['seconds'],'warmup':name=='warmup'}
        stage_invalid=[];stage_failures=[]
        visibility=load.get('commitVisibility',{}).get('phases',{}).get(name,{})
        row['visibility']=visibility_summary(visibility);p95=row['visibility']['allSelectedP95UpperMillis']
        if not row['visibility']['sampleCoverage']['valid']:stage_invalid.append('VISIBILITY_SAMPLE_ACCOUNTING')
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
            schedule=api.get('scheduleDelay',{});delay=schedule.get('p95UpperMillis')
            if schedule.get('samples',0)>0 and schedule.get('above10000Millis',0)>schedule['samples']*.05:generator.append(name+':API_SCHEDULER')
            elif delay is None:stage_invalid.append('API_SCHEDULE_UNRECORDED')
            elif delay>LIMITS['apiScheduleP95Millis']:generator.append(name+':API_SCHEDULER')
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
            'measurementSchemaVersion':schema,'clockReference':clock,'generatorProblems':generator,'measurementProblems':invalid,'sloViolations':failures,'stages':stages}

def include_runtime_diagnostics(analysis,errors,processes):
    """입력 계측 뒤에 확인한 프로세스/저장량 실패를 통과 판정에 반영한다."""
    analysis['measurementProblems'].extend(errors)
    stopped=[role.upper()+'_PROCESS_EXITED' for role,state in processes.items() if state['Status']!='running' or state['OOMKilled']]
    analysis['sloViolations'].extend(stopped)
    if analysis['withinSlo'] and (errors or stopped):
        analysis['classification']='MEASUREMENT_INCOMPLETE' if errors else 'EXCEEDS_SLO';analysis['withinSlo']=False
    return analysis
