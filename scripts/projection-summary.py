#!/usr/bin/env python3
"""#65 원본과 실행 기록을 고정 schema로 정제한다. SQL 값·stack·기존 container 이름은 제외한다."""
import collections,hashlib,importlib.util,json,pathlib,sys
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('capacity_summary',ROOT/'scripts/capacity-summary.py')
capacity=importlib.util.module_from_spec(spec);spec.loader.exec_module(capacity)

SQL_FIELDS=('query_id','template_hash','family','operation','calls','execution_millis','maximum_execution_millis','rows',
            'shared_blks_hit','shared_blks_read','shared_blks_dirtied','temp_blks_read','temp_blks_written','wal_bytes')

def digest(path):return {'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def summarize(directory):
    result=capacity.summarize(directory);raw=json.loads((directory/'result.json').read_text());load=raw.get('load',{})
    execution=directory.parent/(directory.name+'-execution.json')
    if execution.exists():
        e=json.loads(execution.read_text());result['execution']={k:e.get(k) for k in ('startedAt','finishedAt','codeCommit','mainTree','testTree','command','exitCode','logSha256','preexistingContainersPreserved')}
        result['execution']['preexistingContainerCount']=len(e.get('containersBefore',[]));result['executionArtifact']=digest(execution)
    result['finalSqlStatements']=[{k:row[k] for k in SQL_FIELDS} for row in raw.get('finalSqlStatements',[])]
    result['sqlScope']=raw.get('sqlDiagnostics')
    result['threadLocations']={}
    for role in ('probeThreads','analyzerThreads'):
        total=collections.Counter();maximum=collections.Counter();sampled=0
        for sample in load.get('samples',[]):
            if role not in sample:continue
            sampled+=1;locations=sample[role]['locations'];total.update(locations)
            for name,count in locations.items():maximum[name]=max(maximum[name],count)
        result['threadLocations'][role]={'samples':sampled,'sumOfInstantCounts':dict(total),'maximumInstantCount':dict(maximum),
            'scope':'BOUNDED_STACK_PLATFORM_THREAD_SAMPLES; NOT_WAIT_DURATION'}
    result['sqlTimeline']=[]
    for sample in raw.get('resourceSamples',[]):
        if 'sqlStatements' not in sample and 'sqlDiagnosticError' not in sample:continue
        grouped={}
        for row in sample.get('sqlStatements',[]):
            key=row['family']+'/'+row['operation'];value=grouped.setdefault(key,{'calls':0,'executionMillis':0})
            value['calls']+=row['calls'];value['executionMillis']+=row['execution_millis']
        result['sqlTimeline'].append({'hostStartedAt':sample.get('sqlStartedAt'),'querySeconds':sample.get('sqlSeconds'),
            'error':sample.get('sqlDiagnosticError'),'cumulativeStatements':grouped,
            'activity':[{k:row.get(k) for k in ('role','state','wait_event_type','wait_event','sessions','oldest_transaction_millis','oldest_active_millis')} for row in sample.get('sqlActivity',[])]})
    result['sqlTimelineScope']='HOST_SNAPSHOT_TIMES; NOT_ATOMIC_WITH_PROBE_OR_CPU; INCLUDES_STARTUP_DRAIN_AND_FINAL_ORACLE'
    result['apiHealth']=raw.get('analysis',{}).get('api')
    result['coverage']=raw.get('analysis',{}).get('measurementCoverage')
    result['clockTimeline']=[{k:sample.get(k) for k in ('phase','startedAtEpochMillis','elapsedSeconds','probeGcMillis','probeThreadCount','generatorHeapUsedBytes','generatorRssBytes','analyzerHeapUsedBytes')} for sample in load.get('samples',[])]
    result['inputStartedAtEpochMillis']=load.get('inputStartedAtEpochMillis')
    return result

def main(root,output):
    runs=[]
    exclusions=root/'aborted-overlap.json'
    interrupted=json.loads(exclusions.read_text()) if exclusions.exists() else None
    excluded={run['name'] for run in interrupted['runs']} if interrupted else set()
    for directory in sorted(root.iterdir()):
        path=directory/'result.json'
        if not directory.is_dir() or not path.is_file():continue
        if directory.name in excluded:continue
        if json.loads(path.read_text()).get('finishedAt'):runs.append(summarize(directory))
    helpers=['scripts/projection-summary.py','scripts/capacity-summary.py','scripts/product_load_capacity.py']
    output.parent.mkdir(parents=True,exist_ok=True)
    value={'schemaVersion':1,'technicalIds':['T02','T07','T08','T11','T12','T14'],
        'scope':'LOCAL_RF1_FRESH_DB_SYNTHETIC_PROJECTION_COST; NOT_OPERATIONAL_MAXIMUM',
        'curatorFiles':{name:digest(ROOT/name) for name in helpers},
        'analysisProvenance':'CURRENT_ANALYZER_RECALCULATED; ORIGINAL_RECORDED_CLASSIFICATION_RETAINED',
        'interruptedExperiments':interrupted,'interruptedExperimentsArtifact':digest(exclusions) if interrupted else None,
        'runs':runs,'limitations':['SQL server execution excludes JDBC/pool time; counts include probe and final oracle.',
            'JMX and SQL activity are instant samples, not durations or causal proof.',
            'Host resource timestamps are not atomic with VM probe samples.',
            'Other workloads remain running; CPU limits do not isolate shared VM scheduling.',
            'Public source, fleet, long retention, RF3 and multi-host/AZ capacity are unverified.']}
    output.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'runs':len(runs),'bytes':output.stat().st_size,'output':str(output)}))

if __name__=='__main__':main(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]))
