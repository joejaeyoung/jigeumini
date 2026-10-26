"""용량 원본에서 숫자·고정 코드·hash만 남기는 정제기. 원본을 변경하지 않는다."""
import datetime,hashlib,json,pathlib,sys
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from product_load_capacity import analyze_capacity,include_runtime_diagnostics

def digest(path):return {'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
def summarize(directory):
    path=directory/'result.json';run=json.loads(path.read_text());load=run.get('load',{});analysis=run.get('capacityAnalysis',{});plan=run.get('loadContract',{})
    recorded_analysis=analysis
    if run.get('capacityExperiment'):
        analysis=analyze_capacity(plan,load,directory/'probe-output/offsets.jsonl',run.get('syntheticSourceSnapshot'))
        include_runtime_diagnostics(analysis,run.get('measurementErrors',[]),run.get('productProcesses',{}))
    result={'name':directory.name,'startedAt':run['startedAt'],'finishedAt':run.get('finishedAt'),'revision':run['candidate']['revision'],
            'jarSha256':run['candidate']['jarSha256'],'images':{k:v['id'] for k,v in run['candidate']['images'].items()},
            'fixtureClassesManifestSha256':hashlib.sha256(json.dumps(run.get('fixtureClasses',{}),sort_keys=True).encode()).hexdigest(),
            'plan':{k:plan[k] for k in ('name','channels','stages','sourceMode','apiRequestsPerSecond','apiMaxInFlight','resourceProfile','faultAtSeconds','faultSeconds')},
            'executionCorrect':run.get('correct'),'dataReconciled':load.get('correct',False),'classification':analysis.get('classification'),
            'recordedClassification':recorded_analysis.get('classification'),'recordedGeneratorProblems':recorded_analysis.get('generatorProblems',[]),
            'recordedMeasurementProblems':recorded_analysis.get('measurementProblems',[]),
            'generatorProblems':analysis.get('generatorProblems',[]),'measurementProblems':analysis.get('measurementProblems',[]),'sloViolations':analysis.get('sloViolations',[]),
            'failureCode':run.get('failureCode'),'loadFailureCode':load.get('failureCode'),'probeFailureCodes':run.get('probeFailureCodes',[]),
            'probeExitCode':run.get('probeExitCode'),'probeOomKilled':run.get('probeOomKilled'),'cleanupComplete':run.get('cleanupComplete'),
            'sensitiveArtifactsRemoved':run.get('sensitiveArtifactsRemoved'),'productProcesses':run.get('productProcesses'),'fixtureResources':run.get('fixtureResources'),'fixtureLibraries':run.get('fixtureLibraries'),
            'input':{k:load.get(k) for k in ('expectedUnique','expectedPhysical','expectedObservations','expectedOutbox','inputSeconds','drainSeconds','maximumPacingDelayMillis')},
            'publisher':{k:v for k,v in load.get('publisherFinal',{}).items() if k!='channels'},'collector':load.get('collector'),'stages':analysis.get('stages',[])}
    if 'payloadProfile' in plan:result['plan']['payloadProfile']=plan['payloadProfile']
    result['effectivePayloadProfile']=plan.get('payloadProfile','MINIMAL')
    result['payloadProfileDefaulted']='payloadProfile' not in plan
    result['seed']=plan.get('seed')
    result['sourceProcess']=run.get('sourceProcess')
    result['finalOffsets']={'count':len(load.get('finalOffsets',[])),'totalLag':sum(p['lag'] for p in load.get('finalOffsets',[]) if p.get('lag') is not None)}
    result['reconciliation']=load.get('reconciliation')
    result['measurementErrors']=run.get('measurementErrors',[])
    source=run.get('syntheticSourceSnapshot',{})
    result['source']={k:source[k] for k in ('peakConnections','sent','wireBytes','finished','failure','actualSeconds','maximumPacingDelayMillis','protocolErrors','socketErrors','duplicateConnections','statusRequests','tokenRequests','clientPings') if k in source}
    samples=load.get('samples',[])
    fields=('generatorHeapUsedBytes','generatorRssBytes','probeThreadCount','probeOpenFileDescriptors','probeGcMillis','analyzerHeapUsedBytes')
    result['inputMeasurementPeaks']={k:max((s[k] for s in samples if k in s and s.get('phase') in {p['name'] for p in plan['stages']}),default=None) for k in fields}
    db_queries=[s['databaseQueryMillis'] for s in samples if 'databaseQueryMillis' in s]
    result['databaseCountQueryMillis']={'samples':len(db_queries),'maximum':max(db_queries,default=None),'scope':'FULL_TABLE_COUNTS_EVERY_FIVE_SECONDS; INCLUDED_MEASUREMENT_OVERHEAD'}
    result['maximumSampledBufferAgeMillis']=max((s.get('publisher',{}).get('oldestAgeMillis',0) for s in samples),default=None)
    resources=run.get('resourceSamples',[]);cpu={}
    for a,b in zip(resources,resources[1:]):
        seconds=(datetime.datetime.fromisoformat(b['at'])-datetime.datetime.fromisoformat(a['at'])).total_seconds()
        for role,current in b.get('cpuStat',{}).items():
            before=a.get('cpuStat',{}).get(role,{})
            if 'usage_usec' not in current or 'usage_usec' not in before:continue
            periods=current['nr_periods']-before['nr_periods'];throttled=current['nr_throttled']-before['nr_throttled']
            cpu.setdefault(role,[]).append({'from':a['at'],'to':b['at'],'coresApprox':round((current['usage_usec']-before['usage_usec'])/seconds/1e6,4),
                'throttledPeriodsRatio':round(throttled/periods,4) if periods else 0,'throttledSeconds':round((current['throttled_usec']-before['throttled_usec'])/1e6,4)})
    result['cpuSummary']={role:{'intervals':len(values),'minimumCoresApprox':min(v['coresApprox'] for v in values),'maximumCoresApprox':max(v['coresApprox'] for v in values),'meanIntervalCoresApprox':round(sum(v['coresApprox'] for v in values)/len(values),4),'maximumThrottledPeriodsRatio':max(v['throttledPeriodsRatio'] for v in values),'totalThrottledSeconds':round(sum(v['throttledSeconds'] for v in values),4)} for role,values in cpu.items()};result['cpuScope']='ALL_RESOURCE_SAMPLES_INCLUDING_STARTUP_AND_DRAIN; APPROXIMATE_HOST_TIMESTAMPS; NOT_A_PRECISE_PHASE_ALIGNMENT'
    result['resourceSampleCount']=len(resources)
    result['dataDirectoryPeakKiB']={role:max((s['dataDirectoryKiB'][role] for s in resources if type(s.get('dataDirectoryKiB',{}).get(role)) is int),default=None) for role in ('kafka','postgres')}
    before,after=run.get('storageBefore'),run.get('storageAfter')
    if before and after:
        growth=after['databaseBytes']-before['databaseBytes'];unique=load.get('expectedUnique')
        result['storage']={'beforeDatabaseBytes':before['databaseBytes'],'afterDatabaseBytes':after['databaseBytes'],'growthDatabaseBytes':growth,
            'growthBytesPerUnique':round(growth/unique,3) if unique else None,'scope':'SHORT_FRESH_DATABASE_MIXED_INPUT; INCLUDES_INDEXES; EXCLUDES_WAL_BACKUP_AND_REPLICAS; NOT_LONG_TERM_MEASUREMENT',
            'relationGrowthBytes':{k:v['totalBytes']-before['relations'].get(k,{}).get('totalBytes',0) for k,v in after['relations'].items() if v['totalBytes']!=before['relations'].get(k,{}).get('totalBytes',0)}}
    diagnostic=directory/'collector-startup-diagnostic.json'
    if diagnostic.exists():
        detail=json.loads(diagnostic.read_text());result['diagnostic']={k:detail[k] for k in ('processState','hostConfig','cgroup','stopReason','stopRequestedAt') if k in detail}
        result['diagnostic']['heapFailureCodeCounts']={code:sum(marker in line for line in detail.get('heapErrorEvidence',[])) for marker,code in [('Java heap space','JAVA_HEAP_SPACE'),('GC overhead limit exceeded','GC_OVERHEAD_LIMIT'),('unable to create native thread','NATIVE_THREAD_LIMIT')]}
        result['diagnostic']['sourcePeakConnections']=detail.get('sourceAtStop',{}).get('peakConnections')
    result['sourceArtifacts']={str(p.relative_to(directory)):digest(p) for p in [path,*[directory/name for name in ('probe-output/load.json','probe-output/offsets.jsonl','probe-output/samples.jsonl','source-output/source.json','collector-startup-diagnostic.json','load-plan.json')]] if p.exists()}
    manifests=[p for p in directory.parent.glob('images*.json') if json.loads(p.read_text()).get('revision')==run['candidate']['revision']]
    manifest=str(sorted(manifests)[0]) if manifests else '<same-revision-manifest>'
    result['command']=f'./scripts/product-load {manifest} {directory}/load-plan.json <new-output-directory>'
    result['commandProvenance']='RECONSTRUCTED_FROM_FROZEN_PLAN_AND_MATCHING_REVISION_MANIFEST; NOT_CAPTURED_SHELL'
    return result

def main(root,output):
    runs=[]
    for directory in sorted(root.iterdir()):
        if not directory.is_dir() or not (directory/'result.json').is_file():continue
        run=json.loads((directory/'result.json').read_text())
        if run.get('finishedAt'):runs.append(summarize(directory))
    environment=json.loads((root/'environment.json').read_text())
    environment['otherContainerCount']=len(environment.pop('otherContainers'))
    result={'schemaVersion':1,'scope':'LOCAL_SINGLE_HOST_RF1_SYNTHETIC_CAPACITY','curatorSha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
            'analysisCodeSha256':hashlib.sha256((ROOT/'scripts/product_load_capacity.py').read_bytes()).hexdigest(),
            'analysisProvenance':'RECALCULATED_FROM_ORIGINAL_LOAD_SOURCE_AND_OFFSETS; RECORDED_CLASSIFICATION_PRESERVED',
            'technicalIds':['T02','T07','T08','T11','T12','T14','T16'],
            'environment':environment,'provisionalLimits':next((json.loads((root/r['name']/'result.json').read_text())['capacityAnalysis']['provisionalLimits'] for r in reversed(runs) if 'capacityAnalysis' in json.loads((root/r['name']/'result.json').read_text())),{}),
            'runs':runs,'limitations':['Public source policies/network/TLS and fleet lease overhead are not measured.','Metadata external fetch is disabled. API load is seven-route mixture on a fresh database.','Collector-probe includes oracle, API client and sampling overhead. Full-table count queries every five seconds are included.','Short fixed-resource limits do not establish cloud/AZ/HA or long-retention capacity.','Direct smoke overlapped correctness regression and is excluded from sizing.']}
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'runs':len(runs),'output':str(output),'bytes':output.stat().st_size}))
if __name__=='__main__':main(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]))
