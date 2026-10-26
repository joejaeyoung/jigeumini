#!/usr/bin/env python3
"""전체 제품의 격리 부하·소유 분석기 STOP/CONT·최종 대조. 실제 원천 호출 없음."""
import datetime,hashlib,importlib.util,json,pathlib,re,shutil,sys,time
sys.dont_write_bytecode=True
from product_load_results import analyze
from product_load_plan import read as read_plan,RESOURCE_PROFILES
from product_load_capacity import analyze_capacity,include_runtime_diagnostics
from product_load_sql import statements as sql_statements,ACTIVITY
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('deployment_rehearsal',ROOT/'scripts/deploy-rehearsal.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class ProductLoad(module.Rehearsal):
    def __init__(self,manifest,profile,directory):
        self.plan,self.capacity_mode=read_plan(profile)
        self.profile=self.plan['name'];self.duration=sum(s['seconds'] for s in self.plan['stages'])
        self.fault_at,self.fault_seconds=self.plan['faultAtSeconds'],self.plan['faultSeconds'];self.paused=False;self.probe_id=None;self.source_id=None
        super().__init__(manifest,manifest,directory)
        self.deadline=time.monotonic()+self.duration+900
        self.overlay=self.directory/'load-compose.json'
        self.output=self.directory/'probe-output';self.classes=self.directory/'probe-classes';self.libraries=self.directory/'probe-libs'
        self.source_output=self.directory/'source-output';self.frozen_plan=self.directory/'load-plan.json'
        self.result.update({'scope':'SINGLE_HOST_RF1_WHOLE_PRODUCT_SYNTHETIC_LOAD','profile':self.profile,'capacityExperiment':self.capacity_mode,
            'resourceSamples':[],'loadContract':{**self.plan,'seed':20261026,'durationSeconds':self.duration,'resources':'deploy/compose.yaml limits with explicit resourceProfile; probe Xmx384m, separate source Xmx128m/256MiB','jmx':'ISOLATED_INTERNAL_TEST_NETWORK_ONLY'}})
    def memory_budget_bytes(self):return super().memory_budget_bytes()+(256*1024**2 if self.plan['sourceMode']=='WEBSOCKET' else 0)
    def prepare(self):
        services={
            'api':{'environment':{'JAVA_TOOL_OPTIONS':'-Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname=api -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false'},'healthcheck':{'test':['CMD-SHELL',"JAVA_TOOL_OPTIONS= java -Xmx32m -cp '/app/classes:/app/lib/*' io.chatpulse.poc.PocApplication ops probe"]}},
            'load-probe':{'extends':{'file':str(ROOT/'deploy/compose.yaml'),'service':'api'},'environment':{'JAVA_TOOL_OPTIONS':''},'healthcheck':{'disable':True}}
        }
        if self.plan['sourceMode']=='WEBSOCKET':services['synthetic-source']={
            'image':'${SERVER_IMAGE:?}','read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],
            'tmpfs':['/tmp:size=64m'],'networks':['data'],'labels':{'io.jigeumini.deployment.owner':'${DEPLOYMENT_OWNER:?}'},
            'environment':{'JAVA_TOOL_OPTIONS':''},'healthcheck':{'disable':True},'mem_limit':'256m','cpus':1.0,'stop_grace_period':'60s'}
        for service,cpus in RESOURCE_PROFILES[self.plan['resourceProfile']].items():services.setdefault(service,{})['cpus']=cpus
        if self.plan['diagnostics']:services.setdefault('postgres',{})['command']=['postgres','-c','shared_preload_libraries=pg_stat_statements',
            '-c','pg_stat_statements.track_utility=off','-c','pg_stat_statements.save=off']
        self.overlay.write_text(json.dumps({'services':services}))
        self.frozen_plan.write_text(json.dumps(self.plan))
        self.output.mkdir();self.output.chmod(0o777)
        self.source_output.mkdir();self.source_output.chmod(0o777);self.libraries.mkdir()
        class_root=ROOT/'server/build/classes/java/test'
        files=[p for p in class_root.rglob('ProductLoad*.class') if 'Test' not in p.name]
        if not files:raise RuntimeError('LOAD_CLASSES_MISSING')
        for path in files:
            target=self.classes/path.relative_to(class_root);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
        jars=list((ROOT/'server/build/product-load-libs').glob('Java-WebSocket-*.jar'))
        if len(jars)!=1:raise RuntimeError('LOAD_WEBSOCKET_LIBRARY_MISSING')
        shutil.copy2(jars[0],self.libraries/jars[0].name)
        self.result['fixtureClasses']={str(p.relative_to(self.classes)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(self.classes.rglob('*.class'))}
        self.result['fixtureLibraries']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.libraries.glob('*.jar')}
        return {'frozenClasses':len(files)}
    def compose(self,*args,**kwargs):
        if not hasattr(self,'overlay'):return super().compose(*args,**kwargs)
        return self.command(['docker','compose','--env-file',str(self.envfile),'-p',self.project,'-f',str(ROOT/'deploy/compose.yaml'),'-f',str(self.overlay),*args],**kwargs)
    def audit(self):
        result=super().audit();_,info=self.owned('api')
        network_ids=list(info['NetworkSettings']['Networks'])
        networks=json.loads(self.command(['docker','network','inspect',*network_ids]).stdout)
        if not all(n['Internal'] and n['Labels'].get('io.jigeumini.deployment.owner')==self.owner for n in networks):raise RuntimeError('LOAD_EXTERNAL_NETWORK')
        result['testNetwork']={'apiNetworksInternal':True,'jmxPublished':any((info['NetworkSettings']['Ports'] or {}).values()),'apiJvmArguments':info['Config']['Entrypoint'][1:3],
            'probeHeapMaxBytes':384*1024**2,'apiMemoryLimitBytes':info['HostConfig']['Memory'],'apiNanoCpus':info['HostConfig']['NanoCpus']}
        return result
    def storage(self):
        return json.loads(self.sql("SELECT json_build_object('databaseBytes',pg_database_size(current_database()),'relations',(SELECT json_object_agg(relname,json_build_object('totalBytes',pg_total_relation_size(oid),'heapBytes',pg_relation_size(oid),'indexBytes',pg_indexes_size(oid))) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'))").stdout)
    def resources(self):
        sample={'at':self.now()};ids=[];roles={}
        for service in ['api','kafka','postgres']:
            cid,_=self.owned(service);ids.append(cid);roles[service]=cid
            if service=='api':
                try:
                    status=self.command(['docker','exec',cid,'cat','/proc/1/status']).stdout
                    match=re.search(r'^VmRSS:\s+(\d+) kB$',status,re.M)
                    if not match:raise RuntimeError('ANALYZER_RSS_UNAVAILABLE')
                    sample['analyzerRssBytes']=int(match[1])*1024
                except Exception as error:sample['analyzerRssError']=type(error).__name__
        ids.append(self.probe_id);roles['collector-probe']=self.probe_id
        if self.source_id:ids.append(self.source_id);roles['synthetic-source']=self.source_id
        sample['containerRoles']=roles;sample['cpuStat']={}
        for role,cid in roles.items():
            try:sample['cpuStat'][role]={k:int(v) for k,v in (line.split() for line in self.command(['docker','exec',cid,'cat','/sys/fs/cgroup/cpu.stat'],timeout=5).stdout.splitlines())}
            except Exception as error:sample['cpuStat'][role]={'error':type(error).__name__}
        sample['dataDirectoryKiB']={}
        directories=[] if self.plan['measurementProfile']=='NO_COUNTS_OR_DU' else [('kafka','/var/lib/kafka/data'),('postgres','/var/lib/postgresql/data')]
        for role,path in directories:
            try:sample['dataDirectoryKiB'][role]=int(self.command(['docker','exec',roles[role],'du','-sk',path],timeout=5).stdout.split()[0])
            except Exception as error:sample['dataDirectoryKiB'][role]={'error':type(error).__name__}
        try:
            values=self.command(['docker','stats','--no-stream','--format','{{json .}}',*ids],timeout=10).stdout
            sample['containers']=[json.loads(line) for line in values.splitlines()]
        except Exception as error:sample['containerStatsError']=type(error).__name__
        sample['hostFreeDiskBytes']=shutil.disk_usage(ROOT).free
        if self.plan['diagnostics']:
            start=time.monotonic();sample['sqlStartedAt']=self.now()
            try:
                sample['sqlActivity']=json.loads(self.sql(ACTIVITY).stdout)
                sample['sqlStatements']=json.loads(self.sql(sql_statements()).stdout)
            except Exception as error:sample['sqlDiagnosticError']=type(error).__name__
            sample['sqlSeconds']=time.monotonic()-start
        self.result['resourceSamples'].append(sample);self.checkpoint()
        if sample['hostFreeDiskBytes']<2*1024**3:raise RuntimeError('LOAD_DISK_LIMIT')
    def clock_bracket(self,phase):
        before=time.time_ns()//1_000_000
        remote=int(self.command(['docker','exec','-e','JAVA_TOOL_OPTIONS=',self.probe_id,'java','-Xmx32m','-cp','/app/classes:/app/lib/*:/probe','io.chatpulse.operations.ProductLoadProbe','clock'],timeout=10).stdout.strip())
        after=time.time_ns()//1_000_000
        sample={'phase':phase,'hostBeforeEpochMillis':before,'hostAfterEpochMillis':after,'probeEpochMillis':remote,
            'skewLowerMillis':remote-after,'skewUpperMillis':remote-before,'uncertaintyMillis':after-before}
        self.result.setdefault('clockBrackets',[]).append(sample);self.checkpoint()
        # 같은 호스트 시험에서 초 단위 복구값을 읽기 위한 한도다. 실제 bracket을 근거에 남긴다.
        if after-before>1000 or sample['skewLowerMillis']>250 or sample['skewUpperMillis']<-250:raise RuntimeError('LOAD_CLOCK_REFERENCE_UNALIGNED')
        return sample
    def start_fixture(self,service,main,args,output,heap):
        name=self.project+'-'+service
        self.compose('run','-d','--no-deps','--use-aliases','--name',name,'--entrypoint','java','-e','JAVA_TOOL_OPTIONS=',
            '-v',str(self.classes)+':/probe:ro','-v',str(self.libraries)+':/probe-lib:ro','-v',str(self.frozen_plan)+':/load-plan.json:ro',
            '-v',str(output)+':/output',service,'-Xms32m','-Xmx'+heap,'-XX:+ExitOnOutOfMemoryError',
            '-Dsun.rmi.transport.tcp.responseTimeout=2000','-Dsun.rmi.transport.tcp.handshakeTimeout=2000',
            '-cp','/app/classes:/app/lib/*:/probe:/probe-lib/*',main,*args)
        info=json.loads(self.command(['docker','inspect',name]).stdout)[0]
        if info['Config']['Labels'].get('io.jigeumini.deployment.owner')!=self.owner:raise RuntimeError('PROBE_OWNER_MISMATCH')
        if any((info['NetworkSettings']['Ports'] or {}).values()) or not all(n['Internal'] for n in json.loads(self.command(['docker','network','inspect',*info['NetworkSettings']['Networks']]).stdout)):raise RuntimeError('PROBE_EXTERNAL_NETWORK')
        if service=='synthetic-source':
            isolation={'secretMounts':sum(m['Destination']=='/run/secrets' or m['Destination'].startswith('/run/secrets/') for m in info['Mounts']),
                       'pipelineEnvironmentVariables':sum(e.startswith('PIPELINE_') for e in info['Config']['Env'])}
            self.result['syntheticSourceIsolation']=isolation;self.checkpoint()
            if any(isolation.values()):raise RuntimeError('SOURCE_CREDENTIALS_INHERITED')
        self.result.setdefault('fixtureResources',{})[service]={k:info['HostConfig'][k] for k in ('Memory','NanoCpus','PidsLimit')}
        return info['Id']
    def load(self):
        self.result['storageBefore']=self.storage()
        if self.plan['diagnostics']:
            self.sql('CREATE EXTENSION pg_stat_statements')
            self.sql('SELECT pg_stat_statements_reset()')
            self.result['sqlDiagnostics']={'resetAt':self.now(),'scope':'RUNTIME_ROLE_SINCE_RESET_INCLUDING_PROBE; EXECUTION_NOT_JDBC_OR_POOL_TIME; NO_SQL_TEXT_OR_VALUES'}
        if self.plan['sourceMode']=='WEBSOCKET':self.source_id=self.start_fixture('synthetic-source','io.chatpulse.operations.ProductLoadSocketServer',['/load-plan.json','/output/source.json'],self.source_output,'128m')
        self.probe_id=self.start_fixture('load-probe','io.chatpulse.operations.ProductLoadProbe',['/load-plan.json','/output/load.json'],self.output,'384m')
        if not self.capacity_mode or self.fault_at>=0:self.clock_bracket('START')
        else:self.result['clockReference']='SAME_DOCKER_VM; MONOTONIC_RATE_AND_ELAPSED; NO_HOST_RECOVERY_TIMESTAMP_COMPARISON'
        api,_=self.owned('api');fault=None;last_sample=0
        while time.monotonic()<self.deadline-90:
            info=json.loads(self.command(['docker','inspect',self.probe_id]).stdout)[0]
            path=self.output/'progress.json'
            progress=json.loads(path.read_text()) if path.exists() else {}
            if progress:
                plan=progress['plan']
                if plan!=self.plan:raise RuntimeError('LOAD_PLAN_MISMATCH')
            if info['State']['Status']=='exited':
                final=self.output/'load.json';progress=json.loads(final.read_text()) if final.exists() else progress
                break
            elapsed=progress.get('elapsedSeconds',0)
            if fault is None and self.fault_at>=0 and elapsed>=self.fault_at:
                self.command(['docker','kill','--signal','STOP',api]);self.paused=True
                fault={'stoppedAt':self.now(),'stoppedAtEpochMillis':time.time_ns()//1_000_000,'generatorElapsedAtStop':elapsed,'monotonicStop':time.monotonic(),'signal':'SIGSTOP'}
                self.result['fault']=fault;self.checkpoint()
            if self.paused and time.monotonic()-fault['monotonicStop']>=self.fault_seconds:
                self.command(['docker','kill','--signal','CONT',api]);self.paused=False
                fault.update({'resumedAt':self.now(),'resumedAtEpochMillis':time.time_ns()//1_000_000,'actualStoppedSeconds':time.monotonic()-fault['monotonicStop']});self.checkpoint();self.clock_bracket('RESUME')
            if time.monotonic()-last_sample>=10 and (not self.paused or self.fault_seconds-(time.monotonic()-fault['monotonicStop'])>10):self.resources();last_sample=time.monotonic()
            time.sleep(1)
        else:raise RuntimeError('LOAD_PROCESS_TIMEOUT')
        self.result['load']=progress;self.result['probeExitCode']=info['State']['ExitCode'];self.result['probeOomKilled']=info['State']['OOMKilled']
        log=self.command(['docker','logs','--tail','50',self.probe_id],check=False)
        failures={'Java heap space':'JAVA_HEAP_SPACE','GC overhead limit exceeded':'GC_OVERHEAD_LIMIT','unable to create native thread':'NATIVE_THREAD_LIMIT','OutOfMemoryError: Metaspace':'METASPACE_LIMIT'}
        self.result['probeFailureCodes']=[code for marker,code in failures.items() if marker in log.stdout+log.stderr]
        self.final_observations(progress)
        if info['State']['ExitCode']!=0 or info['State']['OOMKilled'] or not progress.get('correct'):raise RuntimeError('LOAD_FAILED')
        if self.result.get('measurementErrors'):raise RuntimeError('LOAD_EVIDENCE_INCOMPLETE')
        if any(p.get('Status')!='running' or p.get('OOMKilled') for p in self.result['productProcesses'].values()):raise RuntimeError('PRODUCT_PROCESS_EXITED')
        if self.fault_at>=0 and (not fault or 'resumedAt' not in fault):raise RuntimeError('LOAD_FAULT_NOT_EXECUTED')
        self.result['analysis']=analyze(progress,self.output/'offsets.jsonl',fault,self.result['resourceSamples'],self.duration)
        if fault:fault.pop('monotonicStop',None)
        self.checkpoint()
        if self.result['analysis']['failures']:raise RuntimeError(self.result['analysis']['failures'][0])
        return {'correct':True,'unique':progress['expectedUnique'],'physical':progress['expectedPhysical'],'inputSeconds':progress['inputSeconds'],'drainSeconds':progress['drainSeconds']}
    def final_observations(self,progress):
        # DB가 이미 종료됐어도 프로세스/원천/offset 진단을 먼저 보존한다.
        errors=self.result.setdefault('measurementErrors',[])
        self.result['productProcesses']={}
        for service in ('api','kafka','postgres'):
            try:
                _,state=self.owned(service)
                self.result['productProcesses'][service]={**{k:state['State'][k] for k in ('Status','ExitCode','OOMKilled')},**{k:state['HostConfig'][k] for k in ('Memory','NanoCpus','PidsLimit')}}
            except Exception:errors.append(service.upper()+'_STATE_UNAVAILABLE')
        source_file=self.source_output/'source.json'
        if self.source_id:
            try:self.result['syntheticSourceSnapshot']=json.loads(source_file.read_text())
            except Exception:errors.append('SOURCE_RESULT_UNAVAILABLE')
            try:
                source_info=json.loads(self.command(['docker','inspect',self.source_id]).stdout)[0]
                self.result['sourceProcess']={k:source_info['State'][k] for k in ('Status','ExitCode','OOMKilled')}
            except Exception:errors.append('SOURCE_STATE_UNAVAILABLE')
        if self.capacity_mode:self.result['capacityAnalysis']=analyze_capacity(self.plan,progress,self.output/'offsets.jsonl',self.result.get('syntheticSourceSnapshot'))
        self.checkpoint()
        try:self.result['storageAfter']=self.storage()
        except Exception:errors.append('STORAGE_AFTER_UNAVAILABLE')
        if self.plan['diagnostics']:
            try:self.result['finalSqlStatements']=json.loads(self.sql(sql_statements()).stdout)
            except Exception:errors.append('SQL_STATEMENTS_UNAVAILABLE')
        if self.capacity_mode:include_runtime_diagnostics(self.result['capacityAnalysis'],errors,self.result['productProcesses'])
        self.checkpoint()
    def scenarios(self):
        self.phase('PREPARE_PROBE',self.prepare);self.phase('PREFLIGHT',self.preflight);self.phase('INITIALIZE',self.initialize)
        self.phase('ANALYZER_IMAGE',lambda:self.up_apps(self.candidate));self.phase('RUNTIME_BOUNDARIES',self.audit)
        self.phase('PRODUCT_LOAD',self.load);self.phase('FINAL_GRACEFUL_STOP',self.stop_api)
    def cleanup(self):
        if self.paused:
            self.cleaning=True
            try:
                cid,_=self.owned('api');self.command(['docker','kill','--signal','CONT',cid],check=False)
            except Exception:pass
            self.paused=False
        super().cleanup()

if __name__=='__main__':
    if len(sys.argv)!=4:raise SystemExit('usage: product-load <image-manifest.json> <smoke|uniform|hot|steps|soak|plan.json> <new-output-directory>')
    raise SystemExit(ProductLoad(sys.argv[1],sys.argv[2],sys.argv[3]).run())
