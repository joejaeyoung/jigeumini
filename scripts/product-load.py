#!/usr/bin/env python3
"""전체 제품의 격리 부하·소유 분석기 STOP/CONT·최종 대조. 실제 원천 호출 없음."""
import datetime,hashlib,importlib.util,json,pathlib,re,shutil,sys,time
sys.dont_write_bytecode=True
from product_load_results import analyze
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('deployment_rehearsal',ROOT/'scripts/deploy-rehearsal.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
PLANS={'smoke':(60,15,20),'uniform':(120,-1,0),'hot':(120,-1,0),'steps':(180,-1,0),'soak':(1800,300,20)}

class ProductLoad(module.Rehearsal):
    def __init__(self,manifest,profile,directory):
        if profile not in PLANS:raise ValueError('LOAD_PROFILE')
        self.profile=profile;self.duration,self.fault_at,self.fault_seconds=PLANS[profile];self.paused=False;self.probe_id=None
        super().__init__(manifest,manifest,directory)
        self.deadline=time.monotonic()+self.duration+900
        self.overlay=self.directory/'load-compose.json'
        self.output=self.directory/'probe-output';self.classes=self.directory/'probe-classes'
        self.result.update({'scope':'SINGLE_HOST_RF1_WHOLE_PRODUCT_SYNTHETIC_LOAD','profile':profile,
            'resourceSamples':[],'loadContract':{'channels':20,'seed':20261026,'durationSeconds':self.duration,'faultAtSeconds':self.fault_at,'faultSeconds':self.fault_seconds,'resources':'deploy/compose.yaml limits; separate probe JVM Xmx384m in api service limits','jmx':'ISOLATED_INTERNAL_TEST_NETWORK_ONLY'}})
    def prepare(self):
        self.overlay.write_text(json.dumps({'services':{
            'api':{'environment':{'JAVA_TOOL_OPTIONS':'-Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname=api -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false'},'healthcheck':{'test':['CMD-SHELL',"JAVA_TOOL_OPTIONS= java -Xmx32m -cp '/app/classes:/app/lib/*' io.chatpulse.poc.PocApplication ops probe"]}},
            'load-probe':{'extends':{'file':str(ROOT/'deploy/compose.yaml'),'service':'api'},'environment':{'JAVA_TOOL_OPTIONS':''},'healthcheck':{'disable':True}}
        }}))
        self.output.mkdir();self.output.chmod(0o777)
        target=self.classes/'io/chatpulse/operations';target.mkdir(parents=True)
        files=list((ROOT/'server/build/classes/java/test/io/chatpulse/operations').glob('ProductLoad*.class'))
        if not files:raise RuntimeError('LOAD_CLASSES_MISSING')
        for path in files:shutil.copy2(path,target/path.name)
        self.result['fixtureClasses']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(target.glob('*.class'))}
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
    def resources(self):
        sample={'at':self.now()};ids=[]
        for service in ['api','kafka','postgres']:
            cid,_=self.owned(service);ids.append(cid)
            if service=='api':
                try:
                    status=self.command(['docker','exec',cid,'cat','/proc/1/status']).stdout
                    match=re.search(r'^VmRSS:\s+(\d+) kB$',status,re.M)
                    if not match:raise RuntimeError('ANALYZER_RSS_UNAVAILABLE')
                    sample['analyzerRssBytes']=int(match[1])*1024
                except Exception as error:sample['analyzerRssError']=type(error).__name__
        ids.append(self.probe_id)
        try:
            values=self.command(['docker','stats','--no-stream','--format','{{json .}}',*ids],timeout=10).stdout
            sample['containers']=[json.loads(line) for line in values.splitlines()]
        except Exception as error:sample['containerStatsError']=type(error).__name__
        sample['hostFreeDiskBytes']=shutil.disk_usage(ROOT).free
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
    def load(self):
        name=self.project+'-probe'
        self.compose('run','-d','--no-deps','--name',name,'--entrypoint','java','-e','JAVA_TOOL_OPTIONS=',
            '-v',str(self.classes)+':/probe:ro','-v',str(self.output)+':/output','load-probe','-Xms64m','-Xmx384m',
            '-Dsun.rmi.transport.tcp.responseTimeout=2000','-Dsun.rmi.transport.tcp.handshakeTimeout=2000',
            '-cp','/app/classes:/app/lib/*:/probe','io.chatpulse.operations.ProductLoadProbe',self.profile,'/output/load.json')
        info=json.loads(self.command(['docker','inspect',name]).stdout)[0]
        if info['Config']['Labels'].get('io.jigeumini.deployment.owner')!=self.owner:raise RuntimeError('PROBE_OWNER_MISMATCH')
        if any((info['NetworkSettings']['Ports'] or {}).values()) or not all(n['Internal'] for n in json.loads(self.command(['docker','network','inspect',*info['NetworkSettings']['Networks']]).stdout)):raise RuntimeError('PROBE_EXTERNAL_NETWORK')
        self.probe_id=info['Id'];self.clock_bracket('START');api,_=self.owned('api');fault=None;last_sample=0
        while time.monotonic()<self.deadline-90:
            info=json.loads(self.command(['docker','inspect',self.probe_id]).stdout)[0]
            path=self.output/'progress.json'
            progress=json.loads(path.read_text()) if path.exists() else {}
            if progress:
                plan=progress['plan']
                if (plan['name']!=self.profile or sum(s['seconds'] for s in plan['stages'])!=self.duration
                    or plan['faultAtSeconds']!=self.fault_at or plan['faultSeconds']!=self.fault_seconds):raise RuntimeError('LOAD_PLAN_MISMATCH')
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
        self.checkpoint()
        if info['State']['ExitCode']!=0 or info['State']['OOMKilled'] or not progress.get('correct'):raise RuntimeError('LOAD_FAILED')
        if self.fault_at>=0 and (not fault or 'resumedAt' not in fault):raise RuntimeError('LOAD_FAULT_NOT_EXECUTED')
        self.result['analysis']=analyze(progress,self.output/'offsets.jsonl',fault,self.result['resourceSamples'],self.duration)
        if fault:fault.pop('monotonicStop',None)
        self.checkpoint()
        if self.result['analysis']['failures']:raise RuntimeError(self.result['analysis']['failures'][0])
        return {'correct':True,'unique':progress['expectedUnique'],'physical':progress['expectedPhysical'],'inputSeconds':progress['inputSeconds'],'drainSeconds':progress['drainSeconds']}
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
    if len(sys.argv)!=4:raise SystemExit('usage: product-load <image-manifest.json> <smoke|uniform|hot|steps|soak> <new-output-directory>')
    raise SystemExit(ProductLoad(sys.argv[1],sys.argv[2],sys.argv[3]).run())
