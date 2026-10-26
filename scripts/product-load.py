#!/usr/bin/env python3
"""전체 제품의 격리 부하·소유 분석기 STOP/CONT·최종 대조. 실제 원천 호출 없음."""
import datetime,hashlib,importlib.util,json,pathlib,re,shutil,sys,time
sys.dont_write_bytecode=True
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
        self.overlay.write_text(json.dumps({'services':{
            'api':{'environment':{'JAVA_TOOL_OPTIONS':'-Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Djava.rmi.server.hostname=api -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false'},'healthcheck':{'test':['CMD-SHELL',"JAVA_TOOL_OPTIONS= java -Xmx32m -cp '/app/classes:/app/lib/*' io.chatpulse.poc.PocApplication ops probe"]}},
            'load-probe':{'extends':{'file':str(ROOT/'deploy/compose.yaml'),'service':'api'},'environment':{'JAVA_TOOL_OPTIONS':''},'healthcheck':{'disable':True}}
        }}))
        self.output=self.directory/'probe-output';self.output.mkdir();self.output.chmod(0o777)
        self.classes=self.directory/'probe-classes';target=self.classes/'io/chatpulse/operations';target.mkdir(parents=True)
        files=list((ROOT/'server/build/classes/java/test/io/chatpulse/operations').glob('ProductLoad*.class'))
        if not files:raise RuntimeError('LOAD_CLASSES_MISSING')
        for path in files:shutil.copy2(path,target/path.name)
        self.result.update({'scope':'SINGLE_HOST_RF1_WHOLE_PRODUCT_SYNTHETIC_LOAD','profile':profile,
            'fixtureClasses':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(target.glob('*.class'))},
            'resourceSamples':[],'loadContract':{'channels':20,'seed':20261026,'durationSeconds':self.duration,'faultAtSeconds':self.fault_at,'faultSeconds':self.fault_seconds,'resources':'deploy/compose.yaml limits; separate probe JVM Xmx384m in api service limits','jmx':'ISOLATED_INTERNAL_TEST_NETWORK_ONLY'}})
    def compose(self,*args,**kwargs):
        if not hasattr(self,'overlay'):return super().compose(*args,**kwargs)
        return self.command(['docker','compose','--env-file',str(self.envfile),'-p',self.project,'-f',str(ROOT/'deploy/compose.yaml'),'-f',str(self.overlay),*args],**kwargs)
    def resources(self):
        sample={'at':self.now()};ids=[]
        for service in ['api','kafka','postgres']:
            cid,_=self.owned(service);ids.append(cid)
            if service=='api':
                status=self.command(['docker','exec',cid,'cat','/proc/1/status']).stdout
                match=re.search(r'^VmRSS:\s+(\d+) kB$',status,re.M)
                if not match:raise RuntimeError('ANALYZER_RSS_UNAVAILABLE')
                sample['analyzerRssBytes']=int(match[1])*1024
        ids.append(self.probe_id)
        values=self.command(['docker','stats','--no-stream','--format','{{json .}}',*ids],timeout=10).stdout
        sample['containers']=[json.loads(line) for line in values.splitlines()]
        sample['hostFreeDiskBytes']=shutil.disk_usage(ROOT).free
        if sample['hostFreeDiskBytes']<2*1024**3:raise RuntimeError('LOAD_DISK_LIMIT')
        self.result['resourceSamples'].append(sample);self.checkpoint()
    def load(self):
        name=self.project+'-probe'
        self.compose('run','-d','--no-deps','--name',name,'--entrypoint','java','-e','JAVA_TOOL_OPTIONS=',
            '-v',str(self.classes)+':/probe:ro','-v',str(self.output)+':/output','load-probe','-Xms64m','-Xmx384m',
            '-Dsun.rmi.transport.tcp.responseTimeout=2000','-Dsun.rmi.transport.tcp.handshakeTimeout=2000',
            '-cp','/app/classes:/app/lib/*:/probe','io.chatpulse.operations.ProductLoadProbe',self.profile,'/output/load.json')
        info=json.loads(self.command(['docker','inspect',name]).stdout)[0]
        if info['Config']['Labels'].get('io.jigeumini.deployment.owner')!=self.owner:raise RuntimeError('PROBE_OWNER_MISMATCH')
        self.probe_id=info['Id'];api,_=self.owned('api');fault=None;last_sample=0
        while time.monotonic()<self.deadline-90:
            info=json.loads(self.command(['docker','inspect',self.probe_id]).stdout)[0]
            path=self.output/'load.json'
            progress=json.loads(path.read_text()) if path.exists() else {}
            if info['State']['Status']=='exited':break
            elapsed=progress.get('elapsedSeconds',0)
            if fault is None and self.fault_at>=0 and elapsed>=self.fault_at:
                self.command(['docker','kill','--signal','STOP',api]);self.paused=True
                fault={'stoppedAt':self.now(),'generatorElapsedAtStop':elapsed,'monotonicStop':time.monotonic(),'signal':'SIGSTOP'}
                self.result['fault']=fault;self.checkpoint()
            if self.paused and time.monotonic()-fault['monotonicStop']>=self.fault_seconds:
                self.command(['docker','kill','--signal','CONT',api]);self.paused=False
                fault.update({'resumedAt':self.now(),'actualStoppedSeconds':time.monotonic()-fault['monotonicStop']});self.checkpoint()
            if time.monotonic()-last_sample>=10:self.resources();last_sample=time.monotonic()
            time.sleep(1)
        else:raise RuntimeError('LOAD_PROCESS_TIMEOUT')
        self.result['load']=progress;self.result['probeExitCode']=info['State']['ExitCode'];self.result['probeOomKilled']=info['State']['OOMKilled']
        self.checkpoint()
        if info['State']['ExitCode']!=0 or info['State']['OOMKilled'] or not progress.get('correct'):raise RuntimeError('LOAD_FAILED')
        if self.fault_at>=0:
            if not fault or 'resumedAt' not in fault:raise RuntimeError('LOAD_FAULT_NOT_EXECUTED')
            after=[s for s in progress['samples']+[progress['finalSample']] if str(s['at'])>=fault['resumedAt']]
            lagged=[s for s in progress['samples'] if str(s['at'])>=fault['stoppedAt'] and any((p.get('lag') or 0)>0 for p in s.get('partitions',[]))]
            recovered=next((s for s in after if len(s.get('partitions',[]))==24 and all(p.get('lag')==0 for p in s['partitions'])),None)
            if not lagged or not recovered:raise RuntimeError('LOAD_FAULT_LAG_NOT_OBSERVED')
            fault['firstZeroLagAt']=recovered['at'];fault['secondsResumeToFirstZeroLagSample']=(datetime.datetime.fromisoformat(str(recovered['at']).replace('Z','+00:00'))-datetime.datetime.fromisoformat(fault['resumedAt'])).total_seconds()
            fault.pop('monotonicStop',None)
        return {'correct':True,'unique':progress['expectedUnique'],'physical':progress['expectedPhysical'],'inputSeconds':progress['inputSeconds'],'drainSeconds':progress['drainSeconds']}
    def scenarios(self):
        self.phase('PREFLIGHT',self.preflight);self.phase('INITIALIZE',self.initialize)
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
