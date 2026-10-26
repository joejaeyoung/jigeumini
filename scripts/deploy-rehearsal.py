#!/usr/bin/env python3
"""소유한 로컬 Compose만 만들고 이미지 교체·별도 DB 복원·브라우저를 대조한다."""
import base64,datetime,hashlib,json,os,pathlib,re,secrets,shutil,subprocess,sys,time,urllib.request,urllib.error,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
class Rehearsal:
    def __init__(self,baseline,candidate,directory):
        self.baseline=json.loads(pathlib.Path(baseline).read_text());self.candidate=json.loads(pathlib.Path(candidate).read_text())
        self.directory=pathlib.Path(directory).resolve();self.directory.mkdir(parents=True,exist_ok=False,mode=0o700)
        self.owner=uuid.uuid4().hex;self.project='jigeumini-deploy-'+self.owner
        self.envfile=self.directory/'compose.env';self.deadline=time.monotonic()+1800;self.cleaning=False;self.started=False;self.base=None
        self.result={'scope':'SINGLE_HOST_RF1_SYNTHETIC_LOCAL_REHEARSAL','owner':self.owner,'project':self.project,'startedAt':self.now(),'baseline':self.baseline,'candidate':self.candidate,'phases':[]}
        secret_dir=self.directory/'secrets';secret_dir.mkdir(mode=0o700)
        for name in ['owner','runtime']:
            p=secret_dir/(name+'.txt');p.write_text(secrets.token_hex(24)+'\n');p.chmod(0o444)
        self.environment={'DEPLOYMENT_OWNER':self.owner,'KAFKA_CLUSTER_ID':base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='),
            'OWNER_PASSWORD_FILE':str(secret_dir/'owner.txt'),'RUNTIME_PASSWORD_FILE':str(secret_dir/'runtime.txt'),'DATABASE_NAME':'chatpulse','CONSUMER_GROUP':'deploy-live'}
        self.image(self.baseline);self.checkpoint()
    def now(self):return datetime.datetime.now(datetime.timezone.utc).isoformat()
    def command(self,args,timeout=60,check=True,input=None,binary=False):
        remaining=180 if self.cleaning else self.deadline-time.monotonic()
        if remaining<=0:raise RuntimeError('REHEARSAL_DEADLINE')
        result=subprocess.run(args,cwd=ROOT,input=input,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=not binary,timeout=min(timeout,remaining))
        if check and result.returncode:raise RuntimeError('COMMAND_FAILED_'+str(result.returncode))
        return result
    def compose(self,*args,**kwargs):return self.command(['docker','compose','--env-file',str(self.envfile),'-p',self.project,'-f',str(ROOT/'deploy/compose.yaml'),*args],**kwargs)
    def checkpoint(self):(self.directory/'result.json').write_text(json.dumps(self.result,ensure_ascii=False,indent=2)+'\n')
    def phase(self,name,action):
        record={'name':name,'startedAt':self.now()};self.result['phases'].append(record);self.checkpoint();start=time.monotonic()
        try:
            data=action()
            if data is not None:record['result']=data
            record['passed']=True
        except Exception:record['passed']=False;raise
        finally:record['seconds']=time.monotonic()-start;self.checkpoint()
    def image(self,manifest):
        self.environment.update({'SERVER_IMAGE':manifest['images']['server']['id'],'WEB_IMAGE':manifest['images']['web']['id']})
        self.write_env()
    def write_env(self):self.envfile.write_text(''.join(k+'='+json.dumps(v)+'\n' for k,v in self.environment.items()))
    def owned(self,service):
        cid=self.compose('ps','-a','-q',service).stdout.strip()
        if not re.fullmatch(r'[a-f0-9]{12,64}',cid):raise RuntimeError('OWNED_CONTAINER_MISSING')
        info=json.loads(self.command(['docker','inspect',cid]).stdout)[0]
        if info['Config']['Labels'].get('io.jigeumini.deployment.owner')!=self.owner:raise RuntimeError('CONTAINER_OWNER_MISMATCH')
        return cid,info
    def ops(self,*args,check=True,group=None):
        extra=[] if group is None else ['-e','PIPELINE_GROUP='+group]
        output=self.compose('run','--rm','--no-deps','-e','DEPLOYMENT_MAINTENANCE=true',*extra,'migrate','ops',*args,check=check)
        if not check and output.returncode:return {'failed':True,'code':output.returncode}
        return json.loads(output.stdout.strip().splitlines()[-1])
    def probe(self,*args):
        command=['run','--rm','--no-deps','--entrypoint','java','-v',str(ROOT/'server/build/classes/java/test')+':/probe:ro','api',
                 '-Xmx128m','-cp','/app/classes:/app/lib/*:/probe','io.chatpulse.operations.DeploymentProbe',*args]
        return json.loads(self.compose(*command).stdout.strip().splitlines()[-1])
    def sql(self,statement,database=None,user='app_owner',check=True,sqlstate=False):
        cid,_=self.owned('postgres');return self.command(['docker','exec',cid,'psql','-X','-v','ON_ERROR_STOP=1',*(['-v','VERBOSITY=sqlstate'] if sqlstate else []),'-U',user,'-d',database or self.environment['DATABASE_NAME'],'-At','-c',statement],check=check)
    def http(self,path):
        try:
            with urllib.request.urlopen(self.base+path,timeout=5) as response:return response.status,response.read()
        except urllib.error.HTTPError as e:return e.code,e.read()
    def until(self,test,seconds=90):
        end=min(self.deadline,time.monotonic()+seconds)
        while time.monotonic()<end:
            try:
                if test():return
            except (urllib.error.URLError,TimeoutError,ConnectionError):pass
            time.sleep(.5)
        raise RuntimeError('CONDITION_TIMEOUT')
    def ready(self):self.until(lambda:self.http('/readyz')[0]==200)
    def up_apps(self,manifest):
        self.image(manifest);self.compose('up','-d','--no-deps','--force-recreate','api','web',timeout=90)
        port=self.compose('port','web','8080').stdout.strip();assert re.fullmatch(r'127\.0\.0\.1:\d+',port)
        self.base='http://'+port;self.ready()
        live=json.loads(self.http('/livez')[1]);revision=json.loads(self.http('/revision.json')[1]);assert live['identity']['revision']==revision['revision']==manifest['revision']
        assert live['identity']['buildId']==manifest['buildId']
        return {'revision':revision['revision'],'buildId':live['identity']['buildId'],'ready':True}
    def stop_api(self):
        begin=time.monotonic();self.owned('api');self.compose('stop','-t','60','api',timeout=75);_,info=self.owned('api')
        assert info['State']['ExitCode'] in (0,143) and not info['State']['OOMKilled']
        return {'seconds':time.monotonic()-begin,'exitCode':info['State']['ExitCode'],'oomKilled':False}
    def snapshot(self,chat,notifications):
        found=None
        def complete():
            nonlocal found
            found=self.probe('snapshot')
            return found['rankingCount']==chat and found['trendCount']==chat and found['notificationCount']==notifications and found['pendingOutbox']==0 and found['category_event']['rows']==chat and found['report_event']['rows']==chat
        self.until(complete,90);return found
    def memory_budget_bytes(self):return (640+640+768+384+64)*1024**2
    def preflight(self):
        if self.command(['git','status','--porcelain']).stdout.strip():raise RuntimeError('REHEARSAL_REQUIRES_CLEAN_COMMIT')
        assert self.command(['git','rev-parse','HEAD']).stdout.strip()==self.candidate['revision']
        info=json.loads(self.command(['docker','info','--format','{{json .}}']).stdout)
        assert info['MemTotal']>=6*1024**3 and shutil.disk_usage(ROOT).free>=2*1024**3
        usage=0
        for line in self.command(['docker','stats','--no-stream','--format','{{.MemUsage}}']).stdout.splitlines():
            match=re.match(r'([0-9.]+)([kKMGT]?i?B) /',line)
            if not match:raise RuntimeError('DOCKER_MEMORY_UNAVAILABLE')
            scale={'B':1,'kB':1000,'KB':1000,'KiB':1024,'MB':1000**2,'MiB':1024**2,'GB':1000**3,'GiB':1024**3,'TB':1000**4,'TiB':1024**4}[match[2]]
            usage+=float(match[1])*scale
        assert usage+self.memory_budget_bytes()<=info['MemTotal']*.90
        for manifest in [self.baseline,self.candidate]:
            for item in manifest['images'].values():assert json.loads(self.command(['docker','image','inspect',item['id']]).stdout)[0]['Id']==item['id']
        return {'dockerMemoryBytes':info['MemTotal'],'hostFreeDiskBytes':shutil.disk_usage(ROOT).free,'applicationImageUsers':['10001:10001','101:101'],'maximumComposeMemoryBytes':self.memory_budget_bytes()}
    def initialize(self):
        self.started=True;self.compose('up','-d','--wait','--wait-timeout','120','postgres','kafka',timeout=150)
        self.ops('migrate');self.ops('bind-sources');initialized=self.ops('initialize-groups')
        source_count=int(self.sql("SELECT count(*) FROM deployment_source_identity WHERE run_id='deploy-v1'").stdout.strip())
        denied=self.sql('CREATE TABLE runtime_must_not_create(id int)',user='app_runtime',check=False,sqlstate=True)
        assert denied.returncode!=0 and re.search(r'ERROR:\s+42501',denied.stderr)
        assert initialized['groups']==6 and source_count==4
        return {'runtimeDdlRejected':True,'runtimeDdlSqlState':'42501','groupsInitialized':initialized['groups'],'sourcesBound':source_count}
    def collector_preflight(self):
        self.image(self.candidate)
        try:
            status=self.compose('run','--rm','--no-deps','api','fleet','status')
            state=json.loads(status.stdout.strip().splitlines()[-1]);assert state['assigned']==0
            denied=self.compose('run','--rm','--no-deps','api','fleet','worker','1','10','/tmp/disabled.json',check=False)
            diagnostic=json.loads(denied.stdout.strip().splitlines()[-1])
            assert denied.returncode!=0 and diagnostic['error']=='FLEET_COMMAND_FAILED' and 'FLEET_LIVE_DISABLED' in diagnostic['codes']
            return {'statusReadWithRuntimeSecret':True,'assigned':state['assigned'],'disabledWorkerRejected':True,'diagnosticCode':'FLEET_LIVE_DISABLED',
                    'sourceMode':'DISABLED_PREFLIGHT','revision':self.candidate['revision']}
        finally:self.image(self.baseline)
    def audit(self):
        services={}
        for service in ['api','web','postgres','kafka']:
            _,info=self.owned(service);ports=info['NetworkSettings']['Ports'] or {}
            bindings=[binding for values in ports.values() if values for binding in values]
            if service!='web':assert not bindings
            if service in ('api','web'):assert info['Config']['User'] not in ('','0','root') and info['HostConfig']['ReadonlyRootfs']
            assert not any(re.match(r'(PIPELINE_DB_PASSWORD|POSTGRES_PASSWORD)=',v) for v in info['Config']['Env'])
            services[service]={'user':info['Config']['User'],'readOnlyRoot':info['HostConfig']['ReadonlyRootfs'],'publishedPorts':bindings,'networkCount':len(info['NetworkSettings']['Networks'])}
        assert not set(self.owned('web')[1]['NetworkSettings']['Networks']) & set(self.owned('postgres')[1]['NetworkSettings']['Networks'])
        return services
    def database_fault(self):
        self.owned('postgres');self.compose('stop','-t','10','postgres',timeout=20)
        self.until(lambda:self.http('/readyz')[0]==503,35);assert self.http('/livez')[0]==200
        status,body=self.http('/api/channels');assert status==503 and json.loads(body)['error']=='API_UNAVAILABLE'
        self.compose('start','postgres');self.ready();return {'readinessDuringFault':503,'livenessDuringFault':200,'apiDuringFault':503,'readinessRecovered':200}
    def backup(self):
        self.stop_api();self.probe('pending-outbox',self.event_base)
        before=self.probe('snapshot');assert before['pendingOutbox']==1
        cid,_=self.owned('postgres');dump=self.command(['docker','exec',cid,'pg_dump','-U','app_owner','-d','chatpulse','-Fc','--no-owner'],binary=True).stdout
        (self.directory/'database.dump').write_bytes(dump)
        self.result['backupSnapshot']=before;self.result['dumpSha256']=hashlib.sha256(dump).hexdigest()
        return {'bytes':len(dump),'sha256':self.result['dumpSha256'],'chat':before['rankingCount'],'pendingOutbox':before['pendingOutbox']}
    def rejected_start(self,expected):
        self.until(lambda:self.owned('api')[1]['State']['Status']=='exited',60)
        _,info=self.owned('api');assert info['State']['ExitCode']!=0 and not info['State']['OOMKilled']
        logs=self.compose('logs','--no-color','--no-log-prefix','api').stdout
        diagnostics=[]
        for line in logs.splitlines():
            try:
                value=json.loads(line)
                if isinstance(value,dict) and value.get('error')=='APPLICATION_START_FAILED':diagnostics.extend(value.get('codes',[]))
            except json.JSONDecodeError:pass
        assert expected in diagnostics
        return {'exitCode':info['State']['ExitCode'],'oomKilled':info['State']['OOMKilled'],'diagnosticCode':expected}
    def schema_failure(self):
        self.stop_api();checksum=int(self.sql("SELECT checksum FROM flyway_schema_history WHERE version='1'").stdout.strip())
        self.sql("UPDATE flyway_schema_history SET checksum=0 WHERE version='1'")
        self.image(self.candidate);self.compose('up','-d','--no-deps','--force-recreate','api')
        rejected=self.rejected_start('SCHEMA_NOT_READY')
        assert self.sql("SELECT checksum FROM flyway_schema_history WHERE version='1'").stdout.strip()=='0'
        self.sql("UPDATE flyway_schema_history SET checksum="+str(checksum)+" WHERE version='1'")
        self.up_apps(self.baseline);assert self.snapshot(20,2)==self.expected
        return {'checksumMismatchStartRejected':rejected,'automaticRepair':False,'previousImageRecovered':True,'scope':'SAME_MIGRATION_SET_CHECKSUM_CORRUPTION'}
    def restore(self,target=None):
        target=target or self.candidate
        self.stop_api();started=time.monotonic();started_at=self.now();original_database=self.sql('SELECT current_database()').stdout.strip();original_group=self.environment['CONSUMER_GROUP']
        self.image(target)
        self.sql('CREATE DATABASE restored OWNER app_owner',database='chatpulse');cid,_=self.owned('postgres')
        self.command(['docker','exec','-i',cid,'pg_restore','-U','app_owner','-d','restored','--exit-on-error','--no-owner'],input=(self.directory/'database.dump').read_bytes(),binary=True)
        self.environment['DATABASE_NAME']='restored';self.write_env()
        restored=self.probe('snapshot');assert restored==self.result['backupSnapshot']
        restored_database=self.sql('SELECT current_database()').stdout.strip();assert restored_database!=original_database
        plan=self.ops('recovery-plan');assert not plan['safeForStartup'] and plan['recoverableFromRetainedLog'];self.result['restoredPlanBefore']=plan
        self.image(target);self.compose('up','-d','--no-deps','--force-recreate','api')
        rejected=self.rejected_start('RECOVERY_ALIGNMENT_REQUIRED')
        prepared=self.ops('recover-groups','deploy-recovered');self.environment['CONSUMER_GROUP']='deploy-recovered';self.write_env()
        self.up_apps(target);actual=self.snapshot(20,2)
        differences=[key for key in self.expected if self.expected[key]!=actual[key]]
        self.result['restoredSnapshot']=actual;self.result['restoredPlanAfter']=self.ops('recovery-plan');assert not differences
        assert self.result['restoredPlanAfter']['safeForStartup']
        old_after=self.ops('recovery-plan',group=original_group)
        def committed(plan):return {(r['group'],r['topic'],r['partition']):r['committed'] for r in plan['partitions']}
        old_changed=committed(plan)!=committed(old_after);assert not old_changed
        self.result['originalGroupsAfterRecovery']=old_after
        return {'startedAt':started_at,'secondsToVerifiedCatchup':time.monotonic()-started,'backupChatGap':self.expected['rankingCount']-restored['rankingCount'],
                'chatGapAfterRecovery':self.expected['rankingCount']-actual['rankingCount'],'businessSnapshotDifferences':differences,'newGroups':prepared['groups'],
                'oldGroupsModified':old_changed,'restoredIntoNewDatabase':restored_database!=original_database,'originalDatabase':original_database,
                'restoredDatabase':restored_database,'alignmentStartRejected':rejected}
    def scenarios(self):
        self.phase('PREFLIGHT',self.preflight);self.phase('INITIALIZE',self.initialize);self.phase('BASELINE_IMAGE',lambda:self.up_apps(self.baseline));self.phase('RUNTIME_BOUNDARIES',self.audit);self.phase('COLLECTOR_CLI_PREFLIGHT',self.collector_preflight)
        self.event_base=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=120)).replace(microsecond=0).isoformat()
        self.result['syntheticEventBase']=self.event_base
        self.phase('SEED_BEFORE_BACKUP',lambda:self.probe('seed','0',self.event_base));self.phase('BASELINE_DATA',lambda:self.snapshot(10,1));self.phase('DATABASE_OUTAGE',self.database_fault)
        self.phase('BACKUP',self.backup);self.phase('UPGRADE_IMAGE',lambda:self.up_apps(self.candidate));self.phase('SEED_AFTER_BACKUP',lambda:self.probe('seed','1',self.event_base))
        self.expected=self.snapshot(20,2);self.result['expectedSnapshot']=self.expected
        self.phase('BROWSER',self.browser)
        self.phase('STOP_NEW_IMAGE',self.stop_api);self.phase('ROLLBACK_IMAGE',lambda:self.up_apps(self.baseline));assert self.snapshot(20,2)==self.expected
        self.phase('SCHEMA_FAILURE_ROLLBACK',self.schema_failure);self.phase('RESTORE_NEW_DATABASE',self.restore);self.phase('FINAL_GRACEFUL_STOP',self.stop_api)
    def run(self):
        try:
            self.scenarios()
            self.result['correct']=True
        except Exception as error:
            self.result.update({'correct':False,'failureType':type(error).__name__})
            if re.fullmatch(r'[A-Z_0-9]+',str(error)):self.result['failureCode']=str(error)
        finally:
            self.cleanup();self.result['finishedAt']=self.now();self.checkpoint()
        print(json.dumps({'correct':self.result['correct'],'cleanupComplete':self.result.get('cleanupComplete')}))
        return 0 if self.result['correct'] and self.result.get('cleanupComplete') else 2
    def browser(self):
        output=self.directory/'browser.json';self.command(['node',str(ROOT/'scripts/deploy-browser.mjs'),self.base,str(output)],timeout=90)
        return json.loads(output.read_text())
    def cleanup(self):
        self.cleaning=True
        try:
            ids=self.command(['docker','ps','-aq','--filter','label=com.docker.compose.project='+self.project]).stdout.split()
            for cid in ids:
                info=json.loads(self.command(['docker','inspect',cid]).stdout)[0]
                if info['Config']['Labels'].get('io.jigeumini.deployment.owner')!=self.owner:raise RuntimeError('CLEANUP_OWNER_MISMATCH')
            if self.started:self.compose('down','--remove-orphans',timeout=150)
            volumes=self.command(['docker','volume','ls','-q','--filter','label=io.jigeumini.deployment.owner='+self.owner]).stdout.split()
            for volume in volumes:
                assert volume.startswith(self.project+'_')
                info=json.loads(self.command(['docker','volume','inspect',volume]).stdout)[0];assert info['Labels']['io.jigeumini.deployment.owner']==self.owner
                self.command(['docker','volume','rm',volume])
            remaining=self.command(['docker','ps','-aq','--filter','label=io.jigeumini.deployment.owner='+self.owner]).stdout.strip()
            remaining_volumes=self.command(['docker','volume','ls','-q','--filter','label=io.jigeumini.deployment.owner='+self.owner]).stdout.strip()
            assert not remaining and not remaining_volumes
            for name in ['secrets/owner.txt','secrets/runtime.txt','database.dump']:(self.directory/name).unlink(missing_ok=True)
            self.result['sensitiveArtifactsRemoved']=not any((self.directory/name).exists() for name in ['secrets/owner.txt','secrets/runtime.txt','database.dump'])
            self.result['cleanupComplete']=True
        except Exception:self.result['cleanupComplete']=False;self.result['correct']=False
if __name__=='__main__':
    try:raise SystemExit(Rehearsal(*sys.argv[1:]).run())
    except Exception as failure:print(json.dumps({'error':'REHEARSAL_SETUP_FAILED','failureType':type(failure).__name__}));raise SystemExit(2)
