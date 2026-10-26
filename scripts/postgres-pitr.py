#!/usr/bin/env python3
"""소유 PostgreSQL physical base·archive로 목표에 멈춘 뒤 Kafka 위치를 정렬한다."""
import datetime,hashlib,importlib.util,json,pathlib,re,time,sys,shutil
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('deployment_rehearsal',ROOT/'scripts/deploy-rehearsal.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class PitrRehearsal(module.Rehearsal):
    def __init__(self,manifest,directory):
        super().__init__(manifest,manifest,directory)
        self.deadline=time.monotonic()+2400;self.database_service='postgres'
        self.overlay=self.directory/'pitr.compose.json';self.target='pitr_'+self.owner+'_target'
        self.result.update({'scope':'LOCAL_PHYSICAL_PITR_NAMED_TARGET_AND_KAFKA_REALIGNMENT','targetName':self.target})
    def write_overlay(self):
        labels={'io.jigeumini.deployment.owner':self.owner};tools=str(ROOT/'deploy/fixtures/pitr')
        services={'postgres':{'command':['postgres','-c','wal_level=replica','-c','archive_mode=on','-c','archive_command=/test-tools/archive-wal %p %f','-c','max_wal_senders=4'],
            'volumes':['pitr-base:/pitr/base','pitr-archive:/pitr/archive',tools+':/test-tools:ro']}}
        for mode in ['restored','missing','gap']:
            services['pitr-'+mode]={'image':'postgres:16.14-alpine','mem_limit':'384m','cpus':1,'shm_size':'128m','labels':labels,'networks':['data'],
                'environment':{'PITR_MODE':mode},'command':['postgres','-c','archive_mode=off'],
                'volumes':['pitr-'+mode+':/var/lib/postgresql/data',('pitr-missing-archive' if mode=='missing' else 'pitr-archive')+':/pitr/archive:ro','pitr-used:/pitr/used',tools+':/test-tools:ro']}
        volumes=['base','archive','restored','missing','missing-archive','gap','used']
        services['pitr-helper']={'image':'postgres:16.14-alpine','profiles':['maintenance'],'labels':labels,'mem_limit':'256m','cpus':.5,'network_mode':'none','user':'0',
            'volumes':['pitr-'+name+':/pitr/'+name for name in volumes]}
        jdbc='jdbc:postgresql://'+self.database_service+':5432/chatpulse'
        for service in ['api','migrate']:services[service]={'environment':{'PIPELINE_JDBC_URL':jdbc}}
        self.overlay.write_text(json.dumps({'services':services,'volumes':{'pitr-'+name:{'labels':labels} for name in volumes}}))
    def compose(self,*args,**kwargs):
        if not hasattr(self,'overlay') or not self.overlay.exists():return super().compose(*args,**kwargs)
        return self.command(['docker','compose','--env-file',str(self.envfile),'-p',self.project,'-f',str(ROOT/'deploy/compose.yaml'),'-f',str(self.overlay),*args],**kwargs)
    def sql(self,statement,database=None,user='app_owner',check=True,sqlstate=False):
        cid,_=self.owned(self.database_service)
        return self.command(['docker','exec',cid,'psql','-X','-v','ON_ERROR_STOP=1',*(['-v','VERBOSITY=sqlstate'] if sqlstate else []),'-U',user,'-d',database or 'chatpulse','-At','-c',statement],check=check)
    def helper(self,script):return self.compose('run','--rm','--no-deps','--entrypoint','sh','pitr-helper','-eu','-c',script,timeout=120)
    def choose_database(self,service):self.database_service=service;self.write_overlay()
    def preflight(self):
        result=super().preflight();usage=0
        for line in self.command(['docker','stats','--no-stream','--format','{{.MemUsage}}']).stdout.splitlines():
            match=re.match(r'([0-9.]+)([kKMGT]?i?B) /',line)
            if not match:raise RuntimeError('DOCKER_MEMORY_UNAVAILABLE')
            scale={'B':1,'kB':1000,'KB':1000,'KiB':1024,'MB':1000**2,'MiB':1024**2,'GB':1000**3,'GiB':1024**3,'TB':1000**4,'TiB':1024**4}[match[2]]
            usage+=float(match[1])*scale
        maximum=(640+640+768+384+64+3*384+256)*1024**2
        assert usage+maximum<=result['dockerMemoryBytes']*.90
        result.update({'maximumComposeMemoryBytes':maximum,'additionalRestoredPostgres':3,'helperMemoryBytes':256*1024**2})
        return result
    def freeze_probe(self):
        self.probe_classes=self.directory/'probe-classes';target=self.probe_classes/'io/chatpulse/operations/DeploymentProbe.class'
        target.parent.mkdir(parents=True);shutil.copyfile(ROOT/'server/build/classes/java/test/io/chatpulse/operations/DeploymentProbe.class',target)
        self.probe_hash=hashlib.sha256(target.read_bytes()).hexdigest()
        self.result['frozenProbe']={'class':'io.chatpulse.operations.DeploymentProbe','sha256':self.probe_hash,'bytes':target.stat().st_size}
        return self.result['frozenProbe']
    def probe(self,*args):
        target=self.probe_classes/'io/chatpulse/operations/DeploymentProbe.class'
        assert hashlib.sha256(target.read_bytes()).hexdigest()==self.probe_hash
        command=['run','--rm','--no-deps','--entrypoint','java','-v',str(self.probe_classes)+':/probe:ro','api',
                 '-Xmx128m','-cp','/app/classes:/app/lib/*:/probe','io.chatpulse.operations.DeploymentProbe',*args]
        return json.loads(self.compose(*command).stdout.strip().splitlines()[-1])
    def initialize(self):
        result=super().initialize()
        self.helper('for name in base archive restored missing missing-archive gap used; do chown postgres:postgres "/pitr/$name"; chmod 700 "/pitr/$name"; done')
        self.sql('CREATE TABLE pitr_fixture_marker(id integer PRIMARY KEY, stage text NOT NULL); GRANT SELECT,INSERT,DELETE ON pitr_fixture_marker TO app_runtime')
        self.sql("INSERT INTO pitr_fixture_marker VALUES (0,'permission_probe'); DELETE FROM pitr_fixture_marker WHERE id=0",user='app_runtime')
        settings=self.sql("SELECT json_build_object('serverVersion',current_setting('server_version'),'walLevel',current_setting('wal_level'),'archiveMode',current_setting('archive_mode'),'fullPageWrites',current_setting('full_page_writes'),'fsync',current_setting('fsync'),'walSegmentBytes',pg_size_bytes(current_setting('wal_segment_size')),'systemIdentifier',(pg_control_system()).system_identifier::text,'timeline',(pg_control_checkpoint()).timeline_id)").stdout.strip()
        self.result['postgres']=json.loads(settings);assert self.result['postgres']['serverVersion'].startswith('16.14')
        assert self.result['postgres']['archiveMode']=='on' and self.result['postgres']['fullPageWrites']=='on' and self.result['postgres']['fsync']=='on'
        return result
    def base_backup(self):
        cid,_=self.owned('postgres');started=self.now()
        self.command(['docker','exec','-u','postgres',cid,'pg_basebackup','-U','app_owner','-D','/pitr/base','-F','plain','-X','stream','--checkpoint=fast','--manifest-checksums=SHA256'],timeout=120)
        verified=self.command(['docker','exec','-u','postgres',cid,'pg_verifybackup','/pitr/base'],timeout=120)
        manifest=self.command(['docker','exec',cid,'cat','/pitr/base/backup_manifest']).stdout;data=json.loads(manifest)
        names=self.command(['docker','exec',cid,'ls','-1','/pitr/base/pg_wal']).stdout.splitlines()
        self.base_wal=[name for name in names if re.fullmatch('[0-9A-F]{24}',name)]
        self.result['baseBackup']={'startedAt':started,'finishedAt':self.now(),'verified':verified.returncode==0,'manifestSha256':hashlib.sha256(manifest.encode()).hexdigest(),'walRanges':data['WAL-Ranges'],'files':len(data['Files']),'includedWal':self.base_wal}
        self.sql("INSERT INTO pitr_fixture_marker VALUES (0,'base_complete'); SELECT pg_switch_wal()")
        return self.result['baseBackup']
    def target_state(self):
        self.up_apps(self.candidate);self.event_base=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=30)).isoformat()
        self.probe('seed','0',self.event_base);self.snapshot(10,1);self.stop_api();self.probe('pending-outbox',self.event_base)
        self.sql("INSERT INTO pitr_fixture_marker VALUES (1,'before_target')",user='app_runtime')
        self.target_snapshot=self.probe('snapshot');assert self.target_snapshot['rankingCount']==10 and self.target_snapshot['pendingOutbox']==1
        self.target_lsn=self.sql("SELECT pg_create_restore_point('"+self.target+"')").stdout.strip()
        self.target_wal=self.sql("SELECT pg_walfile_name('"+self.target_lsn+"'::pg_lsn)").stdout.strip()
        assert re.fullmatch('[0-9A-F]{24}',self.target_wal) and self.target_wal not in self.base_wal
        self.result['targetSnapshot']=self.target_snapshot;self.result['targetLsn']=self.target_lsn;self.result['requiredTargetWal']=self.target_wal
        return {'chat':10,'pendingOutbox':1,'lsn':self.target_lsn,'requiredWalAbsentFromBase':True}
    def after_target(self):
        self.up_apps(self.candidate);self.probe('seed','1',self.event_base);self.expected=self.snapshot(20,2)
        self.sql("INSERT INTO pitr_fixture_marker VALUES (2,'after_target')",user='app_runtime');self.stop_api();self.result['latestSnapshot']=self.expected
        self.sql('SELECT pg_switch_wal()')
        cid,_=self.owned('postgres')
        self.until(lambda:self.command(['docker','exec',cid,'test','-f','/pitr/archive/'+self.target_wal],check=False).returncode==0,60)
        inventory=self.helper('for file in /pitr/archive/*; do test -f "$file" || continue; sha256sum "$file"; done').stdout.splitlines()
        files={path.rsplit('/',1)[-1]:digest for digest,path in (line.split(maxsplit=1) for line in inventory)}
        assert self.target_wal in files
        lsn=lambda text: (int(text.split('/')[0],16)<<32)+int(text.split('/')[1],16)
        segment=self.result['postgres']['walSegmentBytes'];per_log=2**32//segment;timeline=self.result['postgres']['timeline']
        ranges=self.result['baseBackup']['walRanges'];assert len(ranges)==1 and ranges[0]['Timeline']==timeline
        assert lsn(ranges[0]['End-LSN'])<lsn(self.target_lsn)
        first=lsn(ranges[0]['Start-LSN'])//segment;last=(lsn(self.target_lsn)-1)//segment
        required=[f'{timeline:08X}{number//per_log:08X}{number%per_log:08X}' for number in range(first,last+1)]
        assert self.target_wal in required and all(name in self.base_wal or name in files for name in required)
        self.result['walContinuity']={'fromLsn':ranges[0]['Start-LSN'],'throughTargetLsn':self.target_lsn,'segmentBytes':segment,'requiredSegments':required,'missingSegments':[]}
        self.result['archiveHashes']=files
        self.result['archiver']=json.loads(self.sql("SELECT row_to_json(s) FROM (SELECT archived_count,failed_count,last_archived_wal,last_archived_time FROM pg_stat_archiver) s").stdout)
        return {'chat':20,'notifications':2,'archivedFiles':len(files),'targetWalSha256':files[self.target_wal]}
    def prepare_restore(self,mode):
        assert mode in ['restored','missing','gap']
        # baseline과 archive를 복구 대상으로 덮어쓰지 않는다. 각 mode는 새 빈 PGDATA다.
        self.helper('test -z "$(ls -A /pitr/'+mode+')"; cp -a /pitr/base/. /pitr/'+mode+'/; rm -f /pitr/'+mode+'/postmaster.pid /pitr/'+mode+'/standby.signal')
        configuration="\nrestore_command = '/test-tools/restore-wal %f %p'\nrecovery_target_name = '"+self.target+"'\nrecovery_target_timeline = '"+str(self.result['postgres']['timeline'])+"'\nrecovery_target_action = 'pause'\nprimary_conninfo = ''\nprimary_slot_name = ''\n"
        # 설정은 고정값과 이번 실행의 hex ID만 포함하며 shlex로 shell 인자를 인용한다.
        import shlex
        self.helper('printf %s '+shlex.quote(configuration)+' >> /pitr/'+mode+'/postgresql.auto.conf; touch /pitr/'+mode+'/recovery.signal; chown -R postgres:postgres /pitr/'+mode+'; chmod 700 /pitr/'+mode)
    def missing_wal(self):
        self.prepare_restore('missing')
        self.helper('cp -a /pitr/archive/. /pitr/missing-archive/; rm /pitr/missing-archive/'+self.target_wal+'; chown -R postgres:postgres /pitr/missing-archive')
        check=self.helper('test ! -e /pitr/missing-archive/'+self.target_wal+'; test ! -e /pitr/base/pg_wal/'+self.target_wal+'; sha256sum /pitr/archive/'+self.target_wal).stdout.split()[0]
        assert check==self.result['archiveHashes'][self.target_wal]
        self.compose('up','-d','--no-deps','pitr-missing');self.until(lambda:self.owned('pitr-missing')[1]['State']['Status']=='exited',90)
        _,info=self.owned('pitr-missing');assert info['State']['ExitCode']!=0 and not info['State']['OOMKilled']
        logs=self.compose('logs','--no-color','--no-log-prefix','pitr-missing').stdout
        assert 'recovery ended before configured recovery target was reached' in logs
        return {'requiredWal':self.target_wal,'missingFromArchiveCopy':True,'absentFromBase':True,'targetReached':False,'writableSuccess':False,'exitCode':info['State']['ExitCode'],'failureCode':'RECOVERY_ENDED_BEFORE_TARGET','sourceArchivePreserved':True}
    def restore_to_pause(self,mode):
        self.prepare_restore(mode);began=time.monotonic();self.compose('up','-d','--no-deps','pitr-'+mode);self.choose_database('pitr-'+mode)
        def paused():
            query=self.sql("SELECT pg_is_in_recovery()::text||'|'||pg_get_wal_replay_pause_state()",check=False)
            return query.returncode==0 and query.stdout.strip()=='true|paused'
        self.until(paused,90)
        restored=self.probe('snapshot');assert restored==self.target_snapshot
        markers=self.sql('SELECT id FROM pitr_fixture_marker ORDER BY id').stdout.splitlines();assert markers==['0','1']
        readonly=self.sql("INSERT INTO pitr_fixture_marker VALUES (9,'must_fail_in_recovery')",user='app_runtime',check=False,sqlstate=True)
        assert readonly.returncode!=0 and re.search(r'ERROR:\s+25006',readonly.stderr)
        used=self.helper('cat /pitr/used/'+mode+'.log').stdout.splitlines();assert self.target_wal in used
        state=json.loads(self.sql("SELECT json_build_object('replayLsn',pg_last_wal_replay_lsn()::text,'lastReplayTransaction',pg_last_xact_replay_timestamp(),'systemIdentifier',(pg_control_system()).system_identifier::text,'timeline',(pg_control_checkpoint()).timeline_id,'target',current_setting('recovery_target_name'))").stdout)
        assert state['systemIdentifier']==self.result['postgres']['systemIdentifier'] and state['target']==self.target
        return {'secondsToTargetPause':time.monotonic()-began,'snapshotDifferences':[],'inRecovery':True,'pauseState':'paused','readonlySqlState':'25006','markerIds':markers,'archiveFilesActuallyUsed':sorted(set(used)),'state':state}
    def promote(self):
        began=time.monotonic();self.sql('SELECT pg_wal_replay_resume()');self.until(lambda:self.sql('SELECT pg_is_in_recovery()').stdout.strip()=='f',30)
        assert self.sql('SHOW transaction_read_only').stdout.strip()=='off'
        self.sql("INSERT INTO pitr_fixture_marker VALUES (3,'after_promotion')",user='app_runtime');assert self.sql('SELECT count(*) FROM pitr_fixture_marker WHERE id=3',user='app_runtime').stdout.strip()=='1'
        assert self.probe('snapshot')==self.target_snapshot;self.sql('CHECKPOINT')
        control=json.loads(self.sql("SELECT json_build_object('systemIdentifier',(pg_control_system()).system_identifier::text,'timeline',(pg_control_checkpoint()).timeline_id)").stdout)
        assert control['systemIdentifier']==self.result['postgres']['systemIdentifier'] and control['timeline']>self.result['postgres']['timeline']
        cid,_=self.owned(self.database_service);history=f"{control['timeline']:08X}.history"
        history_hash=self.command(['docker','exec',cid,'sha256sum','/var/lib/postgresql/data/pg_wal/'+history]).stdout.split()[0]
        return {'secondsToWritable':time.monotonic()-began,'timelineHistoryFile':history,'timelineHistorySha256':history_hash,'control':control,'targetSnapshotUnchanged':True,'separateConnectionReadCommittedMarker':True}
    def replay(self):
        began=time.monotonic();old_group=self.environment['CONSUMER_GROUP'];before=self.ops('recovery-plan');assert not before['safeForStartup'] and before['recoverableFromRetainedLog']
        self.result['restoredPlanBefore']=before;self.compose('up','-d','--no-deps','--force-recreate','api');rejected=self.rejected_start('RECOVERY_ALIGNMENT_REQUIRED')
        empty=self.ops('recovery-plan',group='pitr-recovered');assert all(row['committed'] is None for row in empty['partitions'])
        groups=self.ops('recover-groups','pitr-recovered');prepared=self.ops('recovery-plan',group='pitr-recovered')
        assert all(row['committed']==row['databaseNext'] and row['code']=='ALIGNED' for row in prepared['partitions'])
        self.result['newGroupsBeforePreparation']=empty;self.result['newGroupsBeforeStartup']=prepared
        self.environment['CONSUMER_GROUP']='pitr-recovered';self.write_env();self.up_apps(self.candidate)
        actual=self.snapshot(20,2);assert actual==self.expected;self.result['replayedSnapshot']=actual;self.stop_api()
        after=self.ops('recovery-plan');assert after['safeForStartup'];old_after=self.ops('recovery-plan',group=old_group)
        committed=lambda plan:{(x['group'],x['topic'],x['partition']):x['committed'] for x in plan['partitions']}
        assert committed(before)==committed(old_after);self.result['originalGroupsAfterReplay']=old_after
        return {'secondsToVerifiedCatchup':time.monotonic()-began,'newGroups':groups['groups'],'alignmentStartRejected':rejected,'oldGroupsModified':False,'businessSnapshotDifferences':[],'chatRecoveredFromKafka':10}
    def retention_gap(self):
        self.restore_to_pause('gap');self.promote();self.environment['CONSUMER_GROUP']='deploy-live';self.write_env()
        before=self.probe('snapshot');trim=self.probe('trim-retained-chat');assert trim['lowWatermark']==11
        plan=self.ops('recovery-plan');assert not plan['recoverableFromRetainedLog'] and not plan['safeForStartup']
        gaps=[x for x in plan['partitions'] if x['code']=='RETENTION_GAP'];assert len(gaps)==4 and all(x['databaseNext']==10 and x['begin']==11 for x in gaps)
        rejected=self.compose('run','--rm','--no-deps','-e','DEPLOYMENT_MAINTENANCE=true','migrate','ops','recover-groups','pitr-denied',check=False)
        failure=json.loads(rejected.stdout.strip().splitlines()[-1]);assert rejected.returncode!=0 and failure['error']=='OPERATIONS_FAILED' and 'RECOVERY_RETENTION_GAP' in failure['codes']
        denied=self.ops('recovery-plan',group='pitr-denied');assert all(row['committed'] is None for row in denied['partitions'])
        after=self.ops('recovery-plan');committed=lambda value:{(r['group'],r['topic'],r['partition']):r['committed'] for r in value['partitions']}
        assert committed(plan)==committed(after)
        assert self.probe('snapshot')==before
        self.compose('up','-d','--no-deps','--force-recreate','api');rejected=self.rejected_start('RECOVERY_ALIGNMENT_REQUIRED')
        return {'trim':trim,'gaps':gaps,'newGroupsRejected':failure,'newGroupOffsetsAbsent':True,'oldGroupOffsetsUnchanged':True,'startupRejected':rejected,'snapshotUnchanged':True}
    def cleanup(self):
        super().cleanup()
        self.result['physicalBackupAndArchiveVolumesRemoved']=self.result.get('cleanupComplete',False)
    def scenarios(self):
        self.phase('PREPARE',self.write_overlay);self.phase('PREFLIGHT',self.preflight);self.phase('FREEZE_PROBE',self.freeze_probe);self.phase('INITIALIZE_ARCHIVE',self.initialize);self.phase('BASE_BACKUP',self.base_backup)
        self.phase('TARGET_T',self.target_state);self.phase('LATEST_C_AND_ARCHIVE',self.after_target)
        self.phase('MISSING_REQUIRED_WAL',self.missing_wal);self.phase('RESTORE_TO_TARGET_PAUSE',lambda:self.restore_to_pause('restored'))
        self.phase('PROMOTE_AFTER_TARGET_VERIFICATION',self.promote);self.phase('KAFKA_REALIGN_AND_REPLAY',self.replay);self.phase('KAFKA_RETENTION_GAP',self.retention_gap)

if __name__=='__main__':
    if len(sys.argv)!=3:raise SystemExit('usage: postgres-pitr <image-manifest> <new-output-directory>')
    raise SystemExit(PitrRehearsal(sys.argv[1],sys.argv[2]).run())
