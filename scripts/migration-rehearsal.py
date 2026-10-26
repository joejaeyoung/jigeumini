#!/usr/bin/env python3
"""V12 백업·시험 V13·구 이미지 거부·이전 schema 복원을 대조한다."""
import datetime, importlib.util, json, pathlib, re, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('deploy_rehearsal', ROOT / 'scripts/deploy-rehearsal.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MigrationRehearsal(module.Rehearsal):
    def __init__(self, images, directory):
        images = pathlib.Path(images)
        super().__init__(images / 'baseline.json', images / 'candidate.json', directory)
        self.failed_candidate = json.loads((images / 'failed.json').read_text())
        self.interrupted_candidate = json.loads((images / 'interrupted.json').read_text())
        self.result.update(scope='SINGLE_HOST_RF1_SYNTHETIC_MIGRATION_FIXTURE', failedCandidate=self.failed_candidate,
                           interruptedCandidate=self.interrupted_candidate)

    def schema(self, database=None):
        versions = self.sql("SELECT version FROM flyway_schema_history WHERE success AND version IS NOT NULL ORDER BY installed_rank", database=database).stdout.splitlines()
        exists = self.sql("SELECT to_regclass('public.release_migration_probe') IS NOT NULL", database=database).stdout.strip() == 't'
        audit = self.sql('SELECT projection,run_id,event_count FROM release_migration_probe ORDER BY projection,run_id', database=database).stdout.splitlines() if exists else []
        return {'versions': versions, 'migrationProbeExists': exists, 'backfill': audit}

    def fixture_preflight(self):
        result = self.preflight()
        for manifest in [self.candidate, self.failed_candidate, self.interrupted_candidate]:
            assert manifest['revision'] == self.baseline['revision']
            assert manifest['fixture']['baselineImageId'] == self.baseline['images']['server']['id']
            assert manifest['fixture']['version'] == '13'
            assert manifest['scope'] == 'LOCAL_MIGRATION_FIXTURE_NO_REGISTRY_PUSH'
            for image in manifest['images'].values():
                assert json.loads(self.command(['docker', 'image', 'inspect', image['id']]).stdout)[0]['Id'] == image['id']
        assert len({m['buildId'] for m in [self.baseline, self.candidate, self.failed_candidate, self.interrupted_candidate]}) == 4
        return result

    def interruption(self):
        before = self.schema()
        data = self.probe('snapshot')
        self.sql('CREATE SEQUENCE release_failure_attempts')
        self.image(self.interrupted_candidate)
        cid = self.compose('run', '-d', '--no-deps', '--name', self.project + '-interrupted',
                           '-e', 'DEPLOYMENT_MAINTENANCE=true', 'migrate', 'ops', 'migrate').stdout.strip()
        assert re.fullmatch(r'[a-f0-9]{12,64}', cid)
        info = json.loads(self.command(['docker', 'inspect', cid]).stdout)[0]
        assert info['Config']['Labels'].get('io.jigeumini.deployment.owner') == self.owner
        try:
            self.until(lambda: self.sql('SELECT is_called FROM release_failure_attempts').stdout.strip() == 't', 60)
            self.command(['docker', 'kill', '--signal', 'KILL', cid])
            exit_code = int(self.command(['docker', 'wait', cid]).stdout.strip())
            assert exit_code == 137
            self.until(lambda: self.schema() == before, 15)
            assert self.probe('snapshot') == data
        finally:
            self.command(['docker', 'rm', '-f', cid])
        attempts = int(self.sql('SELECT last_value FROM release_failure_attempts').stdout.strip())
        assert attempts == 1
        self.sql('DROP SEQUENCE release_failure_attempts')
        return {'exitCode': exit_code, 'ddlAndBackfillReached': True, 'markerAttempts': attempts,
                'schemaBefore': before, 'schemaAfter': self.schema(), 'businessSnapshotUnchanged': True}

    def failed_migration(self):
        before = self.schema()
        assert before['versions'][-1] == '12' and not before['migrationProbeExists']
        data = self.probe('snapshot')
        self.sql('CREATE SEQUENCE release_failure_attempts')
        self.image(self.failed_candidate)
        failed = self.ops('migrate', check=False)
        assert failed['failed'] and failed['code'] != 0
        after = self.schema()
        assert after == before and self.probe('snapshot') == data
        attempts = self.sql('SELECT last_value,is_called FROM release_failure_attempts').stdout.strip()
        assert attempts == '1|t'
        self.sql('DROP SEQUENCE release_failure_attempts')
        self.up_apps(self.baseline)
        assert self.snapshot(10, 2)['rankingCount'] == 10
        self.stop_api()
        return {'migrationRejected': failed, 'schemaBefore': before, 'schemaAfter': after,
                'businessSnapshotUnchangedBeforeRestart': True, 'baselineRestartReady': True,
                'failedDdlAbsent': True, 'ddlAndBackfillReached': True, 'markerAttempts': 1, 'flywayRepairInvoked': False}

    def migrate_candidate(self):
        self.image(self.candidate)
        migrated = self.ops('migrate')
        state = self.schema()
        assert state['versions'] == [str(i) for i in range(1, 14)]
        assert state['backfill'] == ['ranking|deploy-v1|10', 'ranking-trend|deploy-v1|10']
        return {'migration': migrated, 'schema': state}

    def reject_baseline(self):
        self.stop_api()
        before = self.schema()
        self.image(self.baseline)
        self.compose('up', '-d', '--no-deps', '--force-recreate', 'api')
        rejected = self.rejected_start('SCHEMA_NOT_READY')
        assert self.schema() == before and self.probe('snapshot') == self.expected
        self.up_apps(self.candidate)
        assert self.snapshot(20, 2) == self.expected
        return {'baselineRejected': rejected, 'schemaUnchanged': True, 'candidateRestartReady': True}

    def restore_previous_schema(self):
        restored = self.restore(target=self.baseline)
        schema = self.schema()
        original = self.schema(database='chatpulse')
        assert schema['versions'] == [str(i) for i in range(1, 13)] and not schema['migrationProbeExists']
        assert original['versions'][-1] == '13' and original['backfill'] == ['ranking|deploy-v1|10', 'ranking-trend|deploy-v1|10']
        return {**restored, 'restoredSchema': schema, 'originalSchema': original, 'runtimeBuildId': self.baseline['buildId']}

    def scenarios(self):
        self.phase('PREFLIGHT', self.fixture_preflight)
        self.phase('INITIALIZE_V12', self.initialize)
        self.phase('BASELINE_V12', lambda: self.up_apps(self.baseline))
        self.phase('RUNTIME_BOUNDARIES', self.audit)
        self.event_base = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)).replace(microsecond=0).isoformat()
        self.result['syntheticEventBase'] = self.event_base
        self.phase('SEED_BEFORE_BACKUP', lambda: self.probe('seed', '0', self.event_base))
        self.phase('BASELINE_DATA', lambda: self.snapshot(10, 1))
        self.phase('BACKUP_V12', self.backup)
        self.phase('INTERRUPTED_V13_ROLLBACK', self.interruption)
        self.phase('FAILED_V13_ROLLBACK', self.failed_migration)
        self.phase('MIGRATE_V13', self.migrate_candidate)
        self.phase('CANDIDATE_V13', lambda: self.up_apps(self.candidate))
        self.phase('SEED_AFTER_MIGRATION', lambda: self.probe('seed', '1', self.event_base))
        self.expected = self.snapshot(20, 2)
        self.result['expectedSnapshot'] = self.expected
        self.phase('BASELINE_V12_REJECTS_V13', self.reject_baseline)
        self.phase('RESTORE_V12_NEW_DATABASE', self.restore_previous_schema)
        self.phase('RESTORED_BROWSER', self.browser)
        self.phase('FINAL_GRACEFUL_STOP', self.stop_api)


if __name__ == '__main__':
    try:
        raise SystemExit(MigrationRehearsal(*sys.argv[1:]).run())
    except Exception as failure:
        print(json.dumps({'error': 'MIGRATION_REHEARSAL_SETUP_FAILED', 'failureType': type(failure).__name__}))
        raise SystemExit(2)
