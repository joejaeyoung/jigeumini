#!/usr/bin/env python3
"""제품 V12 이미지에 시험 전용 migration을 얹고 별도 이미지·빌드 식별을 남긴다."""
import hashlib, json, pathlib, subprocess, sys, tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def build(baseline_path, directory):
    if run('git', 'status', '--porcelain'):
        raise RuntimeError('MIGRATION_IMAGES_REQUIRE_CLEAN_COMMIT')
    baseline = json.loads(pathlib.Path(baseline_path).read_text())
    revision = run('git', 'rev-parse', 'HEAD')
    if baseline['revision'] != revision:
        raise RuntimeError('BASELINE_REVISION_MISMATCH')
    assert not baseline['workingTreeDirty'] and baseline['scope'] == 'LOCAL_IMAGES_NO_REGISTRY_PUSH'
    migrations = ROOT / 'server/src/main/resources/db/migration'
    assert max(int(p.name.split('__')[0][1:]) for p in migrations.glob('V*__*.sql')) == 12
    for entry in baseline['images'].values():
        assert json.loads(run('docker', 'image', 'inspect', entry['id']))[0]['Id'] == entry['id']
    directory = pathlib.Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'baseline.json').write_text(json.dumps(baseline, ensure_ascii=False, indent=2) + '\n')
    for kind in ['candidate', 'failed', 'interrupted']:
        fixture = ROOT / 'deploy/fixtures/migrations' / kind / 'V13__release_probe.sql'
        sql_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
        build_id = hashlib.sha256(('migration-fixture-v1\0' + baseline['buildId'] + '\0' + sql_hash).encode()).hexdigest()
        with tempfile.TemporaryDirectory(prefix='migration-image-', dir=ROOT / 'work') as temporary:
            context = pathlib.Path(temporary)
            (context / fixture.name).write_bytes(fixture.read_bytes())
            (context / 'replay-build-id.txt').write_text(build_id + '\n')
            (context / 'Dockerfile').write_text('ARG BASE_IMAGE\nFROM ${BASE_IMAGE}\n'
                'COPY --chown=10001:10001 V13__release_probe.sql /app/classes/db/migration/\n'
                'COPY --chown=10001:10001 replay-build-id.txt /app/classes/replay-build-id.txt\n'
                'LABEL io.jigeumini.test.fixture="migration-v13"\n')
            tag = f'jigeumini/migration-fixture:{kind}-{revision}'
            subprocess.run(['docker', 'build', '--build-arg', 'BASE_IMAGE=' + baseline['images']['server']['id'],
                            '-t', tag, str(context)], check=True, cwd=ROOT, stdout=sys.stderr)
        image = json.loads(run('docker', 'image', 'inspect', tag))[0]
        manifest = {**baseline, 'buildId': build_id, 'scope': 'LOCAL_MIGRATION_FIXTURE_NO_REGISTRY_PUSH',
                    'images': {**baseline['images'], 'server': {'tag': tag, 'id': image['Id'], 'sizeBytes': image['Size'], 'user': image['Config']['User']}},
                    'fixture': {'kind': kind, 'version': '13', 'sqlSha256': sql_hash,
                                'baselineImageId': baseline['images']['server']['id'], 'baselineBuildId': baseline['buildId'],
                                'buildIdAlgorithm': 'sha256(migration-fixture-v1 NUL baselineBuildId NUL sqlSha256)',
                                'jarSha256Scope': '변경 전 제품 JAR. 시험 migration과 buildId는 이미지의 classes에만 추가한다.'}}
        (directory / (kind + '.json')).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'scope': 'LOCAL_MIGRATION_FIXTURE_NO_REGISTRY_PUSH', 'revision': revision, 'manifests': 4}))


if __name__ == '__main__':
    try:
        build(*sys.argv[1:])
    except Exception as failure:
        print(json.dumps({'error': 'MIGRATION_IMAGE_BUILD_FAILED', 'failureType': type(failure).__name__}))
        raise SystemExit(2)
