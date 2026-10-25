"""실제 로컬 Git force-push와 shallow clone으로 CI 기준선 처리를 검증한다."""
from pathlib import Path
import os, subprocess, tempfile, textwrap, unittest
workflow=(Path(__file__).resolve().parents[2] / '.github/workflows/check.yml').read_text()
block=workflow.split('      - name: 기술 목표 연결과 기존 ID 보존 검사',1)[1].split('  branch-policy:',1)[0]
script=textwrap.dedent(block.split('        run: |\n',1)[1].split('          python3 scripts/check-doc-links.py',1)[0])
def run(args,cwd,**kwargs):
    return subprocess.run(args,cwd=cwd,text=True,capture_output=True,check=True,**kwargs).stdout.strip()


class CiBaselineTest(unittest.TestCase):
    def test_force_push_fetch_missing_base_and_event_policy(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); source=root/'source'; source.mkdir()
            run(['git','init','-b','main'],source)
            run(['git','config','user.name','검증 fixture'],source)
            run(['git','config','user.email','fixture@example.invalid'],source)
            (source/'sample').write_text('old')
            run(['git','add','sample'],source); run(['git','commit','-m','old'],source)
            old=run(['git','rev-parse','HEAD'],source)
            bare=root/'remote.git';run(['git','clone','--bare',str(source),str(bare)],root)
            run(['git','checkout','--orphan','replacement'],source)
            (source/'sample').write_text('new')
            run(['git','add','sample'],source);run(['git','commit','-m','new'],source)
            run(['git','push','--force',str(bare),'HEAD:main'],source)
            clone=root/'clone';run(['git','clone','--depth','1',bare.as_uri(),str(clone)],root)
            assert subprocess.run(['git','cat-file','-e',old+'^{commit}'],cwd=clone,capture_output=True).returncode!=0
            bins=root/'bin';bins.mkdir()
            stub=bins/'python3';stub.write_text('#!/bin/sh\nprintf "PYTHON:%s\\n" "$*"\n');stub.chmod(0o755)
            env=dict(os.environ,PATH=str(bins)+':'+os.environ['PATH'],BASE_REV=old,EVENT_NAME='push')
            result=subprocess.run(['bash','-e','-c',script],cwd=clone,env=env,text=True,capture_output=True)
            assert result.returncode==0,(result.stdout,result.stderr)
            assert '--base '+old in result.stdout
            missing='1'*40
            for event in ('push','pull_request'):
                env.update(BASE_REV=missing,EVENT_NAME=event)
                result=subprocess.run(['bash','-e','-c',script],cwd=clone,env=env,text=True,capture_output=True)
                if event=='push':
                    assert result.returncode==0 and '::warning::' in result.stdout and 'PYTHON:scripts/check-tech-goals.py\n' in result.stdout
                else:
                    assert result.returncode!=0 and '::error::' in result.stdout and 'PYTHON:' not in result.stdout
            env.update(BASE_REV='0'*40,EVENT_NAME='push')
            result=subprocess.run(['bash','-e','-c',script],cwd=clone,env=env,text=True,capture_output=True)
            assert result.returncode==0 and '--base' not in result.stdout
