#!/usr/bin/env python3
"""고정 commit과 실제 image ID를 연결한다. 비밀 파일은 build context에 복사하지 않는다."""
import hashlib,json,pathlib,shutil,subprocess,sys,tempfile,zipfile
root=pathlib.Path(__file__).resolve().parents[1]
def run(*args): return subprocess.check_output(args,cwd=root,text=True).strip()
if run('git','status','--porcelain'): raise SystemExit('DEPLOY_REQUIRES_CLEAN_COMMIT')
out=pathlib.Path(sys.argv[1]).resolve();out.parent.mkdir(parents=True,exist_ok=True)
if out.exists(): raise SystemExit('DEPLOY_OUTPUT_EXISTS')
revision=run('git','rev-parse','HEAD')
with tempfile.TemporaryDirectory(prefix='images-',dir=root/'work') as temporary:
 context=pathlib.Path(temporary);server=context/'server';server.mkdir();web=context/'web';web.mkdir()
 jar=root/'server/build/libs/chat-pulse-poc.jar'
 with zipfile.ZipFile(jar) as z:
  for member in z.namelist():
   for prefix,target in [('BOOT-INF/classes/','classes/'),('BOOT-INF/lib/','lib/')]:
    if member.startswith(prefix) and not member.endswith('/'):
     destination=server/(target+member[len(prefix):]);destination.parent.mkdir(parents=True,exist_ok=True);destination.write_bytes(z.read(member))
 shutil.copytree(root/'web/dist',web/'dist');shutil.copyfile(root/'deploy/nginx.conf',web/'nginx.conf')
 (web/'dist/revision.json').write_text(json.dumps({'revision':revision})+'\n')
 images={}
 for name,context_dir in [('server',server),('web',web)]:
  tag=f'jigeumini/{name}:{revision}'
  subprocess.run(['docker','build','--build-arg',f'REVISION={revision}','-f',str(root/f'deploy/{name}.Dockerfile'),'-t',tag,str(context_dir)],check=True,cwd=root,stdout=sys.stderr)
  info=json.loads(run('docker','image','inspect',tag))[0];images[name]={'tag':tag,'id':info['Id'],'sizeBytes':info['Size'],'user':info['Config']['User']}
 data={'revision':revision,'workingTreeDirty':False,'jarSha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'buildId':(server/'classes/replay-build-id.txt').read_text().strip(),'images':images,'scope':'LOCAL_IMAGES_NO_REGISTRY_PUSH'}
 with out.open('x') as f:json.dump(data,f,ensure_ascii=False,indent=2);f.write('\n')
 print(json.dumps(data,ensure_ascii=False))
