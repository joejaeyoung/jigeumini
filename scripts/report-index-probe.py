#!/usr/bin/env python3
"""로컬 개발 DB의 임시 테이블에서 세션별 최신 관측 조회 비용을 대조한다. 제품 테이블은 변경하지 않는다."""
from pathlib import Path
import subprocess,json,datetime,hashlib,sys
ROOT=Path(__file__).resolve().parents[1]
if len(sys.argv)!=2:raise SystemExit('usage: report-index-probe.py <new-output.json>')
output=Path(sys.argv[1]);output.parent.mkdir(parents=True,exist_ok=True)
if output.exists():raise SystemExit('OUTPUT_ALREADY_EXISTS')
query="SELECT observed_at,ttl_seconds,live FROM report_observation WHERE run_id='synthetic' AND report_id='00000000-0000-0000-0000-000000000001' ORDER BY observed_at DESC,event_id DESC LIMIT 1"
sql="""BEGIN;
SET LOCAL statement_timeout='5s';
CREATE TEMP TABLE report_observation (run_id text,event_id uuid,channel_id char(32),observed_at timestamptz,ttl_seconds integer,live boolean,report_id uuid);
CREATE INDEX report_observation_time ON report_observation(run_id,channel_id,observed_at DESC,event_id DESC);
CREATE INDEX report_observation_report ON report_observation(run_id,report_id);
INSERT INTO report_observation SELECT 'synthetic',gen_random_uuid(),repeat('a',32),'2026-10-20T00:00:00Z'::timestamptz+i*interval '30 seconds',120,true,'00000000-0000-0000-0000-000000000001' FROM generate_series(1,20160) i;
ANALYZE report_observation;
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) """+query+""";
CREATE INDEX report_observation_report_time ON report_observation(run_id,report_id,observed_at DESC,event_id DESC);
ANALYZE report_observation;
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) """+query+""";
ROLLBACK;
"""
r=subprocess.run(['docker','compose','-p','chat-pulse','-f',str(ROOT/'compose.yaml'),'exec','-T','postgres','psql','-U','chatpulse','-d','chatpulse','-qAt','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True,check=True,cwd=ROOT,timeout=30)
decoder=json.JSONDecoder();text=r.stdout.strip();plans=[]
while text:
 value,offset=decoder.raw_decode(text);plans.append(value[0]);text=text[offset:].lstrip()
keys=['Node Type','Index Name','Actual Startup Time','Actual Total Time','Actual Rows','Actual Loops','Rows Removed by Filter','Sort Method','Sort Space Used','Sort Space Type','Shared Hit Blocks','Shared Read Blocks','Local Hit Blocks','Local Read Blocks','Temp Read Blocks','Temp Written Blocks']
def clean(node):return {**{k:v for k,v in node.items() if k in keys},'Plans':[clean(n) for n in node.get('Plans',[])]}
assert len(plans)==2
out={'executedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'rows':20160,'scope':'SYNTHETIC_TEMP_TABLE_ONE_SESSION_SEVEN_DAYS_AT_30_SECONDS; NOT_PRODUCT_CAPACITY','before':{'planningMillis':plans[0]['Planning Time'],'executionMillis':plans[0]['Execution Time'],'plan':clean(plans[0]['Plan'])},'after':{'planningMillis':plans[1]['Planning Time'],'executionMillis':plans[1]['Execution Time'],'plan':clean(plans[1]['Plan'])},'rollbackCompleted':True,'scriptSha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'output':str(output),'rows':out['rows'],'beforeMillis':out['before']['executionMillis'],'afterMillis':out['after']['executionMillis'],'rollbackCompleted':True}))
