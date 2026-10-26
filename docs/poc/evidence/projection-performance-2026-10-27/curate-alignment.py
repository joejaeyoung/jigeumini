#!/usr/bin/env python3
"""600채널 전후의 공통 경과 축. 인수: output result.json...; 원자 표본·인과 추론이 아님."""
import datetime,hashlib,json,pathlib,sys

def digest(p):return {'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
def epoch(s):return datetime.datetime.fromisoformat(s).timestamp()
runs=[]
for source in map(pathlib.Path,sys.argv[2:]):
 d=json.loads(source.read_text());load=d['load'];start=load['inputStartedAtEpochMillis']/1000;offset=source.parent/'probe-output/offsets.jsonl'
 rows=[json.loads(s) for s in offset.read_text().splitlines()];duration=sum(s['seconds'] for s in d['loadContract']['stages'])
 points=[]
 for row in rows:
  t=row['elapsedSeconds'];parts=[p for p in row['partitions'] if p['group']=='deploy-live-reports' and p['topic']=='chat.received.v1']
  if 30<=t<duration and len(parts)==3:points.append((t,parts))
 lag=[]
 for t,parts in points:
  commits={p['partition']:p['committed'] for p in parts};age=0
  for before,target in points:
   if before>t:break
   if any(commits[p['partition']]<p['end'] for p in target):age=max(age,t-before)
  lag.append({'elapsedSeconds':t,'lag':sum(p['lag'] for p in parts),'unreachedWatermarkAgeSeconds':round(age,3)})
 expected=d['capacityAnalysis']['stages'][-1]['lag']['groups']['deploy-live-reports/chat.received.v1']['maximumUnreachedWatermarkAgeSeconds']
 assert abs(max(x['unreachedWatermarkAgeSeconds'] for x in lag)-expected)<.002
 cpu=[]
 for a,b in zip(d['resourceSamples'],d['resourceSamples'][1:]):
  interval=epoch(b['at'])-epoch(a['at']);roles={};missing=[]
  for role in ('postgres','api','kafka','collector-probe'):
   first=a.get('cpuStat',{}).get(role);last=b.get('cpuStat',{}).get(role)
   if not first or not last or not {'usage_usec','nr_periods','nr_throttled'}<=first.keys() or not {'usage_usec','nr_periods','nr_throttled'}<=last.keys():
    missing.append(role);continue
   periods=last['nr_periods']-first['nr_periods']
   roles[role]={'coresApprox':round((last['usage_usec']-first['usage_usec'])/interval/1e6,4),
     'throttledPeriodsRatio':round((last['nr_throttled']-first['nr_throttled'])/periods,4) if periods else 0}
  cpu.append({'fromElapsedSecondsApprox':round(epoch(a['at'])-start,3),'toElapsedSecondsApprox':round(epoch(b['at'])-start,3),'roles':roles,'unobservedRoles':missing})
 samples=[];previous=None
 for s in load['samples']:
  gc=s.get('probeGcMillis');samples.append({'elapsedSeconds':s['elapsedSeconds'],
   'wallMinusMonotonicMillis':round(s['startedAtEpochMillis']-start*1000-s['elapsedSeconds']*1000,3),
   'probeGcMillis':gc,'probeGcDeltaMillis':gc-previous if gc is not None and previous is not None else None,
   'probeThreadCount':s.get('probeThreadCount'),'databaseQueryMillis':s.get('databaseQueryMillis'),
   'readiness':[{'elapsedSecondsApprox':round(a['startedAtEpochMillis']/1000-start,3),'status':a.get('status'),'millis':a.get('millis')} for a in s.get('api',[]) if a['path']=='/readyz']})
  if gc is not None:previous=gc
 runs.append({'name':source.parent.name,'revision':d['candidate']['revision'],'recordedClassification':d['capacityAnalysis']['classification'],
  'sourceArtifact':digest(source),'offsetArtifact':digest(offset),'reportSteadyLag':lag,'resourceIntervals':cpu,'probeSamples':samples})
output=pathlib.Path(sys.argv[1]);value={'schemaVersion':1,'technicalIds':['T07','T11','T12','T14'],'curatorArtifact':digest(pathlib.Path(__file__)),
 'scope':'INPUT_EPOCH_ALIGNMENT_OF_INDEPENDENT_HOST_AND_PROBE_SAMPLES; NOT_ATOMIC_OR_CAUSAL; INVALID_CLOCK_RUN_RETAINED', 'runs':runs}
output.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'runs':len(runs),'output':str(output)}))
