"""유한 부하 계획. 제품 설정과 실험 전용 원천·조회 예산을 구분한다."""
import json,pathlib,re

RESOURCE_PROFILES={'BASELINE':{},'API_2_CPU':{'api':2},'DB_2_CPU':{'postgres':2},'API_DB_2_CPU':{'api':2,'postgres':2},'KAFKA_2_CPU':{'kafka':2},'COLLECTOR_2_CPU':{'load-probe':2}}

def validate(plan):
    if isinstance(plan,dict):plan={'payloadProfile':'MINIMAL','measurementProfile':'FULL','diagnostics':False,**plan}
    required={'payloadProfile','measurementProfile','diagnostics','name','channels','stages','faultAtSeconds','faultSeconds','sourceMode','apiRequestsPerSecond','apiMaxInFlight','resourceProfile'}
    if not isinstance(plan,dict) or set(plan)!=required:raise ValueError('LOAD_PLAN_FIELDS')
    if not isinstance(plan['name'],str) or not re.fullmatch('[a-z][a-z0-9_-]{0,39}',plan['name']):raise ValueError('LOAD_PLAN_NAME')
    def integer(value,low,high):return type(value) is int and low<=value<=high
    if not integer(plan['channels'],2,1000) or not integer(plan['apiRequestsPerSecond'],0,200) or not integer(plan['apiMaxInFlight'],1,100):raise ValueError('LOAD_PLAN_LIMITS')
    if plan['sourceMode'] not in ('DIRECT','WEBSOCKET') or plan['resourceProfile'] not in RESOURCE_PROFILES:raise ValueError('LOAD_PLAN_MODE')
    if plan['payloadProfile'] not in ('MINIMAL','KOREAN_1K') or (plan['sourceMode']=='DIRECT' and plan['payloadProfile']!='MINIMAL'):raise ValueError('LOAD_PAYLOAD_PROFILE')
    if plan['measurementProfile'] not in ('FULL','NO_COUNTS','NO_COUNTS_OR_DU') or type(plan['diagnostics']) is not bool:raise ValueError('LOAD_MEASUREMENT_PROFILE')
    stages=plan['stages']
    if not isinstance(stages,list) or not 1<=len(stages)<=10:raise ValueError('LOAD_STAGES')
    names=set();duration=events=0
    for stage in stages:
        if not isinstance(stage,dict) or set(stage)!={'name','seconds','eventsPerSecond','hot'}:raise ValueError('LOAD_STAGE_FIELDS')
        if not isinstance(stage['name'],str) or not re.fullmatch('[a-z][a-z0-9_-]{0,39}',stage['name']) or stage['name'] in names:raise ValueError('LOAD_STAGE_NAME')
        names.add(stage['name'])
        if not integer(stage['seconds'],10,1800) or not integer(stage['eventsPerSecond'],10,10000) or stage['eventsPerSecond']%10 or type(stage['hot']) is not bool:raise ValueError('LOAD_STAGE_LIMITS')
        duration+=stage['seconds'];events+=stage['seconds']*stage['eventsPerSecond']
    fault,seconds=plan['faultAtSeconds'],plan['faultSeconds']
    if duration>1800 or events>300000 or not integer(fault,-1,1800) or not integer(seconds,0,30) or ((fault==-1)!=(seconds==0)) or (fault>=0 and fault+seconds>=duration):raise ValueError('LOAD_BUDGET')
    return plan

def read(argument):
    path=pathlib.Path(argument)
    if path.is_file():
        if path.stat().st_size>65536:raise ValueError('LOAD_PLAN_SIZE')
        return validate(json.loads(path.read_text())),True
    settings={'smoke':([(60,20,False)],15,20),'uniform':([(120,100,False)],-1,0),'hot':([(120,100,True)],-1,0),'steps':([(60,50,False),(60,100,False),(60,200,False)],-1,0),'soak':([(1800,50,False)],300,20)}
    if argument not in settings:raise ValueError('LOAD_PROFILE')
    stages,fault,seconds=settings[argument]
    return validate({'name':argument,'channels':20,'stages':[{'name':f'rate{rate}' if argument=='steps' else argument,'seconds':duration,'eventsPerSecond':rate,'hot':hot} for duration,rate,hot in stages],
        'faultAtSeconds':fault,'faultSeconds':seconds,'sourceMode':'DIRECT','apiRequestsPerSecond':0,'apiMaxInFlight':32,'resourceProfile':'BASELINE'}),False
