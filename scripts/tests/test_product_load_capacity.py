"""입력 미달·계측 누락·지연·지속 lag 증가를 용량 통과로 바꾸지 않는다."""
import copy,importlib.util,json,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[2]
def module(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'scripts'/f'{name}.py')
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
capacity=module('product_load_capacity');plans=module('product_load_plan')

class ProductLoadCapacityTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.offsets=pathlib.Path(self.directory.name)/'offsets.jsonl'
        self.plan={'name':'capacity','channels':20,'stages':[{'name':'steady','seconds':60,'eventsPerSecond':100,'hot':False}],
                   'faultAtSeconds':-1,'faultSeconds':0,'sourceMode':'DIRECT','apiRequestsPerSecond':10,'apiMaxInFlight':32,'resourceProfile':'BASELINE'}
        self.load={'correct':True,'maximumPacingDelayMillis':3,'commitVisibility':{'phases':{'steady':{'sampled':300,'latency':{'p95UpperMillis':300}}}},
                   'apiTraffic':{'phases':{'steady':{'offered':600,'issued':600,'successful':600,'failed':0,'notIssuedConcurrentLimit':0,
                                                   'scheduleDelay':{'p95UpperMillis':5},'latency':{'p95UpperMillis':50}}}}}
        self.rows=[{'elapsedSeconds':s,'partitions':[{'group':f'g{p//4}','lag':3} for p in range(24)]} for s in range(60)]
    def analyze(self,source=None):
        self.offsets.write_text('\n'.join(json.dumps(r) for r in self.rows))
        return capacity.analyze_capacity(self.plan,self.load,self.offsets,source)
    def test_latency_and_lag_boundary(self):
        self.assertEqual('WITHIN_SLO',self.analyze()['classification'])
        self.load['commitVisibility']['phases']['steady']['latency']['p95UpperMillis']=2001
        self.assertEqual('EXCEEDS_SLO',self.analyze()['classification'])
        self.load['commitVisibility']['phases']['steady']['latency']['p95UpperMillis']=300
        for i,row in enumerate(self.rows):row['partitions'][0]['lag']=i*20
        self.assertEqual(['g0'],self.analyze()['stages'][0]['lag']['growingGroups'])
    def test_slow_generator_or_missing_measurements_never_pass(self):
        self.load['maximumPacingDelayMillis']=1001
        self.assertEqual('GENERATOR_LIMITED',self.analyze()['classification'])
        self.load['maximumPacingDelayMillis']=3;self.rows=self.rows[:20]
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_dropped_requests_and_pending_callbacks_are_not_success(self):
        api=self.load['apiTraffic']['phases']['steady'];api['notIssuedConcurrentLimit']=1
        self.assertEqual('EXCEEDS_SLO',self.analyze()['classification'])
        api['notIssuedConcurrentLimit']=0;api['successful']=599
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_websocket_source_and_expected_shutdown_are_distinct(self):
        self.plan['sourceMode']='WEBSOCKET';self.load['collector']={'connectedOnce':20,'reasons':{'CANCELLED':20}}
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
        source={'finished':True,'maximumPacingDelayMillis':3}
        self.assertEqual('WITHIN_SLO',self.analyze(source)['classification'])
        self.load['collector']['reasons']['HEARTBEAT_TIMEOUT']=1
        self.assertEqual('EXCEEDS_SLO',self.analyze(source)['classification'])
    def test_plan_budgets_and_original_profiles(self):
        self.assertEqual(90000,sum(s['seconds']*s['eventsPerSecond'] for s in plans.read('soak')[0]['stages']))
        for key,value in [('channels',1001),('channels',True),('apiRequestsPerSecond',201),('sourceMode','PUBLIC'),('resourceProfile','UNKNOWN')]:
            with self.subTest(key=key,value=value):
                plan=copy.deepcopy(self.plan);plan[key]=value
                with self.assertRaises(ValueError):plans.validate(plan)
        self.plan['stages'][0].update(seconds=1800,eventsPerSecond=10000)
        with self.assertRaises(ValueError):plans.validate(self.plan)

if __name__=='__main__':unittest.main()
