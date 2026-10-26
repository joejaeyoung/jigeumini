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
        self.load={'correct':True,'maximumPacingDelayMillis':3,'commitVisibility':{'phases':{'steady':{'sampled':300,'latency':{'samples':300,'p95UpperMillis':300},'inputPartitions':[0,1,2],'sampledByPartition':{'0':100,'1':100,'2':100},'latencyByPartition':{'0':{'p95UpperMillis':300},'1':{'p95UpperMillis':300},'2':{'p95UpperMillis':300}}}}},
                   'apiTraffic':{'phases':{'steady':{'offered':600,'issued':600,'successful':600,'failed':0,'notIssuedConcurrentLimit':0,
                                                   'scheduleDelay':{'p95UpperMillis':5},'latency':{'p95UpperMillis':50}}}}}
        self.rows=[{'elapsedSeconds':s,'partitions':[{'group':f'g{p//4}','topic':'t','partition':p%4,'lag':3,'end':10000+s*25,'committed':9997+s*25} for p in range(24)]} for s in range(60)]
        self.load.update(inputStartedAtEpochMillis=100000,
                         samples=[{'startedAtEpochMillis':100000+s*1000,'elapsedSeconds':s} for s in range(0,60,5)])
    def analyze(self,source=None):
        self.offsets.write_text('\n'.join(json.dumps(r) for r in self.rows))
        return capacity.analyze_capacity(self.plan,self.load,self.offsets,source)
    def test_latency_and_lag_boundary(self):
        self.assertEqual('WITHIN_SLO',self.analyze()['classification'])
        self.load['commitVisibility']['phases']['steady']['latency']['p95UpperMillis']=2001
        self.assertEqual('EXCEEDS_SLO',self.analyze()['classification'])
        self.load['commitVisibility']['phases']['steady']['latency']['p95UpperMillis']=300
        for i,row in enumerate(self.rows):row['partitions'][0]['lag']=i*20;row['partitions'][0]['committed']=row['partitions'][0]['end']-i*20
        self.assertEqual(['g0/t'],self.analyze()['stages'][0]['lag']['growingGroups'])
    def test_slow_generator_or_missing_measurements_never_pass(self):
        self.load['maximumPacingDelayMillis']=1001
        self.assertEqual('GENERATOR_LIMITED',self.analyze()['classification'])
        self.load['maximumPacingDelayMillis']=3;self.rows=self.rows[:20]
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_visibility_quantiles_never_hide_unobserved_samples(self):
        visibility=self.load['commitVisibility']['phases']['steady']
        complete=self.analyze()['stages'][0]['visibility']
        self.assertEqual(300,complete['allSelectedP95UpperMillis'])
        for missing in (1,16,300):
            with self.subTest(missing=missing):
                visibility['latency']['samples']=300-missing;visibility['notVisibleWithin10Seconds']=missing
                original=copy.deepcopy(visibility);result=self.analyze();view=result['stages'][0]['visibility']
                self.assertEqual('EXCEEDS_SLO',result['classification'])
                self.assertEqual(missing,view['sampleCoverage']['unobserved'])
                self.assertAlmostEqual(missing/300,view['sampleCoverage']['unobservedRatio'])
                self.assertIsNone(view['allSelectedP95UpperMillis'])
                self.assertEqual(300,view['latency']['p95UpperMillis'])
                self.assertEqual(original,visibility)
        visibility.pop('notVisibleWithin10Seconds');visibility['latency']['samples']=299
        self.assertEqual('EXCEEDS_SLO',self.analyze()['classification'])
        for count in (None,True,-1,301):
            with self.subTest(invalid_visible=count):
                visibility['latency']['samples']=count
                self.assertIn('steady:VISIBILITY_SAMPLE_ACCOUNTING',self.analyze()['measurementProblems'])
    def test_clock_step_in_either_direction_invalidates_an_otherwise_successful_run(self):
        original=copy.deepcopy(self.load['samples'])
        for step in (-2045,2044):
            with self.subTest(step=step):
                self.load['samples']=copy.deepcopy(original)
                for sample in self.load['samples'][3:]:sample['startedAtEpochMillis']+=step
                result=self.analyze()
                self.assertEqual('MEASUREMENT_INCOMPLETE',result['classification'])
                self.assertIn('PROBE_CLOCK_REFERENCE',result['measurementProblems'])
                self.assertEqual(abs(step),result['clockReference']['maximumAbsoluteResidualMillis'])
        self.load['samples']=original
        self.assertEqual('WITHIN_SLO',self.analyze()['classification'])
    def test_missing_or_nonfinite_clock_measurements_cannot_pass(self):
        original=copy.deepcopy(self.load)
        for missing in ('inputStartedAtEpochMillis','samples'):
            with self.subTest(missing=missing):
                self.load=copy.deepcopy(original);self.load.pop(missing)
                self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
        for value in (None,float('nan'),float('inf'),True):
            with self.subTest(elapsed=value):
                self.load=copy.deepcopy(original);self.load['samples'][0]['elapsedSeconds']=value
                self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_dropped_requests_and_pending_callbacks_are_not_success(self):
        api=self.load['apiTraffic']['phases']['steady'];api['notIssuedConcurrentLimit']=1
        self.assertEqual('EXCEEDS_SLO',self.analyze()['classification'])
        api['notIssuedConcurrentLimit']=0;api['successful']=599
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_websocket_source_and_expected_shutdown_are_distinct(self):
        self.plan['sourceMode']='WEBSOCKET';self.load['collector']={'connectedOnce':20,'reasons':{'CANCELLED':20},'sourceScheduledToReceived':{'steady':{'samples':6000,'p95UpperMillis':10,'maximumMillis':40}}}
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
        source={'finished':True,'maximumPacingDelayMillis':3}
        self.assertEqual('WITHIN_SLO',self.analyze(source)['classification'])
        self.plan['measurementSchemaVersion']=2
        self.assertIn('SOURCE_MONOTONIC_CLOCK_UNRECORDED',self.analyze(source)['measurementProblems'])
        source['clockReference']={'samples':100,'minimumResidualMillis':-.5,'maximumResidualMillis':.5}
        self.assertEqual('WITHIN_SLO',self.analyze(source)['classification'])
        self.load['collector']['reasons']['HEARTBEAT_TIMEOUT']=1
        self.assertEqual('EXCEEDS_SLO',self.analyze(source)['classification'])
    def test_stopped_low_rate_observation_consumer_is_not_hidden_by_chat_rate(self):
        self.plan['channels']=300;self.plan['stages'][0]['eventsPerSecond']=1000
        for second,sample in enumerate(self.rows):
            for row in sample['partitions'][:4]:
                row.update(end=75*(1+second//30),committed=0,lag=75*(1+second//30))
        result=self.analyze();self.assertEqual('EXCEEDS_SLO',result['classification'])
        self.assertTrue(result['stages'][0]['lag']['groups']['g0/t']['staleWatermark'])
    def test_delayed_source_receive_and_biased_visibility_sample_cannot_pass(self):
        self.plan['sourceMode']='WEBSOCKET';self.load['collector']={'connectedOnce':20,'sourceScheduledToReceived':{'steady':{'samples':6000,'p95UpperMillis':1001,'maximumMillis':1100}}}
        self.assertIn('steady:SOURCE_TO_RECEIVED_SLO',self.analyze({'finished':True,'maximumPacingDelayMillis':3})['sloViolations'])
        self.plan['sourceMode']='DIRECT';self.load['commitVisibility']['phases']['steady']['sampledByPartition']['2']=0
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.analyze()['classification'])
    def test_absent_metrics_are_not_evidence_of_a_slow_generator(self):
        self.load.pop('maximumPacingDelayMillis')
        self.load['apiTraffic']['phases']['steady']['scheduleDelay']={}
        result=self.analyze()
        self.assertEqual('MEASUREMENT_INCOMPLETE',result['classification'])
        self.assertEqual([],result['generatorProblems'])
        self.assertIn('DIRECT_PACING_UNRECORDED',result['measurementProblems'])
        self.assertIn('steady:API_SCHEDULE_UNRECORDED',result['measurementProblems'])
        self.load['maximumPacingDelayMillis']=3
        self.load['apiTraffic']['phases']['steady']['scheduleDelay']={'samples':600,'above10000Millis':31,'p95UpperMillis':None}
        result=self.analyze();self.assertEqual('GENERATOR_LIMITED',result['classification'])
        self.assertEqual(['steady:API_SCHEDULER'],result['generatorProblems'])
        self.plan['sourceMode']='WEBSOCKET';self.load={}
        result=self.analyze({'finished':False})
        self.assertEqual('MEASUREMENT_INCOMPLETE',result['classification'])
        self.assertEqual([],result['generatorProblems'])
    def test_plan_budgets_and_original_profiles(self):
        self.assertEqual(90000,sum(s['seconds']*s['eventsPerSecond'] for s in plans.read('soak')[0]['stages']))
        for key,value in [('channels',1001),('channels',True),('apiRequestsPerSecond',201),('sourceMode','PUBLIC'),('resourceProfile','UNKNOWN'),('payloadProfile','UNKNOWN'),('payloadProfile','KOREAN_1K'),('measurementProfile','UNKNOWN'),('measurementSchemaVersion',0),('measurementSchemaVersion',True),('measurementSchemaVersion',3),('diagnostics','true')]:
            with self.subTest(key=key,value=value):
                plan=copy.deepcopy(self.plan);plan[key]=value
                with self.assertRaises(ValueError):plans.validate(plan)
        self.assertEqual('MINIMAL',plans.validate(self.plan)['payloadProfile'])
        self.assertEqual('FULL',plans.validate(self.plan)['measurementProfile'])
        self.assertEqual(2,plans.validate(self.plan)['measurementSchemaVersion'])
        self.assertFalse(plans.validate(self.plan)['diagnostics'])
        socket=copy.deepcopy(self.plan);socket.update(sourceMode='WEBSOCKET',payloadProfile='KOREAN_1K')
        self.assertEqual('KOREAN_1K',plans.validate(socket)['payloadProfile'])
        self.plan['stages'][0].update(seconds=1800,eventsPerSecond=10000)
        with self.assertRaises(ValueError):plans.validate(self.plan)

if __name__=='__main__':unittest.main()
