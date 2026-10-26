"""장기 계측 공백과 재개 직후 readiness 전환을 저장된 표본으로 재현한다."""
import importlib.util,json,pathlib,tempfile,unittest

ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('product_load_results',ROOT/'scripts/product_load_results.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class ProductLoadResultsTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.offsets=pathlib.Path(self.directory.name)/'offsets.jsonl'
        self.load={'inputStartedAtEpochMillis':0,'inputSeconds':60,'samples':[]}
        for second in range(0,60,5):
            at=second*1000
            responses=[{'path':f'/api/{p}' if p<7 else '/readyz','startedAtEpochMillis':at+p+1,
                        'finishedAtEpochMillis':at+p+2,'status':200} for p in range(8)]
            self.load['samples'].append({'startedAtEpochMillis':at,'finishedAtEpochMillis':at+10,'database':{},
                'analyzerHeapUsedBytes':1,'generatorHeapUsedBytes':1,'generatorRssBytes':1,'publisher':{},'api':responses})
        self.rows=[{'partitions':[{'group':'synthetic','topic':'synthetic','partition':p,'end':second,'committed':second,
                                  'lag':0,'observedAtEpochMillis':second*1000} for p in range(24)]} for second in range(1,61)]
        self.resources=[{'analyzerRssBytes':1,'containers':[{}]*4} for _ in range(4)]
    def analyze(self,fault=None):
        self.offsets.write_text('\n'.join(json.dumps(s) for s in self.rows)+'\n')
        return module.analyze(self.load,self.offsets,fault,self.resources,60)
    def test_full_time_series_passes_but_two_successful_samples_do_not(self):
        self.assertEqual([],self.analyze()['failures'])
        self.load['samples']=self.load['samples'][:2]
        result=self.analyze();self.assertIn('LOAD_EVIDENCE_INCOMPLETE',result['failures'])
        self.assertGreater(result['measurementCoverage']['slowTimeCoverage']['maximumNormalGapSeconds'],15)
    def test_readiness_503_after_resume_is_recorded_until_actual_recovery(self):
        fault={'stoppedAtEpochMillis':15000,'resumedAtEpochMillis':35000,'actualStoppedSeconds':20}
        for sample in self.rows[14:36]:
            for row in sample['partitions']:row['committed']=14;row['lag']=row['end']-14
        for sample in self.load['samples']:
            if 15000<=sample['startedAtEpochMillis']<=35000:
                for response in sample['api']:response['status']=503
        result=self.analyze(fault)
        self.assertEqual([],result['failures']);self.assertEqual(8,result['api']['duringRecovery']['503'])
        self.assertEqual(2,result['recovery']['secondsResumeToFixedWatermark'])
        self.assertGreater(result['recovery']['secondsResumeToReadiness200'],5)
        self.load['samples'][-1]['api'][0]['status']=503
        self.assertIn('LOAD_API_ERRORS_OUTSIDE_FAULT_OR_RECOVERY',self.analyze(fault)['failures'])
    def test_missing_metrics_and_stop_lag_are_failures_with_evidence_preserved(self):
        for sample in self.load['samples']:sample.pop('analyzerHeapUsedBytes')
        result=self.analyze({'stoppedAtEpochMillis':15000,'resumedAtEpochMillis':35000,'actualStoppedSeconds':31})
        self.assertIn('LOAD_STOP_DURATION',result['failures']);self.assertIn('LOAD_FAULT_LAG_NOT_OBSERVED',result['failures'])
        self.assertIn('LOAD_EVIDENCE_INCOMPLETE',result['failures']);self.assertIn('offsetEvidence',result)
    def test_missing_container_stats_does_not_count_as_complete_resources(self):
        for sample in self.resources:sample.pop('containers')
        result=self.analyze();self.assertEqual(0,result['measurementCoverage']['containerStatsSuccessRatio'])
        self.assertIn('LOAD_EVIDENCE_INCOMPLETE',result['failures'])

if __name__=='__main__':unittest.main()
