"""DB 장애가 종료 진단과 기존 계측 판정을 지우지 않는지 검증한다."""
import copy,importlib.util,json,pathlib,sys,tempfile,types,unittest
from unittest.mock import Mock,patch
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
spec=importlib.util.spec_from_file_location('capacity_runtime',ROOT/'scripts/product-load.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)

class ProductLoadDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        self.run=object.__new__(runtime.ProductLoad)
        self.run.result={};self.run.source_id='owned-synthetic-source';self.run.capacity_mode=True;self.run.plan={'diagnostics':False}
        self.run.output=self.run.source_output=pathlib.Path(directory.name)
        (self.run.source_output/'source.json').write_text(json.dumps({'finished':True,'sent':1000}))
        self.run.owned=Mock(return_value=('owned',{'State':{'Status':'running','ExitCode':0,'OOMKilled':False},'HostConfig':{'Memory':384*1024**2,'NanoCpus':1000000000,'PidsLimit':None}}))
        self.run.command=Mock(return_value=types.SimpleNamespace(stdout=json.dumps([{'State':{'Status':'running','ExitCode':0,'OOMKilled':False}}])))
        self.run.storage=Mock(return_value={'databaseBytes':2000})
        self.snapshots=[];self.run.checkpoint=lambda:self.snapshots.append(copy.deepcopy(self.run.result))
        self.analysis=patch.object(runtime,'analyze_capacity',return_value={'classification':'WITHIN_SLO','withinSlo':True,'measurementProblems':[],'sloViolations':[]})
        self.analysis.start();self.addCleanup(self.analysis.stop)
    def test_failed_storage_query_preserves_diagnostics_and_cannot_pass(self):
        self.run.storage.side_effect=RuntimeError('PRIVATE_ERROR_TEXT_NOT_FOR_RESULTS')
        self.run.final_observations({})
        self.assertEqual(3,len(self.snapshots[0]['productProcesses']))
        self.assertEqual(1000,self.snapshots[0]['syntheticSourceSnapshot']['sent'])
        self.assertEqual('running',self.run.result['sourceProcess']['Status'])
        self.assertEqual(['STORAGE_AFTER_UNAVAILABLE'],self.run.result['measurementErrors'])
        self.assertEqual('MEASUREMENT_INCOMPLETE',self.run.result['capacityAnalysis']['classification'])
        self.assertFalse(self.run.result['capacityAnalysis']['withinSlo'])
        self.assertNotIn('PRIVATE_ERROR_TEXT_NOT_FOR_RESULTS',json.dumps(self.run.result))
    def test_exited_product_process_is_not_masked_by_correct_probe(self):
        self.run.owned.return_value[1]['State'].update(Status='exited',ExitCode=137,OOMKilled=True)
        self.run.final_observations({'correct':True})
        self.assertEqual('EXCEEDS_SLO',self.run.result['capacityAnalysis']['classification'])
        self.assertIn('POSTGRES_PROCESS_EXITED',self.run.result['capacityAnalysis']['sloViolations'])
        self.assertEqual(2000,self.run.result['storageAfter']['databaseBytes'])

if __name__=='__main__':unittest.main()
