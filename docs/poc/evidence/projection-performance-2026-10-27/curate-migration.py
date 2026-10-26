#!/usr/bin/env python3
"""합성 migration 리허설을 값·경로 원문 없이 정제한다. 인수: result execution output."""
import hashlib,json,pathlib,sys
source,execution,output=map(pathlib.Path,sys.argv[1:])
def artifact(path):
 return {'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
def select(value,names):
 return {name:value[name] for name in names if name in value}
d=json.loads(source.read_text());e=json.loads(execution.read_text())
phases={p['name']:p for p in d['phases']}
restore=phases['RESTORE_BASELINE_NEW_DATABASE']['result'];browser=phases['RESTORED_BROWSER']['result']
out={'schemaVersion':1,'technicalIds':['T16'],'scope':'LOCAL_SYNTHETIC_NEXT_VERSION_MIGRATION_AND_RESTORE; NOT_PRODUCTION_UPGRADE_OR_RTO',
 'sourceArtifact':artifact(source),'executionArtifact':artifact(execution),'curatorArtifact':artifact(pathlib.Path(__file__)),
 'execution':select(e,('codeCommit','mainTree','testTree','command','startedAt','finishedAt','exitCode','logSha256','preexistingContainersPreserved')),
 'preexistingContainerCount':len(e['containersBefore']),
 'result':select(d,('correct','cleanupComplete','sensitiveArtifactsRemoved','startedAt','finishedAt')),
 'images':{name:{**select(d[name],('revision','workingTreeDirty','jarSha256','buildId','images')),
  **({'fixture':d[name]['fixture']} if 'fixture' in d[name] else {})} for name in ('baseline','candidate','failedCandidate','interruptedCandidate')},
 'phases':[select(p,('name','startedAt','passed','seconds')) for p in d['phases']],
 'rollback':{name:{**select(phases[name]['result'],('exitCode','ddlAndBackfillReached','backendEnded','businessSnapshotUnchanged','migrationRejected')),
 'schemaBeforeVersions':phases[name]['result']['schemaBefore']['versions'],
 'schemaAfterVersions':phases[name]['result']['schemaAfter']['versions']} for name in ('INTERRUPTED_CANDIDATE_ROLLBACK','FAILED_CANDIDATE_ROLLBACK')},
 'oldImageRejection':phases['BASELINE_REJECTS_CANDIDATE_SCHEMA']['result'],
 'recovery':{**select(restore,('secondsToVerifiedCatchup','backupChatGap','chatGapAfterRecovery','businessSnapshotDifferences','newGroups','oldGroupsModified','restoredIntoNewDatabase','alignmentStartRejected','runtimeBuildId')),
  'restoredSchemaVersions':restore['restoredSchema']['versions'],'originalSchemaVersions':restore['originalSchema']['versions']},
 'browser':{**select(browser,('failedApiRequests','mobileOverflow','pageErrors','apiErrorUi','apiRetryRecovered','staticHeadersVerified')),
 'pageCount':len(browser['checks']),'pageAll200':all(c['status']==200 for c in browser['checks']),
 'pageAllRefreshed':all(c['refresh'] for c in browser['checks']),'pageAllTitlesVisible':all(c['titleVisible'] for c in browser['checks']),
 'pageAnyOverflow':any(c['overflow'] for c in browser['checks']),
 'apiResponseCount':len(browser['apiResponses']),'apiAll200':all(c['status']==200 for c in browser['apiResponses']),
 'reportDetail':select(browser['reportDetail'],('referenceCount','rendered'))},
 'gracefulStop':phases['FINAL_GRACEFUL_STOP']['result']}
output.write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'phases':len(out['phases']),'correct':out['result']['correct'],'cleanupComplete':out['result']['cleanupComplete'],'output':str(output)}))
