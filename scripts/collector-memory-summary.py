"""수집 메모리 실험의 숫자·고정 코드·hash와 전체 제품 요약을 재현한다."""
import hashlib
from collections import Counter
import importlib.util
import json
import pathlib
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("capacity_summary", ROOT / "scripts/capacity-summary.py")
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)

def digest(path):
    return {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

def revision(ref):
    return subprocess.check_output(["git", "rev-parse", ref], text=True).strip()

def memory(root, name, commit, expected_exit):
    path = root / (name + ".json")
    raw = json.loads(path.read_text())
    log = root / (name + ".log")
    result = {key: raw[key] for key in (
        "scope", "profile", "startedAt", "finishedAt", "channels", "rounds", "maxHeapBytes",
        "tomcat", "java", "os", "processors", "garbageCollectors", "correct", "failureCode") if key in raw}
    result["samples"] = [{key: row[key] for key in (
        "phase", "round", "at", "gcObserved", "heapUsedBytes", "liveThreads", "ioThreads",
        "openFileDescriptors", "sourceConnections", "ioGroupUsers")} for row in raw["samples"]]
    result.update({"name": name, "codeCommit": commit, "mainTree": revision(commit + ":server/src/main"),
                   "experimentSourceSha256": hashlib.sha256(subprocess.check_output([
                       "git", "show", commit + ":server/src/test/java/io/chatpulse/poc/CollectorMemoryExperiment.java"])).hexdigest(),
                   "gradleExitCode": expected_exit, "completed": "finishedAt" in raw,
                   "command": ["./scripts/collector-memory", raw["profile"], str(path)],
                   "artifacts": {path.name: digest(path), log.name: digest(log)}})
    if "Java heap space" in log.read_text(): result["runtimeFailureCode"] = "JAVA_HEAP_SPACE"
    execution = root / (name + "-execution.json")
    result["commandProvenance"] = "CAPTURED_WRAPPER" if execution.exists() else "SESSION_COMMAND_AND_CLEAN_COMMIT_RECONSTRUCTED"
    if execution.exists():
        meta = json.loads(execution.read_text())
        assert meta["codeCommit"] == commit and meta["exitCode"] == expected_exit
    return result

def execution(path):
    raw = json.loads(path.read_text())
    result = {key: raw[key] for key in ("startedAt", "finishedAt", "codeCommit", "mainTree", "testTree", "node",
                                      "command", "exitCode", "steps", "logSha256", "preexistingContainersPreserved") if key in raw}
    result["existingContainerCount"] = len(raw["containersBefore"])
    result["sourceArtifact"] = digest(path)
    return result

def product(directory):
    result = capacity.summarize(directory)
    raw = json.loads((directory / "result.json").read_text())
    result["functionalChecks"] = {key: raw.get("analysis", {}).get(key)
                                  for key in ("api", "measurementCoverage", "failures")}
    result["functionalChecks"]["readinessStatuses"] = dict(Counter(
        str(response.get("status", "ERROR")) for sample in raw.get("load", {}).get("samples", [])
        for response in sample.get("api", []) if response.get("path") == "/readyz"))
    load = raw.get("load", {})
    base = load.get("inputStartedAtEpochMillis")
    result["wallVsMonotonicResidual"] = {
        "formula": "sample.startedAtEpochMillis - inputStartedAtEpochMillis - sample.elapsedSeconds * 1000",
        "scope": "PROBE_SLOW_SAMPLES; CLOCK_DIVERGENCE_OBSERVATION_NOT_CAUSE_IDENTIFICATION",
        "samples": [{"elapsedSeconds": round(sample["elapsedSeconds"], 3),
                     "residualMillis": round(sample["startedAtEpochMillis"] - base - sample["elapsedSeconds"] * 1000, 3)}
                    for sample in load.get("samples", []) if base is not None
                    and "elapsedSeconds" in sample and "startedAtEpochMillis" in sample]}
    return result

def main(root, output):
    output.mkdir(parents=True, exist_ok=False)
    baseline = revision("f9b7713")
    candidate = json.loads((root / "images-candidate.json").read_text())["revision"]
    environment = json.loads((root / "environment.json").read_text())
    environment["otherContainerCount"] = len(environment.pop("otherContainers"))
    reports = [memory(root, name, commit, code) for name, commit, code in (
        ("connections-baseline", baseline, 1), ("lifecycle-baseline", baseline, 1),
        ("connections-fixed", candidate, 0), ("lifecycle-fixed", candidate, 0))]
    before = json.loads((root / "baseline180/result.json").read_text())
    after = json.loads((root / "candidate180/result.json").read_text())
    comparability = {
        "product180SamePlan": before["loadContract"] == after["loadContract"],
        "product180SameFixtureClasses": before["fixtureClasses"] == after["fixtureClasses"],
        "product180SameFixtureLibraries": before["fixtureLibraries"] == after["fixtureLibraries"],
        "sameIsolatedExperimentSource": len({row["experimentSourceSha256"] for row in reports}) == 1,
        "baselineProductAndIsolatedMainTreeMatch": reports[0]["mainTree"] == revision(before["candidate"]["revision"] + ":server/src/main")}
    assert all(comparability.values())
    runs = [product(directory) for directory in sorted(root.iterdir())
            if directory.is_dir() and (directory / "result.json").exists()
            and json.loads((directory / "result.json").read_text()).get("finishedAt")]
    result = {"schemaVersion": 1, "scope": "SYNTHETIC_COLLECTOR_MEMORY_AND_SINGLE_HOST_RF1_PRODUCT",
              "technicalIds": ["T02", "T08", "T11", "T12", "T14"], "environment": environment,
              "comparability": comparability,
              "curators": {name: digest(path) for name, path in (
                  ("collector", pathlib.Path(__file__)), ("capacity", ROOT / "scripts/capacity-summary.py"),
                  ("analysis", ROOT / "scripts/product_load_capacity.py"))},
              "isolatedMemory": reports, "productRuns": runs,
              "executions": {path.name: execution(path) for path in sorted(root.glob("*-execution.json"))},
              "limitations": [
                  "Pure loopback uses Xmx64MiB and excludes HTTP/TLS/Kafka/DB/oracle/API clients; heap is GC-observed, not RSS.",
                  "Whole-product probe RSS/heap includes HTTP, oracle, API client and sampling; peaks are not retained heap or production-only cost.",
                  "Connection-only input holds total chat/API rates fixed; it does not establish an equal number of popular live channels.",
                  "The new memory limit bounds one complete message; simultaneous large messages still require bounded per-message assembly memory.",
                  "No public source, fleet lease scale, long-retention DB, cloud sizing or RF3/multi-host capacity is established."]}
    (output / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"memoryReports": len(reports), "productRuns": len(runs), "output": str(output)}))

if __name__ == "__main__":
    if len(sys.argv) != 3: raise SystemExit("사용법: collector-memory-summary.py <원본 디렉터리> <새 정제 디렉터리>")
    main(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
