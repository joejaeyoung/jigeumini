"""관리 CLI의 프로세스 종료·인자·JAR 복사본 계약을 합성 실행기로 확인한다."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CliRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="chat-pulse-cli-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts/lib").mkdir(parents=True)
        (self.root / "bin").mkdir()
        for name in ("replay", "selection", "fleet", "lib/run-cli"):
            shutil.copy2(ROOT / "scripts" / name, self.root / "scripts" / name)
        self.executable("gradlew", """#!/usr/bin/env bash
set -e
echo synthetic-build >&2
mkdir -p server/build/libs
printf synthetic-jar > server/build/libs/chat-pulse-poc.jar
""")
        self.executable("bin/java", """#!/usr/bin/env python3
import json, os, pathlib, signal, stat, sys, time
jar = pathlib.Path(sys.argv[2])
report = pathlib.Path(os.environ['CLI_TEST_REPORT'])
def stop(signum, frame):
    report.with_suffix('.stopping').touch()
    time.sleep(0.1)
    report.with_suffix('.stopped').write_text(str(jar.exists()))
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
report.write_text(json.dumps({'args': sys.argv[3:], 'jar': str(jar), 'content': jar.read_text(),
    'mode': stat.S_IMODE(jar.stat().st_mode)}))
if os.environ.get('CLI_TEST_WAIT') == 'true':
    while True: time.sleep(0.05)
sys.exit(int(os.environ.get('CLI_TEST_EXIT', '0')))
""")

    def executable(self, name, text):
        target = self.root / name
        target.write_text(text)
        target.chmod(0o755)

    def launch(self, mode, *args, wait=False, exit_code=0, label="result"):
        report = self.root / (label + ".json")
        env = {**os.environ, "PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"],
               "CLI_TEST_REPORT": str(report), "CLI_TEST_WAIT": str(wait).lower(), "CLI_TEST_EXIT": str(exit_code)}
        process = subprocess.Popen([str(self.root / "scripts" / mode), *args], cwd=self.root,
                                   env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        def cleanup():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        return process, report

    def await_report(self, path):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                return json.loads(path.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.01)
        self.fail("합성 실행기가 준비되지 않음")

    def test_arguments_exit_status_and_copy_cleanup_for_all_commands(self):
        for mode, command in (("replay", "compare"), ("selection", "fixture"), ("fleet", "preview")):
            with self.subTest(mode=mode):
                process, report = self.launch(mode, command, "path with spaces", "literal$()", exit_code=7, label=mode)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 7)
                self.assertEqual(stdout, b"")
                self.assertIn(b"synthetic-build", stderr)
                result = self.await_report(report)
                self.assertEqual(result["args"], [mode, command, "path with spaces", "literal$()"])
                self.assertEqual(result["content"], "synthetic-jar")
                self.assertEqual(result["mode"], 0o400)
                self.assertFalse(Path(result["jar"]).exists())

    def test_signal_waits_for_child_before_removing_copy(self):
        process, report = self.launch("fleet", "worker", wait=True)
        result = self.await_report(report)
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=5)
        self.assertEqual(process.returncode, 130)
        self.assertEqual(report.with_suffix(".stopped").read_text(), "True")
        self.assertFalse(Path(result["jar"]).exists())

    def test_concurrent_launches_keep_separate_immutable_copies(self):
        first, a = self.launch("replay", "worker", wait=True, label="first")
        left = self.await_report(a)
        second, b = self.launch("replay", "worker", wait=True, label="second")
        right = self.await_report(b)
        self.assertNotEqual(left["jar"], right["jar"])
        (self.root / "server/build/libs/chat-pulse-poc.jar").write_text("next-build")
        self.assertEqual(Path(left["jar"]).read_text(), "synthetic-jar")
        self.assertEqual(Path(right["jar"]).read_text(), "synthetic-jar")
        first.send_signal(signal.SIGTERM)
        first.communicate(timeout=5)
        self.assertFalse(Path(left["jar"]).exists())
        self.assertTrue(Path(right["jar"]).exists())
        second.send_signal(signal.SIGTERM)
        second.communicate(timeout=5)
        self.assertFalse(Path(right["jar"]).exists())

    def test_build_failure_never_launches_java(self):
        self.executable("gradlew", "#!/usr/bin/env bash\nexit 9\n")
        process, report = self.launch("replay", "worker")
        process.communicate(timeout=5)
        self.assertEqual(process.returncode, 9)
        self.assertFalse(report.exists())
        self.assertFalse((self.root / "work/runtime").exists())
