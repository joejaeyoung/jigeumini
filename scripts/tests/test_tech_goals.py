"""추적 누락과 근거 없는 완료가 실제로 거절되는지 검사한다."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check-tech-goals.py"
spec = importlib.util.spec_from_file_location("tech_goals", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

GOALS = """### T01 커밋 시점
### T02 재전달
<!-- tech-features:start -->
| 기능 ID | 기능·이슈 | 기술 ID | 기여/검증 |
|---|---|---|---|
| F01 | 파이프라인 | T01, T02 | 장애 재현 |
<!-- tech-features:end -->
"""
STATUS = """<!-- tech-status:start -->
| 기술 ID | 상태 | 검증 근거 | 확인한 범위 | 남은 문제·다음 행동 |
|---|---|---|---|---|
| T01 | 검증됨 | [결과](evidence.md) | commit 직후 종료 | 고부하는 별도 실험 |
| T02 | 미착수 | — | 아직 실험 없음 | 중복 입력 시험 |
<!-- tech-status:end -->
"""


class TrackingContractTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "docs").mkdir()
        (self.root / "docs/evidence.md").write_text("합성 입력 실험 결과")

    def errors(self, goals=GOALS, status=STATUS, baseline=None):
        return checker.validate(goals, status, self.root, baseline)[0]

    def test_valid_mapping(self):
        self.assertEqual([], self.errors(baseline=GOALS))

    def test_goal_missing_from_status(self):
        status = "\n".join(line for line in STATUS.splitlines() if not line.startswith("| T02 |"))
        self.assertTrue(any("STATUS에 없는 목표: T02" in e for e in self.errors(status=status)))

    def test_unknown_reference(self):
        self.assertTrue(any("정의되지 않은" in e for e in self.errors(goals=GOALS.replace("T01, T02", "T01, T99"))))

    def test_goal_without_feature(self):
        self.assertTrue(any("연결되지 않은 목표: T02" in e for e in self.errors(goals=GOALS.replace("T01, T02", "T01"))))

    def test_verified_without_evidence(self):
        self.assertTrue(any("검증 근거" in e for e in self.errors(status=STATUS.replace("[결과](evidence.md)", "—"))))

    def test_missing_evidence_file(self):
        (self.root / "docs/evidence.md").unlink()
        self.assertTrue(any("검증 근거" in e for e in self.errors()))

    def test_duplicate_tracking_id(self):
        self.assertTrue(any("중복 ID" in e for e in self.errors(status=STATUS.replace("| T02 |", "| T01 |"))))

    def test_empty_next_action(self):
        self.assertTrue(any("다음 행동" in e for e in self.errors(status=STATUS.replace("중복 입력 시험", "-"))))

    def test_coordinated_removal_is_rejected_against_baseline(self):
        goals = GOALS.replace("### T02 재전달\n", "").replace("T01, T02", "T01")
        status = "\n".join(line for line in STATUS.splitlines() if not line.startswith("| T02 |"))
        self.assertEqual([], self.errors(goals, status))
        self.assertTrue(any("ID 삭제: T02" in e for e in self.errors(goals, status, GOALS)))

    def test_feature_id_removal_is_rejected(self):
        self.assertTrue(any("ID 삭제: F01" in e for e in self.errors(goals=GOALS.replace("F01", "F02"), baseline=GOALS)))

    def test_broken_markers(self):
        self.assertTrue(any("표식" in e for e in self.errors(status=STATUS.replace("tech-status:end", "missing:end"))))


if __name__ == "__main__":
    unittest.main()
