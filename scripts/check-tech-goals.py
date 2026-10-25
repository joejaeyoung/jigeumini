#!/usr/bin/env python3
"""기술 목표·기능·진행 기록의 연결을 검사한다. 외부 서비스는 조회하지 않는다."""
import argparse
from collections import Counter
from pathlib import Path
import re
import subprocess


GOALS = "docs/TECH_GOALS.md"
STATUS = "docs/STATUS.md"


def rows(text, marker, columns, errors):
    start, end = f"<!-- {marker}:start -->", f"<!-- {marker}:end -->"
    if text.count(start) != 1 or text.count(end) != 1 or text.index(start) > text.index(end):
        errors.append(f"{marker}: 시작/끝 표식이 정확히 한 쌍 필요합니다")
        return []
    section = text.split(start, 1)[1].split(end, 1)[0]
    result = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if cells[0] in ("기능 ID", "기술 ID") or all(re.fullmatch(r"[-:]+", cell) for cell in cells):
            continue
        if len(cells) != columns or not all(cells):
            errors.append(f"{marker}: 비어 있거나 잘못된 행: {line}")
            continue
        result.append(cells)
    if not result:
        errors.append(f"{marker}: 추적할 행이 없습니다")
    return result


def unique(ids, label, errors):
    for value, count in Counter(ids).items():
        if count > 1:
            errors.append(f"{label}: 중복 ID {value}")


def validate(goals, status, root, baseline=None):
    errors = []
    ids = re.findall(r"^### (T\d{2,})\s+.+$", goals, re.M)
    unique(ids, "목표", errors)
    known = set(ids)
    if not known:
        errors.append("기술 목표 정의가 없습니다")
    features = rows(goals, "tech-features", 4, errors)
    unique([r[0] for r in features], "기능", errors)
    mapped = set()
    for feature, name, references, contribution in features:
        if not re.fullmatch(r"F\d{2,}", feature):
            errors.append(f"잘못된 기능 ID: {feature}")
        if references == "없음":
            continue
        refs = [ref.strip() for ref in references.split(",")]
        unique(refs, feature, errors)
        for ref in refs:
            if ref not in known:
                errors.append(f"{feature}: 정의되지 않은 기술 ID {ref}")
        mapped.update(refs)
    for missing in sorted(known - mapped):
        errors.append(f"기능/실험에 연결되지 않은 목표: {missing}")

    tracking = rows(status, "tech-status", 5, errors)
    unique([r[0] for r in tracking], "상태", errors)
    tracked = {r[0] for r in tracking}
    for missing in sorted(known - tracked):
        errors.append(f"STATUS에 없는 목표: {missing}")
    for unknown in sorted(tracked - known):
        errors.append(f"STATUS의 알 수 없는 목표: {unknown}")
    for tech_id, state, evidence, scope, next_action in tracking:
        if state not in {"미착수", "진행 중", "부분 검증", "검증됨", "보류"}:
            errors.append(f"{tech_id}: 허용되지 않은 상태 {state}")
        if next_action in {"-", "없음", "TBD"}:
            errors.append(f"{tech_id}: 다음 행동/범위 한계를 기록하세요")
        if state in {"부분 검증", "검증됨"}:
            links = re.findall(r"\]\(([^)]+)\)", evidence)
            local = [link.split("#")[0] for link in links if ":" not in link and not link.startswith(("#", "/"))]
            if not local or not all((root / "docs" / link).is_file() for link in local):
                errors.append(f"{tech_id}: 저장소 안의 유효한 검증 근거 링크가 필요합니다")
            if scope in {"-", "없음", "TBD"}:
                errors.append(f"{tech_id}: 실제 검증 범위가 필요합니다")

    if baseline is not None:
        previous_ids = set(re.findall(r"^### (T\d{2,})\s+.+$", baseline, re.M))
        previous_features = set(re.findall(r"^\| (F\d{2,}) \|", baseline, re.M))
        current_features = {r[0] for r in features}
        for removed in sorted((previous_ids - known) | (previous_features - current_features)):
            errors.append(f"기준 commit의 ID 삭제: {removed} — ID를 유지하고 보류/대체 근거를 기록하세요")
    return errors, len(known), len(features)


def read_baseline(root, ref):
    # 잘못된 ref는 누락된 기준선으로 취급하지 않는다. 최초 문서 추가만 비교를 생략한다.
    subprocess.run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=root,
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    files = subprocess.check_output(["git", "ls-tree", "--name-only", ref, "--", GOALS], cwd=root, text=True)
    if not files.strip():
        return None
    return subprocess.check_output(["git", "show", f"{ref}:{GOALS}"], cwd=root, text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="HEAD", help="삭제 방지 비교 기준 commit (기본 HEAD)")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    try:
        baseline = read_baseline(root, args.base)
        errors, goals, features = validate((root / GOALS).read_text(), (root / STATUS).read_text(), root, baseline)
    except (OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"기술 목표 검사 입력/기준 commit 확인 실패: {error}") from error
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"기술 목표 {goals}개 · 기능 {features}개: ID·매핑·상태·검증 근거·삭제 방지 확인")


if __name__ == "__main__":
    main()
