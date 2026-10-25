#!/usr/bin/env python3
"""저장소 문서의 상대 파일 링크를 검사한다. 외부 URL은 요청하지 않는다."""
from pathlib import Path
import re

root = Path(__file__).resolve().parent.parent
files = [root / "README.md", root / "AGENTS.md", root / "CLAUDE.md", root / "server/README.md", root / "web/README.md", *sorted((root / "docs").rglob("*.md")), *sorted((root / ".github").rglob("*.md"))]
errors = []
for path in files:
    text = path.read_text()
    if text.count("```") % 2:
        errors.append(f"{path.relative_to(root)}: 닫히지 않은 코드 블록")
    for target in re.findall(r"\]\(([^)]+)\)", text):
        if "://" in target or target.startswith("#"):
            continue
        target = target.split("#")[0]
        if not (path.parent / target).exists():
            errors.append(f"{path.relative_to(root)}: 없는 링크 {target}")
if errors:
    raise SystemExit("\n".join(errors))
print(f"문서 {len(files)}개: 상대 파일 링크·코드 블록 확인")
