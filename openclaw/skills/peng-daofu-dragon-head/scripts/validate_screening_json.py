#!/usr/bin/env python3
"""将 JSON 校验为 DragonHeadScreening。用法（项目根目录）:
  PYTHONPATH=. python3 openclaw/skills/peng-daofu-dragon-head/scripts/validate_screening_json.py path.json
stdin:
  cat x.json | PYTHONPATH=. python3 openclaw/skills/peng-daofu-dragon-head/scripts/validate_screening_json.py -
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    # .../qdhub/openclaw/skills/peng-daofu-dragon-head/scripts/this.py -> parents[4] = 仓库根
    return Path(__file__).resolve().parents[4]


def main() -> None:
    root = _repo_root()
    sys.path.insert(0, str(root))
    from openclaw.schemas.dragon_head import DragonHeadScreening

    arg = sys.argv[1] if len(sys.argv) > 1 else "-"
    if arg == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(arg).read_text(encoding="utf-8")
    data = json.loads(raw)
    m = DragonHeadScreening.model_validate(data)
    out = m.model_dump(mode="json")
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
