from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.contract import validate_guided_kit


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def validate_batch(path: Path) -> Dict[str, Any]:
    try:
        records = read_jsonl(path)
    except (OSError, json.JSONDecodeError) as error:
        return {"ok": False, "path": str(path), "error": str(error), "decisions": []}
    decisions = []
    for record in records:
        validation = validate_guided_kit(record)
        model_slots = sum(
            slot.get("source") == "model-draft"
            for slot in ((record.get("payload") or {}).get("slots") or {}).values()
        )
        errors = list(validation["errors"])
        if model_slots < 1 and "no-model-draft-contribution" not in errors:
            errors.append("no-model-draft-contribution")
        decisions.append(
            {
                "record_id": record.get("record_id"),
                "ok": not errors,
                "errors": errors,
                "checks": validation.get("checks", 0),
                "model_draft_slots": model_slots,
            }
        )
    return {
        "ok": bool(records) and all(item["ok"] for item in decisions),
        "path": str(path),
        "record_count": len(records),
        "accepted": sum(item["ok"] for item in decisions),
        "rejected": sum(not item["ok"] for item in decisions),
        "decisions": decisions,
        "fresh_process": True,
    }


def main(argv: List[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1:
        print(json.dumps({"ok": False, "error": "expected one batch JSONL path"}))
        return 2
    report = validate_batch(Path(arguments[0]))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
