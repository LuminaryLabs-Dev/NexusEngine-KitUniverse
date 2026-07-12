from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.contract import validate_guided_kit


def run_post_codex_validation(
    repo_root: Path,
    final_kits_path: Path,
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "workflow_harnesses.guided_kit_builder.post_codex_validation",
        str(final_kits_path),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "error": str(error), "command": command}
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        payload = {
            "ok": False,
            "error": "fresh validator did not return JSON",
            "stdout": result.stdout[-2000:],
        }
    payload["returncode"] = result.returncode
    payload["stderr_tail"] = result.stderr[-2000:]
    payload["fresh_process"] = True
    payload["ok"] = bool(payload.get("ok") and result.returncode == 0)
    return payload


def validate_artifact(path: Path) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    malformed = []
    if not path.exists():
        return {"ok": False, "error": "final kits artifact missing", "path": str(path)}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as error:
            malformed.append({"line": line_number, "error": str(error)})
    validations = [validate_guided_kit(record) for record in records]
    model_draft_slots = sum(
        1
        for record in records
        for slot in ((record.get("payload") or {}).get("slots") or {}).values()
        if slot.get("source") == "model-draft"
    )
    generator_contribution_ok = model_draft_slots > 0
    return {
        "ok": (
            len(records) == 1
            and not malformed
            and all(item["ok"] for item in validations)
            and generator_contribution_ok
        ),
        "path": str(path),
        "record_count": len(records),
        "malformed": malformed,
        "validations": validations,
        "generator_contribution": {
            "ok": generator_contribution_ok,
            "model_draft_slots": model_draft_slots,
            "required_minimum": 1,
        },
    }


def main(argv: List[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1:
        print(json.dumps({"ok": False, "error": "expected one final-kits.jsonl path"}))
        return 2
    report = validate_artifact(Path(arguments[0]))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
