from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL


def _parse_json_object(text: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text[text.find("{") : text.rfind("}") + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("Sol review did not return a JSON object")
    return parsed


def run_batch_review(
    repo_root: Path,
    batch_dir: Path,
    candidate_path: Path,
    simulator_report_path: Path,
    duplicate_report_path: Path,
    record_ids: List[str],
    timeout_seconds: int = 900,
) -> Dict[str, Any]:
    raw_output_path = batch_dir / "sol-review.raw.txt"
    prompt = f"""
Review one KitUniverse batch in read-only mode.

READ:
- candidates: {candidate_path}
- NexusSimulator proof: {simulator_report_path}
- duplicate report: {duplicate_report_path}

For every candidate, decide whether it is a coherent atomic reusable kit, remains aligned to its source evidence, is not a semantic near-duplicate of another candidate, and is supported by the deterministic and NexusSimulator evidence. Do not edit files or generate kits.

Return only one JSON object:
{{"ok": true, "decisions": [{{"record_id": "exact id", "accepted": true, "reasons": []}}], "systemic_errors": []}}

Every one of these record ids must appear exactly once:
{json.dumps(record_ids)}
""".strip()
    command = [
        str(CODEX_BINARY),
        "exec",
        "--ephemeral",
        "--color",
        "never",
        "-C",
        str(repo_root),
        "-s",
        "read-only",
        "-m",
        CODEX_MODEL,
        "-c",
        'model_reasoning_effort="medium"',
        "-o",
        str(raw_output_path),
        prompt,
    ]
    started = time.time()
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
        return {
            "ok": False,
            "error": str(error),
            "command": command,
            "elapsed_seconds": round(time.time() - started, 3),
        }
    response: Dict[str, Any] = {
        "ok": False,
        "command": command,
        "returncode": result.returncode,
        "elapsed_seconds": round(time.time() - started, 3),
        "stderr_tail": result.stderr[-3000:],
        "model": CODEX_MODEL,
        "reasoning_effort": "medium",
        "raw_output": str(raw_output_path),
    }
    if result.returncode != 0 or not raw_output_path.exists():
        response["error"] = "Sol batch review command failed"
        return response
    try:
        parsed = _parse_json_object(raw_output_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        response["error"] = f"malformed Sol batch review: {error}"
        return response
    decisions = parsed.get("decisions") or []
    decision_ids = [str(item.get("record_id", "")) for item in decisions if isinstance(item, dict)]
    complete = sorted(decision_ids) == sorted(record_ids) and len(decision_ids) == len(set(decision_ids))
    typed = all(isinstance(item.get("accepted"), bool) for item in decisions if isinstance(item, dict))
    response.update(parsed)
    response["review_outcome_ok"] = bool(parsed.get("ok"))
    response["complete"] = complete
    response["typed"] = typed
    response["ok"] = bool(complete and typed)
    if not response["ok"] and "error" not in response:
        response["error"] = "Sol batch review was incomplete or rejected the batch envelope"
    return response
