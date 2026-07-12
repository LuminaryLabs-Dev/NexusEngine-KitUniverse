from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL


def _parse_object(text: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text[text.find("{") : text.rfind("}") + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("cluster review did not return an object")
    return parsed


def run_cluster_review(
    repo_root: Path,
    workspace_root: Path,
    candidates_path: Path,
    inventory_path: Path,
    cluster_ids: List[str],
    timeout_seconds: int = 900,
) -> Dict[str, Any]:
    raw_path = workspace_root / "cluster-review.raw.txt"
    prompt = f"""
Review RAWG-derived capability clusters in read-only mode.

READ:
- cluster candidates: {candidates_path}
- live Nexus/ProtoKits inventory: {inventory_path}

Accept a cluster only when its capability is reusable, mechanically meaningful, source-backed, and genuinely missing or incomplete in the inventory. Reject story nouns, broad genres, renamed duplicates, presentation-only concepts, and unsupported claims. Do not edit files or generate kits.

Return only:
{{"ok": true, "decisions": [{{"cluster_id": "exact id", "accepted": true, "reasons": []}}], "systemic_errors": []}}

Every cluster id must appear exactly once:
{json.dumps(cluster_ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(repo_root),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"',
        "-o", str(raw_path), prompt,
    ]
    started = time.time()
    try:
        result = subprocess.run(command, cwd=repo_root, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "error": str(error), "command": command, "elapsed_seconds": round(time.time() - started, 3)}
    response: Dict[str, Any] = {
        "ok": False,
        "command": command,
        "returncode": result.returncode,
        "elapsed_seconds": round(time.time() - started, 3),
        "stderr_tail": result.stderr[-3000:],
        "model": CODEX_MODEL,
        "reasoning_effort": "medium",
        "raw_output": str(raw_path),
    }
    if result.returncode != 0 or not raw_path.exists():
        response["error"] = "Codex cluster review command failed"
        return response
    try:
        parsed = _parse_object(raw_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        response["error"] = f"malformed cluster review: {error}"
        return response
    decisions = parsed.get("decisions") or []
    ids = [str(item.get("cluster_id", "")) for item in decisions if isinstance(item, dict)]
    complete = sorted(ids) == sorted(cluster_ids) and len(ids) == len(set(ids))
    typed = all(isinstance(item.get("accepted"), bool) for item in decisions if isinstance(item, dict))
    response.update(parsed)
    response["complete"] = complete
    response["typed"] = typed
    response["ok"] = bool(complete and typed)
    if not response["ok"]:
        response["error"] = "Codex cluster review was incomplete or malformed"
    return response
