from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any, Dict

from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL


def run_bounded_review(node: Dict[str, Any], workspace: Path, packet: Dict[str, Any]) -> Dict[str, Any]:
    loop = node.get("loop") or {}
    max_passes = int(loop.get("max_passes", 1))
    threshold = float(loop.get("stop_score", 0.9))
    state = {
        "schema_version": "kituniverse.codex-loop-state.v1",
        "original_goal": node.get("goal"),
        "accepted": [],
        "rejected": [],
        "open_issues": [],
        "latest_output": None,
        "passes": [],
    }
    loop_root = workspace / "codex-loop"
    loop_root.mkdir(parents=True, exist_ok=True)
    packet_path = loop_root / "review-packet.json"
    _write_json(packet_path, packet)
    started = time.monotonic()
    for pass_index in range(1, max_passes + 1):
        state_path = loop_root / "loop-state.json"
        _write_json(state_path, state)
        output = loop_root / f"pass-{pass_index:02d}.raw.txt"
        prompt = f"""
Act as the bounded KitUniverse fast-lane reviewer.
Original goal: {node.get('goal')}
Read only: {packet_path}, {state_path}, and these bounded recall paths: {json.dumps(node.get('recall_paths') or [])}.
Review the latest artifacts for evidence preservation, grouping quality, projected speed, and safe Codex-call reduction. You may recommend configuration edits, but may not edit repositories, execute mutations, or promote kits. Preserve useful rejected evidence as feedback.
Return only JSON:
{{"ok":true,"score":0.0,"stop":false,"accepted":[],"rejected":[],"open_issues":[],"operations":[],"summary":""}}
""".strip()
        command = [
            str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
            "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(output), prompt,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        value = _parse_output(output) if result.returncode == 0 and output.exists() else {"ok": False, "error": result.stderr[-1000:]}
        state["passes"].append({"pass": pass_index, "result": value})
        state["latest_output"] = str(output)
        state["accepted"] = value.get("accepted") or state["accepted"]
        state["rejected"] = value.get("rejected") or state["rejected"]
        state["open_issues"] = value.get("open_issues") or []
        if value.get("stop") or float(value.get("score") or 0) >= threshold:
            break
    state["elapsed_seconds"] = round(time.monotonic() - started, 3)
    state["ok"] = bool(state["passes"] and state["passes"][-1]["result"].get("ok"))
    _write_json(loop_root / "loop-state.json", state)
    return state


def run_kit_proposal_review(node: Dict[str, Any], workspace: Path, packet: Dict[str, Any]) -> Dict[str, Any]:
    review_root = workspace / "kit-proposal-review"
    review_root.mkdir(parents=True, exist_ok=True)
    packet_path = review_root / "candidate-packet.json"
    output = review_root / "codex-kit-proposals.raw.txt"
    _write_json(packet_path, packet)
    candidate_ids = [item["candidate_id"] for item in packet.get("candidates") or []]
    if not candidate_ids:
        result = {"ok": True, "skipped": True, "reason": "no-validated-candidates", "decisions": [], "kits": []}
        _write_json(review_root / "codex-kit-proposals.json", result)
        return result
    prompt = f"""
Act as the final proposal-only KitUniverse architect.
Read {packet_path}, then use targeted read-only searches in /Users/crimsonwheeler/Documents/GitHub/NexusEngine and /Users/crimsonwheeler/Documents/GitHub/NexusEngine-ProtoKits.
Decide every candidate exactly once. Accept only one atomic, reusable, evidence-entailed behavior with clear owned inputs, state/rule transition, and outputs that is not already implemented or a semantic alias. Preserve RAWG source IDs and evidence hashes. Produce at most six proposal-only kits. Do not edit repositories, promote code, or create implementations.
Return only JSON:
{{"ok":true,"decisions":[{{"candidate_id":"exact-id","accepted":true,"reasons":[]}}],"kits":[{{"candidate_id":"exact-id","kit_id":"kebab-id","name":"Name","domain":"domain","owned_behavior":"one behavior","inputs":[],"outputs":[],"novelty_reason":"why"}}],"systemic_errors":[]}}
Decide these exact candidate IDs: {json.dumps(candidate_ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(Path.cwd()),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(output), prompt,
    ]
    started = time.monotonic()
    try:
        process = subprocess.run(command, capture_output=True, text=True, timeout=900, check=False)
        value = _parse_output(output) if process.returncode == 0 and output.exists() else {"ok": False, "error": process.stderr[-1000:]}
    except (OSError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as error:
        value = {"ok": False, "error": str(error)}
        process = None
    decisions = [item for item in value.get("decisions") or [] if isinstance(item, dict)]
    decided = [item.get("candidate_id") for item in decisions]
    complete = sorted(decided) == sorted(candidate_ids) and len(decided) == len(set(decided))
    result = {
        **value,
        "ok": bool(value.get("ok")) and complete and all(isinstance(item.get("accepted"), bool) for item in decisions),
        "complete": complete,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "model": CODEX_MODEL,
        "returncode": process.returncode if process else None,
        "raw_output": str(output),
    }
    _write_json(review_root / "codex-kit-proposals.json", result)
    return result


def _parse_output(path: Path) -> Dict[str, Any]:
    content = path.read_text(encoding="utf-8")
    value = json.loads(content[content.find("{") : content.rfind("}") + 1])
    return value if isinstance(value, dict) else {"ok": False, "error": "non-object response"}


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
