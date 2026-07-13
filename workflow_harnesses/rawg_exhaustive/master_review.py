from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from workflow_harnesses.guided_kit_builder.codex_cli_review import CODEX_BINARY, CODEX_MODEL


def run_master_review(repo_root: Path, review_root: Path, batch_id: str, candidates: List[Dict[str, Any]], timeout_seconds: int = 900) -> Dict[str, Any]:
    review_root.mkdir(parents=True, exist_ok=True)
    packet_path = review_root / f"{batch_id}.packet.json"
    output_path = review_root / f"{batch_id}.raw.txt"
    packet_path.write_text(json.dumps({"candidates": candidates}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    ids = [item["master_kit_id"] for item in candidates]
    prompt = f"""
Act as the final read-only RAWG master-kit gate.
Read {packet_path}. Use targeted read-only searches in NexusEngine, NexusEngine-ProtoKits, and `runs/kit-universe-1000/kits.jsonl` plus its promotion audit when needed. Prior `runs/rawg-881k/*` packages and reports are benchmark or validation evidence, not integrated capabilities; do not reject a candidate merely because the same candidate was smoke-built there.
Decide every candidate exactly once. For an ordinary direct mechanic, accept only when the exact action-to-target relation is grammatically and mechanically entailed by quoted evidence. For pointer-derived candidates, independently verify the seed and facet_basis: capability-root is one atomic action/target kit whose required_facets are internal contract obligations rather than separate gameplay claims; adapter-root needs an explicit platform; domain-root may own only a generic boundary for an evidenced domain; mechanic-entailed needs a valid direct relation; kit-quality-required may derive a narrow invariant; explicit-evidence-required needs literal facet evidence. An LFM rejection is advisory and must not automatically discard a protected basis.
Every accepted boundary must be one atomic, composition-useful reusable behavior, own a real transition or query, and not duplicate an implemented capability. Reject nearby-word accidents, noun/adjective/passive senses, narrative outcomes, branding, unjustified cross-products, generic filler, composites, and aliases. If a grounded candidate is useful but its proposed contract is weak, repair it instead of rejecting it by returning a complete `contract` object with name, owns, does_not_own, inputs, outputs, idempotency, reset_snapshot, proof, domain, and subdomain. Do not edit or build anything.
Return only JSON:
{{"ok":true,"decisions":[{{"master_kit_id":"exact-id","accepted":true,"reasons":[],"contract":null}}],"systemic_errors":[]}}
Decide these exact IDs: {json.dumps(ids)}
""".strip()
    command = [
        str(CODEX_BINARY), "exec", "--ephemeral", "--color", "never", "-C", str(repo_root),
        "-s", "read-only", "-m", CODEX_MODEL, "-c", 'model_reasoning_effort="medium"', "-o", str(output_path), prompt,
    ]
    started = time.monotonic()
    try:
        process = subprocess.run(command, cwd=repo_root, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "error": str(error), "batch_id": batch_id, "elapsed_seconds": round(time.monotonic() - started, 3)}
    if process.returncode != 0 or not output_path.exists():
        return {"ok": False, "error": "Codex master review failed", "batch_id": batch_id, "returncode": process.returncode, "stderr_tail": process.stderr[-2000:]}
    try:
        raw = output_path.read_text(encoding="utf-8")
        fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
        candidate = fenced.group(1) if fenced else raw[raw.find("{") : raw.rfind("}") + 1]
        value = json.loads(candidate)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return {"ok": False, "error": f"malformed Codex master review: {error}", "batch_id": batch_id}
    decisions = [item for item in value.get("decisions") or [] if isinstance(item, dict)]
    decided = [str(item.get("master_kit_id") or "") for item in decisions]
    complete = sorted(decided) == sorted(ids) and len(decided) == len(set(decided))
    typed = all(isinstance(item.get("accepted"), bool) for item in decisions)
    return {
        **value,
        "ok": bool(value.get("ok")) and complete and typed,
        "complete": complete,
        "typed": typed,
        "batch_id": batch_id,
        "model": CODEX_MODEL,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "packet_path": str(packet_path),
        "raw_output": str(output_path),
        "returncode": process.returncode,
    }
