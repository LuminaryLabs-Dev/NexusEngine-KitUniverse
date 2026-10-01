from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from .codex_runner import (
    DEFAULT_CODEX_MODEL,
    DEFAULT_REASONING_EFFORT,
    run_codex_lane,
)


def run_master_review(
    repo_root: Path,
    review_root: Path,
    batch_id: str,
    candidates: List[Dict[str, Any]],
    timeout_seconds: int = 900,
    *,
    model: str = DEFAULT_CODEX_MODEL,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    retries: int = 1,
) -> Dict[str, Any]:
    review_root.mkdir(parents=True, exist_ok=True)
    packet_root = review_root / "packets"
    packet_root.mkdir(parents=True, exist_ok=True)
    packet_path = packet_root / f"{batch_id}.json"
    packet_path.write_text(json.dumps({"candidates": candidates}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    ids = [item["master_kit_id"] for item in candidates]
    candidates_by_id = {str(item["master_kit_id"]): item for item in candidates}
    base_prompt = f"""
Act as the final read-only RAWG master-kit gate.
Read {packet_path}. Use targeted read-only searches in NexusEngine, NexusEngine-ProtoKits, and `runs/kit-universe-1000/kits.jsonl` plus its promotion audit when needed. Prior `runs/rawg-881k/*` packages and reports are benchmark or validation evidence, not integrated capabilities; do not reject a candidate merely because the same candidate was smoke-built there.
Decide every candidate exactly once. Each source_context contains exact RAWG source provenance; cite the evidence and source identity in the decision. A candidate carrying `missing-exact-source-provenance` or `narrative-goal-language` in deterministic_warnings cannot be accepted: the latter marks goals such as a character who “must save the world,” not an operational mechanic. For an ordinary direct mechanic, accept only when the exact action-to-target relation is grammatically and mechanically entailed by quoted evidence. For pointer-derived candidates, independently verify the seed and facet_basis: capability-root is one atomic action/target kit whose required_facets are internal contract obligations rather than separate gameplay claims; adapter-root needs an explicit platform; domain-root may own only a generic boundary for an evidenced domain; mechanic-entailed needs a valid direct relation; kit-quality-required may derive a narrow invariant; explicit-evidence-required needs literal facet evidence. An LFM rejection is advisory and must not automatically discard a protected basis.
Every accepted boundary must be one atomic, composition-useful reusable behavior, own a real transition or query, and not duplicate an implemented capability. `mechanics`, `general`, `misc`, `other`, `unknown`, and `unclassified` are clustering placeholders, not admissible final domains; repair an accepted contract into a specific reusable technical control domain and subdomain. Reject nearby-word accidents, noun/adjective/passive senses, narrative outcomes, branding, unjustified cross-products, generic filler, composites, and aliases. If a grounded candidate is useful but its proposed contract is weak, repair it instead of rejecting it by returning a complete `contract` object with name, owns, does_not_own, inputs, outputs, idempotency, reset_snapshot, proof, domain, and subdomain. Do not edit or build anything.
Return only JSON:
{{"ok":true,"decisions":[{{"master_kit_id":"exact-id","accepted":true,"reasons":[],"contract":null}}],"systemic_errors":[]}}
Decide these exact IDs: {json.dumps(ids)}
""".strip()
    attempts: List[Dict[str, Any]] = []
    last_error = "Codex master review did not run"
    for attempt in range(max(0, retries) + 1):
        repair = (
            f"\nYour prior response was invalid: {last_error}. Return a complete corrected JSON envelope now."
            if attempt
            else ""
        )
        execution = run_codex_lane(
            repo_root=repo_root,
            artifact_root=review_root,
            scratch_root=review_root.parent / ".codex-scratch" / "master-review",
            job_id=batch_id,
            prompt=base_prompt + repair,
            model=model,
            reasoning_effort=reasoning_effort,
            timeout_seconds=timeout_seconds,
            attempt=attempt,
        )
        attempts.append(execution)
        if not execution.get("ok") or not execution.get("response"):
            last_error = str(execution.get("error") or f"Codex CLI return code {execution.get('returncode')}")
            continue
        try:
            raw = Path(str(execution["response"])).read_text(encoding="utf-8")
            value = _parse_object(raw)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            last_error = f"malformed Codex master review: {error}"
            continue
        decisions = [item for item in value.get("decisions") or [] if isinstance(item, dict)]
        decided = [str(item.get("master_kit_id") or "") for item in decisions]
        complete = sorted(decided) == sorted(ids) and len(decided) == len(set(decided))
        typed = all(isinstance(item.get("accepted"), bool) for item in decisions)
        reasoned = all(
            isinstance(item.get("reasons"), list)
            and any(isinstance(reason, str) and reason.strip() for reason in item["reasons"])
            for item in decisions
        )
        contract_valid = all(
            item.get("accepted") is not True
            or _contract_valid(item.get("contract"))
            or _contract_valid(candidates_by_id.get(str(item.get("master_kit_id")), {}).get("contract"))
            for item in decisions
        )
        systemic_typed = isinstance(value.get("systemic_errors"), list)
        warning_valid = all(
            item.get("accepted") is not True
            or not candidates_by_id.get(str(item.get("master_kit_id")), {}).get("source_context", {}).get("deterministic_warnings")
            for item in decisions
        )
        if value.get("ok") is not True or not complete or not typed or not reasoned or not contract_valid or not systemic_typed or not warning_valid:
            last_error = (
                "invalid-review-envelope:"
                f"complete={complete}:typed={typed}:reasoned={reasoned}:"
                f"contract_valid={contract_valid}:systemic_typed={systemic_typed}:warning_valid={warning_valid}"
            )
            continue
        return {
            **value,
            "ok": True,
            "complete": True,
            "typed": True,
            "reasoned": True,
            "contract_valid": True,
            "warning_valid": True,
            "batch_id": batch_id,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in attempts), 3),
            "packet_path": str(packet_path),
            "raw_output": execution["response"],
            "execution_report": execution.get("execution_report"),
            "attempts": len(attempts),
            "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in attempts),
            "_started_monotonic": min(float(item["_started_monotonic"]) for item in attempts),
            "_finished_monotonic": max(float(item["_finished_monotonic"]) for item in attempts),
        }
    last = attempts[-1] if attempts else {}
    return {
        "ok": False,
        "error": last_error,
        "batch_id": batch_id,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "elapsed_seconds": round(sum(float(item.get("elapsed_seconds") or 0) for item in attempts), 3),
        "packet_path": str(packet_path),
        "raw_output": last.get("response"),
        "execution_report": last.get("execution_report"),
        "attempts": len(attempts),
        "scratch_cleaned": all(item.get("scratch_cleaned") is True for item in attempts),
        "_started_monotonic": min((float(item["_started_monotonic"]) for item in attempts), default=0.0),
        "_finished_monotonic": max((float(item["_finished_monotonic"]) for item in attempts), default=0.0),
    }


def _parse_object(raw: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else raw[raw.find("{") : raw.rfind("}") + 1]
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("review-output-not-object")
    return value


def _contract_valid(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    required_strings = ("name", "owns", "does_not_own", "idempotency", "reset_snapshot", "proof", "domain", "subdomain")
    if any(not isinstance(value.get(key), str) or not value[key].strip() for key in required_strings):
        return False
    if value.get("proof") is False or str(value.get("proof") or "").strip().lower() == "false":
        return False
    if not all(isinstance(value.get(key), list) and value[key] for key in ("inputs", "outputs")):
        return False
    catch_all = {"general", "generic", "mechanics", "misc", "miscellaneous", "other", "unknown", "unclassified"}
    return str(value["domain"]).strip().lower() not in catch_all and str(value["subdomain"]).strip().lower() not in catch_all
