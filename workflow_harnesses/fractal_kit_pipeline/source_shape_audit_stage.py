from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


KIT_SLOT_FIELDS = [
    "name",
    "domain",
    "domain_path",
    "requires",
    "provides",
    "resources",
    "events",
    "systems",
    "public_api",
    "inputs",
    "outputs",
    "state_rules",
    "tests",
    "snapshot",
    "renderer_boundary",
    "promotion",
]

LIST_SLOT_FIELDS = [
    "requires",
    "provides",
    "resources",
    "events",
    "systems",
    "public_api",
    "inputs",
    "outputs",
    "state_rules",
    "tests",
]


def build_source_shape_audit(
    run_dir: Path,
    records: List[Dict[str, Any]],
    target_count: int,
) -> Dict[str, Any]:
    research = _read_json(run_dir / "research-pack.json")
    required_shape = research.get("kit_shape", {}).get("required", [])
    public_sources = research.get("public_sources_reviewed", [])
    source_checks = [
        _check(
            "public-protokits-source",
            "research pack records public ProtoKits source evidence",
            any("LuminaryLabs-Agents/NexusRealtime-ProtoKits" in source for source in public_sources),
            {"public_sources_reviewed": public_sources},
        ),
        _check(
            "kit-shape-source",
            "research pack records the required kit shape",
            all(
                item in required_shape
                for item in [
                    "stable id",
                    "domain path",
                    "requires/provides tokens",
                    "snapshot/reset policy",
                    "validation path",
                    "renderer boundary",
                ]
            ),
            {"required_shape": required_shape},
        ),
    ]
    record_failures = []
    renderer_owned = 0
    non_atomic = 0
    non_idempotent = 0
    missing_slots = 0
    promotion_gaps = 0
    for index, record in enumerate(records):
        payload = record.get("payload", {})
        failures = _record_failures(payload)
        if failures:
            if "renderer-owned" in failures:
                renderer_owned += 1
            if "not-atomic" in failures:
                non_atomic += 1
            if "not-idempotent" in failures:
                non_idempotent += 1
            if any(failure.startswith("missing-") or failure.startswith("invalid-") for failure in failures):
                missing_slots += 1
            if "missing-promotion-proof" in failures:
                promotion_gaps += 1
            record_failures.append(
                {
                    "index": index,
                    "record_id": record.get("record_id"),
                    "name": payload.get("name"),
                    "failures": failures,
                }
            )
    checks = [
        *source_checks,
        _check(
            "target-records",
            "source-shape audit covers the selected final record set",
            len(records) == target_count,
            {"records": len(records), "target_count": target_count},
        ),
        _check(
            "domain-first-records",
            "every record has domain path, canonical domain, and dependency tokens",
            missing_slots == 0,
            {"records_with_missing_or_invalid_slots": missing_slots},
        ),
        _check(
            "atomic-idempotent-records",
            "every record remains atomic and idempotent after canonicalization",
            non_atomic == 0 and non_idempotent == 0,
            {"non_atomic": non_atomic, "non_idempotent": non_idempotent},
        ),
        _check(
            "renderer-agnostic-records",
            "records do not own DOM, canvas, or renderer objects",
            renderer_owned == 0,
            {"renderer_owned": renderer_owned},
        ),
        _check(
            "promotion-proof-records",
            "records include deterministic validation or smoke-test promotion criteria",
            promotion_gaps == 0,
            {"promotion_gaps": promotion_gaps},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks) and not record_failures,
        "stage": "source-shape-audit",
        "target_count": target_count,
        "records_checked": len(records),
        "public_sources_reviewed": public_sources,
        "required_shape": required_shape,
        "renderer_owned": renderer_owned,
        "non_atomic": non_atomic,
        "non_idempotent": non_idempotent,
        "missing_slots": missing_slots,
        "promotion_gaps": promotion_gaps,
        "record_failure_count": len(record_failures),
        "record_failures": record_failures[:200],
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _record_failures(payload: Dict[str, Any]) -> List[str]:
    failures = []
    for field in KIT_SLOT_FIELDS:
        if not payload.get(field):
            failures.append(f"missing-{field}")
    for field in LIST_SLOT_FIELDS:
        value = payload.get(field)
        if not isinstance(value, list) or not value:
            failures.append(f"invalid-{field}")
    if not payload.get("canonical_domain_path") or not payload.get("canonical_kit_record_id"):
        failures.append("missing-canonical-metadata")
    if payload.get("atomic") is not True:
        failures.append("not-atomic")
    if payload.get("idempotent") is not True:
        failures.append("not-idempotent")
    boundary = payload.get("renderer_boundary", {})
    if not isinstance(boundary, dict) or any(
        boundary.get(key) for key in ["ownsDom", "ownsCanvas", "ownsThreeObjects"]
    ):
        failures.append("renderer-owned")
    promotion = payload.get("promotion", {})
    criteria = promotion.get("criteria") if isinstance(promotion, dict) else []
    joined = " ".join(str(item).lower() for item in criteria)
    if not any(token in joined for token in ["smoke", "validation", "snapshot", "headless"]):
        failures.append("missing-promotion-proof")
    try:
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        failures.append("not-json-serializable")
    return failures


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
