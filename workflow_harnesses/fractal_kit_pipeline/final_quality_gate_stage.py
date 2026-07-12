from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, List, Tuple


REQUIRED_PAYLOAD_FIELDS = [
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
    "merge_key",
    "semantic_key",
    "canonical_domain_path",
    "domain_aliases",
    "domain_merge_evidence",
    "canonical_kit_record_id",
    "canonical_kit_name",
    "kit_alias_record_ids",
    "kit_merge_evidence",
]

LIST_FIELDS = [
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
    "domain_aliases",
    "kit_alias_record_ids",
]


def run_final_quality_gate(
    records: List[Dict[str, Any]],
    target_count: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    reasons: Counter[str] = Counter()
    rejected = []
    record_ids = set()
    semantic_keys = set()
    merge_keys = set()
    canonical_domains = set()
    canonical_kits = set()
    for index, record in enumerate(records):
        errors = _record_errors(record, record_ids, semantic_keys)
        payload = record.get("payload", {})
        merge_key = payload.get("merge_key")
        canonical_domain = payload.get("canonical_domain_path")
        canonical_kit = payload.get("canonical_kit_record_id")
        if merge_key:
            merge_keys.add(merge_key)
        if canonical_domain:
            canonical_domains.add(canonical_domain)
        if canonical_kit:
            canonical_kits.add(canonical_kit)
        if errors:
            for error in errors:
                reasons[error] += 1
            rejected.append(
                {
                    "index": index,
                    "record_id": record.get("record_id"),
                    "name": payload.get("name"),
                    "errors": errors,
                }
            )
    report = {
        "stage": "final-quality-gate",
        "target_count": target_count,
        "input_records": len(records),
        "kept_records": len(records) - len(rejected),
        "rejected_records": len(rejected),
        "unique_record_ids": len(record_ids),
        "unique_semantic_keys": len(semantic_keys),
        "unique_merge_keys": len(merge_keys),
        "canonical_domain_count": len(canonical_domains),
        "canonical_kit_count": len(canonical_kits),
        "rejection_reasons": dict(sorted(reasons.items())),
        "rejections": rejected[:200],
        "gate_rule": "final records must be unique, serializable, atomic, idempotent, slot-filled, and carry domain plus kit canonicalization metadata",
        "ok": len(records) == target_count and not rejected,
    }
    return records, report


def _record_errors(record: Dict[str, Any], record_ids: set[str], semantic_keys: set[str]) -> List[str]:
    errors = []
    record_id = str(record.get("record_id", "")).strip()
    payload = record.get("payload")
    if not record_id:
        errors.append("missing-record-id")
    elif record_id in record_ids:
        errors.append("duplicate-record-id")
    else:
        record_ids.add(record_id)
    if not isinstance(payload, dict):
        return [*errors, "missing-payload"]
    for field in REQUIRED_PAYLOAD_FIELDS:
        if not payload.get(field):
            errors.append(f"missing-{field}")
    for field in LIST_FIELDS:
        if not isinstance(payload.get(field), list) or not payload.get(field):
            errors.append(f"invalid-list-{field}")
    semantic_key = str(payload.get("semantic_key", "")).strip()
    if not semantic_key:
        errors.append("missing-semantic-key")
    elif semantic_key in semantic_keys:
        errors.append("duplicate-semantic-key")
    else:
        semantic_keys.add(semantic_key)
    if payload.get("atomic") is not True:
        errors.append("not-atomic")
    if payload.get("idempotent") is not True:
        errors.append("not-idempotent")
    renderer_boundary = payload.get("renderer_boundary", {})
    if not isinstance(renderer_boundary, dict):
        errors.append("invalid-renderer-boundary")
    elif any(renderer_boundary.get(key) for key in ["ownsDom", "ownsCanvas", "ownsThreeObjects"]):
        errors.append("renderer-owned")
    try:
        json.dumps(record, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        errors.append("not-json-serializable")
    return errors
