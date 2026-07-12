from __future__ import annotations

from typing import Any, Dict, List, Tuple


def filter_atomic_idempotent(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    kept = []
    rejected = []
    seen = set()
    for record in records:
        payload = record["payload"]
        reasons = []
        if not payload["name"] or not payload["domain"]:
            reasons.append("missing-name-or-domain")
        if len(payload["requires"]) > 1:
            reasons.append("too-many-primary-dependencies")
        if len(payload["state_rules"]) < 2:
            reasons.append("missing-state-rules")
        if not payload["idempotent"]:
            reasons.append("not-idempotent")
        key = payload["semantic_key"]
        if key in seen:
            reasons.append("duplicate-semantic-key")
        if reasons:
            rejected.append({"record_id": record["record_id"], "reasons": reasons})
            continue
        seen.add(key)
        kept.append(record)
    return kept, {
        "stage": "filter-atomic-idempotent",
        "raw_count": len(records),
        "kept_count": len(kept),
        "rejected_count": len(rejected),
        "rejections": rejected[:200],
    }
