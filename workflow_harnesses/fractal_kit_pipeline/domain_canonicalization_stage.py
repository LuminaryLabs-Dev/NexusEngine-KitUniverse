from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Tuple


def apply_domain_canonicalization(
    records: List[Dict[str, Any]],
    domain_merge_report: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    alias_index = _alias_index(domain_merge_report)
    output = []
    canonicalized = 0
    preserved = 0
    for record in records:
        next_record = deepcopy(record)
        payload = next_record.setdefault("payload", {})
        domain_path = str(payload.get("domain_path", ""))
        group = alias_index.get(domain_path)
        if group:
            payload["canonical_domain_path"] = group["canonical_domain"]
            payload["domain_aliases"] = group["aliases"]
            payload["domain_group_size"] = group["source_domain_count"]
            payload["domain_source_record_count"] = group["source_record_count"]
            payload["domain_merge_evidence"] = {
                "needs": group.get("needs", [])[:8],
                "dependencies": group.get("dependencies", [])[:8],
                "source_record_ids": group.get("source_record_ids", [])[:16],
            }
            canonicalized += 1
        else:
            payload["canonical_domain_path"] = domain_path
            payload["domain_aliases"] = [domain_path] if domain_path else []
            payload["domain_group_size"] = 1
            payload["domain_source_record_count"] = 1
            payload["domain_merge_evidence"] = {
                "needs": [payload.get("need", "")] if payload.get("need") else [],
                "dependencies": payload.get("requires", [])[:8],
                "source_record_ids": [record.get("record_id", "")],
            }
            preserved += 1
        output.append(next_record)
    report = {
        "stage": "domain-canonicalization",
        "input_records": len(records),
        "output_records": len(output),
        "canonicalized_records": canonicalized,
        "preserved_records": preserved,
        "canonical_groups": len(domain_merge_report.get("groups", [])),
        "merge_rule": "apply reviewed canonical domain groups while preserving aliases, needs, dependencies, and source ids",
        "ok": len(output) == len(records),
    }
    return output, report


def _alias_index(domain_merge_report: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    index = {}
    for group in domain_merge_report.get("groups", []):
        canonical = str(group.get("canonical_domain", "")).strip()
        aliases = [str(alias).strip() for alias in group.get("aliases", []) if str(alias).strip()]
        for alias in {canonical, *aliases}:
            if alias:
                index[alias] = group
    return index
