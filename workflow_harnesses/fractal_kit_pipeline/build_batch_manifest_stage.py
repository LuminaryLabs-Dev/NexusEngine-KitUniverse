from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List


DEFAULT_BATCH_SIZE = 128


def build_batch_manifest(
    final_records: List[Dict[str, Any]],
    target_count: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in sorted(final_records, key=lambda item: item.get("record_id", "")):
        payload = record.get("payload", {})
        groups[str(payload.get("primary_dependency", "unknown"))].append(record)

    batches = []
    for group_name in sorted(groups):
        records = groups[group_name]
        for batch_index, start in enumerate(range(0, len(records), batch_size)):
            chunk = records[start : start + batch_size]
            batches.append(_batch(group_name, batch_index, chunk))

    assigned_ids = [record_id for batch in batches for record_id in batch["record_ids"]]
    duplicate_assignments = len(assigned_ids) - len(set(assigned_ids))
    missing_assignments = len(final_records) - len(set(assigned_ids))
    max_batch_size = max((batch["record_count"] for batch in batches), default=0)
    checks = [
        _check(
            "all-records-assigned",
            "every final record is assigned to exactly one build batch",
            len(set(assigned_ids)) == len(final_records) and missing_assignments == 0,
            {
                "record_count": len(final_records),
                "assigned_unique_records": len(set(assigned_ids)),
                "missing_assignments": missing_assignments,
            },
        ),
        _check(
            "no-duplicate-assignments",
            "no final record appears in more than one build batch",
            duplicate_assignments == 0,
            {"duplicate_assignments": duplicate_assignments},
        ),
        _check(
            "bounded-batch-size",
            "each batch stays within the configured build batch size",
            max_batch_size <= batch_size,
            {"max_batch_size": max_batch_size, "batch_size": batch_size},
        ),
        _check(
            "target-count",
            "assigned batch records match the configured target count",
            len(final_records) == target_count and len(assigned_ids) == target_count,
            {"record_count": len(final_records), "assigned_records": len(assigned_ids), "target_count": target_count},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "build-batch-manifest",
        "record_count": len(final_records),
        "target_count": target_count,
        "batch_size": batch_size,
        "batch_count": len(batches),
        "group_count": len(groups),
        "max_batch_size": max_batch_size,
        "duplicate_assignments": duplicate_assignments,
        "missing_assignments": missing_assignments,
        "group_counts": _top_counts(Counter({name: len(records) for name, records in groups.items()}), len(groups)),
        "batches": batches,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _batch(group_name: str, batch_index: int, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    payloads = [record.get("payload", {}) for record in records]
    categories = Counter(str(payload.get("category", "unknown")) for payload in payloads)
    domains = Counter(str(payload.get("canonical_domain_path", "unknown")) for payload in payloads)
    return {
        "batch_id": f"{_slug(group_name)}-{batch_index:04d}",
        "primary_dependency": group_name,
        "batch_index": batch_index,
        "record_count": len(records),
        "record_ids": [record.get("record_id") for record in records],
        "categories": _top_counts(categories, 8),
        "canonical_domains": _top_counts(domains, 8),
        "resume_hint": "Build this batch from final-kits.jsonl by matching record_ids in listed order.",
    }


def _slug(value: str) -> str:
    return value.replace(":", "-").replace("/", "-").lower() or "unknown"


def _top_counts(counter: Counter, limit: int) -> List[Dict[str, Any]]:
    return [{"value": value, "count": count} for value, count in counter.most_common(limit)]


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
