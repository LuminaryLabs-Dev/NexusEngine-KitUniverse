from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Tuple


def build_diversity_audit(final_records: List[Dict[str, Any]], target_count: int) -> Dict[str, Any]:
    payloads = [record.get("payload", {}) for record in final_records]
    record_ids = [record.get("record_id") for record in final_records]
    names = [payload.get("name") for payload in payloads]
    semantic_keys = [payload.get("semantic_key") for payload in payloads]
    merge_keys = [payload.get("merge_key") for payload in payloads]
    canonical_kit_ids = [payload.get("canonical_kit_record_id") for payload in payloads]
    canonical_domains = [payload.get("canonical_domain_path") for payload in payloads]
    categories = [payload.get("category") for payload in payloads]
    signatures = [_build_signature(payload) for payload in payloads]

    record_count = len(final_records)
    thresholds = _thresholds(target_count)
    signature_counter = Counter(signatures)
    checks = [
        _check(
            "target-count",
            "final record count matches requested target",
            record_count == target_count,
            {"record_count": record_count, "target_count": target_count},
        ),
        _check(
            "record-id-unique",
            "record ids are unique",
            _unique_count(record_ids) == record_count,
            {"unique_record_ids": _unique_count(record_ids), "record_count": record_count},
        ),
        _check(
            "semantic-key-unique",
            "semantic keys are unique",
            _unique_count(semantic_keys) == record_count,
            {"unique_semantic_keys": _unique_count(semantic_keys), "record_count": record_count},
        ),
        _check(
            "merge-key-unique",
            "merge keys are unique after reviewed duplicate suppression",
            _unique_count(merge_keys) == record_count,
            {"unique_merge_keys": _unique_count(merge_keys), "record_count": record_count},
        ),
        _check(
            "canonical-kit-unique",
            "canonical kit ids remain unique in the final selected set",
            _unique_count(canonical_kit_ids) == record_count,
            {"unique_canonical_kit_ids": _unique_count(canonical_kit_ids), "record_count": record_count},
        ),
        _check(
            "domain-breadth",
            "canonical domains are broad enough for the configured target",
            _unique_count(canonical_domains) >= thresholds["min_domains"],
            {
                "unique_canonical_domains": _unique_count(canonical_domains),
                "min_domains": thresholds["min_domains"],
            },
        ),
        _check(
            "category-breadth",
            "top-level categories are broad enough for the configured target",
            _unique_count(categories) >= thresholds["min_categories"],
            {
                "unique_categories": _unique_count(categories),
                "min_categories": thresholds["min_categories"],
            },
        ),
        _check(
            "signature-uniqueness",
            "build-relevant signatures are unique enough to become separate kits",
            _unique_count(signatures) >= thresholds["min_unique_signatures"],
            {
                "unique_signatures": _unique_count(signatures),
                "min_unique_signatures": thresholds["min_unique_signatures"],
                "max_duplicate_signature_count": max(signature_counter.values()) if signature_counter else 0,
            },
        ),
        _check(
            "name-uniqueness",
            "kit names are unique enough for artifact generation",
            _unique_count(names) == record_count,
            {"unique_names": _unique_count(names), "record_count": record_count},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "diversity-audit",
        "record_count": record_count,
        "target_count": target_count,
        "thresholds": thresholds,
        "unique_record_ids": _unique_count(record_ids),
        "unique_names": _unique_count(names),
        "unique_semantic_keys": _unique_count(semantic_keys),
        "unique_merge_keys": _unique_count(merge_keys),
        "unique_canonical_kit_ids": _unique_count(canonical_kit_ids),
        "unique_canonical_domains": _unique_count(canonical_domains),
        "unique_categories": _unique_count(categories),
        "unique_signatures": _unique_count(signatures),
        "max_duplicate_signature_count": max(signature_counter.values()) if signature_counter else 0,
        "top_categories": _top_counts(categories, 12),
        "top_canonical_domains": _top_counts(canonical_domains, 12),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _thresholds(target_count: int) -> Dict[str, int]:
    return {
        "min_domains": min(24, max(1, target_count // 128)),
        "min_categories": min(12, max(1, target_count // 256)),
        "min_unique_signatures": int(target_count * 0.98),
    }


def _build_signature(payload: Dict[str, Any]) -> Tuple[str, str, str, Tuple[str, ...], Tuple[str, ...]]:
    return (
        str(payload.get("canonical_domain_path", "")),
        str(payload.get("need", "")),
        str(payload.get("primary_dependency", "")),
        tuple(payload.get("requires") or []),
        tuple(payload.get("provides") or []),
    )


def _unique_count(values: List[Any]) -> int:
    return len(set(values))


def _top_counts(values: List[Any], limit: int) -> List[Dict[str, Any]]:
    return [{"value": value, "count": count} for value, count in Counter(values).most_common(limit)]


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
