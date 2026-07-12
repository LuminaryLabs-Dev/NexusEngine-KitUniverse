from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Set

from workflow_harnesses.fractal_kit_pipeline.kit_contract import key


REQUIRED_PAYLOAD_FIELDS = {
    "name",
    "domain_path",
    "requires",
    "provides",
    "resources",
    "events",
    "systems",
    "public_api",
    "tests",
    "source_evidence",
    "atomic",
    "idempotent",
    "semantic_key",
    "merge_key",
}


def build_idea_matrix_audit(
    idea_records: List[Dict[str, Any]],
    reveal_records: List[Dict[str, Any]],
    target_count: int,
) -> Dict[str, Any]:
    expected_records = target_count + max(512, target_count // 10)
    record_ids = [record.get("record_id") for record in idea_records]
    payloads = [record.get("payload", {}) for record in idea_records if isinstance(record.get("payload"), dict)]
    semantic_keys = [payload.get("semantic_key") for payload in payloads]
    merge_keys = [payload.get("merge_key") for payload in payloads]
    missing_payload_fields = _missing_payload_fields(payloads)
    non_atomic = [payload.get("name") for payload in payloads if payload.get("atomic") is not True]
    non_idempotent = [payload.get("name") for payload in payloads if payload.get("idempotent") is not True]
    revealed_signals = [
        str(payload.get("source_evidence", {}).get("revealed_signal", "")).strip()
        for payload in payloads
        if isinstance(payload.get("source_evidence"), dict)
    ]
    reveal_source_values = {str(record.get("reduced") or record.get("expansion_point") or "").strip() for record in reveal_records}
    reveal_source_keys = {key(value) for value in reveal_source_values if key(value)}
    signal_keys = {key(value) for value in revealed_signals if key(value)}
    domains = [payload.get("domain_path") for payload in payloads]
    categories = [payload.get("category") for payload in payloads]
    primary_dependencies = [payload.get("primary_dependency") for payload in payloads]
    tests = [test for payload in payloads for test in payload.get("tests", []) if isinstance(payload.get("tests"), list)]
    expected_dependency_breadth = min(8, max(1, len(idea_records) // (121 * 12)))
    checks = [
        _check(
            "target-buffer-count",
            "idea matrix contains target count plus deterministic expansion buffer",
            len(idea_records) >= expected_records,
            {
                "records": len(idea_records),
                "target_count": target_count,
                "expected_records": expected_records,
            },
        ),
        _check(
            "payload-shape",
            "all sampled matrix records expose required kit candidate payload fields",
            not missing_payload_fields and len(payloads) == len(idea_records),
            {
                "records": len(idea_records),
                "payloads": len(payloads),
                "missing_payload_fields": missing_payload_fields[:25],
            },
        ),
        _check(
            "record-id-unique",
            "matrix record ids are unique",
            len(record_ids) == len(set(record_ids)),
            {"records": len(record_ids), "unique_record_ids": len(set(record_ids))},
        ),
        _check(
            "semantic-key-unique",
            "matrix semantic keys are unique before atomic filtering",
            len(semantic_keys) == len(set(semantic_keys)),
            {"semantic_keys": len(semantic_keys), "unique_semantic_keys": len(set(semantic_keys))},
        ),
        _check(
            "atomic-idempotent-flags",
            "matrix candidates enter filtering already marked atomic and idempotent",
            not non_atomic and not non_idempotent,
            {"non_atomic": non_atomic[:25], "non_idempotent": non_idempotent[:25]},
        ),
        _check(
            "reveal-signal-coverage",
            "matrix source evidence covers the reduced reveal signals feeding it",
            reveal_source_keys.issubset(signal_keys),
            {
                "reveal_sources": len(reveal_source_keys),
                "matrix_signals": len(signal_keys),
                "missing_reveal_sources": sorted(reveal_source_keys - signal_keys)[:50],
            },
        ),
        _check(
            "domain-breadth",
            "matrix spans broad domain, category, dependency, and proof surfaces before filtering",
            len(set(domains)) >= min(64, len(idea_records))
            and len(set(categories)) >= min(12, len(idea_records))
            and len(set(primary_dependencies)) >= expected_dependency_breadth
            and len(set(tests)) >= min(4, len(idea_records)),
            {
                "unique_domains": len(set(domains)),
                "unique_categories": len(set(categories)),
                "unique_primary_dependencies": len(set(primary_dependencies)),
                "expected_primary_dependencies": expected_dependency_breadth,
                "unique_tests": len(set(tests)),
            },
        ),
        _check(
            "merge-keys-present",
            "matrix records expose deterministic merge keys before later merge review",
            len(merge_keys) == len(idea_records) and all(merge_keys),
            {"unique_merge_keys": len(set(merge_keys)), "max_merge_key_count": _max_count(merge_keys)},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "idea-matrix-audit",
        "target_count": target_count,
        "expected_records": expected_records,
        "record_count": len(idea_records),
        "unique_record_ids": len(set(record_ids)),
        "unique_semantic_keys": len(set(semantic_keys)),
        "unique_merge_keys": len(set(merge_keys)),
        "unique_domains": len(set(domains)),
        "unique_categories": len(set(categories)),
        "unique_reveal_signals": len(signal_keys),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _missing_payload_fields(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    missing = []
    for index, payload in enumerate(payloads[:256]):
        absent = sorted(field for field in REQUIRED_PAYLOAD_FIELDS if field not in payload)
        if absent:
            missing.append({"index": index, "missing": absent, "name": payload.get("name")})
    return missing


def _max_count(values: List[Any]) -> int:
    counter = Counter(value for value in values if value)
    return max(counter.values()) if counter else 0


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
