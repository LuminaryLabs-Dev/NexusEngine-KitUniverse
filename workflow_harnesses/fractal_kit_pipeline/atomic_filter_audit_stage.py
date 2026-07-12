from __future__ import annotations

from typing import Any, Dict, List, Set


def build_atomic_filter_audit(
    idea_records: List[Dict[str, Any]],
    filtered_records: List[Dict[str, Any]],
    filter_report: Dict[str, Any],
    target_count: int,
) -> Dict[str, Any]:
    matrix_ids = {record.get("record_id") for record in idea_records}
    filtered_ids = [record.get("record_id") for record in filtered_records]
    payloads = [record.get("payload", {}) for record in filtered_records if isinstance(record.get("payload"), dict)]
    semantic_keys = [payload.get("semantic_key") for payload in payloads]
    non_atomic = [payload.get("name") for payload in payloads if payload.get("atomic") is not True]
    non_idempotent = [payload.get("name") for payload in payloads if payload.get("idempotent") is not True]
    too_many_deps = [
        payload.get("name")
        for payload in payloads
        if not isinstance(payload.get("requires"), list) or len(payload.get("requires", [])) > 1
    ]
    missing_state_rules = [
        payload.get("name")
        for payload in payloads
        if not isinstance(payload.get("state_rules"), list) or len(payload.get("state_rules", [])) < 2
    ]
    missing_name_domain = [
        payload.get("name") or payload.get("domain")
        for payload in payloads
        if not payload.get("name") or not payload.get("domain")
    ]
    checks = [
        _check(
            "count-accounting",
            "filter report raw, kept, and rejected counts account for matrix and filtered artifacts",
            filter_report.get("raw_count") == len(idea_records)
            and filter_report.get("kept_count") == len(filtered_records)
            and filter_report.get("rejected_count") == len(idea_records) - len(filtered_records),
            {
                "matrix_records": len(idea_records),
                "filtered_records": len(filtered_records),
                "raw_count": filter_report.get("raw_count"),
                "kept_count": filter_report.get("kept_count"),
                "rejected_count": filter_report.get("rejected_count"),
            },
        ),
        _check(
            "target-preserved",
            "filter keeps enough records for final selection",
            len(filtered_records) >= target_count,
            {"filtered_records": len(filtered_records), "target_count": target_count},
        ),
        _check(
            "subset-of-matrix",
            "every filtered record id came from idea-matrix.jsonl",
            set(filtered_ids).issubset(matrix_ids),
            {
                "filtered_records": len(filtered_ids),
                "missing_from_matrix": sorted(set(filtered_ids) - matrix_ids)[:50],
            },
        ),
        _check(
            "record-id-unique",
            "filtered record ids are unique",
            len(filtered_ids) == len(set(filtered_ids)),
            {"filtered_records": len(filtered_ids), "unique_record_ids": len(set(filtered_ids))},
        ),
        _check(
            "semantic-key-unique",
            "filtered semantic keys are unique",
            len(semantic_keys) == len(set(semantic_keys)),
            {"semantic_keys": len(semantic_keys), "unique_semantic_keys": len(set(semantic_keys))},
        ),
        _check(
            "atomic-idempotent-contract",
            "kept records satisfy atomic, idempotent, dependency, state-rule, name, and domain gates",
            not non_atomic
            and not non_idempotent
            and not too_many_deps
            and not missing_state_rules
            and not missing_name_domain,
            {
                "non_atomic": non_atomic[:25],
                "non_idempotent": non_idempotent[:25],
                "too_many_deps": too_many_deps[:25],
                "missing_state_rules": missing_state_rules[:25],
                "missing_name_domain": missing_name_domain[:25],
            },
        ),
        _check(
            "rejection-reasons-known",
            "filter rejection reasons stay within the atomic/idempotent filter contract",
            _rejection_reasons(filter_report).issubset(
                {
                    "missing-name-or-domain",
                    "too-many-primary-dependencies",
                    "missing-state-rules",
                    "not-idempotent",
                    "duplicate-semantic-key",
                }
            ),
            {"rejection_reasons": sorted(_rejection_reasons(filter_report))},
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "atomic-filter-audit",
        "target_count": target_count,
        "matrix_records": len(idea_records),
        "filtered_records": len(filtered_records),
        "unique_record_ids": len(set(filtered_ids)),
        "unique_semantic_keys": len(set(semantic_keys)),
        "rejected_count": filter_report.get("rejected_count", 0),
        "rejection_reasons": sorted(_rejection_reasons(filter_report)),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _rejection_reasons(filter_report: Dict[str, Any]) -> Set[str]:
    reasons: Set[str] = set()
    for rejection in filter_report.get("rejections", []):
        for reason in rejection.get("reasons", []):
            if reason:
                reasons.add(str(reason))
    return reasons


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
