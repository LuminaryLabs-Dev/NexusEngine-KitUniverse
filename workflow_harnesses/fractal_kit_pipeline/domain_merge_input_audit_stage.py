from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Set


def build_domain_merge_input_audit(
    filtered_records: List[Dict[str, Any]],
    domain_merge_report: Dict[str, Any],
    target_count: int,
) -> Dict[str, Any]:
    filtered_ids = [str(record.get("record_id", "")).strip() for record in filtered_records]
    filtered_id_set = {record_id for record_id in filtered_ids if record_id}
    duplicate_record_ids = len(filtered_ids) - len(filtered_id_set)
    missing_payload_fields: List[Dict[str, Any]] = []
    unsafe_domain_paths: List[str] = []
    domain_index: Dict[str, Dict[str, Any]] = {}
    domains_by_parent: Dict[str, Set[str]] = defaultdict(set)
    parent_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()

    for record in filtered_records:
        record_id = str(record.get("record_id", "")).strip()
        payload = record.get("payload") or {}
        missing = [
            field
            for field in ["domain_path", "domain", "parent_domain", "category", "need", "requires", "aliases"]
            if _missing(payload.get(field))
        ]
        if missing:
            missing_payload_fields.append({"record_id": record_id, "missing": missing})
        domain_path = str(payload.get("domain_path", "")).strip()
        parent_domain = str(payload.get("parent_domain", "")).strip()
        category = str(payload.get("category", "")).strip()
        if domain_path and not domain_path.startswith("n:"):
            unsafe_domain_paths.append(domain_path)
        if not domain_path:
            continue
        entry = domain_index.setdefault(
            domain_path,
            {
                "domain_path": domain_path,
                "record_ids": [],
                "parent_domain": parent_domain,
                "category": category,
                "needs": set(),
                "dependencies": set(),
                "aliases": set(),
            },
        )
        entry["record_ids"].append(record_id)
        entry["needs"].add(str(payload.get("need", "")).strip())
        entry["dependencies"].update(str(value).strip() for value in payload.get("requires", []))
        entry["aliases"].update(str(value).strip() for value in payload.get("aliases", []))
        domains_by_parent[parent_domain].add(domain_path)
        parent_counts[parent_domain] += 1
        category_counts[category] += 1

    review_depth = int(domain_merge_report.get("review_depth", 0) or 0)
    pairs_reviewed = int(domain_merge_report.get("pairs_reviewed", 0) or 0)
    pairs_requested = int(domain_merge_report.get("pairs_requested", 0) or 0)
    pair_capacity = sum(max(0, len(paths) - 1) for paths in domains_by_parent.values()) * max(1, review_depth)
    reviews = domain_merge_report.get("reviews", [])
    groups = domain_merge_report.get("groups", [])
    invalid_review_outputs = [
        {
            "left_domain": review.get("left_domain"),
            "right_domain": review.get("right_domain"),
            "same": review.get("same"),
        }
        for review in reviews
        if review.get("same") not in {"Y", "N"}
    ]
    invalid_review_depths = [
        review.get("depth")
        for review in reviews
        if int(review.get("depth", 0) or 0) < 1
        or (review_depth > 0 and int(review.get("depth", 0) or 0) > review_depth)
    ]
    invalid_group_source_ids = sorted(
        {
            str(source_id)
            for group in groups
            for source_id in group.get("source_record_ids", [])
            if str(source_id) not in filtered_id_set
        }
    )
    groups_missing_shape = [
        group.get("canonical_domain", "")
        for group in groups
        if not str(group.get("canonical_domain", "")).strip()
        or not group.get("aliases")
        or int(group.get("source_record_count", 0) or 0) < 1
    ]
    checks = [
        _check(
            "filtered-count",
            "filtered candidates preserve target-sized input for domain merge review",
            len(filtered_records) >= target_count and duplicate_record_ids == 0,
            {
                "target_count": target_count,
                "filtered_records": len(filtered_records),
                "duplicate_record_ids": duplicate_record_ids,
            },
        ),
        _check(
            "domain-index-shape",
            "every filtered record exposes the fields needed to build a namespaced domain index",
            not missing_payload_fields and not unsafe_domain_paths and len(domain_index) > 0,
            {
                "missing_payload_fields": missing_payload_fields[:50],
                "unsafe_domain_paths": unsafe_domain_paths[:50],
                "unique_domains": len(domain_index),
            },
        ),
        _check(
            "domain-breadth",
            "domain merge input keeps broad first-stage coverage instead of collapsing early",
            len(domain_index) >= min(64, len(filtered_records))
            and len(parent_counts) >= min(12, max(1, len(domain_index) // 4))
            and len(category_counts) >= min(12, max(1, len(domain_index) // 4)),
            {
                "unique_domains": len(domain_index),
                "unique_parent_domains": len(parent_counts),
                "unique_categories": len(category_counts),
            },
        ),
        _check(
            "pairable-parent-domains",
            "same-parent domain pairs exist for recursive Y/N merge review",
            pair_capacity >= pairs_reviewed and pairs_reviewed > 0 and pairs_reviewed <= pairs_requested,
            {
                "pair_capacity": pair_capacity,
                "pairs_reviewed": pairs_reviewed,
                "pairs_requested": pairs_requested,
                "review_depth": review_depth,
            },
        ),
        _check(
            "report-counts-align",
            "domain merge report counts align with computed filtered-candidate domain index",
            domain_merge_report.get("domain_count") == len(domain_index)
            and int(domain_merge_report.get("canonical_group_count", 0) or 0) <= len(domain_index),
            {
                "computed_domain_count": len(domain_index),
                "reported_domain_count": domain_merge_report.get("domain_count"),
                "canonical_group_count": domain_merge_report.get("canonical_group_count"),
            },
        ),
        _check(
            "review-output-clean",
            "recursive domain merge reviews only produce clean Y/N decisions at valid depths",
            len(reviews) == pairs_reviewed and not invalid_review_outputs and not invalid_review_depths,
            {
                "review_count": len(reviews),
                "pairs_reviewed": pairs_reviewed,
                "invalid_review_outputs": invalid_review_outputs[:50],
                "invalid_review_depths": invalid_review_depths[:50],
            },
        ),
        _check(
            "group-source-ids-valid",
            "canonical domain groups preserve source ids from filtered candidates",
            not invalid_group_source_ids and not groups_missing_shape and len(groups) > 0,
            {
                "group_count": len(groups),
                "invalid_group_source_ids": invalid_group_source_ids[:50],
                "groups_missing_shape": groups_missing_shape[:50],
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "domain-merge-input-audit",
        "target_count": target_count,
        "filtered_records": len(filtered_records),
        "unique_record_ids": len(filtered_id_set),
        "duplicate_record_ids": duplicate_record_ids,
        "unique_domains": len(domain_index),
        "unique_parent_domains": len(parent_counts),
        "unique_categories": len(category_counts),
        "pair_capacity": pair_capacity,
        "pairs_requested": pairs_requested,
        "pairs_reviewed": pairs_reviewed,
        "canonical_group_count": domain_merge_report.get("canonical_group_count", 0),
        "invalid_review_outputs": invalid_review_outputs,
        "invalid_group_source_ids": invalid_group_source_ids,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return not value
    return False


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
