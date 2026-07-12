from __future__ import annotations

from typing import Any, Dict, List


def build_merge_review_coverage(
    domain_merge_report: Dict[str, Any],
    domain_canonicalization_report: Dict[str, Any],
    kit_merge_report: Dict[str, Any],
    kit_canonicalization_report: Dict[str, Any],
    final_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    domain_reviews = domain_merge_report.get("reviews", [])
    kit_reviews = kit_merge_report.get("reviews", [])
    final_payloads = [record.get("payload", {}) for record in final_records]
    domain_depths = _depths(domain_reviews)
    kit_depths = _depths(kit_reviews)
    checks = [
        _check(
            "domain-review-ran",
            "domain merge review ran Y/N comparisons",
            domain_merge_report.get("pairs_reviewed", 0) > 0 and len(domain_reviews) == domain_merge_report.get("pairs_reviewed", 0),
            {
                "pairs_requested": domain_merge_report.get("pairs_requested", 0),
                "pairs_reviewed": domain_merge_report.get("pairs_reviewed", 0),
                "review_count": len(domain_reviews),
            },
        ),
        _check(
            "kit-review-ran",
            "kit merge review ran Y/N comparisons",
            kit_merge_report.get("pairs_reviewed", 0) > 0 and len(kit_reviews) == kit_merge_report.get("pairs_reviewed", 0),
            {
                "pairs_requested": kit_merge_report.get("pairs_requested", 0),
                "pairs_reviewed": kit_merge_report.get("pairs_reviewed", 0),
                "review_count": len(kit_reviews),
            },
        ),
        _check(
            "domain-review-depth",
            "domain merge review used valid recursive review depths",
            _depths_valid(domain_depths, domain_merge_report.get("review_depth", 0)),
            {"configured_depth": domain_merge_report.get("review_depth", 0), "observed_depths": domain_depths},
        ),
        _check(
            "kit-review-depth",
            "kit merge review used valid recursive review depths",
            _depths_valid(kit_depths, kit_merge_report.get("review_depth", 0)),
            {"configured_depth": kit_merge_report.get("review_depth", 0), "observed_depths": kit_depths},
        ),
        _check(
            "domain-yes-no-clean",
            "domain merge reviewer outputs resolve to Y or N",
            _yes_no_clean(domain_reviews),
            {"bad_reviews": _bad_yes_no(domain_reviews)[:20]},
        ),
        _check(
            "kit-yes-no-clean",
            "kit merge reviewer outputs resolve to Y or N",
            _yes_no_clean(kit_reviews),
            {"bad_reviews": _bad_yes_no(kit_reviews)[:20]},
        ),
        _check(
            "domain-canonicalization-applied",
            "domain canonicalization preserved record count and produced canonical groups",
            bool(domain_canonicalization_report.get("ok"))
            and domain_canonicalization_report.get("input_records", 0)
            == domain_canonicalization_report.get("output_records", -1)
            and domain_merge_report.get("canonical_group_count", 0) > 0
            and domain_merge_report.get("canonical_group_count", 0) <= domain_merge_report.get("domain_count", 0),
            {
                "input_records": domain_canonicalization_report.get("input_records", 0),
                "output_records": domain_canonicalization_report.get("output_records", 0),
                "domain_count": domain_merge_report.get("domain_count", 0),
                "canonical_group_count": domain_merge_report.get("canonical_group_count", 0),
            },
        ),
        _check(
            "kit-canonicalization-applied",
            "kit canonicalization preserved record count and produced canonical groups",
            bool(kit_canonicalization_report.get("ok"))
            and kit_canonicalization_report.get("input_records", 0)
            == kit_canonicalization_report.get("output_records", -1)
            and kit_canonicalization_report.get("canonical_groups", 0) > 0
            and kit_canonicalization_report.get("canonical_groups", 0) <= kit_canonicalization_report.get("input_records", 0),
            {
                "input_records": kit_canonicalization_report.get("input_records", 0),
                "output_records": kit_canonicalization_report.get("output_records", 0),
                "canonical_groups": kit_canonicalization_report.get("canonical_groups", 0),
                "reviewed_same_pairs": kit_canonicalization_report.get("reviewed_same_pairs", 0),
            },
        ),
        _check(
            "final-domain-merge-metadata",
            "every final record carries reviewed domain canonical metadata",
            all(_domain_metadata_ok(payload) for payload in final_payloads),
            {
                "missing": [
                    record.get("record_id", "")
                    for record in final_records
                    if not _domain_metadata_ok(record.get("payload", {}))
                ][:20]
            },
        ),
        _check(
            "final-kit-merge-metadata",
            "every final record carries reviewed kit canonical metadata",
            all(_kit_metadata_ok(payload) for payload in final_payloads),
            {
                "missing": [
                    record.get("record_id", "")
                    for record in final_records
                    if not _kit_metadata_ok(record.get("payload", {}))
                ][:20]
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "merge-review-coverage",
        "counts": {
            "domain_pairs_reviewed": domain_merge_report.get("pairs_reviewed", 0),
            "domain_same_pairs": domain_merge_report.get("same_pairs", 0),
            "domain_count": domain_merge_report.get("domain_count", 0),
            "domain_canonical_groups": domain_merge_report.get("canonical_group_count", 0),
            "domain_depths": domain_depths,
            "kit_pairs_reviewed": kit_merge_report.get("pairs_reviewed", 0),
            "kit_same_pairs": kit_merge_report.get("same_pairs", 0),
            "kit_canonical_groups": kit_canonicalization_report.get("canonical_groups", 0),
            "kit_depths": kit_depths,
            "final_records_checked": len(final_records),
        },
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _depths(reviews: List[Dict[str, Any]]) -> List[int]:
    return sorted({int(review.get("depth", 0)) for review in reviews if int(review.get("depth", 0)) > 0})


def _depths_valid(depths: List[int], configured_depth: int) -> bool:
    return bool(depths) and min(depths) >= 1 and max(depths) <= configured_depth and max(depths) == configured_depth


def _yes_no_clean(reviews: List[Dict[str, Any]]) -> bool:
    return bool(reviews) and not _bad_yes_no(reviews)


def _bad_yes_no(reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bad = []
    for review in reviews:
        if review.get("same") not in {"Y", "N"}:
            bad.append(review)
            continue
        raw = str(review.get("raw", "")).strip().upper()
        if not raw or raw[0] not in {"Y", "N"}:
            bad.append(review)
    return bad


def _domain_metadata_ok(payload: Dict[str, Any]) -> bool:
    evidence = payload.get("domain_merge_evidence", {})
    return (
        bool(str(payload.get("canonical_domain_path", "")).strip())
        and bool(payload.get("domain_aliases"))
        and int(payload.get("domain_group_size", 0)) >= 1
        and bool(evidence.get("source_record_ids"))
    )


def _kit_metadata_ok(payload: Dict[str, Any]) -> bool:
    evidence = payload.get("kit_merge_evidence", {})
    return (
        bool(str(payload.get("canonical_kit_record_id", "")).strip())
        and bool(str(payload.get("canonical_kit_name", "")).strip())
        and bool(payload.get("kit_alias_record_ids"))
        and int(payload.get("kit_merge_group_size", 0)) >= 1
        and ("needs" in evidence)
        and ("dependencies" in evidence)
    )


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
