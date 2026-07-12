from __future__ import annotations

from typing import Any, Dict, List, Set


def build_domain_canonicalization_audit(
    filtered_records: List[Dict[str, Any]],
    canonicalized_records: List[Dict[str, Any]],
    domain_merge_report: Dict[str, Any],
    canonicalization_report: Dict[str, Any],
    target_count: int,
) -> Dict[str, Any]:
    filtered_by_id = {str(record.get("record_id", "")).strip(): record for record in filtered_records}
    canonicalized_by_id = {str(record.get("record_id", "")).strip(): record for record in canonicalized_records}
    filtered_ids = set(filtered_by_id)
    canonicalized_ids = set(canonicalized_by_id)
    alias_index = _alias_index(domain_merge_report)
    canonical_domains = {str(group.get("canonical_domain", "")).strip() for group in domain_merge_report.get("groups", [])}
    missing_record_ids = sorted(filtered_ids - canonicalized_ids)
    extra_record_ids = sorted(canonicalized_ids - filtered_ids)
    missing_canonical_fields: List[str] = []
    invalid_canonical_paths: List[Dict[str, Any]] = []
    missing_alias_links: List[Dict[str, Any]] = []
    invalid_evidence_ids: Set[str] = set()
    changed_source_fields: List[Dict[str, Any]] = []
    canonicalized_count = 0
    preserved_count = 0

    for record_id, canonicalized in canonicalized_by_id.items():
        source = filtered_by_id.get(record_id, {})
        source_payload = source.get("payload") or {}
        payload = canonicalized.get("payload") or {}
        domain_path = str(payload.get("domain_path", "")).strip()
        canonical_path = str(payload.get("canonical_domain_path", "")).strip()
        aliases = [str(alias).strip() for alias in payload.get("domain_aliases", []) if str(alias).strip()]
        evidence = payload.get("domain_merge_evidence") or {}
        evidence_ids = {str(source_id).strip() for source_id in evidence.get("source_record_ids", []) if str(source_id).strip()}
        expected_group = alias_index.get(domain_path)
        if expected_group:
            canonicalized_count += 1
        else:
            preserved_count += 1
        for field in ["canonical_domain_path", "domain_aliases", "domain_group_size", "domain_source_record_count", "domain_merge_evidence"]:
            if _missing(payload.get(field)):
                missing_canonical_fields.append(record_id)
                break
        if not canonical_path.startswith("n:"):
            invalid_canonical_paths.append({"record_id": record_id, "canonical_domain_path": canonical_path})
        if expected_group:
            expected_canonical = str(expected_group.get("canonical_domain", "")).strip()
            expected_aliases = {
                str(alias).strip()
                for alias in [expected_canonical, *expected_group.get("aliases", [])]
                if str(alias).strip()
            }
            if canonical_path != expected_canonical or domain_path not in aliases or not expected_aliases.issubset(set(aliases)):
                missing_alias_links.append(
                    {
                        "record_id": record_id,
                        "domain_path": domain_path,
                        "canonical_domain_path": canonical_path,
                        "expected_canonical": expected_canonical,
                    }
                )
        elif canonical_path != domain_path or aliases != ([domain_path] if domain_path else []):
            missing_alias_links.append(
                {
                    "record_id": record_id,
                    "domain_path": domain_path,
                    "canonical_domain_path": canonical_path,
                    "expected_canonical": domain_path,
                }
            )
        invalid_evidence_ids.update(source_id for source_id in evidence_ids if source_id not in filtered_ids)
        for field in ["name", "domain_path", "need", "requires", "provides", "atomic", "idempotent"]:
            if source_payload.get(field) != payload.get(field):
                changed_source_fields.append({"record_id": record_id, "field": field})
                break

    checks = [
        _check(
            "target-sized-records",
            "domain canonicalization preserves a target-sized record set",
            len(canonicalized_records) >= target_count and len(canonicalized_records) == len(filtered_records),
            {
                "target_count": target_count,
                "filtered_records": len(filtered_records),
                "canonicalized_records": len(canonicalized_records),
            },
        ),
        _check(
            "record-id-preservation",
            "domain canonicalization preserves exactly the filtered candidate record ids",
            not missing_record_ids and not extra_record_ids and len(canonicalized_ids) == len(canonicalized_records),
            {
                "missing_record_ids": missing_record_ids[:50],
                "extra_record_ids": extra_record_ids[:50],
                "duplicate_canonicalized_ids": len(canonicalized_records) - len(canonicalized_ids),
            },
        ),
        _check(
            "canonical-fields-present",
            "every canonicalized record has canonical domain metadata and merge evidence",
            not missing_canonical_fields and not invalid_canonical_paths,
            {
                "missing_canonical_fields": missing_canonical_fields[:50],
                "invalid_canonical_paths": invalid_canonical_paths[:50],
            },
        ),
        _check(
            "merge-groups-applied",
            "canonical domain paths and aliases match the reviewed domain merge groups",
            not missing_alias_links
            and canonicalized_count == canonicalization_report.get("canonicalized_records", -1)
            and preserved_count == canonicalization_report.get("preserved_records", -1),
            {
                "missing_alias_links": missing_alias_links[:50],
                "canonicalized_count": canonicalized_count,
                "reported_canonicalized_records": canonicalization_report.get("canonicalized_records"),
                "preserved_count": preserved_count,
                "reported_preserved_records": canonicalization_report.get("preserved_records"),
            },
        ),
        _check(
            "evidence-source-ids-valid",
            "domain merge evidence source ids point back to filtered candidates",
            not invalid_evidence_ids,
            {"invalid_evidence_ids": sorted(invalid_evidence_ids)[:50]},
        ),
        _check(
            "source-payload-preserved",
            "canonicalization does not rewrite original kit source fields before kit merge review",
            not changed_source_fields,
            {"changed_source_fields": changed_source_fields[:50]},
        ),
        _check(
            "report-counts-align",
            "domain canonicalization report aligns with merge report and output records",
            bool(canonicalization_report.get("ok"))
            and canonicalization_report.get("input_records") == len(filtered_records)
            and canonicalization_report.get("output_records") == len(canonicalized_records)
            and canonicalization_report.get("canonical_groups") == len(canonical_domains),
            {
                "report": canonicalization_report,
                "merge_report_canonical_groups": len(canonical_domains),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "domain-canonicalization-audit",
        "target_count": target_count,
        "filtered_records": len(filtered_records),
        "canonicalized_records": len(canonicalized_records),
        "canonicalized_count": canonicalized_count,
        "preserved_count": preserved_count,
        "canonical_groups": len(canonical_domains),
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _alias_index(domain_merge_report: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for group in domain_merge_report.get("groups", []):
        canonical = str(group.get("canonical_domain", "")).strip()
        aliases = [str(alias).strip() for alias in group.get("aliases", []) if str(alias).strip()]
        for alias in {canonical, *aliases}:
            if alias:
                index[alias] = group
    return index


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
