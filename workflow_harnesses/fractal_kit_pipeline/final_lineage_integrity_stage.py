from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set


def build_final_lineage_integrity(run_dir: Path, target_count: int) -> Dict[str, Any]:
    idea = _read_jsonl(run_dir / "idea-matrix.jsonl")
    filtered = _read_jsonl(run_dir / "filtered-candidates.jsonl")
    domain = _read_jsonl(run_dir / "domain-canonicalized.jsonl")
    kit = _read_jsonl(run_dir / "kit-canonicalized.jsonl")
    selected = _read_jsonl(run_dir / "selected-final-records.jsonl")
    final = _read_jsonl(run_dir / "final-kits.jsonl")

    idea_ids = _ids(idea)
    filtered_ids = _ids(filtered)
    domain_ids = _ids(domain)
    kit_ids = _ids(kit)
    selected_ids = _ids(selected)
    final_ids = _ids(final)

    final_payloads = [record.get("payload", {}) for record in final]
    canonical_kit_ids = {
        str(payload.get("canonical_kit_record_id", "")).strip()
        for payload in final_payloads
        if str(payload.get("canonical_kit_record_id", "")).strip()
    }
    domain_evidence_ids = {
        str(record_id).strip()
        for payload in final_payloads
        for record_id in payload.get("domain_merge_evidence", {}).get("source_record_ids", [])
        if str(record_id).strip()
    }
    kit_alias_ids = {
        str(record_id).strip()
        for payload in final_payloads
        for record_id in payload.get("kit_alias_record_ids", [])
        if str(record_id).strip()
    }
    checks = [
        _check(
            "final-count",
            "final lineage covers the configured target count",
            len(final) == target_count,
            {"target_count": target_count, "final_count": len(final)},
        ),
        _check(
            "selected-equals-final",
            "selected-final-records.jsonl and final-kits.jsonl contain the same record ids",
            selected_ids == final_ids,
            {
                "selected_count": len(selected_ids),
                "final_count": len(final_ids),
                "missing_from_final": sorted(selected_ids - final_ids)[:20],
                "extra_in_final": sorted(final_ids - selected_ids)[:20],
            },
        ),
        _check(
            "final-descends-from-kit-canonicalized",
            "every final record id exists in kit-canonicalized.jsonl",
            final_ids <= kit_ids,
            {"missing_from_kit_canonicalized": sorted(final_ids - kit_ids)[:20]},
        ),
        _check(
            "kit-descends-from-domain-canonicalized",
            "every final record id exists in domain-canonicalized.jsonl",
            final_ids <= domain_ids,
            {"missing_from_domain_canonicalized": sorted(final_ids - domain_ids)[:20]},
        ),
        _check(
            "domain-descends-from-filtered",
            "every final record id exists in filtered-candidates.jsonl",
            final_ids <= filtered_ids,
            {"missing_from_filtered": sorted(final_ids - filtered_ids)[:20]},
        ),
        _check(
            "filtered-descends-from-idea-matrix",
            "every final record id exists in idea-matrix.jsonl",
            final_ids <= idea_ids,
            {"missing_from_idea_matrix": sorted(final_ids - idea_ids)[:20]},
        ),
        _check(
            "canonical-kit-records-exist",
            "canonical kit record ids referenced by final records exist in kit-canonicalized.jsonl",
            canonical_kit_ids <= kit_ids,
            {"missing_canonical_kit_record_ids": sorted(canonical_kit_ids - kit_ids)[:20]},
        ),
        _check(
            "kit-alias-records-exist",
            "kit alias record ids referenced by final records exist in kit-canonicalized.jsonl",
            kit_alias_ids <= kit_ids,
            {"missing_kit_alias_record_ids": sorted(kit_alias_ids - kit_ids)[:20]},
        ),
        _check(
            "domain-evidence-records-exist",
            "domain merge evidence source ids referenced by final records exist in filtered-candidates.jsonl",
            domain_evidence_ids <= filtered_ids,
            {"missing_domain_evidence_record_ids": sorted(domain_evidence_ids - filtered_ids)[:20]},
        ),
        _check(
            "source-evidence-preserved",
            "every final record keeps source evidence from the expansion/reveal path",
            all(_source_evidence_ok(payload) for payload in final_payloads),
            {
                "missing_source_evidence": [
                    record.get("record_id", "")
                    for record in final
                    if not _source_evidence_ok(record.get("payload", {}))
                ][:20]
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "final-lineage-integrity",
        "target_count": target_count,
        "counts": {
            "idea_matrix": len(idea),
            "filtered_candidates": len(filtered),
            "domain_canonicalized": len(domain),
            "kit_canonicalized": len(kit),
            "selected_final": len(selected),
            "final": len(final),
            "canonical_kit_refs": len(canonical_kit_ids),
            "kit_alias_refs": len(kit_alias_ids),
            "domain_evidence_refs": len(domain_evidence_ids),
        },
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def _ids(records: List[Dict[str, Any]]) -> Set[str]:
    return {str(record.get("record_id", "")).strip() for record in records if str(record.get("record_id", "")).strip()}


def _source_evidence_ok(payload: Dict[str, Any]) -> bool:
    evidence = payload.get("source_evidence", {})
    return all(str(evidence.get(key, "")).strip() for key in ["operation", "proof", "revealed_signal", "surface"])


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
