from __future__ import annotations

from typing import Any, Dict, List, Set

from workflow_harnesses.fractal_kit_pipeline.kit_contract import GENERIC_FILLER, key
from workflow_harnesses.fractal_kit_pipeline.reveal_reduce_stage import REDUCE_GENERIC_FILLER


def build_revealed_reduced_audit(
    expansion_points: List[Dict[str, Any]],
    reveal_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    expansion_values = [str(record.get("value", "")).strip() for record in expansion_points]
    expansion_keys = {key(value) for value in expansion_values if key(value)}
    reveal_points = [str(record.get("expansion_point", "")).strip() for record in reveal_records]
    reveal_point_keys = {key(value) for value in reveal_points if key(value)}
    revealed_values = [str(record.get("revealed", "")).strip() for record in reveal_records]
    reduced_values = [str(record.get("reduced", "")).strip() for record in reveal_records]
    reduced_keys = [key(value) for value in reduced_values if key(value)]
    generic_reduced = [value for value in reduced_values if key(value) in REDUCE_GENERIC_FILLER]
    empty_revealed = [record for record in reveal_records if not key(record.get("revealed", ""))]
    empty_reduced = [record for record in reveal_records if not key(record.get("reduced", ""))]
    failed_calls = [record for record in reveal_records if not record.get("ok")]
    long_values = [
        {
            "expansion_point": record.get("expansion_point"),
            "revealed": record.get("revealed"),
            "reduced": record.get("reduced"),
        }
        for record in reveal_records
        if _word_count(record.get("revealed", "")) > 16 or _word_count(record.get("reduced", "")) > 12
    ]
    evidence_linked = _evidence_link_count(reveal_records)
    checks = [
        _check(
            "one-to-one-coverage",
            "every accepted expansion point has one revealed/reduced record",
            len(reveal_records) == len(expansion_points) and expansion_keys == reveal_point_keys,
            {
                "expansion_points": len(expansion_points),
                "reveal_records": len(reveal_records),
                "missing_expansion_keys": sorted(expansion_keys - reveal_point_keys)[:50],
                "extra_reveal_keys": sorted(reveal_point_keys - expansion_keys)[:50],
            },
        ),
        _check(
            "model-calls-ok",
            "reveal and reduce calls succeeded or produced fallback-safe records",
            not failed_calls,
            {"failed_calls": len(failed_calls), "examples": failed_calls[:10]},
        ),
        _check(
            "non-empty-values",
            "revealed and reduced values are non-empty",
            not empty_revealed and not empty_reduced,
            {"empty_revealed": len(empty_revealed), "empty_reduced": len(empty_reduced)},
        ),
        _check(
            "generic-filler-rejected",
            "reduced values do not collapse to generic filler",
            not generic_reduced,
            {"generic_reduced": generic_reduced[:50]},
        ),
        _check(
            "short-phrase-shape",
            "revealed and reduced values stay compact enough for feed-forward stages",
            not long_values,
            {"long_values": long_values[:25]},
        ),
        _check(
            "reduced-breadth",
            "reduced capability phrases preserve enough breadth for matrix expansion",
            len(set(reduced_keys)) >= max(1, min(len(reveal_records), len(reveal_records) // 2)),
            {"unique_reduced": len(set(reduced_keys)), "records": len(reveal_records)},
        ),
        _check(
            "evidence-linkage",
            "most reduced/revealed records retain lexical evidence from their expansion point",
            not reveal_records or evidence_linked / len(reveal_records) >= 0.5,
            {
                "evidence_linked": evidence_linked,
                "records": len(reveal_records),
                "ratio": round(evidence_linked / len(reveal_records), 4) if reveal_records else 0,
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "revealed-reduced-audit",
        "expansion_points": len(expansion_points),
        "reveal_records": len(reveal_records),
        "unique_reduced": len(set(reduced_keys)),
        "generic_reduced": len(generic_reduced),
        "failed_calls": len(failed_calls),
        "evidence_linked": evidence_linked,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _evidence_link_count(records: List[Dict[str, Any]]) -> int:
    linked = 0
    for record in records:
        source_terms = _terms(record.get("expansion_point", ""))
        output_terms = _terms(record.get("revealed", "")) | _terms(record.get("reduced", ""))
        if source_terms and output_terms and (source_terms & output_terms):
            linked += 1
    return linked


def _terms(value: Any) -> Set[str]:
    return {part for part in key(str(value)).split("-") if len(part) >= 4}


def _word_count(value: Any) -> int:
    return len([part for part in str(value).strip().split() if part])


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
